// Package traffic 管理函数的流量权重分配。
//
// 权重调整以整份 Split 为单位、通过乐观锁（generation CAS）原子生效，
// 非法权重或指向不存在版本的分配会被整体拒绝，不会出现半生效状态。
package traffic

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/lisama569/serverless-revision-autoscale-go/internal/fcerr"
)

// TotalWeight 是权重的基数（百分比）。
const TotalWeight = 100

// GenAny 表示不检查 generation 的通配期望值。
const GenAny int64 = -1

// Split 是某一时刻的完整流量分配快照（不可变）。
type Split struct {
	Gen       int64
	Weights   map[string]int
	UpdatedAt time.Time
}

// Manager 维护当前生效的流量分配并提供 CAS 更新。
type Manager struct {
	mu      sync.Mutex
	current Split
}

// NewManager 创建空的流量管理器（Gen=0 表示尚无生效分配）。
func NewManager() *Manager {
	return &Manager{current: Split{Gen: 0, Weights: map[string]int{}}}
}

// Current 返回当前流量分配快照（拷贝，调用方可安全持有）。
func (m *Manager) Current() Split {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneSplit(m.current)
}

// Validate 校验权重：必须非空、每个权重为正、合计必须恰好 100，
// 且每个版本 ID 都必须存在于 exists 判定中。任何一项不满足则整体拒绝。
func Validate(weights map[string]int, exists func(revID string) bool) error {
	if len(weights) == 0 {
		return fcerr.WithMsg(fcerr.ErrInvalidWeights, "traffic split must not be empty")
	}
	sum := 0
	for revID, w := range weights {
		if w <= 0 {
			return fcerr.WithMsg(fcerr.ErrInvalidWeights,
				"weight for %s must be positive, got %d", revID, w)
		}
		if w > TotalWeight {
			return fcerr.WithMsg(fcerr.ErrInvalidWeights,
				"weight for %s exceeds %d, got %d", revID, TotalWeight, w)
		}
		if exists != nil && !exists(revID) {
			return fcerr.WithMsg(fcerr.ErrRevisionNotFound,
				"traffic target revision does not exist: %s", revID)
		}
		sum += w
	}
	if sum != TotalWeight {
		return fcerr.WithMsg(fcerr.ErrInvalidWeights,
			"weights must sum to %d, got %d", TotalWeight, sum)
	}
	return nil
}

// Apply 在 expectedGen 与当前代次匹配时整体替换权重并返回新快照。
// expectedGen 为 GenAny 时不检查代次。
// 代次不匹配返回 ErrConflict；非法权重返回 ErrInvalidWeights / ErrRevisionNotFound。
// 校验失败时当前分配完全不变（不存在半生效中间状态）。
func (m *Manager) Apply(expectedGen int64, weights map[string]int, exists func(revID string) bool, now time.Time) (Split, error) {
	copied := make(map[string]int, len(weights))
	for k, v := range weights {
		copied[k] = v
	}
	if err := Validate(copied, exists); err != nil {
		return Split{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if expectedGen != GenAny && expectedGen != m.current.Gen {
		return cloneSplit(m.current), fcerr.WithMsg(fcerr.ErrConflict,
			"traffic generation mismatch: expected=%d actual=%d", expectedGen, m.current.Gen)
	}
	m.current = Split{
		Gen:       m.current.Gen + 1,
		Weights:   copied,
		UpdatedAt: now,
	}
	return cloneSplit(m.current), nil
}

func cloneSplit(s Split) Split {
	out := Split{Gen: s.Gen, UpdatedAt: s.UpdatedAt, Weights: make(map[string]int, len(s.Weights))}
	for k, v := range s.Weights {
		out.Weights[k] = v
	}
	return out
}

// WeightedSelector 依据权重按整数区间选择版本；rnd 可注入以便确定性测试。
type WeightedSelector struct {
	entries []entry
	rnd     func(n int) int
}

type entry struct {
	revID  string
	weight int
}

// NewWeightedSelector 构造选择器；rnd 为 nil 时使用全局随机源。
// 传入的权重应为已校验的合法分配。
func NewWeightedSelector(weights map[string]int, rnd func(n int) int) *WeightedSelector {
	if rnd == nil {
		rnd = rand.Intn
	}
	ws := &WeightedSelector{rnd: rnd}
	// 固定排序，保证区间构造可重复（map 迭代顺序随机）。
	ids := make([]string, 0, len(weights))
	for id := range weights {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		ws.entries = append(ws.entries, entry{revID: id, weight: weights[id]})
	}
	return ws
}

// Pick 按权重返回一个版本 ID；无有效版本时 ok=false。
func (w *WeightedSelector) Pick() (revID string, ok bool) {
	total := 0
	for _, e := range w.entries {
		total += e.weight
	}
	if total <= 0 {
		return "", false
	}
	x := w.rnd(total)
	cum := 0
	for _, e := range w.entries {
		cum += e.weight
		if x < cum {
			return e.revID, true
		}
	}
	// 理论不可达：x<total 且累计终值=total。
	return w.entries[len(w.entries)-1].revID, true
}

// String 返回可读的权重描述，便于日志。
func (s Split) String() string {
	return fmt.Sprintf("gen=%d weights=%v", s.Gen, s.Weights)
}

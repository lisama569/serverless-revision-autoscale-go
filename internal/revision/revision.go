// Package revision 管理函数的不可变版本（revision）。
//
// 版本一经发布即为不可变：ID 全局可寻址、内容不可修改；新版本只能追加。
package revision

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/serverless-revision-autoscale-go/internal/fcerr"
)

// DefaultMaxConcurrency 是 Spec.MaxConcurrency<=0 时使用的单实例默认并发。
const DefaultMaxConcurrency = 1

// Handler 是本地模拟的函数业务逻辑：输入字节，输出字节。
type Handler func(ctx context.Context, in []byte) ([]byte, error)

// Spec 是创建一个新版本时的不可变规格。
type Spec struct {
	// Handler 为函数调用逻辑，必填。
	Handler Handler
	// ColdStart 模拟冷启动耗时（拉起实例的初始化时间）。
	ColdStart time.Duration
	// MaxConcurrency 为单实例并发上限，<=0 时取默认值 1。
	MaxConcurrency int
	// Meta 为可选的版本元数据。
	Meta map[string]string
}

// Revision 是一个不可变的已发布版本。
type Revision struct {
	FunctionName string
	ID           string
	Seq          int
	CreatedAt    time.Time
	Spec         Spec
}

// MaxConcurrency 返回生效的单实例并发上限。
func (r *Revision) MaxConcurrency() int {
	if r.Spec.MaxConcurrency > 0 {
		return r.Spec.MaxConcurrency
	}
	return DefaultMaxConcurrency
}

// Store 是单个函数的版本追加存储。
type Store struct {
	mu    sync.Mutex
	name  string
	seq   int
	byID  map[string]*Revision
	order []string
}

// NewStore 为指定函数创建版本存储。
func NewStore(functionName string) *Store {
	return &Store{name: functionName, byID: map[string]*Revision{}}
}

func formatID(seq int) string { return fmt.Sprintf("rev-%05d", seq) }

// NextID 返回下一个版本将使用的 ID（格式 rev-00001），不推进序号。
func (s *Store) NextID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return formatID(s.seq + 1)
}

// Publish 以原子追加方式发布一个新版本；spec 非法时返回 fcerr 错误。
func (s *Store) Publish(spec Spec, now time.Time) (*Revision, error) {
	if spec.Handler == nil {
		return nil, fcerr.WithMsg(fcerr.ErrInvocationFailed,
			"revision spec handler must not be nil (function=%s)", s.name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	rev := &Revision{
		FunctionName: s.name,
		ID:           formatID(s.seq),
		Seq:          s.seq,
		CreatedAt:    now,
		// 拷贝元数据，避免调用方后续修改影响已发布版本。
		Spec: cloneSpec(spec),
	}
	s.byID[rev.ID] = rev
	s.order = append(s.order, rev.ID)
	return rev, nil
}

func cloneSpec(spec Spec) Spec {
	cp := spec
	if spec.Meta != nil {
		cp.Meta = make(map[string]string, len(spec.Meta))
		for k, v := range spec.Meta {
			cp.Meta[k] = v
		}
	}
	return cp
}

// Get 按 ID 取版本；不存在返回 ErrRevisionNotFound。
func (s *Store) Get(id string) (*Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.byID[id]
	if !ok {
		return nil, fcerr.WithMsg(fcerr.ErrRevisionNotFound,
			"function=%s revision=%s", s.name, id)
	}
	return r, nil
}

// Latest 返回最新版本；尚无版本时 ok=false。
func (s *Store) Latest() (*Revision, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seq == 0 {
		return nil, false
	}
	return s.byID[s.order[s.seq-1]], true
}

// List 按发布顺序返回全部版本。
func (s *Store) List() []*Revision {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Revision, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.byID[id])
	}
	return out
}

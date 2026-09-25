package fc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Function 是一个函数的本地控制面 + 数据面入口。
type Function struct {
	mu        sync.Mutex
	name      string
	revisions map[RevisionID]*Revision
	weights   map[RevisionID]int
	epoch     uint64
	pools     map[RevisionID]*pool
	rr        atomic.Uint64 // 确定性加权路由计数器
	log       Logger
	now       func() time.Time
}

// NewFunction 创建一个函数。
func NewFunction(name string, log Logger) *Function {
	if log == nil {
		log = noopLogger
	}
	return &Function{
		name:      name,
		revisions: map[RevisionID]*Revision{},
		weights:   map[RevisionID]int{},
		pools:     map[RevisionID]*pool{},
		log:       log,
		now:       time.Now,
	}
}

// Epoch 返回当前纪元（每次发布/流量变更/回滚递增）。
func (f *Function) Epoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.epoch
}

// Publish 发布一个不可变版本，并可选地原子切换流量。
// expectedEpoch 用于乐观并发控制：与当前纪元不匹配则拒绝（含与回滚冲突的场景）。
// weights 为 nil 表示不改动流量；否则整体原子替换。
func (f *Function) Publish(handler Handler, cfg RevisionConfig, weights map[RevisionID]int, expectedEpoch uint64) (RevisionID, error) {
	return "", nil
}

// UpdateTraffic 原子替换流量权重；expectedEpoch 不匹配则拒绝。
func (f *Function) UpdateTraffic(weights map[RevisionID]int, expectedEpoch uint64) error {
	return nil
}

// Rollback 将流量整体切回指定历史版本；同样受纪元保护。
func (f *Function) Rollback(to RevisionID, expectedEpoch uint64) error {
	return nil
}

// Traffic 返回当前流量策略快照。
func (f *Function) Traffic() TrafficPolicy {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := make(map[RevisionID]int, len(f.weights))
	for k, v := range f.weights {
		w[k] = v
	}
	return TrafficPolicy{Weights: w, Epoch: f.epoch}
}

// Invoke 按当前权重快照路由一次调用。
func (f *Function) Invoke(ctx context.Context, req *Request) (*Response, error) {
	return nil, nil
}

// Tick 驱动缩容判定（回收空闲实例，可缩到零）。测试用注入时钟调用。
func (f *Function) Tick() {}

// Close 排空所有实例。
func (f *Function) Close() {}

// Stats 返回各版本实例数与在途请求数（用于测试断言与日志）。
func (f *Function) Stats() map[RevisionID]PoolStats { return nil }

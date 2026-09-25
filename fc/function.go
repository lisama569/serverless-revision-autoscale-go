package fc

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Function 是一个函数的本地控制面 + 数据面入口。
type Function struct {
	mu        sync.Mutex
	name      string
	seq       int
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

// Publish 发布一个不可变版本，并可选地在同一原子操作中切换流量。
//
// expectedEpoch 为乐观并发控制：与当前纪元不匹配（例如与并发回滚冲突）则拒绝。
// weights 为 nil 表示不改动流量；非 nil 时整体原子替换当前权重，
// 其中键 ""（空 RevisionID）指代本次发布的新版本。
func (f *Function) Publish(handler Handler, cfg RevisionConfig, weights map[RevisionID]int, expectedEpoch uint64) (RevisionID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.checkEpochLocked(expectedEpoch); err != nil {
		return "", err
	}

	f.seq++
	id := RevisionID(fmt.Sprintf("%s-v%06d", f.name, f.seq))
	rev := &Revision{
		ID:        id,
		Function:  f.name,
		Handler:   handler,
		Config:    cfg.withDefaults(),
		CreatedAt: f.now(),
	}
	f.revisions[id] = rev
	f.pools[id] = newPool(rev, f.log, f.now)
	f.log("[function %s] published revision %s", f.name, id)

	if weights != nil {
		final := make(map[RevisionID]int, len(weights))
		for k, v := range weights {
			if k == "" {
				k = id
			}
			final[k] = v
		}
		if err := f.applyTrafficLocked(final); err != nil {
			return "", err
		}
	}
	f.epoch++
	return id, nil
}

// UpdateTraffic 原子替换流量权重；expectedEpoch 不匹配则拒绝。
func (f *Function) UpdateTraffic(weights map[RevisionID]int, expectedEpoch uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkEpochLocked(expectedEpoch); err != nil {
		return err
	}
	if err := f.applyTrafficLocked(weights); err != nil {
		return err
	}
	f.epoch++
	return nil
}

// Rollback 将流量整体切回指定历史版本；同样受纪元保护。
func (f *Function) Rollback(to RevisionID, expectedEpoch uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkEpochLocked(expectedEpoch); err != nil {
		return err
	}
	if _, ok := f.revisions[to]; !ok {
		return newError(ErrKindUnknownRevision, "rollback target "+string(to)+" does not exist")
	}
	f.log("[function %s] rollback to %s", f.name, to)
	if err := f.applyTrafficLocked(map[RevisionID]int{to: 1}); err != nil {
		return err
	}
	f.epoch++
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

func (f *Function) checkEpochLocked(expected uint64) error {
	if expected != f.epoch {
		return newError(ErrKindConflict,
			fmt.Sprintf("epoch mismatch: expected %d, current %d (concurrent publish/rollback?)", expected, f.epoch))
	}
	return nil
}

// applyTrafficLocked 校验并整体替换权重，随后异步排空失去流量的版本池。
// 校验失败时不产生任何副作用。
func (f *Function) applyTrafficLocked(weights map[RevisionID]int) error {
	if len(weights) == 0 {
		return newError(ErrKindInvalidWeight, "traffic weights must not be empty")
	}
	total := 0
	for id, w := range weights {
		if w < 0 {
			return newError(ErrKindInvalidWeight, fmt.Sprintf("negative weight %d for %s", w, id))
		}
		if _, ok := f.revisions[id]; !ok {
			return newError(ErrKindUnknownRevision, "weight references unknown revision "+string(id))
		}
		total += w
	}
	if total <= 0 {
		return newError(ErrKindInvalidWeight, "at least one weight must be positive")
	}

	f.weights = make(map[RevisionID]int, len(weights))
	for id, w := range weights {
		if w > 0 {
			f.weights[id] = w
		}
	}
	f.log("[function %s] traffic updated (epoch=%d): %v", f.name, f.epoch+1, f.weights)

	// 重新获得流量的版本若池正在排空，重建新池（旧池继续排空，互不影响）。
	for id, w := range f.weights {
		if w > 0 && f.pools[id].isDraining() {
			f.pools[id] = newPool(f.revisions[id], f.log, f.now)
			f.log("[function %s] revision %s regained traffic: fresh pool created", f.name, id)
		}
	}

	// 失去流量的版本：在同一把锁下标记排空（与权重变更、流量恢复序列化），
	// 再异步等待在途请求完成，不影响其他版本。
	for id, p := range f.pools {
		if f.weights[id] == 0 && p.markDraining() {
			go p.drain()
		}
	}
	return nil
}

// snapshot 返回当前权重与池的一致快照。
func (f *Function) snapshot() (weights map[RevisionID]int, pools map[RevisionID]*pool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	weights = make(map[RevisionID]int, len(f.weights))
	for k, v := range f.weights {
		weights[k] = v
	}
	pools = make(map[RevisionID]*pool, len(f.pools))
	for k, v := range f.pools {
		pools[k] = v
	}
	return weights, pools
}

// pick 按权重确定性选择版本：计数器在按 ID 排序展开的权重序列上轮转。
func pick(weights map[RevisionID]int, n uint64) RevisionID {
	ids := make([]string, 0, len(weights))
	for id := range weights {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	total := 0
	for _, id := range ids {
		total += weights[RevisionID(id)]
	}
	r := int(n % uint64(total))
	for _, id := range ids {
		w := weights[RevisionID(id)]
		if r < w {
			return RevisionID(id)
		}
		r -= w
	}
	return RevisionID(ids[len(ids)-1])
}

// Invoke 按当前权重快照路由一次调用。
func (f *Function) Invoke(ctx context.Context, req *Request) (*Response, error) {
	for attempt := 0; attempt < 8; attempt++ {
		weights, pools := f.snapshot()
		if len(weights) == 0 {
			return nil, newError(ErrKindNoTraffic, "function "+f.name+" has no traffic configured")
		}
		id := pick(weights, f.rr.Add(1)-1)
		resp, err := pools[id].invoke(ctx, req)
		if IsKind(err, ErrKindDraining) {
			continue // 与并发流量切换竞争：按新快照重路由
		}
		if err == nil {
			f.log("[function %s] request %q -> revision %s instance %s", f.name, req.Payload, resp.Revision, resp.InstanceID)
		}
		return resp, err
	}
	return nil, newError(ErrKindDraining, "function "+f.name+" traffic is in transition")
}

// Tick 驱动缩容判定（回收空闲实例，可缩到零）。测试用注入时钟调用。
func (f *Function) Tick() {
	_, pools := f.snapshot()
	for _, p := range pools {
		p.tick()
	}
}

// Close 排空所有实例。
func (f *Function) Close() {
	_, pools := f.snapshot()
	var wg sync.WaitGroup
	for _, p := range pools {
		p.markDraining()
		wg.Add(1)
		go func(p *pool) { defer wg.Done(); p.drain() }(p)
	}
	wg.Wait()
}

// Stats 返回各版本实例数与在途请求数（用于测试断言与日志）。
func (f *Function) Stats() map[RevisionID]PoolStats {
	_, pools := f.snapshot()
	out := make(map[RevisionID]PoolStats, len(pools))
	for id, p := range pools {
		out[id] = p.stats()
	}
	return out
}

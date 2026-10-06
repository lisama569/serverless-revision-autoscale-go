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
	rr        atomic.Uint64  // 确定性加权路由计数器
	rollout   *rolloutState  // 进行中/终态的渐进放量计划，nil 表示从未启动
	drainWG   sync.WaitGroup // 追踪所有后台排空 goroutine，Close 时汇合
	closed    bool
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
	if err := f.validateWeightsLocked(weights); err != nil {
		return err
	}

	f.weights = make(map[RevisionID]int, len(weights))
	for id, w := range weights {
		if w > 0 {
			f.weights[id] = w
		}
	}
	f.log("[function %s] traffic updated (epoch=%d): %v", f.name, f.epoch+1, f.weights)

	// 任意一次普通权重变更（UpdateTraffic / 带权重 Publish / Rollback）都整体取代
	// 进行中的放量计划：计划标记为 canceled，但当前权重已经是完整自洽的新权重。
	if f.rollout != nil && f.rollout.phase == RolloutRunning {
		f.rollout.phase = RolloutCanceled
		f.log("[function %s] rollout superseded by a direct traffic update", f.name)
	}

	// 重新获得流量的版本若池正在排空，重建新池（旧池继续排空，互不影响）。
	for id, w := range f.weights {
		if w > 0 && f.pools[id].isDraining() {
			f.pools[id] = newPool(f.revisions[id], f.log, f.now)
			f.log("[function %s] revision %s regained traffic: fresh pool created", f.name, id)
		}
	}
	f.ensureResidencyLocked()

	// 失去流量的版本：在同一把锁下标记排空（与权重变更、流量恢复序列化），
	// 再异步等待在途请求完成，不影响其他版本。进行中放量计划仍会引用的版本保留。
	f.reconcileDrainLocked()
	return nil
}

// validateWeightsLocked 校验权重表，不产生副作用。
func (f *Function) validateWeightsLocked(weights map[RevisionID]int) error {
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
	return nil
}

// rolloutResidentLocked 报告版本是否仍应保留实例：当前有流量，或正被进行中的
// 放量计划的某一步（含目标）引用。计划取消/走完后不再提供 residency。
func (f *Function) rolloutResidentLocked(id RevisionID) bool {
	if f.weights[id] > 0 {
		return true
	}
	if f.rollout == nil || f.rollout.phase != RolloutRunning {
		return false
	}
	for _, w := range f.rollout.stepWeights {
		if w[id] > 0 {
			return true
		}
	}
	return false
}

// ensureResidencyLocked 为所有应保留实例的版本补齐保底常驻实例（MinInstances）。
// 未配置保底的版本不做任何事，行为与按需冷启动一致。
func (f *Function) ensureResidencyLocked() {
	for id, p := range f.pools {
		if !f.rolloutResidentLocked(id) {
			continue
		}
		// 版本此前失去过流量、池已排空：为其重建新池（旧池继续后台排空）。
		if p.isDraining() {
			f.pools[id] = newPool(f.revisions[id], f.log, f.now)
			f.log("[function %s] revision %s becomes resident again: fresh pool created", f.name, id)
		}
		f.pools[id].ensureMin()
	}
}

// reconcileDrainLocked 排空所有既无当前流量、又不被进行中放量计划保留的版本池。
func (f *Function) reconcileDrainLocked() {
	if f.closed {
		return
	}
	for id, p := range f.pools {
		if f.weights[id] == 0 && !f.rolloutResidentLocked(id) && p.markDraining() {
			f.startDrain(p)
		}
	}
}

// startDrain 在后台排空一个池，并通过 drainWG 追踪，使 Close 能等待全部排空结束。
func (f *Function) startDrain(p *pool) {
	f.drainWG.Add(1)
	go func() {
		defer f.drainWG.Done()
		p.drain()
	}()
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

// routeLocked 在 f.mu 保护下完成「放量推进 + 选版本」，保证每次看到的都是完整权重：
//   - 进行中的放量计划每累计 perStep 个请求就整体切到下一完整步，
//     切换点只取决于请求计数，因而确定可复现；
//   - 带 RouteKey 的请求按键做粘性选择，同一权重表下永远命中同一版本；
//   - 其余请求按加权轮转计数器确定性分配。
//
// 返回本次路由选中的版本。
func (f *Function) routeLocked(req *Request) RevisionID {
	if f.rollout != nil && f.rollout.phase == RolloutRunning && f.rollout.step < f.rollout.steps {
		st := f.rollout
		st.inStep++
		if st.inStep >= st.perStep {
			// 每累计 perStep 个请求，本请求起整体使用下一完整步的权重；
			// 切换点只由请求计数决定，确定且可复现。
			st.inStep = 0
			st.step++
			f.weights = cloneWeights(st.stepWeights[st.step])
			f.log("[function %s] rollout advanced to step %d/%d (epoch=%d): %v",
				f.name, st.step, st.steps, f.epoch+1, f.weights)
			f.ensureResidencyLocked()
			f.reconcileDrainLocked()
			if st.step == st.steps {
				st.phase = RolloutComplete
				f.log("[function %s] rollout complete: final weights equal to target %v", f.name, f.weights)
			}
			f.epoch++
		}
	}

	if req.RouteKey != "" {
		return stickyPick(f.weights, req.RouteKey)
	}
	return pick(f.weights, f.rr.Add(1)-1)
}

// Invoke 按当前权重快照路由一次调用。
func (f *Function) Invoke(ctx context.Context, req *Request) (*Response, error) {
	for attempt := 0; attempt < 8; attempt++ {
		f.mu.Lock()
		if len(f.weights) == 0 {
			f.mu.Unlock()
			return nil, newError(ErrKindNoTraffic, "function "+f.name+" has no traffic configured")
		}
		id := f.routeLocked(req)
		p := f.pools[id]
		f.mu.Unlock()

		resp, err := p.invoke(ctx, req)
		if IsKind(err, ErrKindDraining) {
			continue // 与并发流量切换竞争：按新快照重路由
		}
		if err == nil {
			f.log("[function %s] request %q key=%q -> revision %s instance %s",
				f.name, req.Payload, req.RouteKey, resp.Revision, resp.InstanceID)
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

// Close 排空所有实例，并等待全部后台排空 goroutine 结束（保证测试日志不跨用例）。
func (f *Function) Close() {
	f.mu.Lock()
	f.closed = true
	var toDrain []*pool
	for _, p := range f.pools {
		if p.markDraining() {
			toDrain = append(toDrain, p)
		}
	}
	f.mu.Unlock()
	for _, p := range toDrain {
		f.startDrain(p)
	}
	f.drainWG.Wait()
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

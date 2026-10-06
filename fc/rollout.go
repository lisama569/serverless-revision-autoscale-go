package fc

import (
	"hash/fnv"
	"sort"
)

// rolloutState 是一次渐进放量计划的内部状态。
// stepWeights[i] 为第 i 步生效后的权重；stepWeights[0] 即起步权重（= 启动时当前权重），
// stepWeights[steps] 与目标权重逐键相等。
type rolloutState struct {
	phase       RolloutPhase
	steps       int
	perStep     int
	step        int
	inStep      int
	target      map[RevisionID]int
	stepWeights []map[RevisionID]int
}

// StartRollout 启动一个渐进放量计划。
//
// 计划从当前权重出发，每路由 RequestsPerStep 个请求整体推进一步，
// 每一步都是一次普通的原子权重切换；最终权重与 plan.Target 逐键一致。
// expectedEpoch 为乐观并发控制；已有进行中的计划时返回冲突，需先取消或等其走完。
func (f *Function) StartRollout(plan RolloutPlan, expectedEpoch uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkEpochLocked(expectedEpoch); err != nil {
		return err
	}
	if plan.Steps <= 0 {
		return newError(ErrKindInvalidWeight, "rollout steps must be positive")
	}
	if plan.RequestsPerStep <= 0 {
		return newError(ErrKindInvalidWeight, "rollout requests-per-step must be positive")
	}
	if err := f.validateWeightsLocked(plan.Target); err != nil {
		return err
	}
	if f.rollout != nil && f.rollout.phase == RolloutRunning {
		return newError(ErrKindConflict, "rollout already in progress; cancel it or wait for completion")
	}
	if len(f.weights) == 0 {
		return newError(ErrKindNoTraffic, "cannot start rollout: function has no traffic configured")
	}

	steps := planStepWeights(f.weights, plan.Target, plan.Steps)
	st := &rolloutState{
		phase:       RolloutRunning,
		steps:       plan.Steps,
		perStep:     plan.RequestsPerStep,
		target:      cloneWeights(plan.Target),
		stepWeights: steps,
	}
	f.rollout = st
	f.log("[function %s] rollout started: %d steps, %d requests/step, target=%v",
		f.name, plan.Steps, plan.RequestsPerStep, st.target)

	// 目标版本（含起步时权重为 0 的）按保底配置立即预热，避免首批真实请求冷启动。
	f.ensureResidencyLocked()
	f.epoch++
	return nil
}

// CancelRollout 取消进行中的放量计划：权重停在当前这一完整步上（始终自洽），
// 计划目标中已无流量的版本正常进入排空。
func (f *Function) CancelRollout(expectedEpoch uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkEpochLocked(expectedEpoch); err != nil {
		return err
	}
	if f.rollout == nil || f.rollout.phase != RolloutRunning {
		return newError(ErrKindConflict, "no rollout in progress")
	}
	f.rollout.phase = RolloutCanceled
	f.log("[function %s] rollout canceled at step %d/%d; weights remain %v",
		f.name, f.rollout.step, f.rollout.steps, f.weights)
	// 取消后计划不再保留中间步涉及的版本：无流量者立即排空，有流量者维持保底。
	f.ensureResidencyLocked()
	f.reconcileDrainLocked()
	f.epoch++
	return nil
}

// RolloutState 返回放量计划状态快照；从未启动过时 phase=idle。
func (f *Function) RolloutState() RolloutStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := cloneWeights(f.weights)
	if f.rollout == nil {
		return RolloutStatus{Phase: RolloutIdle, CurrentWeights: cur}
	}
	st := f.rollout
	return RolloutStatus{
		Phase:           st.phase,
		Step:            st.step,
		Steps:           st.steps,
		RequestsInStep:  st.inStep,
		RequestsPerStep: st.perStep,
		CurrentWeights:  cur,
		TargetWeights:   cloneWeights(st.target),
	}
}

// planStepWeights 确定性地生成 steps+1 个权重表：
//   - 索引 0 原样复制 start；
//   - 索引 steps 原样复制 target，因此计划终态与直接做一次权重切换完全一致；
//   - 中间步把 start/target 各自确定性归一化到同一总和 D=steps*L（L 为两者原总和的
//     较大值），再做四舍五入的整数线性插值。
//
// 相同入参永远产生相同序列，不依赖时间与随机数。
func planStepWeights(start, target map[RevisionID]int, steps int) []map[RevisionID]int {
	out := make([]map[RevisionID]int, steps+1)
	out[0] = cloneWeights(start)
	out[steps] = cloneWeights(target)
	if steps == 1 {
		return out
	}

	ids := unionSortedIDs(start, target)
	L := sumWeights(start)
	if t := sumWeights(target); t > L {
		L = t
	}
	D := steps * L
	s := normalizeScaled(start, ids, D)
	tg := normalizeScaled(target, ids, D)

	for k := 1; k < steps; k++ {
		w := make(map[RevisionID]int, len(ids))
		for _, id := range ids {
			// 四舍五入的整数线性插值：(s*(steps-k) + t*k) / steps
			num := s[id]*(steps-k) + tg[id]*k
			v := (2*num + steps) / (2 * steps)
			if v > 0 {
				w[id] = v
			}
		}
		out[k] = w
	}
	return out
}

// normalizeScaled 把 weights 确定性地缩放到总和恰为 D：
// 先按整数商分配，再把余量按最大余数法（余数相同按 ID 升序）逐个补 1。
func normalizeScaled(weights map[RevisionID]int, ids []RevisionID, D int) map[RevisionID]int {
	total := sumWeights(weights)
	out := make(map[RevisionID]int, len(ids))
	if total == 0 || D <= 0 {
		return out
	}
	type cand struct {
		id RevisionID
		r  int
	}
	cands := make([]cand, 0, len(ids))
	assigned := 0
	for _, id := range ids {
		w := weights[id]
		q := w * D / total
		r := w*D - q*total
		out[id] = q
		assigned += q
		cands = append(cands, cand{id, r})
	}
	// 余数降序、ID 升序，确定且可复现。
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].r != cands[j].r {
			return cands[i].r > cands[j].r
		}
		return cands[i].id < cands[j].id
	})
	for i := 0; i < D-assigned; i++ {
		out[cands[i%len(cands)].id]++
	}
	return out
}

func sumWeights(w map[RevisionID]int) int {
	s := 0
	for _, v := range w {
		s += v
	}
	return s
}

func cloneWeights(w map[RevisionID]int) map[RevisionID]int {
	c := make(map[RevisionID]int, len(w))
	for k, v := range w {
		c[k] = v
	}
	return c
}

func unionSortedIDs(a, b map[RevisionID]int) []RevisionID {
	set := map[RevisionID]struct{}{}
	for id := range a {
		set[id] = struct{}{}
	}
	for id := range b {
		set[id] = struct{}{}
	}
	ids := make([]RevisionID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// stickyPick 按业务路由键确定性地选择版本：FNV-1a 64 位哈希对权重展开总长取模，
// 再映射到按 ID 排序的累计权重区间。同一权重表下同一键永远得到同一版本，无随机数。
func stickyPick(weights map[RevisionID]int, key string) RevisionID {
	ids := make([]RevisionID, 0, len(weights))
	for id := range weights {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	total := 0
	for _, id := range ids {
		total += weights[id]
	}
	h := fnv.New64a()
	h.Write([]byte(key))
	r := int(h.Sum64() % uint64(total))
	for _, id := range ids {
		w := weights[id]
		if r < w {
			return id
		}
		r -= w
	}
	return ids[len(ids)-1]
}

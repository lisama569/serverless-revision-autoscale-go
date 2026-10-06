package fc

import "fmt"

// RolloutStep 描述渐进放量计划中的一步。
type RolloutStep struct {
	Percent  int   // 该步新版本的目标流量百分比 (0,100]，需严格递增
	Requests int64 // 该步生效后，再经过多少次调用进入下一步（最后一步忽略）
}

// RolloutStatus 是渐进放量当前状态的快照（用于观测与测试断言）。
type RolloutStatus struct {
	Active         bool // 是否有进行中的放量
	NewRevision    RevisionID
	CurrentStep    int // 当前已生效的步序号（从 0 开始）
	TotalSteps     int
	CurrentPercent int // 当前已生效的新版本百分比
}

// rolloutState 是进行中的渐进放量计划（只在 Function.mu 下访问）。
type rolloutState struct {
	newRev RevisionID
	base   map[RevisionID]int // 启动时的权重快照（作为其他版本份额的基准）
	steps  []RolloutStep
	step   int   // 当前已生效的步序号
	remain int64 // 距下一步还需经过的调用次数
}

// StartRollout 为已发布的新版本启动渐进放量：按请求节奏逐步把流量从当前比例
// 推到目标比例。第一步立即随本调用原子生效；之后每经过对应当前步的 Requests
// 次调用，下一步的权重整体原子生效（不存在只切一半的中间状态）。
//
// 计划全部走完后，权重表与直接做一次等比例 UpdateTraffic 的结果完全一致
// （按最大公约数归一化）。expectedEpoch 语义与 UpdateTraffic 相同。
// 已有放量进行中、或计划非法时拒绝且无副作用。
func (f *Function) StartRollout(newRev RevisionID, steps []RolloutStep, expectedEpoch uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.checkEpochLocked(expectedEpoch); err != nil {
		return err
	}
	if f.rollout != nil {
		return newError(ErrKindRolloutConflict,
			fmt.Sprintf("rollout for %s already in progress (step %d/%d)",
				f.rollout.newRev, f.rollout.step+1, len(f.rollout.steps)))
	}
	if _, ok := f.revisions[newRev]; !ok {
		return newError(ErrKindUnknownRevision, "rollout target "+string(newRev)+" does not exist")
	}
	if len(f.weights) == 0 {
		return newError(ErrKindNoTraffic, "function "+f.name+" has no traffic to shift from")
	}
	if err := validateSteps(steps); err != nil {
		return err
	}

	base := make(map[RevisionID]int, len(f.weights))
	for id, w := range f.weights {
		base[id] = w
	}
	f.rollout = &rolloutState{newRev: newRev, base: base, steps: steps}
	f.log("[function %s] rollout started: %s -> %d%% over %d step(s)", f.name, newRev, steps[len(steps)-1].Percent, len(steps))

	// 第一步立即生效；单步计划即等同于一次普通权重切换。
	f.applyRolloutStepLocked(0)
	if len(steps) > 1 {
		f.rollout.remain = steps[0].Requests
	}
	f.epoch++
	return nil
}

// CancelRollout 取消进行中的放量：流量停在上一个已整体生效的步比例上，
// 状态自洽、无半生效残留。无进行中的放量时返回 nil。
// 取消是显式中止操作，不需要纪元。
func (f *Function) CancelRollout() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rollout == nil {
		return nil
	}
	f.log("[function %s] rollout cancelled at step %d/%d (%s at %d%%); traffic stays at %v",
		f.name, f.rollout.step+1, len(f.rollout.steps), f.rollout.newRev,
		f.rollout.steps[f.rollout.step].Percent, f.weights)
	f.rollout = nil
	return nil
}

// Rollout 返回当前放量状态快照。
func (f *Function) Rollout() RolloutStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rollout == nil {
		return RolloutStatus{}
	}
	return RolloutStatus{
		Active:         true,
		NewRevision:    f.rollout.newRev,
		CurrentStep:    f.rollout.step,
		TotalSteps:     len(f.rollout.steps),
		CurrentPercent: f.rollout.steps[f.rollout.step].Percent,
	}
}

// tickRollout 在每次调用到来时推进放量节奏（每次 Invoke 计一次）。
// 到达步边界时在锁内整体替换权重，保证切换原子生效。
func (f *Function) tickRollout() {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.rollout
	if r == nil {
		return
	}
	r.remain--
	if r.remain > 0 {
		return
	}
	next := r.step + 1
	f.applyRolloutStepLocked(next)
	if next < len(r.steps)-1 {
		r.remain = r.steps[next].Requests
	}
	f.epoch++
}

// applyRolloutStepLocked 计算并原子应用第 i 步的权重表；走到最后一步时结束放量。
// 调用方必须持有 f.mu 且 f.rollout 非 nil。
func (f *Function) applyRolloutStepLocked(i int) {
	r := f.rollout
	r.step = i
	weights := rolloutWeights(r.base, r.newRev, r.steps[i].Percent)
	if err := f.applyTrafficLocked(weights); err != nil {
		// 权重由内部从已校验的状态推导，不会失败。
		panic(fmt.Sprintf("rollout step produced invalid weights: %v", err))
	}
	f.log("[function %s] rollout step %d/%d: %s at %d%%, weights=%v",
		f.name, i+1, len(r.steps), r.newRev, r.steps[i].Percent, f.weights)
	if i == len(r.steps)-1 {
		f.log("[function %s] rollout completed: %s at %d%%", f.name, r.newRev, r.steps[i].Percent)
		f.rollout = nil
	}
}

// cancelRolloutLocked 在权重被其他控制面操作整体替换前取消进行中的放量。
func (f *Function) cancelRolloutLocked(reason string) {
	if f.rollout == nil {
		return
	}
	f.log("[function %s] rollout for %s superseded by %s at step %d/%d",
		f.name, f.rollout.newRev, reason, f.rollout.step+1, len(f.rollout.steps))
	f.rollout = nil
}

// rolloutWeights 由基准权重与目标百分比推导权重表：
// 新版本占精确的 percent%，其余版本按基准比例瓜分剩余份额。
// 结果按最大公约数归一化，与手写同比例权重表完全一致。
func rolloutWeights(base map[RevisionID]int, newRev RevisionID, percent int) map[RevisionID]int {
	total := 0
	for id, w := range base {
		if id != newRev {
			total += w
		}
	}
	out := map[RevisionID]int{newRev: percent * total}
	for id, w := range base {
		if id == newRev {
			continue
		}
		if scaled := w * (100 - percent); scaled > 0 {
			out[id] = scaled
		}
	}
	g := 0
	for _, w := range out {
		g = gcd(g, w)
	}
	if g > 1 {
		for id := range out {
			out[id] /= g
		}
	}
	return out
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func validateSteps(steps []RolloutStep) error {
	if len(steps) == 0 {
		return newError(ErrKindInvalidWeight, "rollout plan must have at least one step")
	}
	prev := 0
	for i, s := range steps {
		if s.Percent <= 0 || s.Percent > 100 {
			return newError(ErrKindInvalidWeight,
				fmt.Sprintf("rollout step %d: percent %d out of range (0,100]", i, s.Percent))
		}
		if s.Percent <= prev {
			return newError(ErrKindInvalidWeight,
				fmt.Sprintf("rollout step %d: percent %d must be strictly greater than previous %d", i, s.Percent, prev))
		}
		if i < len(steps)-1 && s.Requests <= 0 {
			return newError(ErrKindInvalidWeight,
				fmt.Sprintf("rollout step %d: requests %d must be positive for non-final steps", i, s.Requests))
		}
		prev = s.Percent
	}
	return nil
}

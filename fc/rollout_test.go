package fc

import (
	"context"
	"testing"
)

// startRolloutEnv 构建 v1=100% 的函数并发布无流量的 v2。
func startRolloutEnv(t *testing.T) (*Function, RevisionID, RevisionID) {
	t.Helper()
	f := newTestFunction(t)
	v1, err := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	v2, err := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	return f, v1, v2
}

// invokeN 发送 n 个无路由键请求，返回每步命中版本序列。
func invokeN(t *testing.T, f *Function, n int) []RevisionID {
	t.Helper()
	got := make([]RevisionID, n)
	for i := 0; i < n; i++ {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "r"})
		if err != nil {
			t.Fatalf("invoke %d: %v", i, err)
		}
		got[i] = resp.Revision
	}
	return got
}

// TestRolloutStepwise 渐进放量按请求计数整体推进一步，每一步权重原子生效；
// 计划走完后的权重与直接做一次普通权重切换完全一致。
func TestRolloutStepwise(t *testing.T) {
	const perStep = 5
	f, v1, v2 := startRolloutEnv(t)
	target := map[RevisionID]int{v1: 1, v2: 1}

	if err := f.StartRollout(RolloutPlan{Target: target, Steps: 2, RequestsPerStep: perStep}, f.Epoch()); err != nil {
		t.Fatalf("start rollout: %v", err)
	}

	// 期望窗口：请求 0..3 起步 v1-only；请求 4..8 中间步；请求 9.. 目标 1:1。
	got := invokeN(t, f, 20)
	mid := map[RevisionID]int{}
	for i, id := range got {
		switch {
		case i < 4:
			if id != v1 {
				t.Fatalf("window0 request %d hit %s, want v1", i, id)
			}
		case i < 9:
			mid[id]++
		default:
			if (i-9)%2 == 0 && id != v2 {
				t.Fatalf("target window request %d hit %s, want v2", i, id)
			}
			if (i-9)%2 == 1 && id != v1 {
				t.Fatalf("target window request %d hit %s, want v1", i, id)
			}
		}
	}
	_ = got
	t.Logf("intermediate step distribution over 5 requests: v1=%d v2=%d", mid[v1], mid[v2])
	if mid[v1]+mid[v2] != 5 {
		t.Fatalf("intermediate window lost requests: %v", mid)
	}
	st := f.RolloutState()
	if st.Phase != RolloutComplete || st.Step != 2 {
		t.Fatalf("rollout not complete: %+v", st)
	}
	tp := f.Traffic()
	if tp.Weights[v1] != 1 || tp.Weights[v2] != 1 {
		t.Fatalf("final weights %v do not equal target 1:1", tp.Weights)
	}

	// 终态等价性：另起一个直接切换到同一目标的函数，把路由计数器对齐到同一位置后，
	// 比较继续 400 个请求的版本分布，必须完全一致。
	g, g1, g2 := startRolloutEnv(t)
	// 对齐 g 的加权轮转计数器：先在 v1-only 下消费 20 个请求，再直接切换目标。
	invokeN(t, g, 20)
	if err := g.UpdateTraffic(target, g.Epoch()); err != nil {
		t.Fatalf("direct update: %v", err)
	}
	counts := func(fn *Function, n int) map[RevisionID]int {
		c := map[RevisionID]int{}
		for _, id := range invokeN(t, fn, n) {
			c[id]++
		}
		return c
	}
	after := counts(f, 400)
	direct := counts(g, 400)
	t.Logf("post-rollout distribution=%v direct-switch distribution=%v", after, direct)
	if after[v1] != direct[g1] || after[v2] != direct[g2] {
		t.Fatalf("post-rollout distribution %v differs from direct switch %v", after, direct)
	}
}

// TestRolloutReproducible 同样的请求顺序与步长重复执行，每一步切换点与流量分布完全一致。
func TestRolloutReproducible(t *testing.T) {
	run := func() []RevisionID {
		f, v1, v2 := startRolloutEnv(t)
		if err := f.StartRollout(RolloutPlan{
			Target:          map[RevisionID]int{v1: 1, v2: 3},
			Steps:           3,
			RequestsPerStep: 7,
		}, f.Epoch()); err != nil {
			t.Fatalf("start rollout: %v", err)
		}
		return invokeN(t, f, 28)
	}
	a := run()
	b := run()
	if len(a) != len(b) {
		t.Fatal("length mismatch")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("non-reproducible routing at request %d: %s vs %s", i, a[i], b[i])
		}
	}
	t.Logf("reproducible sequence over %d requests", len(a))

	// 切换点同样可复现：第 7、14、21 个请求起整体推进到下一步，且每步权重都是完整表。
	f, v1, v2 := startRolloutEnv(t)
	if err := f.StartRollout(RolloutPlan{
		Target:          map[RevisionID]int{v1: 1, v2: 3},
		Steps:           3,
		RequestsPerStep: 7,
	}, f.Epoch()); err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	for i := 1; i <= 21; i++ {
		if _, err := f.Invoke(context.Background(), &Request{Payload: "r"}); err != nil {
			t.Fatalf("invoke %d: %v", i, err)
		}
		st := f.RolloutState()
		wantStep := i / 7
		if wantStep > 3 {
			wantStep = 3
		}
		if st.Step != wantStep {
			t.Fatalf("after %d requests step=%d, want %d (weights=%v)", i, st.Step, wantStep, st.CurrentWeights)
		}
		if st.Phase == RolloutRunning && st.Step < 3 {
			if total := sumWeights(st.CurrentWeights); total <= 0 {
				t.Fatalf("non-self-consistent weights at step %d: %v", st.Step, st.CurrentWeights)
			}
		}
	}
	st := f.RolloutState()
	if st.Phase != RolloutComplete || st.CurrentWeights[v1] != 1 || st.CurrentWeights[v2] != 3 {
		t.Fatalf("unexpected final state: %+v", st)
	}
}

// TestRolloutCancel 中途取消：停在当前完整步的自洽比例上，无半生效状态，
// 之后请求严格按当前权重走，计划不再推进。
func TestRolloutCancel(t *testing.T) {
	const perStep = 5
	f, v1, v2 := startRolloutEnv(t)
	if err := f.StartRollout(RolloutPlan{
		Target:          map[RevisionID]int{v1: 1, v2: 1},
		Steps:           4,
		RequestsPerStep: perStep,
	}, f.Epoch()); err != nil {
		t.Fatalf("start rollout: %v", err)
	}

	first := invokeN(t, f, 7) // 跨过第一个切换点（请求 5）
	if len(first) != 7 {
		t.Fatalf("want 7 requests, got %d", len(first))
	}
	st := f.RolloutState()
	t.Logf("after 7 requests: phase=%s step=%d inStep=%d weights=%v",
		st.Phase, st.Step, st.RequestsInStep, st.CurrentWeights)
	if st.Step != 1 || st.RequestsInStep != 2 {
		t.Fatalf("unexpected progress: %+v", st)
	}
	snapshotWeights := cloneWeights(st.CurrentWeights)

	if err := f.CancelRollout(f.Epoch()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	st = f.RolloutState()
	if st.Phase != RolloutCanceled {
		t.Fatalf("want canceled, got %s", st.Phase)
	}
	if !weightsEqual(st.CurrentWeights, snapshotWeights) {
		t.Fatalf("weights changed during cancel: before=%v after=%v", snapshotWeights, st.CurrentWeights)
	}

	// 取消后来再多请求，权重与步序号都必须冻结。
	invokeN(t, f, 50)
	st = f.RolloutState()
	if st.Phase != RolloutCanceled || st.Step != 1 {
		t.Fatalf("canceled rollout kept advancing: %+v", st)
	}
	if !weightsEqual(f.Traffic().Weights, snapshotWeights) {
		t.Fatalf("weights drifted after cancel: %v", f.Traffic().Weights)
	}
	t.Logf("rollout frozen at %v", snapshotWeights)
}

// TestRolloutSuperseded 放量途中直接做普通权重切换：计划被标记取消，新权重整体立即生效。
func TestRolloutSuperseded(t *testing.T) {
	f, v1, v2 := startRolloutEnv(t)
	if err := f.StartRollout(RolloutPlan{
		Target:          map[RevisionID]int{v1: 1, v2: 1},
		Steps:           4,
		RequestsPerStep: 5,
	}, f.Epoch()); err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	invokeN(t, f, 6)

	if err := f.UpdateTraffic(map[RevisionID]int{v2: 1}, f.Epoch()); err != nil {
		t.Fatalf("update traffic: %v", err)
	}
	st := f.RolloutState()
	if st.Phase != RolloutCanceled {
		t.Fatalf("rollout must be superseded/canceled, got %s", st.Phase)
	}
	for _, id := range invokeN(t, f, 20) {
		if id != v2 {
			t.Fatalf("new traffic policy not fully effective, got %s", id)
		}
	}
}

// TestStartRolloutValidation 非法计划被拒绝且无副作用；纪元失配返回冲突。
func TestStartRolloutValidation(t *testing.T) {
	f, v1, v2 := startRolloutEnv(t)
	before := f.Traffic()

	cases := []struct {
		name string
		plan RolloutPlan
		want ErrorKind
	}{
		{"zero steps", RolloutPlan{Target: map[RevisionID]int{v2: 1}, Steps: 0, RequestsPerStep: 5}, ErrKindInvalidWeight},
		{"zero rate", RolloutPlan{Target: map[RevisionID]int{v2: 1}, Steps: 2, RequestsPerStep: 0}, ErrKindInvalidWeight},
		{"unknown target", RolloutPlan{Target: map[RevisionID]int{"ghost": 1}, Steps: 2, RequestsPerStep: 5}, ErrKindUnknownRevision},
		{"all-zero target", RolloutPlan{Target: map[RevisionID]int{v2: 0}, Steps: 2, RequestsPerStep: 5}, ErrKindInvalidWeight},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := f.StartRollout(tc.plan, f.Epoch()); !IsKind(err, tc.want) {
				t.Fatalf("want %s, got %v", tc.want, err)
			}
		})
	}
	after := f.Traffic()
	if after.Epoch != before.Epoch || !weightsEqual(after.Weights, before.Weights) {
		t.Fatalf("rejected rollout mutated state: %+v", after)
	}

	// 进行中的计划不能重复启动。
	if err := f.StartRollout(RolloutPlan{
		Target: map[RevisionID]int{v1: 1, v2: 1}, Steps: 2, RequestsPerStep: 5,
	}, f.Epoch()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := f.StartRollout(RolloutPlan{
		Target: map[RevisionID]int{v2: 1}, Steps: 2, RequestsPerStep: 5,
	}, f.Epoch()); !IsKind(err, ErrKindConflict) {
		t.Fatalf("duplicate start must conflict, got %v", err)
	}

	// 陈旧纪元必须冲突。
	if err := f.CancelRollout(before.Epoch); !IsKind(err, ErrKindConflict) {
		t.Fatalf("stale epoch cancel must conflict, got %v", err)
	}
}

func weightsEqual(a, b map[RevisionID]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

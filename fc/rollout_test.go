package fc

import (
	"context"
	"fmt"
	"testing"
)

// runRolloutScenario 跑一次完整的渐进放量场景，返回每次调用的归因序列，
// 并沿途断言每一步的权重表与切换点。
func runRolloutScenario(t *testing.T) []RevisionID {
	t.Helper()
	f := NewFunction("fn", t.Logf)
	v1, err := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	v2, err := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// 计划：10% -(3 次调用)-> 50% -(3 次调用)-> 100%。
	steps := []RolloutStep{{Percent: 10, Requests: 3}, {Percent: 50, Requests: 3}, {Percent: 100}}
	if err := f.StartRollout(v2, steps, f.Epoch()); err != nil {
		t.Fatalf("start rollout: %v", err)
	}

	// 第一步立即整体生效：v2 精确占 10%（{v1:9, v2:1}）。
	if w := f.Traffic().Weights; w[v1] != 9 || w[v2] != 1 || len(w) != 2 {
		t.Fatalf("step 1 weights: want {v1:9 v2:1}, got %v", w)
	}
	if st := f.Rollout(); !st.Active || st.CurrentStep != 0 || st.CurrentPercent != 10 || st.TotalSteps != 3 {
		t.Fatalf("rollout status after start: %+v", st)
	}

	var seq []RevisionID
	invoke := func() {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "probe"})
		if err != nil {
			t.Fatalf("invoke: %v", err)
		}
		seq = append(seq, resp.Revision)
	}

	// 前 2 次调用后仍在第一步；第 3 次调用推进到第二步。
	invoke()
	invoke()
	if w := f.Traffic().Weights; w[v2] != 1 {
		t.Fatalf("weights changed before step boundary: %v", w)
	}
	invoke() // 第 3 次：推进到 50%
	if w := f.Traffic().Weights; w[v1] != 1 || w[v2] != 1 || len(w) != 2 {
		t.Fatalf("step 2 weights: want {v1:1 v2:1}, got %v", w)
	}
	if st := f.Rollout(); !st.Active || st.CurrentStep != 1 || st.CurrentPercent != 50 {
		t.Fatalf("rollout status at step 2: %+v", st)
	}

	invoke()
	invoke()
	if w := f.Traffic().Weights; w[v2] != 1 || len(w) != 2 {
		t.Fatalf("weights changed before step boundary: %v", w)
	}
	invoke() // 第 6 次：推进到 100%，计划完成
	w := f.Traffic().Weights
	if len(w) != 1 || w[v2] != 1 {
		t.Fatalf("final weights: want {v2:1}, got %v", w)
	}
	if st := f.Rollout(); st.Active {
		t.Fatalf("rollout should be completed, got %+v", st)
	}

	// 完成后的状态与直接做一次普通权重切换完全一致。
	ref := NewFunction("ref", t.Logf)
	rv1, _ := ref.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	rv2, _ := ref.Publish(okHandler("v2"), RevisionConfig{}, nil, ref.Epoch())
	if err := ref.UpdateTraffic(map[RevisionID]int{rv2: 1}, ref.Epoch()); err != nil {
		t.Fatalf("reference update: %v", err)
	}
	rw := ref.Traffic().Weights
	if len(w) != len(rw) || w[v2] != rw[rv2] || w[v1] != rw[rv1] {
		t.Fatalf("rollout result %v differs from direct switch %v", w, rw)
	}

	// 计划结束后再调用，权重不再变化。
	invoke()
	if w := f.Traffic().Weights; len(w) != 1 || w[v2] != 1 {
		t.Fatalf("weights mutated after rollout completed: %v", w)
	}
	return seq
}

// TestProgressiveRollout 渐进放量按请求节奏逐步推进，每步整体生效，
// 最终状态与直接权重切换一致；同样请求序列重跑结果完全一致。
func TestProgressiveRollout(t *testing.T) {
	seq1 := runRolloutScenario(t)
	seq2 := runRolloutScenario(t)
	if fmt.Sprint(seq1) != fmt.Sprint(seq2) {
		t.Fatalf("same request sequence produced different attribution:\n%v\n%v", seq1, seq2)
	}
	t.Logf("reproducible attribution sequence: %v", seq1)
}

// TestRolloutCancel 中途取消：停在上一个已整体生效的步比例上，无残留状态。
func TestRolloutCancel(t *testing.T) {
	f := NewFunction("fn", t.Logf)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	v2, _ := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())

	steps := []RolloutStep{{Percent: 20, Requests: 2}, {Percent: 80, Requests: 2}, {Percent: 100}}
	if err := f.StartRollout(v2, steps, f.Epoch()); err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	// 第一步 20%：{v1:4, v2:1}。
	if w := f.Traffic().Weights; w[v1] != 4 || w[v2] != 1 {
		t.Fatalf("step 1 weights: %v", w)
	}
	// 推进到第二步 80%：{v1:1, v2:4}。
	for i := 0; i < 2; i++ {
		if _, err := f.Invoke(context.Background(), &Request{Payload: "p"}); err != nil {
			t.Fatalf("invoke: %v", err)
		}
	}
	if w := f.Traffic().Weights; w[v1] != 1 || w[v2] != 4 {
		t.Fatalf("step 2 weights: %v", w)
	}

	if err := f.CancelRollout(); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if st := f.Rollout(); st.Active {
		t.Fatalf("rollout still active after cancel: %+v", st)
	}
	// 取消后流量停在 80% 这一自洽比例上，继续调用不再变化。
	for i := 0; i < 5; i++ {
		if _, err := f.Invoke(context.Background(), &Request{Payload: "p"}); err != nil {
			t.Fatalf("invoke after cancel: %v", err)
		}
	}
	if w := f.Traffic().Weights; w[v1] != 1 || w[v2] != 4 || len(w) != 2 {
		t.Fatalf("weights after cancel: want stable {v1:1 v2:4}, got %v", w)
	}
	// 重复取消是空操作。
	if err := f.CancelRollout(); err != nil {
		t.Fatalf("second cancel: %v", err)
	}
}

// TestRolloutRollbackMidway 放量中途回退：权重整体切回旧版本，放量状态无残留。
func TestRolloutRollbackMidway(t *testing.T) {
	f := NewFunction("fn", t.Logf)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	v2, _ := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())

	steps := []RolloutStep{{Percent: 30, Requests: 2}, {Percent: 100}}
	if err := f.StartRollout(v2, steps, f.Epoch()); err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	if err := f.Rollback(v1, f.Epoch()); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if st := f.Rollout(); st.Active {
		t.Fatalf("rollout should be superseded by rollback: %+v", st)
	}
	for i := 0; i < 4; i++ {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "p"})
		if err != nil {
			t.Fatalf("invoke after rollback: %v", err)
		}
		if resp.Revision != v1 {
			t.Fatalf("request after rollback went to %s", resp.Revision)
		}
	}
	if w := f.Traffic().Weights; len(w) != 1 || w[v1] != 1 {
		t.Fatalf("weights after rollback: %v", w)
	}
}

// TestRolloutValidation 非法计划与冲突被明确拒绝，且无副作用。
func TestRolloutValidation(t *testing.T) {
	f := NewFunction("fn", t.Logf)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	v2, _ := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())

	cases := []struct {
		name  string
		rev   RevisionID
		steps []RolloutStep
		want  ErrorKind
	}{
		{"empty plan", v2, nil, ErrKindInvalidWeight},
		{"zero percent", v2, []RolloutStep{{Percent: 0, Requests: 1}, {Percent: 100}}, ErrKindInvalidWeight},
		{"percent over 100", v2, []RolloutStep{{Percent: 101}}, ErrKindInvalidWeight},
		{"not increasing", v2, []RolloutStep{{Percent: 50, Requests: 1}, {Percent: 50}}, ErrKindInvalidWeight},
		{"decreasing", v2, []RolloutStep{{Percent: 60, Requests: 1}, {Percent: 30}}, ErrKindInvalidWeight},
		{"non-positive requests", v2, []RolloutStep{{Percent: 50, Requests: 0}, {Percent: 100}}, ErrKindInvalidWeight},
		{"unknown revision", "ghost", []RolloutStep{{Percent: 100}}, ErrKindUnknownRevision},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := f.Traffic()
			err := f.StartRollout(tc.rev, tc.steps, before.Epoch)
			if !IsKind(err, tc.want) {
				t.Fatalf("want %s, got %v", tc.want, err)
			}
			after := f.Traffic()
			if after.Epoch != before.Epoch || len(after.Weights) != 1 || after.Weights[v1] != 1 {
				t.Fatalf("rejected rollout mutated state: %+v", after)
			}
			if st := f.Rollout(); st.Active {
				t.Fatalf("rejected rollout left active state: %+v", st)
			}
		})
	}

	// 纪元不匹配。
	if err := f.StartRollout(v2, []RolloutStep{{Percent: 100}}, 999); !IsKind(err, ErrKindConflict) {
		t.Fatalf("want epoch conflict, got %v", err)
	}

	// 进行中的放量不允许再启动第二个。
	if err := f.StartRollout(v2, []RolloutStep{{Percent: 10, Requests: 100}, {Percent: 100}}, f.Epoch()); err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	if err := f.StartRollout(v2, []RolloutStep{{Percent: 100}}, f.Epoch()); !IsKind(err, ErrKindRolloutConflict) {
		t.Fatalf("want rollout_in_progress, got %v", err)
	}
	if err := f.CancelRollout(); err != nil {
		t.Fatalf("cancel: %v", err)
	}
}

// TestRolloutAtomicSteps 并发观测者看到的权重表永远等于某一步的完整权重表，
// 不会出现只切了一半的中间状态。
func TestRolloutAtomicSteps(t *testing.T) {
	f := NewFunction("fn", t.Logf)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	v2, _ := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())

	steps := []RolloutStep{{Percent: 25, Requests: 50}, {Percent: 60, Requests: 50}, {Percent: 100}}
	if err := f.StartRollout(v2, steps, f.Epoch()); err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	valid := []map[RevisionID]int{
		{v1: 3, v2: 1}, // 25%
		{v1: 2, v2: 3}, // 60%
		{v2: 1},        // 100%
	}
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			w := f.Traffic().Weights
			ok := false
			for _, want := range valid {
				if len(w) == len(want) && w[v1] == want[v1] && w[v2] == want[v2] {
					ok = true
					break
				}
			}
			if !ok {
				t.Errorf("observed half-applied weights: %v", w)
				return
			}
		}
	}()
	defer close(done)

	for i := 0; i < 120; i++ {
		if _, err := f.Invoke(context.Background(), &Request{Payload: "p"}); err != nil {
			t.Fatalf("invoke: %v", err)
		}
	}
	if st := f.Rollout(); st.Active {
		t.Fatalf("rollout should be completed: %+v", st)
	}
}

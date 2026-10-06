package fc

import (
	"context"
	"testing"
	"time"
)

// TestMinInstancesScaleBeyondFloor 超出保底的实例空闲后照常回收。
func TestMinInstancesScaleBeyondFloor(t *testing.T) {
	clock := newFakeClock()
	f := newTestFunction(t)
	f.now = clock.Now

	v1, err := f.Publish(okHandler("v1"), RevisionConfig{
		MaxConcurrency: 1,
		MaxInstances:   4,
		IdleTimeout:    time.Minute,
		MinInstances:   1,
	}, map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := f.Stats()[v1].Instances; got != 1 {
		t.Fatalf("warm-up: want 1, got %d", got)
	}

	// 4 个并发请求把池扩到 4。
	release := make(chan struct{})
	block := func(ctx context.Context, req *Request) (string, error) {
		<-release
		return "ok", nil
	}
	f.revisions[v1].Handler = block
	done := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		go func() {
			_, err := f.Invoke(context.Background(), &Request{Payload: "load"})
			if err != nil {
				t.Errorf("invoke: %v", err)
			}
			done <- struct{}{}
		}()
	}
	waitFor(t, func() bool { return f.Stats()[v1].Instances == 4 })
	t.Logf("scaled up to floor+burst: %+v", f.Stats()[v1])

	close(release)
	for i := 0; i < 4; i++ {
		<-done
	}
	clock.Advance(2 * time.Minute)
	f.Tick()
	st := f.Stats()[v1]
	t.Logf("after drain of burst: %+v (min=1)", st)
	if st.Instances != 1 {
		t.Fatalf("want reclaim back to floor 1, got %d", st.Instances)
	}
}

// TestDefaultStillScalesToZero 未配置 MinInstances 的版本行为不变：缩到零并冷启动。
func TestDefaultStillScalesToZero(t *testing.T) {
	clock := newFakeClock()
	f := newTestFunction(t)
	f.now = clock.Now
	v1, err := f.Publish(okHandler("v1"), RevisionConfig{IdleTimeout: time.Minute},
		map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := f.Stats()[v1].Instances; got != 0 {
		t.Fatalf("default revision must not be pre-warmed, got %d instances", got)
	}
	if _, err := f.Invoke(context.Background(), &Request{Payload: "x"}); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	clock.Advance(2 * time.Minute)
	f.Tick()
	if got := f.Stats()[v1].Instances; got != 0 {
		t.Fatalf("default revision must scale to zero, got %d", got)
	}
}

// TestMinInstancesDrainOnRetire 版本彻底下线（失去全部流量且不在放量计划中）时，
// 保底实例也必须被排空回收，不漏掉。
func TestMinInstancesDrainOnRetire(t *testing.T) {
	f := newTestFunction(t)
	v1, err := f.Publish(okHandler("v1"), RevisionConfig{MinInstances: 2},
		map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	v2, err := f.Publish(okHandler("v2"), RevisionConfig{MinInstances: 1}, nil, f.Epoch())
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	if got := f.Stats()[v1].Instances; got != 2 {
		t.Fatalf("v1 min instances: want 2, got %d", got)
	}

	// 全量切到 v2：v1 彻底失去流量，保底实例也要排空到零。
	if err := f.UpdateTraffic(map[RevisionID]int{v2: 1}, f.Epoch()); err != nil {
		t.Fatalf("traffic: %v", err)
	}
	waitFor(t, func() bool { return f.Stats()[v1].Instances == 0 })
	t.Logf("retired v1 drained: %+v; v2 warmed: %+v", f.Stats()[v1], f.Stats()[v2])
	if got := f.Stats()[v2].Instances; got != 1 {
		t.Fatalf("v2 min instances after taking traffic: want 1, got %d", got)
	}
}

// TestMinInstancesWarmDuringRollout 渐进放量期间，零流量起步的目标版本也提前保有底实例；
// 计划取消且该版本无流量后，保底实例被收走。
func TestMinInstancesWarmDuringRollout(t *testing.T) {
	f := newTestFunction(t)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{MinInstances: 1},
		map[RevisionID]int{"": 1}, 0)
	v2, err := f.Publish(okHandler("v2"), RevisionConfig{MinInstances: 2}, nil, f.Epoch())
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	if got := f.Stats()[v2].Instances; got != 0 {
		t.Fatalf("v2 should be cold before rollout, got %d", got)
	}

	if err := f.StartRollout(RolloutPlan{
		Target:          map[RevisionID]int{v1: 1, v2: 1},
		Steps:           4,
		RequestsPerStep: 10,
	}, f.Epoch()); err != nil {
		t.Fatalf("rollout: %v", err)
	}
	// 计划刚启动、尚未切任何流量：v2 已有保底实例，首批灰度请求不撞冷启动。
	if got := f.Stats()[v2].Instances; got != 2 {
		t.Fatalf("v2 should be pre-warmed during rollout, got %d", got)
	}
	t.Logf("canary target pre-warmed: v1=%+v v2=%+v", f.Stats()[v1], f.Stats()[v2])

	// 在第一步之前取消：v2 回到零流量 resident 集合之外，保底实例排空。
	if err := f.CancelRollout(f.Epoch()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	waitFor(t, func() bool { return f.Stats()[v2].Instances == 0 })
	t.Logf("after cancel v2 min instances reclaimed: %+v", f.Stats()[v2])
	if got := f.Stats()[v1].Instances; got != 1 {
		t.Fatalf("v1 floor must remain, got %d", got)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}

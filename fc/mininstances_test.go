package fc

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestMinInstancesKeepWarm 保底常驻：获得流量即预热，空闲后保底之外的实例照常回收，
// 保底内的实例即使空闲也保留，避免第一个真实请求冷启动。
func TestMinInstancesKeepWarm(t *testing.T) {
	clock := newFakeClock()
	f := NewFunction("fn", t.Logf)
	f.now = clock.Now

	release := make(chan struct{})
	handler := func(ctx context.Context, req *Request) (string, error) {
		<-release
		return "ok", nil
	}
	cfg := RevisionConfig{
		MaxConcurrency: 1, MaxInstances: 4, MinInstances: 2,
		IdleTimeout: time.Minute,
	}
	v1, err := f.Publish(handler, cfg, map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// 获得流量即预热：还没有任何请求，保底实例已就位。
	if got := f.Stats()[v1].Instances; got != 2 {
		t.Fatalf("want 2 pre-warmed instances, got %d", got)
	}
	t.Logf("pre-warmed before any request: %+v", f.Stats()[v1])

	// 突发并发把池扩到上限 4。
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.Invoke(context.Background(), &Request{Payload: "burst"}); err != nil {
				t.Errorf("burst invoke: %v", err)
			}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for f.Stats()[v1].Instances != 4 {
		if time.Now().After(deadline) {
			t.Fatalf("pool did not scale to 4: %+v", f.Stats()[v1])
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("scaled up under burst: %+v", f.Stats()[v1])
	close(release)
	wg.Wait()

	// 空闲超时后回收，但保底 2 个常驻。
	clock.Advance(2 * time.Minute)
	f.Tick()
	if got := f.Stats()[v1].Instances; got != 2 {
		t.Fatalf("want floor of 2 instances after idle, got %d", got)
	}
	t.Logf("after idle tick: %+v", f.Stats()[v1])

	// 继续空闲也不会跌破保底。
	clock.Advance(10 * time.Minute)
	f.Tick()
	if got := f.Stats()[v1].Instances; got != 2 {
		t.Fatalf("floor breached: got %d instances", got)
	}
}

// TestMinInstancesDefaultUnchanged 未配置保底的版本行为不变：空闲缩容到零。
func TestMinInstancesDefaultUnchanged(t *testing.T) {
	clock := newFakeClock()
	f := NewFunction("fn", t.Logf)
	f.now = clock.Now

	v1, err := f.Publish(okHandler("v1"), RevisionConfig{IdleTimeout: time.Minute}, map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := f.Invoke(context.Background(), &Request{Payload: "p"}); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if got := f.Stats()[v1].Instances; got != 1 {
		t.Fatalf("want 1 instance, got %d", got)
	}
	clock.Advance(2 * time.Minute)
	f.Tick()
	if got := f.Stats()[v1].Instances; got != 0 {
		t.Fatalf("default config must scale to zero, got %d", got)
	}
	t.Logf("default config scaled to zero as before")
}

// TestMinInstancesDrainedOnTrafficLoss 版本彻底失去流量进入排空时，
// 保底实例一并被收走，不泄漏。
func TestMinInstancesDrainedOnTrafficLoss(t *testing.T) {
	clock := newFakeClock()
	f := NewFunction("fn", t.Logf)
	f.now = clock.Now

	cfg := RevisionConfig{MinInstances: 2, IdleTimeout: time.Minute}
	v1, err := f.Publish(okHandler("v1"), cfg, map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	v2, err := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	if got := f.Stats()[v1].Instances; got != 2 {
		t.Fatalf("want 2 warm instances, got %d", got)
	}

	// 流量整体切到 v2：v1 进入排空，保底实例必须被收走。
	if err := f.UpdateTraffic(map[RevisionID]int{v2: 1}, f.Epoch()); err != nil {
		t.Fatalf("update traffic: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		st := f.Stats()[v1]
		if st.Instances == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("min instances leaked after traffic loss: %+v", st)
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("min instances drained after traffic loss: %+v", f.Stats()[v1])

	// 排空后 Tick 不会为无流量版本重新预热保底实例。
	clock.Advance(10 * time.Minute)
	f.Tick()
	if got := f.Stats()[v1].Instances; got != 0 {
		t.Fatalf("drained revision re-warmed: got %d instances", got)
	}
}

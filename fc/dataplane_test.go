package fc

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeClock 注入式时钟，使缩容判定可重复。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// TestWeightDistribution 权重切换整体生效，流量分布确定可重复。
func TestWeightDistribution(t *testing.T) {
	f := NewFunction("fn", t.Logf)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	v2, err := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// 原子切换为 1:3。
	if err := f.UpdateTraffic(map[RevisionID]int{v1: 1, v2: 3}, f.Epoch()); err != nil {
		t.Fatalf("update traffic: %v", err)
	}

	counts := map[RevisionID]int{}
	const total = 400
	for i := 0; i < total; i++ {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "req"})
		if err != nil {
			t.Fatalf("invoke %d: %v", i, err)
		}
		counts[resp.Revision]++
	}
	t.Logf("distribution over %d requests: v1=%d v2=%d", total, counts[v1], counts[v2])
	if counts[v1] != 100 || counts[v2] != 300 {
		t.Fatalf("deterministic 1:3 distribution broken: %v", counts)
	}
}

// TestScaleToZeroAndColdStart 无流量缩容到零，新请求到来时重新拉起。
func TestScaleToZeroAndColdStart(t *testing.T) {
	clock := newFakeClock()
	f := NewFunction("fn", t.Logf)
	f.now = clock.Now

	v1, err := f.Publish(okHandler("v1"), RevisionConfig{IdleTimeout: time.Minute}, map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	resp, err := f.Invoke(context.Background(), &Request{Payload: "first"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	firstInstance := resp.InstanceID
	if got := f.Stats()[v1].Instances; got != 1 {
		t.Fatalf("want 1 instance, got %d", got)
	}

	// 空闲不足：不回收。
	clock.Advance(30 * time.Second)
	f.Tick()
	if got := f.Stats()[v1].Instances; got != 1 {
		t.Fatalf("instance reclaimed too early, instances=%d", got)
	}

	// 空闲超时：缩容到零。
	clock.Advance(31 * time.Second)
	f.Tick()
	st := f.Stats()[v1]
	t.Logf("after idle timeout: %+v", st)
	if st.Instances != 0 {
		t.Fatalf("want scale-to-zero, got %+v", st)
	}

	// 新请求冷启动新实例。
	resp, err = f.Invoke(context.Background(), &Request{Payload: "second"})
	if err != nil {
		t.Fatalf("cold start invoke: %v", err)
	}
	t.Logf("cold start: %s -> %s (was %s)", resp.Revision, resp.InstanceID, firstInstance)
	if resp.InstanceID == firstInstance {
		t.Fatal("expected a fresh instance after scale-to-zero")
	}
	if got := f.Stats()[v1].Instances; got != 1 {
		t.Fatalf("want 1 instance after cold start, got %d", got)
	}
}

// TestBackpressure 超出并发上限的请求排队，队列满后以可区分原因拒绝。
func TestBackpressure(t *testing.T) {
	f := NewFunction("fn", t.Logf)
	release := make(chan struct{})
	handler := func(ctx context.Context, req *Request) (string, error) {
		<-release
		return "done", nil
	}
	cfg := RevisionConfig{MaxConcurrency: 1, MaxInstances: 1, QueueSize: 2}
	v1, err := f.Publish(handler, cfg, map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = f.Invoke(context.Background(), &Request{Payload: "r"})
		}(i)
	}

	// 等队列填满：1 在途 + 2 排队。
	deadline := time.Now().Add(2 * time.Second)
	for {
		st := f.Stats()[v1]
		if st.InFlight == 1 && st.Queued == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue did not fill: %+v", st)
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("saturated: %+v", f.Stats()[v1])

	// 第 4 个请求必须被 QueueFull 拒绝。
	_, err = f.Invoke(context.Background(), &Request{Payload: "overflow"})
	if !IsKind(err, ErrKindQueueFull) {
		t.Fatalf("want queue-full backpressure, got %v", err)
	}
	t.Logf("overflow rejected with: %v", err)

	close(release)
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Fatalf("queued request %d failed: %v", i, e)
		}
	}
	t.Logf("all queued requests completed after release")
}

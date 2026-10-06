package fc

import (
	"context"
	"testing"
	"time"
)

// TestMinInstancesWarmNoColdStart 配置 MinInstances 的版本发布即预热：
// 无任何真实流量时已有保底实例，首个请求直接落在既有实例上；空闲回收停在保底。
func TestMinInstancesWarmNoColdStart(t *testing.T) {
	clock := newFakeClock()
	f := newTestFunction(t)
	f.now = clock.Now

	v1, err := f.Publish(okHandler("v1"), RevisionConfig{
		IdleTimeout:  time.Minute,
		MinInstances: 2,
	}, map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	warm := f.Stats()[v1]
	if warm.Instances != 2 {
		t.Fatalf("want 2 pre-warmed instances before any traffic, got %+v", warm)
	}

	resp, err := f.Invoke(context.Background(), &Request{Payload: "warm"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	after := f.Stats()[v1]
	if after.Instances != 2 {
		t.Fatalf("first request must hit a pre-warmed instance, instances=%d", after.Instances)
	}
	t.Logf("first request served by pre-warmed %s, pool=%+v", resp.InstanceID, after)

	// 空闲再久也只回收到保底 2 个。
	clock.Advance(10 * time.Minute)
	f.Tick()
	if got := f.Stats()[v1].Instances; got != 2 {
		t.Fatalf("want floor 2 after long idle, got %d", got)
	}
	t.Logf("idle reclaim stopped at floor: %+v", f.Stats()[v1])
}

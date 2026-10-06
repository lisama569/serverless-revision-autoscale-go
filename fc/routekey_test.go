package fc

import (
	"context"
	"testing"
)

func invokeKey(t *testing.T, f *Function, key string) RevisionID {
	t.Helper()
	resp, err := f.Invoke(context.Background(), &Request{Payload: "p", RouteKey: key})
	if err != nil {
		t.Fatalf("invoke key=%q: %v", key, err)
	}
	return resp.Revision
}

// TestStickyRoutingStable 同一权重策略下，同一路由键始终落到同一版本；无键请求仍按比例分配。
func TestStickyRoutingStable(t *testing.T) {
	f := newTestFunction(t)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	v2, _ := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())
	if err := f.UpdateTraffic(map[RevisionID]int{v1: 1, v2: 3}, f.Epoch()); err != nil {
		t.Fatalf("traffic: %v", err)
	}

	keys := []string{"user-a", "user-b", "user-c", "user-d", "user-e", "user-f", "订单-1", "订单-2"}
	pinned := map[string]RevisionID{}
	for round := 0; round < 50; round++ {
		for _, k := range keys {
			got := invokeKey(t, f, k)
			if want, ok := pinned[k]; !ok {
				pinned[k] = got
			} else if got != want {
				t.Fatalf("key %q moved from %s to %s within the same policy", k, want, got)
			}
		}
	}
	t.Logf("sticky pinning over %d keys x 50 rounds: %v", len(keys), pinned)

	// 多个键之间要能覆盖到两个版本（1:3 下 8 个不同键实际覆盖）。
	seen := map[RevisionID]bool{}
	for _, id := range pinned {
		seen[id] = true
	}
	if !seen[v1] || !seen[v2] {
		t.Fatalf("keys did not cover both revisions: %v", pinned)
	}

	// 无键请求仍精确按 1:3 分布。
	counts := map[RevisionID]int{}
	for i := 0; i < 400; i++ {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "plain"})
		if err != nil {
			t.Fatalf("invoke: %v", err)
		}
		counts[resp.Revision]++
	}
	t.Logf("keyless distribution: v1=%d v2=%d", counts[v1], counts[v2])
	if counts[v1] != 100 || counts[v2] != 300 {
		t.Fatalf("keyless weighted distribution broken: %v", counts)
	}
}

// TestStickyRoutingReproducible 同一请求序列重复跑，粘性归属完全一致（不依赖随机数）。
func TestStickyRoutingReproducible(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"}
	run := func() []RevisionID {
		f := newTestFunction(t)
		v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
		v2, _ := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())
		if err := f.UpdateTraffic(map[RevisionID]int{v1: 2, v2: 5}, f.Epoch()); err != nil {
			t.Fatalf("traffic: %v", err)
		}
		out := make([]RevisionID, len(keys))
		for i, k := range keys {
			out[i] = invokeKey(t, f, k)
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("key %s pinned to %s vs %s across runs", keys[i], a[i], b[i])
		}
	}
	t.Logf("deterministic pinning: %v", a)
}

// TestStickyRoutingSwitchAtomic 权重切换后，新策略对之后的请求整体立即生效；
// 但切换前后各自内部，同键归属保持稳定（纯函数策略，无历史残留状态）。
func TestStickyRoutingSwitchAtomic(t *testing.T) {
	f := newTestFunction(t)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	v2, _ := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())

	// v1-only：所有键都在 v1。
	for _, k := range []string{"a", "b", "c"} {
		if got := invokeKey(t, f, k); got != v1 {
			t.Fatalf("before switch key %s -> %s, want v1", k, got)
		}
	}

	// 原子切到 v2-only：之后的请求（含同键）全部立即到 v2，没有半个策略的中间态。
	if err := f.UpdateTraffic(map[RevisionID]int{v2: 1}, f.Epoch()); err != nil {
		t.Fatalf("traffic: %v", err)
	}
	for round := 0; round < 10; round++ {
		for _, k := range []string{"a", "b", "c", "d"} {
			if got := invokeKey(t, f, k); got != v2 {
				t.Fatalf("after switch key %s -> %s, want v2", k, got)
			}
		}
	}
	t.Logf("all keys moved atomically to v2 after policy switch")
}

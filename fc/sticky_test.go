package fc

import (
	"context"
	"fmt"
	"testing"
)

// runStickyScenario 用固定键序列跑一次粘性路由场景，返回每次调用的归因序列。
func runStickyScenario(t *testing.T, keys []string) []RevisionID {
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
	if err := f.UpdateTraffic(map[RevisionID]int{v1: 1, v2: 1}, f.Epoch()); err != nil {
		t.Fatalf("update traffic: %v", err)
	}

	var seq []RevisionID
	for _, k := range keys {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "p", RoutingKey: k})
		if err != nil {
			t.Fatalf("invoke key %q: %v", k, err)
		}
		seq = append(seq, resp.Revision)
		t.Logf("key %q -> %s", k, resp.Revision)
	}
	return seq
}

// TestStickyRouting 同一路由键在流量策略不变时始终落到同一版本；
// 同一请求序列重复跑结果完全一致（不依赖随机数）。
func TestStickyRouting(t *testing.T) {
	// 交错重复的键序列：同一键多次出现，中间穿插其他键。
	keys := []string{
		"user-a", "user-b", "user-c", "user-a", "user-d", "user-b",
		"user-a", "user-c", "user-b", "user-d", "user-a", "user-c",
	}
	seq1 := runStickyScenario(t, keys)

	// 同一键的每次出现必须落到同一版本。
	affinity := map[string]RevisionID{}
	for i, k := range keys {
		if prev, ok := affinity[k]; ok && prev != seq1[i] {
			t.Fatalf("key %q flipped from %s to %s without traffic change", k, prev, seq1[i])
		}
		affinity[k] = seq1[i]
	}
	t.Logf("stable affinity: %v", affinity)

	// 可复现：同一序列重跑，归因完全一致。
	seq2 := runStickyScenario(t, keys)
	if fmt.Sprint(seq1) != fmt.Sprint(seq2) {
		t.Fatalf("sticky routing not reproducible:\n%v\n%v", seq1, seq2)
	}
}

// TestStickyAfterWeightChange 权重切换后新策略对之后的请求立即整体生效：
// 粘性映射按新权重表重新计算并再次保持稳定；不带键的请求仍按比例分配。
func TestStickyAfterWeightChange(t *testing.T) {
	f := NewFunction("fn", t.Logf)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	v2, _ := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())
	if err := f.UpdateTraffic(map[RevisionID]int{v1: 1, v2: 1}, f.Epoch()); err != nil {
		t.Fatalf("update traffic: %v", err)
	}

	invokeKey := func(k string) RevisionID {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "p", RoutingKey: k})
		if err != nil {
			t.Fatalf("invoke key %q: %v", k, err)
		}
		return resp.Revision
	}

	// 切换前的粘性映射。
	before := map[string]RevisionID{}
	keys := []string{"tenant-1", "tenant-2", "tenant-3", "tenant-4", "tenant-5", "tenant-6"}
	for _, k := range keys {
		before[k] = invokeKey(k)
	}

	// 整体切换为 1:3。
	if err := f.UpdateTraffic(map[RevisionID]int{v1: 1, v2: 3}, f.Epoch()); err != nil {
		t.Fatalf("update traffic: %v", err)
	}

	// 切换后：同一键在新策略下再次稳定（允许映射变化，但不允许漂移）。
	after := map[string]RevisionID{}
	for round := 0; round < 3; round++ {
		for _, k := range keys {
			got := invokeKey(k)
			if prev, ok := after[k]; ok && prev != got {
				t.Fatalf("key %q drifted after weight change: %s -> %s", k, prev, got)
			}
			after[k] = got
		}
	}
	t.Logf("affinity before=%v after=%v", before, after)

	// 不带键的请求仍按 1:3 比例分配（确定性轮转）。
	counts := map[RevisionID]int{}
	for i := 0; i < 400; i++ {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "no-key"})
		if err != nil {
			t.Fatalf("invoke: %v", err)
		}
		counts[resp.Revision]++
	}
	t.Logf("no-key distribution after 1:3: %v", counts)
	if counts[v1] != 100 || counts[v2] != 300 {
		t.Fatalf("no-key requests not weighted 1:3: %v", counts)
	}
}

// TestStickyDoesNotDisturbRoundRobin 带键请求不影响无键请求的确定性轮转分布。
func TestStickyDoesNotDisturbRoundRobin(t *testing.T) {
	f := NewFunction("fn", t.Logf)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	v2, _ := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())
	if err := f.UpdateTraffic(map[RevisionID]int{v1: 1, v2: 1}, f.Epoch()); err != nil {
		t.Fatalf("update traffic: %v", err)
	}

	// 交错发送带键与不带键请求。
	counts := map[RevisionID]int{}
	for i := 0; i < 100; i++ {
		if _, err := f.Invoke(context.Background(), &Request{Payload: "k", RoutingKey: fmt.Sprintf("key-%d", i%7)}); err != nil {
			t.Fatalf("keyed invoke: %v", err)
		}
		resp, err := f.Invoke(context.Background(), &Request{Payload: "no-key"})
		if err != nil {
			t.Fatalf("plain invoke: %v", err)
		}
		counts[resp.Revision]++
	}
	t.Logf("no-key distribution interleaved with keyed traffic: %v", counts)
	if counts[v1] != 50 || counts[v2] != 50 {
		t.Fatalf("keyed traffic disturbed round-robin: %v", counts)
	}
}

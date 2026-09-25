package controlplane

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/serverless-revision-autoscale-go/internal/autoscale"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/clock"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/fcerr"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/logging"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/revision"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/traffic"
)

const fn = "svc"

func testCP(t *testing.T, cfg autoscale.Config) (*ControlPlane, clock.Clock) {
	t.Helper()
	clk := clock.RealClock{}
	cp := New(Config{Scale: cfg}, clk, logging.NewTestLogger(t))
	t.Cleanup(func() { cp.Close() })
	return cp, clk
}

func baseCfg() autoscale.Config {
	return autoscale.Config{
		MaxScale:      4,
		QueueCapacity: 8,
		IdleTimeout:   80 * time.Millisecond,
		DrainTimeout:  time.Second,
		InvokeTimeout: 5 * time.Second,
		IdleTick:      20 * time.Millisecond,
	}
}

func echoSpec(tag string) revision.Spec {
	return revision.Spec{
		MaxConcurrency: 4,
		Handler: func(ctx context.Context, in []byte) ([]byte, error) {
			return []byte(tag), nil
		},
	}
}

// TestPublishImmutableAndAddressable：版本不可变、可寻址，仅追加。
func TestPublishImmutableAndAddressable(t *testing.T) {
	cp, _ := testCP(t, baseCfg())
	r1, err := cp.Publish(fn, echoSpec("v1"))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := cp.Publish(fn, echoSpec("v2"))
	if err != nil {
		t.Fatal(err)
	}
	if r1.ID == r2.ID || r2.Seq != r1.Seq+1 {
		t.Fatalf("revisions must be distinct & sequential: %+v %+v", r1, r2)
	}
	got, err := cp.GetRevision(fn, r1.ID)
	if err != nil || got.ID != r1.ID {
		t.Fatalf("revision must be addressable: %v", err)
	}
	list, err := cp.ListRevisions(fn)
	if err != nil || len(list) != 2 {
		t.Fatalf("list: %v %d", err, len(list))
	}
	// 未发布前函数查询返回 not found（这里函数已存在，但访问幽灵版本）。
	if _, err := cp.GetRevision(fn, "rev-09999"); !errors.Is(err, fcerr.ErrRevisionNotFound) {
		t.Fatalf("ghost revision: %v", err)
	}
}

// TestTrafficValidationAndCAS：非法权重、幽灵版本、过期 generation 均被整体拒绝，流量不变。
func TestTrafficValidationAndCAS(t *testing.T) {
	cp, _ := testCP(t, baseCfg())
	r1, _, err := cp.Release(fn, echoSpec("v1"), traffic.GenAny)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := cp.Publish(fn, echoSpec("v2"))
	if err != nil {
		t.Fatal(err)
	}

	// 合计不为 100。
	if _, err := cp.SetTraffic(fn, traffic.GenAny, map[string]int{r1.ID: 90}); !errors.Is(err, fcerr.ErrInvalidWeights) {
		t.Fatalf("invalid weights: %v", err)
	}
	// 幽灵版本。
	if _, err := cp.SetTraffic(fn, traffic.GenAny, map[string]int{r1.ID: 50, "rev-09999": 50}); !errors.Is(err, fcerr.ErrRevisionNotFound) {
		t.Fatalf("ghost: %v", err)
	}
	split0, _ := cp.Traffic(fn) // 仍 100% r1, gen=1

	// 正确设置 50/50。
	split, err := cp.SetTraffic(fn, traffic.GenAny, map[string]int{r1.ID: 50, r2.ID: 50})
	if err != nil {
		t.Fatal(err)
	}
	if split.Gen != split0.Gen+1 {
		t.Fatalf("gen should advance: %d -> %d", split0.Gen, split.Gen)
	}
	// 过期 generation 冲突。
	if _, err := cp.SetTraffic(fn, split0.Gen, map[string]int{r2.ID: 100}); !errors.Is(err, fcerr.ErrConflict) {
		t.Fatalf("stale gen must conflict: %v", err)
	}
	// 冲突后流量保持 50/50，无半生效。
	cur, _ := cp.Traffic(fn)
	if cur.Weights[r1.ID] != 50 || cur.Weights[r2.ID] != 50 {
		t.Fatalf("traffic changed after conflict: %v", cur.Weights)
	}
}

// TestConcurrentReleasesOnlyOneWins：并发 Release 竞争同一 generation，恰一个成功；
// 最终所有发布的版本都存在且不可变，流量指向唯一胜出者。
func TestConcurrentReleasesOnlyOneWins(t *testing.T) {
	cp, _ := testCP(t, baseCfg())
	r0, _, err := cp.Release(fn, echoSpec("v0"), traffic.GenAny)
	if err != nil {
		t.Fatal(err)
	}
	baseGen, _ := cp.Traffic(fn)

	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners, conflicts, publishes := 0, 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			spec := echoSpec(fmt.Sprintf("c%d", i))
			rev, _, rErr := cp.Release(fn, spec, baseGen.Gen)
			mu.Lock()
			defer mu.Unlock()
			if rErr == nil {
				winners++
				_ = rev
			} else if errors.Is(rErr, fcerr.ErrConflict) {
				conflicts++
			}
			if rev != nil {
				publishes++
			}
		}(i)
	}
	wg.Wait()
	if winners != 1 || conflicts != n-1 {
		t.Fatalf("want 1 winner %d conflicts, got winners=%d conflicts=%d", n-1, winners, conflicts)
	}
	// 即便切流冲突，版本仍被追加（发布与切流是两步；冲突只影响流量）。
	revs, _ := cp.ListRevisions(fn)
	if len(revs) != 1+n {
		t.Fatalf("all publishes should append revisions: got %d want %d", len(revs), 1+n)
	}
	split, _ := cp.Traffic(fn)
	if split.Gen != baseGen.Gen+1 {
		t.Fatalf("gen must advance exactly once: %d -> %d", baseGen.Gen, split.Gen)
	}
	if len(split.Weights) != 1 {
		t.Fatalf("winner split must be single target, got %v", split.Weights)
	}
	if w := split.Weights[r0.ID]; w == 100 {
		t.Fatalf("traffic must point at a concurrent winner, not baseline %s", r0.ID)
	}
}

// TestWeightedSwitchDistributionStable：确定性随机序列下，权重分布精确可重复。
func TestWeightedSwitchDistributionStable(t *testing.T) {
	cp, _ := testCP(t, baseCfg())
	r1, _, err := cp.Release(fn, echoSpec("v1"), traffic.GenAny)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := cp.Publish(fn, echoSpec("v2"))
	if err != nil {
		t.Fatal(err)
	}
	r3, err := cp.Publish(fn, echoSpec("v3"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cp.SetTraffic(fn, traffic.GenAny,
		map[string]int{r1.ID: 80, r2.ID: 15, r3.ID: 5}); err != nil {
		t.Fatal(err)
	}
	i := 0
	det := func(n int) int { x := i % n; i++; return x }
	counts := map[string]int{}
	ctx := context.Background()
	for k := 0; k < 100; k++ {
		r, err := cp.Invoke(ctx, fn, nil, det)
		if err != nil {
			t.Fatalf("invoke %d: %v", k, err)
		}
		counts[r.RevisionID]++
	}
	want := map[string]int{r1.ID: 80, r2.ID: 15, r3.ID: 5}
	if fmt.Sprint(counts) != fmt.Sprint(want) {
		t.Fatalf("distribution=%v want=%v", counts, want)
	}
	t.Logf("稳定流量分布: %v", counts)
}

// TestRollbackConflictAndTarget：回滚到历史版本；幽灵版本拒绝；与并发切流冲突时拒绝。
func TestRollbackConflictAndTarget(t *testing.T) {
	cp, _ := testCP(t, baseCfg())
	r1, _, err := cp.Release(fn, echoSpec("v1"), traffic.GenAny)
	if err != nil {
		t.Fatal(err)
	}
	r2, _, err := cp.Release(fn, echoSpec("v2"), traffic.GenAny)
	if err != nil {
		t.Fatal(err)
	}
	gen2, _ := cp.Traffic(fn)

	// 回滚到幽灵版本。
	if _, err := cp.Rollback(fn, traffic.GenAny, "rev-09999"); !errors.Is(err, fcerr.ErrRevisionNotFound) {
		t.Fatalf("rollback ghost: %v", err)
	}
	// 并发冲突：gen2 已过期（当前就是 gen2，但我们用 gen2-1 模拟过期）。
	if _, err := cp.Rollback(fn, gen2.Gen-1, r1.ID); !errors.Is(err, fcerr.ErrConflict) {
		t.Fatalf("rollback stale gen: %v", err)
	}
	// 正确回滚到 r1。
	split, err := cp.Rollback(fn, traffic.GenAny, r1.ID)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if len(split.Weights) != 1 || split.Weights[r1.ID] != 100 {
		t.Fatalf("rollback split: %v", split.Weights)
	}
	r, err := cp.Invoke(context.Background(), fn, nil, nil)
	if err != nil || r.RevisionID != r1.ID || string(r.Output) != "v1" {
		t.Fatalf("post-rollback invoke: %+v err=%v", r, err)
	}
	_ = r2
}

// TestScaleToZeroAndRecoldAcrossSwitch：调用后空闲缩零，切换权重后再请求自动冷启动。
func TestScaleToZeroAndRecoldAcrossSwitch(t *testing.T) {
	cfg := baseCfg()
	cfg.IdleTimeout = 60 * time.Millisecond
	cfg.IdleTick = 15 * time.Millisecond
	cp, _ := testCP(t, cfg)
	r1, _, err := cp.Release(fn, echoSpec("v1"), traffic.GenAny)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := cp.Publish(fn, echoSpec("v2"))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := cp.Invoke(context.Background(), fn, nil, nil); err != nil {
		t.Fatal(err)
	}
	// 等待空闲缩零。
	waitUntil(t, cp, 3*time.Second, func() bool {
		st, _ := cp.RevisionStats(fn, r1.ID)
		return st.ScaledToZero
	}, "r1 scaled to zero")

	// 切到 r2 并调用：r2 从未有实例，应冷启动并带 cold 归因。
	if _, err := cp.SetTraffic(fn, traffic.GenAny, map[string]int{r2.ID: 100}); err != nil {
		t.Fatal(err)
	}
	r, err := cp.Invoke(context.Background(), fn, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.RevisionID != r2.ID || !r.Cold {
		t.Fatalf("expect cold call on r2, got %+v", r)
	}
	t.Logf("缩零后切流到 %s 冷启动实例 %s", r2.ID, r.InstanceID)
}

// TestDrainDoesNotAffectOtherRevisions：回收某版本实例不影响另一版本承载流量。
func TestDrainDoesNotAffectOtherRevisions(t *testing.T) {
	cp, _ := testCP(t, baseCfg())
	r1, _, err := cp.Release(fn, echoSpec("v1"), traffic.GenAny)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := cp.Publish(fn, echoSpec("v2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cp.SetTraffic(fn, traffic.GenAny,
		map[string]int{r1.ID: 50, r2.ID: 50}); err != nil {
		t.Fatal(err)
	}
	// 各打一次以确保两个版本都有热实例。
	hitR2 := false
	for k := 0; k < 40 && !hitR2; k++ {
		r, err := cp.Invoke(context.Background(), fn, nil, func(n int) int {
			if k%2 == 0 {
				return 0 // r1（id 排序靠前）
			}
			return n - 1 // r2
		})
		if err != nil {
			t.Fatal(err)
		}
		if r.RevisionID == r2.ID {
			hitR2 = true
		}
	}
	if !hitR2 {
		t.Fatal("did not warm r2")
	}
	// 强制排空回收 r1 的全部实例。
	if errs := cp.RetireAllInstances(fn, r1.ID); len(errs) != 0 {
		t.Fatalf("retire r1: %v", errs)
	}
	st1, _ := cp.RevisionStats(fn, r1.ID)
	if !st1.ScaledToZero {
		t.Fatalf("r1 should be zero: %+v", st1)
	}
	// r2 仍有实例且能继续服务（确定性地把全部流量导向 r2）。
	if _, err := cp.SetTraffic(fn, traffic.GenAny, map[string]int{r2.ID: 100}); err != nil {
		t.Fatal(err)
	}
	for k := 0; k < 5; k++ {
		r, err := cp.Invoke(context.Background(), fn, nil, nil)
		if err != nil || r.RevisionID != r2.ID || string(r.Output) != "v2" {
			t.Fatalf("r2 must keep serving: r=%+v err=%v", r, err)
		}
	}
	t.Logf("回收 %s 后，%s 仍稳定服务", r1.ID, r2.ID)
}

// TestInvokeNoTrafficRejected：无流量配置时调用必须明确拒绝。
func TestInvokeNoTrafficRejected(t *testing.T) {
	cp, _ := testCP(t, baseCfg())
	if _, err := cp.Publish(fn, echoSpec("v1")); err != nil {
		t.Fatal(err)
	}
	_, err := cp.Invoke(context.Background(), fn, nil, nil)
	if !errors.Is(err, fcerr.ErrInvalidWeights) {
		t.Fatalf("invoke with no traffic split must be invalid_weights, got %v", err)
	}
}

func waitUntil(t *testing.T, cp *ControlPlane, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		cp.OnTick()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout: %s", msg)
}

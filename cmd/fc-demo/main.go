// Command fc-demo 在本地演示版本发布、流量切换、背压、优雅排空与缩容到零，
// 不依赖任何外部服务或凭据。
//
// 运行：go run ./cmd/fc-demo
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/lisama569/serverless-revision-autoscale-go/internal/autoscale"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/clock"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/controlplane"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/fcerr"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/logging"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/revision"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/traffic"
)

const fn = "hello"

func main() {
	log := logging.NewTextLogger(os.Stdout)
	clk := clock.RealClock{}
	cp := controlplane.New(controlplane.Config{Scale: autoscale.Config{
		MaxScale:      2,
		QueueCapacity: 2,
		IdleTimeout:   400 * time.Millisecond,
		DrainTimeout:  25 * time.Millisecond,
		InvokeTimeout: 2 * time.Second,
		IdleTick:      50 * time.Millisecond,
	}}, clk, log)
	defer cp.Close()

	ctx := context.Background()

	// 1) 首次发布 rev-00001，自动承载 100% 流量。
	rev1, _, err := cp.Release(fn, handlerSpec("v1", 0), traffic.GenAny)
	must(err)

	// 2) 发布 rev-00002 但不切流（仅追加不可变版本）。
	rev2, err := cp.Publish(fn, handlerSpec("v2", 0))
	must(err)

	// 3) 非法权重 / 不存在版本必须被整体拒绝。
	_, err = cp.SetTraffic(fn, traffic.GenAny, map[string]int{rev1.ID: 90})
	fmt.Printf(">> 拒绝非法权重(合计90): reason=%s err=%v\n", fcerr.ReasonOf(err), err)
	_, err = cp.SetTraffic(fn, traffic.GenAny, map[string]int{rev1.ID: 50, "rev-09999": 50})
	fmt.Printf(">> 拒绝幽灵版本: reason=%s err=%v\n", fcerr.ReasonOf(err), err)

	// 4) 蓝绿：90/10 权重整体生效。
	split, err := cp.SetTraffic(fn, traffic.GenAny, map[string]int{rev1.ID: 90, rev2.ID: 10})
	must(err)

	// 5) 用确定性随机验证权重分布可重复：固定 [0,100) 序列统计落点。
	counts := map[string]int{}
	// rnd 返回固定 0..99，等价于全空间均匀扫描。
	i := 0
	rnd := func(n int) int { x := i % n; i++; return x }
	for k := 0; k < 100; k++ {
		r, err := cp.Invoke(ctx, fn, []byte("ping"), rnd)
		must(err)
		counts[r.RevisionID]++
	}
	fmt.Printf(">> 100 次调用分布(gen=%d): %v (期望 90/10)\n", split.Gen, counts)

	// 6) 100% 切到 rev-00002；旧 generation CAS 必须冲突拒绝。
	staleGen := split.Gen - 1
	_, err = cp.SetTraffic(fn, staleGen, map[string]int{rev2.ID: 100}) // gen 过期
	fmt.Printf(">> 过期 generation(%d, 当前%d) 被拒绝: reason=%s err=%v\n",
		staleGen, split.Gen, fcerr.ReasonOf(err), err)
	split2, err := cp.SetTraffic(fn, split.Gen, map[string]int{rev2.ID: 100})
	must(err)
	r, err := cp.Invoke(ctx, fn, nil, nil)
	must(err)
	fmt.Printf(">> 全切后调用归因: revision=%s instance=%s cold=%v gen=%d\n",
		r.RevisionID, r.InstanceID, r.Cold, split2.Gen)

	// 7) 回滚到 rev-00001。
	split3, err := cp.Rollback(fn, traffic.GenAny, rev1.ID)
	must(err)
	r, err = cp.Invoke(ctx, fn, nil, nil)
	must(err)
	fmt.Printf(">> 回滚后调用归因: revision=%s (期望 %s) gen=%d\n",
		r.RevisionID, rev1.ID, split3.Gen)

	// 8) 背压：MaxConcurrency=1、MaxScale=2、QueueCapacity=2 =>
	//    实例槽位 2 + 排队 2 = 容量 4；第 5、6 个请求必须立即得到 ErrQueueFull。
	//    用 gate 阻塞 Handler，使槽位被稳定占满，保证结果可重复。
	gate := make(chan struct{})
	revSlow, _, err := cp.Release(fn, blockedHandlerSpec("slow", gate), traffic.GenAny)
	must(err)

	var wg sync.WaitGroup
	var qfMu sync.Mutex
	queueFull, canceled, okN, other := 0, 0, 0, 0
	for k := 0; k < 6; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, err := cp.Invoke(cctx, fn, nil, nil)
			qfMu.Lock()
			defer qfMu.Unlock()
			switch {
			case errors.Is(err, fcerr.ErrQueueFull):
				queueFull++
			case errors.Is(err, fcerr.ErrUpstreamCanceled):
				canceled++
			case err != nil:
				other++
			default:
				okN++
			}
		}()
	}
	// 等待 2 个槽位 + 2 个队列被占满。
	deadline := time.After(2 * time.Second)
	for {
		st, _ := cp.RevisionStats(fn, revSlow.ID)
		if st.Total >= 2 && st.Inflight+st.Pending >= 4 {
			break
		}
		select {
		case <-deadline:
			fmt.Printf(">> 等待容量占满超时: %+v\n", st)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	fmt.Printf(">> 容量占满: 实例=%d 在途=%d 排队=%d, 队列满拒绝=%d (期望2)\n",
		mustStats(cp, fn, revSlow.ID).Total, mustStats(cp, fn, revSlow.ID).Inflight,
		mustStats(cp, fn, revSlow.ID).Pending, queueFull)
	close(gate) // 放行 2 执行 + 2 排队
	wg.Wait()
	fmt.Printf(">> 背压场景: 成功=%d 队列满拒绝=%d 取消=%d 其他=%d (总6)\n",
		okN, queueFull, canceled, other)

	// 9) 优雅排空：占用一个实例做在途长请求，再回收该版本实例 -> drain 超时，
	//    在途请求拿到明确 ErrDrainTimeout。
	gate2 := make(chan struct{})
	revDrain, _, err := cp.Release(fn, blockedHandlerSpec("drain", gate2), traffic.GenAny)
	must(err)
	longDone := make(chan error, 1)
	go func() {
		_, e := cp.Invoke(ctx, fn, nil, nil)
		longDone <- e
	}()
	waitUntil(func() bool {
		st, _ := cp.RevisionStats(fn, revDrain.ID)
		return st.Inflight >= 1
	})
	st, _ := cp.RevisionStats(fn, revDrain.ID)
	fmt.Printf(">> 排空版本状态: 实例=%d 在途=%d (缩零=%v)\n", st.Total, st.Inflight, st.ScaledToZero)
	drainErrs := cp.RetireAllInstances(fn, revDrain.ID)
	select {
	case e := <-longDone:
		fmt.Printf(">> 在途请求结果: reason=%s err=%v (期望 drain_timeout); retire返回错误数=%d\n",
			fcerr.ReasonOf(e), e, len(drainErrs))
	case <-time.After(2 * time.Second):
		fmt.Println(">> 在途请求未按预期结束")
	}
	close(gate2) // 兜底放行（此时实例已强制回收）
	// 其他版本流量不受影响：回滚到 rev1 仍可正常调用。
	_, err = cp.Rollback(fn, traffic.GenAny, rev1.ID)
	must(err)
	r2, err := cp.Invoke(ctx, fn, nil, nil)
	must(err)
	fmt.Printf(">> 排空不影响其他版本: 调用归因 revision=%s (期望 %s)\n", r2.RevisionID, rev1.ID)

	// 10) 缩容到零：空闲超过 IdleTimeout 后 tick 回收全部实例。
	//     当前 rev1 有实例（第7步用过），等待并驱动若干 tick。
	for k := 0; k < 12; k++ {
		time.Sleep(50 * time.Millisecond)
		cp.OnTick()
	}
	stRev1, _ := cp.RevisionStats(fn, rev1.ID)
	stRev2, _ := cp.RevisionStats(fn, rev2.ID)
	fmt.Printf(">> 空闲后状态: rev1 缩零=%v rev2 缩零=%v\n",
		stRev1.ScaledToZero, stRev2.ScaledToZero)

	// 11) 缩零后新请求自动重新拉起（冷启动）。
	r, err = cp.Invoke(ctx, fn, nil, nil)
	must(err)
	fmt.Printf(">> 缩零后重拉调用归因: revision=%s instance=%s cold=%v\n",
		r.RevisionID, r.InstanceID, r.Cold)

	fmt.Println(">> demo 完成")
}

func handlerSpec(tag string, delay time.Duration) revision.Spec {
	return revision.Spec{
		MaxConcurrency: 1,
		Handler: func(ctx context.Context, in []byte) ([]byte, error) {
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return []byte("resp-from-" + tag), nil
		},
	}
}

func blockedHandlerSpec(tag string, gate <-chan struct{}) revision.Spec {
	return revision.Spec{
		MaxConcurrency: 1,
		Handler: func(ctx context.Context, in []byte) ([]byte, error) {
			select {
			case <-gate:
				return []byte("resp-from-" + tag), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
}

func mustStats(cp *controlplane.ControlPlane, fn, revID string) autoscale.Stats {
	st, err := cp.RevisionStats(fn, revID)
	if err != nil {
		panic(err)
	}
	return st
}

func waitUntil(cond func() bool) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

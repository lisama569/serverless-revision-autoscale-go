package instance

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/serverless-revision-autoscale-go/internal/clock"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/fcerr"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/logging"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/revision"
)

func mkRev(t *testing.T, cold time.Duration, mc int, h revision.Handler) *revision.Revision {
	t.Helper()
	if h == nil {
		h = func(ctx context.Context, in []byte) ([]byte, error) { return in, nil }
	}
	st := revision.NewStore("fn")
	rev, err := st.Publish(revision.Spec{Handler: h, ColdStart: cold, MaxConcurrency: mc}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	return rev
}

func newFakeInstance(t *testing.T, rev *revision.Revision, invokeTimeout time.Duration) (*Instance, *clock.FakeClock) {
	t.Helper()
	clk := clock.NewFakeClock(time.Unix(0, 0))
	in := New(Config{FunctionName: "fn", Rev: rev, ID: "i1", InvokeTimeout: invokeTimeout,
		Clk: clk, Log: logging.NewTestLogger(t)})
	t.Cleanup(func() { in.ForceStop() })
	return in, clk
}

func waitTimer(t *testing.T, clk *clock.FakeClock, n int) {
	t.Helper()
	if !clk.WaitUntilScheduled(n, 2*time.Second) {
		t.Fatalf("timeout waiting for %d fake timers", n)
	}
}

func TestColdStartAndInvoke(t *testing.T) {
	rev := mkRev(t, 10*time.Millisecond, 1, nil)
	in, clk := newFakeInstance(t, rev, time.Second)

	// 冷启动未完成时不可服务。
	if in.State() != StateStarting || in.FreeSlots() != 0 {
		t.Fatalf("want starting with 0 free slots, got state=%s free=%d", in.State(), in.FreeSlots())
	}
	waitTimer(t, clk, 1) // coldStart=10ms，存在冷启动定时器
	clk.Advance(10 * time.Millisecond)
	if err := in.ColdStart(context.Background()); err != nil {
		t.Fatalf("cold start: %v", err)
	}
	if in.State() != StateActive {
		t.Fatalf("want active got=%s", in.State())
	}
	res, err := in.Invoke(context.Background(), []byte("hi"))
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if string(res.Output) != "hi" || !res.Cold || res.Instance != "i1" {
		t.Fatalf("unexpected result: %+v", res)
	}
	// 第二次调用不再是冷启动。
	res2, err := in.Invoke(context.Background(), nil)
	if err != nil || res2.Cold {
		t.Fatalf("second invoke: res=%+v err=%v", res2, err)
	}
}

func TestColdStartTimeoutVsCancel(t *testing.T) {
	rev := mkRev(t, time.Hour, 1, nil)
	in, _ := newFakeInstance(t, rev, time.Second)

	// 上游 deadline 在冷启动期间到期。
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_ = cancel
	err := in.ColdStart(ctx)
	if !errors.Is(err, fcerr.ErrColdStartTimeout) {
		t.Fatalf("want cold_start_timeout got=%v", err)
	}

	// 另一个实例：上游主动取消，必须可区分。
	in2 := New(Config{FunctionName: "fn", Rev: rev, ID: "i2",
		Clk: clock.NewFakeClock(time.Unix(0, 0)), Log: logging.NewTestLogger(t)})
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	if err := in2.ColdStart(cctx); !errors.Is(err, fcerr.ErrUpstreamCanceled) {
		t.Fatalf("want upstream_canceled got=%v", err)
	}
}

func TestInvokeTimeoutDistinctFromCancel(t *testing.T) {
	gate := make(chan struct{})
	gate2 := make(chan struct{})
	h := func(ctx context.Context, in []byte) ([]byte, error) {
		<-gate // 由 t.Cleanup 释放前一直阻塞
		return nil, nil
	}
	rev := mkRev(t, 0, 1, h)
	in, clk := newFakeInstance(t, rev, 50*time.Millisecond)
	clk.Advance(time.Millisecond)
	if err := in.ColdStart(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 实例侧调用超时（假时钟）。
	errCh := make(chan error, 1)
	go func() { _, err := in.Invoke(context.Background(), nil); errCh <- err }()
	waitInflight(t, in, 1)
	waitTimer(t, clk, 1)
	clk.Advance(50 * time.Millisecond)
	if err := <-errCh; !errors.Is(err, fcerr.ErrInvokeTimeout) {
		t.Fatalf("want invoke_timeout got=%v", err)
	}

	close(gate)

	// 上游主动取消必须是 upstream_canceled（使用并发=2 的独立实例）。
	rev2 := mkRev(t, 0, 2, func(ctx context.Context, in []byte) ([]byte, error) {
		<-gate2
		return nil, nil
	})
	in2, clk2 := newFakeInstance(t, rev2, 50*time.Millisecond)
	clk2.Advance(time.Millisecond)
	if err := in2.ColdStart(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctxCancel, doCancel := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { _, err := in2.Invoke(ctxCancel, nil); done2 <- err }()
	waitInflight(t, in2, 1)
	doCancel()
	if err := <-done2; !errors.Is(err, fcerr.ErrUpstreamCanceled) {
		t.Fatalf("want upstream_canceled got=%v", err)
	}
	close(gate2)
}

func TestHandlerFailureIsInvocationFailed(t *testing.T) {
	h := func(ctx context.Context, in []byte) ([]byte, error) {
		return nil, errors.New("boom")
	}
	rev := mkRev(t, 0, 1, h)
	in, clk := newFakeInstance(t, rev, time.Second)
	clk.Advance(time.Millisecond)
	if err := in.ColdStart(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := in.Invoke(context.Background(), nil)
	if !errors.Is(err, fcerr.ErrInvocationFailed) {
		t.Fatalf("want invocation_failed got=%v", err)
	}
}

func TestDrainRejectsNewButCompletesInflight(t *testing.T) {
	proceed := make(chan struct{})
	finished := make(chan struct{})
	started := make(chan struct{})
	h2 := func(ctx context.Context, in []byte) ([]byte, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-proceed
		close(finished)
		return []byte("ok"), nil
	}
	rev := mkRev(t, 0, 2, h2)
	in, clk := newFakeInstance(t, rev, time.Second)
	clk.Advance(time.Millisecond)
	if err := in.ColdStart(context.Background()); err != nil {
		t.Fatal(err)
	}

	resCh := make(chan error, 1)
	go func() {
		_, err := in.Invoke(context.Background(), nil)
		resCh <- err
	}()
	<-started
	waitInflight(t, in, 1)

	// 进入排空：新流量被明确拒绝（drain_timeout 原因表示实例正在退出）。
	in.BeginDrain()
	if in.State() != StateDraining {
		t.Fatalf("want draining got=%s", in.State())
	}
	if _, err := in.Invoke(context.Background(), nil); !errors.Is(err, fcerr.ErrDrainTimeout) {
		t.Fatalf("new traffic must be rejected during drain, got=%v", err)
	}

	// 排空在有在途请求时超时（假时钟需要显式推进）。
	drainErr := make(chan error, 1)
	go func() { drainErr <- in.Drain(context.Background(), time.Millisecond) }()
	// 此时已存在在途请求的 1s invoke 定时器；等待 drain 的 1ms 定时器也完成注册（共 2 个）。
	waitTimer(t, clk, 2)
	clk.Advance(time.Millisecond)
	if err := <-drainErr; !errors.Is(err, fcerr.ErrDrainTimeout) {
		t.Fatalf("want drain_timeout got=%v", err)
	}
	if in.State() != StateDraining {
		t.Fatalf("instance must remain draining after timeout, got=%s", in.State())
	}

	// 放行在途请求后排空成功。
	close(proceed)
	if err := <-resCh; err != nil {
		t.Fatalf("in-flight invoke should succeed: %v", err)
	}
	<-finished
	if err := in.Drain(context.Background(), time.Second); err != nil {
		t.Fatalf("drain after completion: %v", err)
	}
	if in.State() != StateStopped {
		t.Fatalf("want stopped got=%s", in.State())
	}
}

func TestForceStopFailsInflight(t *testing.T) {
	gate := make(chan struct{})
	h := func(ctx context.Context, in []byte) ([]byte, error) {
		<-gate
		return nil, nil
	}
	rev := mkRev(t, 0, 1, h)
	in, clk := newFakeInstance(t, rev, time.Second)
	clk.Advance(time.Millisecond)
	if err := in.ColdStart(context.Background()); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { _, err := in.Invoke(context.Background(), nil); errCh <- err }()
	waitInflight(t, in, 1)
	in.ForceStop()
	if err := <-errCh; !errors.Is(err, fcerr.ErrDrainTimeout) {
		t.Fatalf("force stop must fail inflight with drain_timeout, got=%v", err)
	}
	close(gate)
}

func TestConcurrencySlots(t *testing.T) {
	gate := make(chan struct{})
	h := func(ctx context.Context, in []byte) ([]byte, error) {
		<-gate
		return nil, nil
	}
	rev := mkRev(t, 0, 3, h)
	in, clk := newFakeInstance(t, rev, time.Second)
	clk.Advance(time.Millisecond)
	if err := in.ColdStart(context.Background()); err != nil {
		t.Fatal(err)
	}
	if in.FreeSlots() != 3 {
		t.Fatalf("want 3 free slots got=%d", in.FreeSlots())
	}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = in.Invoke(context.Background(), nil) }()
	}
	waitInflight(t, in, 3)
	if in.FreeSlots() != 0 {
		t.Fatalf("want 0 free slots got=%d", in.FreeSlots())
	}
	// 槽位满：返回 queue_full 背压，不阻塞、不静默丢弃。
	_, err := in.Invoke(context.Background(), nil)
	if !errors.Is(err, fcerr.ErrQueueFull) {
		t.Fatalf("want queue_full got=%v", err)
	}
	close(gate)
	wg.Wait()
}

func waitInflight(t *testing.T, in *Instance, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if in.Inflight() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting inflight=%d (got=%d)", want, in.Inflight())
}

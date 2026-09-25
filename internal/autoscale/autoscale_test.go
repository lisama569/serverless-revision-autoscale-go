package autoscale

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

func testRev(t *testing.T, cold time.Duration, mc int, h revision.Handler) *revision.Revision {
	t.Helper()
	st := revision.NewStore("fn")
	if h == nil {
		h = func(ctx context.Context, in []byte) ([]byte, error) { return in, nil }
	}
	rev, err := st.Publish(revision.Spec{Handler: h, ColdStart: cold, MaxConcurrency: mc}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	return rev
}

func testCfg() Config {
	return Config{
		MaxScale:      2,
		QueueCapacity: 2,
		IdleTimeout:   30 * time.Second,
		DrainTimeout:  5 * time.Second,
		InvokeTimeout: time.Hour, // 默认不触发，按需覆盖
		IdleTick:      time.Second,
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

// TestScaleFromZeroAndCold：初始缩零 -> 首个请求触发冷启动并带 cold 归因 -> 复用热实例。
func TestScaleFromZeroAndCold(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	cfg := testCfg()
	rev := testRev(t, 10*time.Millisecond, 1, nil)
	s := New(rev, cfg, clk, logging.NewTestLogger(t))
	t.Cleanup(func() { s.Close() })

	if st := s.Stats(); !st.ScaledToZero || st.Total != 0 {
		t.Fatalf("initial state should be scaled to zero: %+v", st)
	}

	// 推进时钟让 10ms 冷启动完成（定时器由 New 内部 goroutine 注册）。
	done := make(chan struct{})
	go func() {
		clk.WaitUntilScheduled(1, time.Second)
		clk.Advance(10 * time.Millisecond)
		close(done)
	}()
	res, err := s.Invoke(context.Background(), []byte("a"))
	<-done
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if !res.Cold || string(res.Output) != "a" {
		t.Fatalf("unexpected first result: %+v", res)
	}
	st := s.Stats()
	if st.Total != 1 || st.ScaledToZero {
		t.Fatalf("expect 1 warm instance, got %+v", st)
	}
	// 第二次调用复用热实例，cold=false。
	res2, err := s.Invoke(context.Background(), []byte("b"))
	if err != nil || res2.Cold || res2.Instance != res.Instance {
		t.Fatalf("expect reuse same warm instance: r=%+v err=%v", res2, err)
	}
}

// TestQueueFullBackpressure：执行容量 2（MaxScale2*mc1）+ 队列 2 = 4；
// 第 5、6 个请求必须立即得到 ErrQueueFull（可区分、不阻塞、不静默丢弃）。
func TestQueueFullBackpressure(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	cfg := testCfg()
	gate := make(chan struct{})
	rev := testRev(t, 0, 1, func(ctx context.Context, in []byte) ([]byte, error) {
		<-gate
		return []byte("ok"), nil
	})
	s := New(rev, cfg, clk, logging.NewTestLogger(t))
	var gateOnce sync.Once
	releaseGate := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(func() {
		releaseGate()
		s.Close()
	})

	var wg sync.WaitGroup
	var mu sync.Mutex
	okN, qf, other := 0, 0, 0
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Invoke(context.Background(), nil)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				okN++
			case errors.Is(err, fcerr.ErrQueueFull):
				qf++
			default:
				other++
			}
		}()
	}
	// 等待两个实例拉起且执行槽占满（2 个在途），其余 2 个在队列、2 个被拒。
	waitFor(t, 2*time.Second, func() bool {
		st := s.Stats()
		return st.Total == 2 && st.Inflight == 2
	}, "two instances busy")
	waitFor(t, 2*time.Second, func() bool { return s.Stats().Rejected >= 2 }, "two rejections")
	releaseGate()
	wg.Wait()
	if okN != 4 || qf != 2 || other != 0 {
		t.Fatalf("want ok=4 queueFull=2 other=0, got ok=%d qf=%d other=%d", okN, qf, other)
	}
}

// TestScaleToZeroOnIdle：空闲超过 IdleTimeout 后 OnTick 回收实例直到缩零。
func TestScaleToZeroOnIdle(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	cfg := testCfg()
	rev := testRev(t, 0, 1, nil)
	s := New(rev, cfg, clk, logging.NewTestLogger(t))
	t.Cleanup(func() { s.Close() })

	res, err := s.Invoke(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.Stats().Total != 1 {
		t.Fatalf("expect 1 instance: %+v", s.Stats())
	}
	// 未到空闲阈值：不回收。
	clk.Advance(cfg.IdleTimeout - time.Millisecond)
	s.OnTick()
	if s.Stats().Total != 1 {
		t.Fatalf("instance must survive before idle timeout: %+v", s.Stats())
	}
	// 达到阈值：tick 回收，缩到零。
	clk.Advance(2 * time.Millisecond)
	s.OnTick()
	waitFor(t, time.Second, func() bool { return s.Stats().ScaledToZero }, "scale to zero")
	if !s.Stats().ScaledToZero {
		t.Fatalf("expect scaled to zero after idle, got %+v", s.Stats())
	}
	// 缩零后新请求重新拉起（冷启动，cold=true）。
	res2, err := s.Invoke(context.Background(), nil)
	if err != nil || !res2.Cold || res2.Instance == res.Instance {
		t.Fatalf("expect new cold instance: r=%+v err=%v", res2, err)
	}
}

// TestRetireDrainsInflight：优雅回收时在途请求正常完成，新请求落到其他/新实例。
func TestRetireDrainsInflight(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	cfg := testCfg()
	proceed := make(chan struct{})
	started := make(chan struct{})
	var proceedOnce sync.Once
	proceedClose := func() { proceedOnce.Do(func() { close(proceed) }) }
	rev := testRev(t, 0, 1, func(ctx context.Context, in []byte) ([]byte, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-proceed
		return []byte("done"), nil
	})
	s := New(rev, cfg, clk, logging.NewTestLogger(t))
	t.Cleanup(func() {
		proceedClose()
		s.Close()
	})

	inflightErr := make(chan error, 1)
	inflightRes := make(chan *Result1, 1)
	go func() {
		r, err := s.Invoke(context.Background(), nil)
		inflightErr <- err
		if r != nil {
			inflightRes <- &Result1{r.Instance}
		}
	}()
	<-started
	waitFor(t, time.Second, func() bool { return s.Stats().Inflight == 1 }, "inflight=1")
	ids := s.InstanceIDs()
	if len(ids) != 1 {
		t.Fatalf("expect 1 instance, got %v", ids)
	}
	// 在途期间回收：必须等待其完成而非强制打断（给足 drain timeout）。
	retired := make(chan error, 1)
	go func() { retired <- s.RetireInstance(ids[0]) }()
	// drain 应在等待，未超时。
	select {
	case <-retired:
		t.Fatal("retire must not return while inflight still running")
	case <-time.After(50 * time.Millisecond):
	}
	proceedClose()
	select {
	case err := <-inflightErr:
		if err != nil {
			t.Fatalf("inflight must complete cleanly during drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inflight did not complete")
	}
	select {
	case err := <-retired:
		if err != nil {
			t.Fatalf("retire should succeed after graceful drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retire did not return")
	}
	if !s.Stats().ScaledToZero {
		t.Fatalf("expect scaled to zero after retire: %+v", s.Stats())
	}
}

// Result1 仅测试用。
type Result1 struct{ Instance string }

// TestRetireDrainTimeoutForceStops：排空超时后强制回收，在途请求得到 drain_timeout。
func TestRetireDrainTimeoutForceStops(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	cfg := testCfg()
	cfg.DrainTimeout = 10 * time.Millisecond
	gate := make(chan struct{})
	rev := testRev(t, 0, 1, func(ctx context.Context, in []byte) ([]byte, error) {
		<-gate
		return nil, nil
	})
	s := New(rev, cfg, clk, logging.NewTestLogger(t))
	t.Cleanup(func() {
		close(gate)
		s.Close()
	})

	errCh := make(chan error, 1)
	go func() { _, err := s.Invoke(context.Background(), nil); errCh <- err }()
	waitFor(t, time.Second, func() bool { return s.Stats().Inflight == 1 }, "inflight=1")
	ids := s.InstanceIDs()
	go func() { _ = s.RetireInstance(ids[0]) }()
	// 等待 drain 的 10ms 定时器注册（另有 invoke 的 1h 定时器）。
	clk.WaitUntilScheduled(2, time.Second)
	clk.Advance(10 * time.Millisecond)
	select {
	case err := <-errCh:
		if !errors.Is(err, fcerr.ErrDrainTimeout) {
			t.Fatalf("inflight must fail with drain_timeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inflight did not get drain timeout")
	}
	waitFor(t, time.Second, func() bool { return s.Stats().ScaledToZero }, "removed after force stop")
}

// TestCancelQueuedRequestIsDistinct：执行容量=1、队列容量=1。
// 第 1 个请求占住唯一执行槽；第 2 个请求进入队列；取消第 2 个请求，
// 必须得到 ErrUpstreamCanceled（区别于 queue_full / invoke_timeout），
// 且不影响占槽请求与后续请求。
func TestCancelQueuedRequestIsDistinct(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	cfg := testCfg()
	cfg.MaxScale = 1
	cfg.QueueCapacity = 1
	gate := make(chan struct{})
	var gateOnce sync.Once
	closeGate := func() { gateOnce.Do(func() { close(gate) }) }
	rev := testRev(t, 0, 1, func(ctx context.Context, in []byte) ([]byte, error) {
		<-gate
		return []byte("ok"), nil
	})
	s := New(rev, cfg, clk, logging.NewTestLogger(t))
	t.Cleanup(func() {
		closeGate()
		s.Close()
	})

	// 第 1 个：占用唯一执行槽。
	firstErr := make(chan error, 1)
	go func() { _, err := s.Invoke(context.Background(), nil); firstErr <- err }()
	waitFor(t, 2*time.Second, func() bool { return s.Stats().Inflight == 1 }, "first inflight")

	// 第 2 个：进入队列。
	ctx, cancel := context.WithCancel(context.Background())
	queuedErr := make(chan error, 1)
	go func() { _, err := s.Invoke(ctx, nil); queuedErr <- err }()
	waitFor(t, 2*time.Second, func() bool { return s.Stats().Pending == 2 }, "second admitted")

	// 取消排队中的请求。
	cancel()
	select {
	case err := <-queuedErr:
		if !errors.Is(err, fcerr.ErrUpstreamCanceled) {
			t.Fatalf("queued cancel must be upstream_canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued request did not return after cancel")
	}

	// 占槽请求仍可正常完成（取消排队请求不波及它）。
	closeGate()
	select {
	case err := <-firstErr:
		if err != nil {
			t.Fatalf("inflight request must succeed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inflight request did not finish")
	}
}

// TestColdStartTimeoutBackpressure：冷启动超过请求截止时间，得到 ErrColdStartTimeout（背压可重试）。
func TestColdStartTimeoutBackpressure(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	cfg := testCfg()
	rev := testRev(t, 100*time.Millisecond, 1, nil)
	s := New(rev, cfg, clk, logging.NewTestLogger(t))
	t.Cleanup(func() { s.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := s.Invoke(ctx, nil)
	// 截止发生在冷启动等待阶段：必须是可重试背压（cold_start_timeout）。
	if !errors.Is(err, fcerr.ErrColdStartTimeout) {
		t.Fatalf("want cold_start_timeout, got %v (%s)", err, fcerr.ReasonOf(err))
	}
	if !fcerr.Backpressure(err) {
		t.Fatalf("cold-start timeout must be retryable backpressure, got %v", err)
	}
}

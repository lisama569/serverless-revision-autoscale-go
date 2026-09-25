// Package instance 实现单个函数版本的本地运行实例。
//
// 实例是版本的运行载体，具备：并发槽位、在途计数、Active/Draining/Stopped 状态机，
// 以及冷启动、带超时的调用与优雅排空（drain）。
package instance

import (
	"context"
	"sync"
	"time"

	"github.com/lisama569/serverless-revision-autoscale-go/internal/clock"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/fcerr"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/logging"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/revision"
)

// State 表示实例生命周期状态。
type State int

const (
	// StateStarting：冷启动中。
	StateStarting State = iota
	// StateActive：可接收新流量。
	StateActive
	// StateDraining：排空中，不再接收新流量。
	StateDraining
	// StateStopped：已回收。
	StateStopped
)

func (s State) String() string {
	switch s {
	case StateStarting:
		return "Starting"
	case StateActive:
		return "Active"
	case StateDraining:
		return "Draining"
	case StateStopped:
		return "Stopped"
	default:
		return "Unknown"
	}
}

// Result 是一次调用的结果与归因信息。
type Result struct {
	Output   []byte
	Instance string
	Revision string
	Cold     bool
}

// Config 构造实例所需参数。
type Config struct {
	FunctionName  string
	Rev           *revision.Revision
	ID            string
	InvokeTimeout time.Duration // <=0 表示不设实例侧超时
	Clk           clock.Clock   // 必填
	Log           logging.Logger
}

// Instance 是一个可调用的运行实例。
type Instance struct {
	cfg Config
	log logging.Logger

	mu        sync.Mutex
	state     State
	active    int
	last      time.Time
	readyOnce sync.Once
	stopOnce  sync.Once
	ready     chan struct{}
	stopCh    chan struct{}
	wg        sync.WaitGroup // 跟踪全部在途 Invoke
	coldWG    sync.WaitGroup // 跟踪冷启动 goroutine

	cold      bool // 本实例是否尚未提供过任何调用（用于 cold 归因）
	mc        int
	coldStart time.Duration
}

// New 创建实例并立即在后台开始冷启动。
func New(cfg Config) *Instance {
	mc := revision.DefaultMaxConcurrency
	var cold time.Duration
	if cfg.Rev != nil {
		mc = cfg.Rev.MaxConcurrency()
		cold = cfg.Rev.Spec.ColdStart
	}
	i := &Instance{
		cfg:       cfg,
		log:       cfg.Log,
		state:     StateStarting,
		ready:     make(chan struct{}),
		stopCh:    make(chan struct{}),
		cold:      true,
		mc:        mc,
		coldStart: cold,
	}
	if i.log == nil {
		i.log = logging.NewJSONLogger(nil)
	}
	i.log.Info("instance.cold_start_begin",
		"function", cfg.FunctionName, "revision", i.RevisionID(),
		"instance", cfg.ID, "cold_start", cold.String(), "max_concurrency", mc)
	i.coldWG.Add(1)
	go func() {
		defer i.coldWG.Done()
		i.runColdStart()
	}()
	return i
}

// WaitColdStartStopped 等待冷启动 goroutine 退出（回收路径使用，避免后台日志/资源泄漏）。
func (i *Instance) WaitColdStartStopped() { i.coldWG.Wait() }

func (i *Instance) runColdStart() {
	if i.coldStart > 0 {
		t := i.cfg.Clk.NewTimer(i.coldStart)
		select {
		case <-i.stopCh:
			t.Stop()
			return
		case <-t.C:
		}
	}
	i.mu.Lock()
	becameActive := false
	if i.state == StateStarting {
		i.state = StateActive
		becameActive = true
	}
	i.last = i.cfg.Clk.Now()
	i.mu.Unlock()
	i.readyOnce.Do(func() { close(i.ready) })
	if becameActive {
		i.log.Info("instance.cold_start_done",
			"revision", i.RevisionID(), "instance", i.cfg.ID)
	}
}

// ID 返回实例 ID。
func (i *Instance) ID() string { return i.cfg.ID }

// RevisionID 返回所属版本 ID。
func (i *Instance) RevisionID() string {
	if i.cfg.Rev == nil {
		return ""
	}
	return i.cfg.Rev.ID
}

// State 返回当前状态。
func (i *Instance) State() State {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.state
}

// Inflight 返回在途请求数。
func (i *Instance) Inflight() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.active
}

// FreeSlots 返回可立即接收的新请求数（仅 Active 实例可能 >0）。
func (i *Instance) FreeSlots() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.state != StateActive {
		return 0
	}
	free := i.mc - i.active
	if free < 0 {
		return 0
	}
	return free
}

// MaxConcurrency 返回实例并发上限。
func (i *Instance) MaxConcurrency() int { return i.mc }

// LastUsed 返回最近一次完成调用（或冷启动完成）的时间。
func (i *Instance) LastUsed() time.Time {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.last
}

// Ready 返回冷启动完成的信号通道。
func (i *Instance) Ready() <-chan struct{} { return i.ready }

// ColdStart 同步等待冷启动完成。
// 调用方 ctx 截止返回 ErrColdStartTimeout；取消返回 ErrUpstreamCanceled；
// 冷启动被强制回收中断返回 ErrDrainTimeout。
func (i *Instance) ColdStart(ctx context.Context) error {
	select {
	case <-i.ready:
		return nil
	case <-i.stopCh:
		return fcerr.WithMsg(fcerr.ErrDrainTimeout,
			"cold start aborted by force stop: instance=%s", i.cfg.ID)
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return fcerr.WithMsg(fcerr.ErrUpstreamCanceled,
				"cold start canceled while waiting: instance=%s", i.cfg.ID)
		}
		return fcerr.WithMsg(fcerr.ErrColdStartTimeout,
			"cold start deadline exceeded: instance=%s", i.cfg.ID)
	}
}

// Invoke 在该实例上执行一次调用。
//
// 失败原因可区分：
//   - ErrDrainTimeout：实例在排空/已回收（含 ForceStop 打断在途请求）；
//   - ErrQueueFull：并发槽位已满（正常应由上层排队，不会静默丢弃）；
//   - ErrInvokeTimeout：实例侧调用超时或调用方 ctx 截止；
//   - ErrUpstreamCanceled：上游主动取消；
//   - ErrInvocationFailed：函数 Handler 自身返回错误。
func (i *Instance) Invoke(ctx context.Context, in []byte) (*Result, error) {
	// 原子地完成状态检查与并发占位，避免与 BeginDrain 竞争后仍落到排空实例上。
	i.mu.Lock()
	switch i.state {
	case StateDraining, StateStopped:
		i.mu.Unlock()
		return nil, fcerr.WithMsg(fcerr.ErrDrainTimeout,
			"instance %s is %s and rejects new traffic", i.cfg.ID, i.state)
	}
	if i.active >= i.mc {
		i.mu.Unlock()
		return nil, fcerr.WithMsg(fcerr.ErrQueueFull,
			"instance %s at concurrency limit %d", i.cfg.ID, i.mc)
	}
	i.active++
	isCold := i.cold
	i.mu.Unlock()
	i.wg.Add(1)

	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 仅在成功占位后创建超时定时器，并在退出时停止，避免被拒绝路径
	// 在假时钟/真时钟上遗留悬挂定时器。
	var timer *clock.Timer
	var timeoutCh <-chan time.Time
	if i.cfg.InvokeTimeout > 0 {
		timer = i.cfg.Clk.NewTimer(i.cfg.InvokeTimeout)
		timeoutCh = timer.C
		defer timer.Stop()
	}

	defer func() {
		i.mu.Lock()
		i.active--
		i.last = i.cfg.Clk.Now()
		i.cold = false
		i.mu.Unlock()
		i.wg.Done()
	}()

	type handlerResult struct {
		out []byte
		err error
	}
	resCh := make(chan handlerResult, 1)
	go func() {
		out, err := i.cfg.Rev.Spec.Handler(callCtx, in)
		resCh <- handlerResult{out: out, err: err}
	}()

	i.log.Debug("instance.invoke_start",
		"revision", i.RevisionID(), "instance", i.cfg.ID, "inflight", i.Inflight(), "cold", isCold)

	select {
	case <-i.stopCh:
		// 强制回收：在途请求得到明确失败，原因是排空/回收。
		return nil, fcerr.WithMsg(fcerr.ErrDrainTimeout,
			"in-flight invoke aborted by force stop: instance=%s", i.cfg.ID)
	case <-timeoutCh:
		cancel()
		return nil, fcerr.WithMsg(fcerr.ErrInvokeTimeout,
			"invoke timeout %s exceeded: instance=%s", i.cfg.InvokeTimeout, i.cfg.ID)
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return nil, fcerr.WithMsg(fcerr.ErrUpstreamCanceled,
				"in-flight invoke canceled by upstream: instance=%s", i.cfg.ID)
		}
		// 调用方携带的截止时间，同样归因为调用超时。
		return nil, fcerr.WithMsg(fcerr.ErrInvokeTimeout,
			"in-flight invoke deadline exceeded: instance=%s", i.cfg.ID)
	case r := <-resCh:
		if r.err != nil {
			return nil, fcerr.WithMsg(fcerr.ErrInvocationFailed,
				"handler error on instance=%s revision=%s: %v", i.cfg.ID, i.RevisionID(), r.err)
		}
		i.log.Debug("instance.invoke_done",
			"revision", i.RevisionID(), "instance", i.cfg.ID, "cold", isCold)
		return &Result{Output: r.out, Instance: i.cfg.ID, Revision: i.RevisionID(), Cold: isCold}, nil
	}
}

// BeginDrain 将实例切换到 Draining（幂等）。切换后不再接收新流量，
// 已接收的在途请求继续执行直到完成或超时/取消。
func (i *Instance) BeginDrain() {
	i.mu.Lock()
	if i.state == StateActive || i.state == StateStarting {
		i.state = StateDraining
		i.log.Info("instance.drain_begin",
			"revision", i.RevisionID(), "instance", i.cfg.ID, "inflight", i.active)
	}
	i.mu.Unlock()
}

// Drain 等待在途请求排空。timeout<=0 表示只受 ctx 约束。
//   - 排空完成：实例进入 Stopped，返回 nil；
//   - 超时：返回 ErrDrainTimeout，实例仍为 Draining，可再次 Drain 或 ForceStop；
//   - 调用方 ctx 取消：返回 ErrUpstreamCanceled。
func (i *Instance) Drain(ctx context.Context, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		i.wg.Wait()
		close(done)
	}()
	var timer *clock.Timer
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer = i.cfg.Clk.NewTimer(timeout)
		timeoutCh = timer.C
	}
	select {
	case <-done:
		i.markStopped()
		i.log.Info("instance.drain_complete",
			"revision", i.RevisionID(), "instance", i.cfg.ID)
		return nil
	case <-timeoutCh:
		return fcerr.WithMsg(fcerr.ErrDrainTimeout,
			"drain timeout %s exceeded, inflight remain: instance=%s inflight=%d",
			timeout, i.cfg.ID, i.Inflight())
	case <-ctx.Done():
		return fcerr.WithMsg(fcerr.ErrUpstreamCanceled,
			"drain canceled by caller: instance=%s", i.cfg.ID)
	}
}

func (i *Instance) markStopped() {
	i.mu.Lock()
	i.state = StateStopped
	i.mu.Unlock()
	i.stopOnce.Do(func() { close(i.stopCh) })
}

// ForceStop 立即回收实例：状态置 Stopped 并广播 stopCh，
// 所有在途请求以 ErrDrainTimeout 失败返回。幂等。
func (i *Instance) ForceStop() {
	i.mu.Lock()
	wasStopped := i.state == StateStopped
	i.state = StateStopped
	i.mu.Unlock()
	if !wasStopped {
		i.log.Warn("instance.force_stop",
			"revision", i.RevisionID(), "instance", i.cfg.ID, "inflight", i.Inflight())
	}
	i.stopOnce.Do(func() { close(i.stopCh) })
}

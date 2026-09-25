package fc

import (
	"context"
	"sync"
	"time"
)

// instanceState 实例生命周期状态。
type instanceState int

const (
	stateActive   instanceState = iota
	stateDraining               // 不再接收新流量，等待在途请求完成
	stateDead                   // 已回收
)

func (s instanceState) String() string {
	switch s {
	case stateActive:
		return "active"
	case stateDraining:
		return "draining"
	default:
		return "dead"
	}
}

// instance 是一个函数实例，按 MaxConcurrency 限制并发，支持优雅排空。
type instance struct {
	id  string
	rev *Revision
	log Logger
	now func() time.Time

	mu       sync.Mutex
	state    instanceState
	inflight int
	sem      chan struct{} // 容量 = MaxConcurrency
	lastIdle time.Time

	drainCtx    context.Context    // 实例级排空上下文，创建即建立
	drainCancel context.CancelFunc // 排空超时时取消，中断在途调用
	drained     chan struct{}      // 排空开始且在途归零后关闭
}

func newInstance(id string, rev *Revision, log Logger, now func() time.Time) *instance {
	dctx, cancel := context.WithCancel(context.Background())
	in := &instance{
		id:          id,
		rev:         rev,
		log:         log,
		now:         now,
		state:       stateActive,
		sem:         make(chan struct{}, rev.Config.MaxConcurrency),
		lastIdle:    now(),
		drainCtx:    dctx,
		drainCancel: cancel,
		drained:     make(chan struct{}),
	}
	log("[instance %s] created for revision %s", id, rev.ID)
	return in
}

// tryAcquire 尝试占用一个并发槽；实例非 active 返回 errDraining，已满返回 errFull。
func (in *instance) tryAcquire() error {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.state != stateActive {
		return newError(ErrKindDraining, "instance "+in.id+" is "+in.state.String())
	}
	select {
	case in.sem <- struct{}{}:
		in.inflight++
		return nil
	default:
		return errInstanceFull
	}
}

var errInstanceFull = newError(ErrKindQueueFull, "instance concurrency slots exhausted")

func (in *instance) release() {
	in.mu.Lock()
	defer in.mu.Unlock()
	<-in.sem
	in.inflight--
	if in.inflight == 0 {
		in.lastIdle = in.now()
		if in.state != stateActive {
			in.closeDrainedLocked()
		}
	}
}

func (in *instance) closeDrainedLocked() {
	select {
	case <-in.drained:
	default:
		close(in.drained)
	}
}

// startDrain 停止接收新流量；drainCtx 到期表示排空超时，
// 超时通过实例级 drainCtx 取消传播给所有在途 exec（含已开始执行的）。
func (in *instance) startDrain(drainCtx context.Context) {
	in.mu.Lock()
	if in.state == stateDead {
		in.mu.Unlock()
		return
	}
	in.state = stateDraining
	if in.inflight == 0 {
		in.closeDrainedLocked()
	}
	in.log("[instance %s] draining (rev %s, inflight=%d)", in.id, in.rev.ID, in.inflight)
	in.mu.Unlock()

	go func() {
		<-drainCtx.Done()
		in.drainCancel()
	}()
}

// kill 由缩容回收调用：要求实例无在途请求。
func (in *instance) kill() {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.state = stateDead
	in.closeDrainedLocked()
	in.log("[instance %s] reclaimed (rev %s)", in.id, in.rev.ID)
}

// exec 在实例上执行一次调用，区分排空超时 / 调用超时 / 上游取消。
// 调用前必须已成功 tryAcquire，返回后由调用方 release。
func (in *instance) exec(ctx context.Context, req *Request) (string, error) {
	toCtx, cancel := context.WithTimeout(ctx, in.rev.Config.InvokeTimeout)
	defer cancel()

	type result struct {
		body string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		body, err := in.rev.Handler(toCtx, req)
		done <- result{body, err}
	}()

	select {
	case r := <-done:
		return r.body, r.err
	case <-in.drainCtx.Done():
		cancel() // 通知 handler 尽快退出
		return "", newError(ErrKindDrainTimeout,
			"instance "+in.id+" drain deadline exceeded while request in flight")
	case <-toCtx.Done():
		if ctx.Err() != nil {
			return "", newError(ErrKindUpstreamCancel, "caller cancelled the invocation")
		}
		return "", newError(ErrKindInvokeTimeout,
			"invocation exceeded "+in.rev.Config.InvokeTimeout.String())
	}
}

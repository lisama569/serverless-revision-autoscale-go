// Package autoscale 为单个函数版本维护实例池：
// 按并发负载扩缩、无流量空闲时缩容到零、排队与可区分背压、回收时优雅排空。
package autoscale

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/serverless-revision-autoscale-go/internal/clock"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/fcerr"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/instance"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/logging"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/revision"
)

// Defaults：未显式配置时使用的伸缩参数。
const (
	DefaultMaxScale      = 4
	DefaultQueueCapacity = 32
	DefaultIdleTimeout   = 2 * time.Minute
	DefaultDrainTimeout  = 10 * time.Second
	DefaultInvokeTimeout = 5 * time.Second
	DefaultIdleTick      = 10 * time.Second
)

// Config 是 Scaler 的伸缩与超时参数。
type Config struct {
	MaxScale      int           // 最大实例数
	QueueCapacity int           // 等待队列容量（超出返回 ErrQueueFull）
	IdleTimeout   time.Duration // 无流量多久后缩零
	DrainTimeout  time.Duration // 回收实例的排空超时
	InvokeTimeout time.Duration // 单次调用超时
	IdleTick      time.Duration // 空闲回收检查周期
}

func (c *Config) withDefaults() {
	if c.MaxScale <= 0 {
		c.MaxScale = DefaultMaxScale
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = DefaultQueueCapacity
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = DefaultIdleTimeout
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
	if c.InvokeTimeout <= 0 {
		c.InvokeTimeout = DefaultInvokeTimeout
	}
	if c.IdleTick <= 0 {
		c.IdleTick = DefaultIdleTick
	}
}

type queuedReq struct {
	ctx context.Context
	in  []byte
	res chan<- qresult
}

type qresult struct {
	res *instance.Result
	err error
}

// Scaler 管理单个版本的实例池与调度队列。
type Scaler struct {
	rev *revision.Revision
	cfg Config
	clk clock.Clock
	log logging.Logger

	idSeq atomic.Int64

	mu        sync.Mutex
	insts     []*instance.Instance
	startingN int
	starting  []*instance.Instance
	closed    bool
	wake      chan struct{}
	admitted  atomic.Int64 // 已准入但未完成的请求（在队列或在执行）
	invoked   atomic.Int64
	rejected  atomic.Int64

	queue chan *queuedReq
	sem   chan struct{} // 执行令牌：容量 = MaxScale * 单实例并发
	done  chan struct{}
	wg    sync.WaitGroup // 跟踪 dispatchLoop 与全部 serveOne
}

// capacity 返回准入上限：执行并发（MaxScale*单实例并发）+ 排队容量。
func (s *Scaler) capacity() int { return cap(s.sem) + s.cfg.QueueCapacity }

// New 创建一个版本的 Scaler 并启动调度循环。
func New(rev *revision.Revision, cfg Config, clk clock.Clock, log logging.Logger) *Scaler {
	cfg.withDefaults()
	if clk == nil {
		panic("autoscale.New: clock is required")
	}
	if log == nil {
		log = logging.NewJSONLogger(nil)
	}
	s := &Scaler{
		rev:   rev,
		cfg:   cfg,
		clk:   clk,
		log:   log.With("revision", rev.ID),
		queue: make(chan *queuedReq, cfg.QueueCapacity),
		sem:   make(chan struct{}, cfg.MaxScale*rev.MaxConcurrency()),
		wake:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	s.wg.Add(1)
	go s.dispatchLoop()
	s.log.Info("scaler.init",
		"max_scale", cfg.MaxScale, "queue_capacity", cfg.QueueCapacity,
		"idle_timeout", cfg.IdleTimeout.String(), "drain_timeout", cfg.DrainTimeout.String(),
		"invoke_timeout", cfg.InvokeTimeout.String())
	return s
}

// RevisionID 返回托管的版本 ID。
func (s *Scaler) RevisionID() string { return s.rev.ID }

// Stats 是实例池的可观测状态快照。
type Stats struct {
	RevisionID   string
	Total        int
	Active       int
	Draining     int
	Starting     int
	Inflight     int
	Pending      int
	Invoked      int64
	Rejected     int64
	ScaledToZero bool
}

// Stats 返回当前伸缩状态快照。
func (s *Scaler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{
		RevisionID:   s.rev.ID,
		Total:        len(s.insts) + s.startingN,
		Starting:     s.startingN,
		Pending:      int(s.admitted.Load()),
		Invoked:      s.invoked.Load(),
		Rejected:     s.rejected.Load(),
		ScaledToZero: len(s.insts) == 0 && s.startingN == 0,
	}
	for _, in := range s.insts {
		switch in.State() {
		case instance.StateActive:
			st.Active++
		case instance.StateDraining:
			st.Draining++
		}
		st.Inflight += in.Inflight()
	}
	return st
}

func (s *Scaler) newID() string {
	n := s.idSeq.Add(1)
	return fmt.Sprintf("inst-%s-%03d", s.rev.ID, n)
}

// broadcastWake 唤醒所有等待槽位的 serveOne（调用方持 s.mu）。
func (s *Scaler) broadcastWake() {
	close(s.wake)
	s.wake = make(chan struct{})
}

func (s *Scaler) dispatchLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case req := <-s.queue:
			s.wg.Add(1)
			go s.serveOne(req)
		}
	}
}

// Invoke 提交一次调用到有界队列。
//
// 错误分类：
//   - ErrQueueFull：队列已满（可区分的背压，拒绝而非静默丢弃）；
//   - ErrUpstreamCanceled / ErrInvokeTimeout：排队等待期间上游取消或截止；
//   - ErrColdStartTimeout：冷启动超时；
//   - ErrInvokeTimeout / ErrUpstreamCanceled / ErrInvocationFailed /
//     ErrDrainTimeout：执行阶段的实例错误，原因可区分。
func (s *Scaler) Invoke(ctx context.Context, in []byte) (*instance.Result, error) {
	resCh := make(chan qresult, 1)
	req := &queuedReq{ctx: ctx, in: in, res: resCh}
	// 准入控制与消费者调度速率解耦：admitted 以 CAS 精确限制在
	// 执行容量+队列容量之内，超出立即以 ErrQueueFull 拒绝（有界背压，不静默丢弃）。
	for {
		cur := s.admitted.Load()
		if cur >= int64(s.capacity()) {
			s.rejected.Add(1)
			st := s.Stats()
			s.log.Warn("scaler.reject_queue_full",
				"queue_capacity", s.cfg.QueueCapacity, "instances", st.Total,
				"inflight", st.Inflight, "admitted", cur)
			return nil, fcerr.WithMsg(fcerr.ErrQueueFull,
				"revision=%s capacity %d reached (exec+queue; instances=%d inflight=%d); backpressure",
				s.rev.ID, s.capacity(), st.Total, st.Inflight)
		}
		if s.admitted.CompareAndSwap(cur, cur+1) {
			break
		}
	}
	s.queue <- req
	s.log.Debug("scaler.request_admitted", "admitted", s.admitted.Load())
	// 准入名额由 serveOne 在请求最终落地（取消/失败/成功）时统一释放，
	// 调用方不再释放，避免双重释放。
	doneCh := make(chan qresult, 1)
	go func() {
		r := <-resCh
		s.admitted.Add(-1)
		doneCh <- r
	}()
	select {
	case r := <-doneCh:
		if r.err == nil {
			s.invoked.Add(1)
		}
		return r.res, r.err
	case <-ctx.Done():
		// 上游不再等待，但请求仍在系统中排队/执行。等待其最终落地以获得精确归因
		// （冷启动超时 / 调用超时 / 取消 / 成功）并转移准入名额，避免名额泄漏或重复释放。
		r := <-doneCh
		if r.err == nil {
			s.invoked.Add(1)
		}
		if r.err != nil {
			// 以服务端最终归因为准（可能是 cold_start_timeout / invoke_timeout /
			// upstream_canceled / drain_timeout / invocation_failed）。
			return nil, r.err
		}
		// 服务端在取消送达前已执行成功：结果对调用方仍视为因取消而放弃，保持可区分。
		if ctx.Err() == context.Canceled {
			return nil, fcerr.WithMsg(fcerr.ErrUpstreamCanceled,
				"upstream canceled after request completed: revision=%s", s.rev.ID)
		}
		return nil, fcerr.WithMsg(fcerr.ErrInvokeTimeout,
			"deadline exceeded after request completed: revision=%s", s.rev.ID)
	}
}

// finish 向调用方交付恰好一次结果（resCh 带缓冲）。
func finish(req *queuedReq, r qresult) { req.res <- r }

// serveOne 处理一个已入队请求：拿执行令牌 -> 选实例/冷启动 -> 调用。
func (s *Scaler) serveOne(req *queuedReq) {
	defer s.wg.Done()
	// 执行令牌将「冷启动等待 + 正在执行」的请求数限制为总并发容量，
	// 超出部分在队列中等待，不会无限堆积 goroutine/实例。
	select {
	case s.sem <- struct{}{}:
	case <-req.ctx.Done():
		finish(req, qresult{err: mapWaitCtxErr(req.ctx)})
		return
	case <-s.done:
		finish(req, qresult{err: fcerr.WithMsg(fcerr.ErrDrainTimeout, "scaler closed: revision=%s", s.rev.ID)})
		return
	}
	defer func() { <-s.sem }()

	inst, err := s.acquireWithWait(req.ctx)
	if err != nil {
		finish(req, qresult{err: err})
		return
	}
	res, iErr := inst.Invoke(req.ctx, req.in)
	// 调用结束释放实例槽位，唤醒等待者。
	s.mu.Lock()
	s.broadcastWake()
	s.mu.Unlock()
	finish(req, qresult{res: res, err: iErr})
}

// acquireWithWait 直到拿到一个可调用实例、上游取消或 scaler 关闭。
func (s *Scaler) acquireWithWait(ctx context.Context) (*instance.Instance, error) {
	for {
		inst, err := s.acquireOrStart(ctx)
		if err != nil {
			return nil, err
		}
		if inst != nil {
			return inst, nil
		}
		// (nil, nil)：全部实例占满且已达 MaxScale，等待槽位释放。
		if err := s.waitWake(ctx); err != nil {
			return nil, err
		}
	}
}

// acquireOrStart 返回立即可用的实例；必要时同步完成一次冷启动。
//   - (inst, nil)：实例就绪；
//   - (nil, nil)：无空槽且已达 MaxScale，调用方应等待后重试；
//   - (nil, err)：冷启动失败（ErrColdStartTimeout / ErrUpstreamCanceled / ErrDrainTimeout）。
func (s *Scaler) acquireOrStart(ctx context.Context) (*instance.Instance, error) {
	s.mu.Lock()
	for _, in := range s.insts {
		if in.FreeSlots() > 0 {
			s.mu.Unlock()
			return in, nil
		}
	}
	if len(s.insts)+s.startingN >= s.cfg.MaxScale {
		s.mu.Unlock()
		return nil, nil
	}
	inst := instance.New(instance.Config{
		FunctionName:  s.rev.FunctionName,
		Rev:           s.rev,
		ID:            s.newID(),
		InvokeTimeout: s.cfg.InvokeTimeout,
		Clk:           s.clk,
		Log:           s.log,
	})
	s.startingN++
	s.starting = append(s.starting, inst)
	s.log.Info("scaler.scale_out",
		"decision", "create_instance", "instance", inst.ID(),
		"total_after", len(s.insts)+s.startingN, "reason", "no_free_slots_under_max")
	s.mu.Unlock()

	cErr := inst.ColdStart(ctx)
	if cErr != nil {
		s.mu.Lock()
		s.startingN--
		s.removeStartingLocked(inst)
		s.removeLocked(inst)
		s.broadcastWake()
		s.mu.Unlock()
		inst.ForceStop()
		inst.WaitColdStartStopped()
		s.log.Warn("scaler.cold_start_failed", "instance", inst.ID(), "err", cErr.Error())
		// 上游在冷启动期间取消：归因为取消；否则统一为冷启动超时背压。
		if ctx.Err() == context.Canceled {
			return nil, fcerr.WithMsg(fcerr.ErrUpstreamCanceled,
				"cold start canceled: instance=%s", inst.ID())
		}
		return nil, cErr
	}

	s.mu.Lock()
	s.startingN--
	s.removeStartingLocked(inst)
	s.insts = append(s.insts, inst)
	s.broadcastWake()
	s.mu.Unlock()
	return inst, nil
}

// waitWake 阻塞到槽位变化（实例完成调用/回收）或上游取消。
func (s *Scaler) waitWake(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fcerr.WithMsg(fcerr.ErrDrainTimeout, "scaler closed: revision=%s", s.rev.ID)
	}
	w := s.wake
	s.mu.Unlock()
	select {
	case <-w:
		return nil
	case <-ctx.Done():
		return mapWaitCtxErr(ctx)
	case <-s.done:
		return fcerr.WithMsg(fcerr.ErrDrainTimeout, "scaler closed: revision=%s", s.rev.ID)
	}
}

func (s *Scaler) removeLocked(target *instance.Instance) bool {
	for i, in := range s.insts {
		if in == target {
			s.insts = append(s.insts[:i], s.insts[i+1:]...)
			return true
		}
	}
	return false
}

func (s *Scaler) removeStartingLocked(target *instance.Instance) bool {
	for i, in := range s.starting {
		if in == target {
			s.starting = append(s.starting[:i], s.starting[i+1:]...)
			return true
		}
	}
	return false
}

func mapWaitCtxErr(ctx context.Context) error {
	if ctx.Err() == context.Canceled {
		return fcerr.WithMsg(fcerr.ErrUpstreamCanceled, "canceled while waiting for an instance slot")
	}
	return fcerr.WithMsg(fcerr.ErrInvokeTimeout, "deadline exceeded while waiting for an instance slot")
}

// OnTick 驱动一次空闲回收检查：无在途且空闲超过 IdleTimeout 的实例被排空回收，
// 最终在无流量时缩容到零。
func (s *Scaler) OnTick() {
	now := s.clk.Now()
	var victims []*instance.Instance
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	for _, in := range s.insts {
		if in.Inflight() == 0 && in.State() == instance.StateActive &&
			now.Sub(in.LastUsed()) >= s.cfg.IdleTimeout {
			victims = append(victims, in)
		}
	}
	s.mu.Unlock()
	for _, in := range victims {
		idleFor := now.Sub(in.LastUsed())
		if err := s.retire(in); err != nil {
			s.log.Warn("scaler.idle_retire_done",
				"decision", "reap_idle_instance", "instance", in.ID(),
				"idle_for", idleFor.String(), "drain_timeout", true)
		} else {
			s.log.Info("scaler.idle_retire_done",
				"decision", "reap_idle_instance", "instance", in.ID(),
				"idle_for", idleFor.String(), "drain_timeout", false)
		}
	}
}

// RetireInstance 主动回收指定实例：停止接收新流量，等待在途排空，超时则强制回收。
// 实例不存在返回 ErrRevisionNotFound；排空超时返回 ErrDrainTimeout（已强制回收）。
func (s *Scaler) RetireInstance(id string) error {
	s.mu.Lock()
	var target *instance.Instance
	for _, in := range s.insts {
		if in.ID() == id {
			target = in
			break
		}
	}
	s.mu.Unlock()
	if target == nil {
		return fcerr.WithMsg(fcerr.ErrRevisionNotFound, "instance not found: %s", id)
	}
	return s.retire(target)
}

func (s *Scaler) retire(in *instance.Instance) error {
	in.BeginDrain()
	dErr := in.Drain(context.Background(), s.cfg.DrainTimeout)
	timedOut := errors.Is(dErr, fcerr.ErrDrainTimeout)
	if dErr != nil {
		in.ForceStop()
	}
	s.mu.Lock()
	s.removeLocked(in)
	remaining := len(s.insts) + s.startingN
	s.broadcastWake()
	s.mu.Unlock()
	s.log.Info("scaler.instance_retired",
		"instance", in.ID(), "drain_timeout", timedOut,
		"remaining", remaining, "scaled_to_zero", remaining == 0)
	if timedOut {
		return fcerr.WithMsg(fcerr.ErrDrainTimeout,
			"instance %s drained with timeout and was force-stopped", in.ID())
	}
	return nil
}

// Close 停止接新请求，拒绝队列中未处理的请求，并排空回收全部实例（幂等）。
func (s *Scaler) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	insts := append([]*instance.Instance(nil), s.insts...)
	starting := append([]*instance.Instance(nil), s.starting...)
	s.insts = nil
	s.starting = nil
	close(s.done)
	s.broadcastWake()
	s.mu.Unlock()

	// 停止仍在冷启动的实例并等待其 goroutine 退出，避免回收后仍写日志/占资源。
	for _, in := range starting {
		in.ForceStop()
	}

	// 立即拒绝仍停留在队列中的请求，避免调用方无限等待。
drainQueue:
	for {
		select {
		case req := <-s.queue:
			req.res <- qresult{err: fcerr.WithMsg(fcerr.ErrDrainTimeout, "scaler closing: revision=%s", s.rev.ID)}
		default:
			break drainQueue
		}
	}

	var errs []error
	for _, in := range insts {
		in.BeginDrain()
		if err := in.Drain(context.Background(), s.cfg.DrainTimeout); err != nil {
			in.ForceStop()
			errs = append(errs, err)
		}
		in.WaitColdStartStopped()
	}
	for _, in := range starting {
		in.WaitColdStartStopped()
	}
	s.wg.Wait()
	s.log.Info("scaler.closed", "drain_timeouts", len(errs))
	return errors.Join(errs...)
}

// IdleTick 返回配置的空闲检查周期，供上层定时驱动。
func (s *Scaler) IdleTick() time.Duration { return s.cfg.IdleTick }

// IdleTimeout 返回配置的空闲缩零时间。
func (s *Scaler) IdleTimeout() time.Duration { return s.cfg.IdleTimeout }

// InstanceIDs 返回当前池中实例 ID 的快照（供运维/测试使用）。
func (s *Scaler) InstanceIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.insts))
	for _, in := range s.insts {
		out = append(out, in.ID())
	}
	return out
}

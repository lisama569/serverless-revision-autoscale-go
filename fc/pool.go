package fc

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// PoolStats 描述一个版本池的运行状态。
type PoolStats struct {
	Instances int // 存活实例数（含排空中）
	Active    int // 接收新流量的实例数
	Draining  int // 排空中的实例数
	InFlight  int // 在途请求数
	Queued    int // 排队中的请求数
}

// pool 是单个版本的实例池：按并发负载伸缩、有界排队背压、空闲缩容到零、整体优雅排空。
type pool struct {
	rev *Revision
	log Logger
	now func() time.Time

	mu        sync.Mutex
	instances []*instance
	nextID    int
	draining  bool          // 版本已摘除流量，整池排空，不再接收新请求
	queue     chan struct{} // 有界排队槽，容量 = QueueSize
}

func newPool(rev *Revision, log Logger, now func() time.Time) *pool {
	return &pool{
		rev:   rev,
		log:   log,
		now:   now,
		queue: make(chan struct{}, rev.Config.QueueSize),
	}
}

// invoke 在本版本上执行一次调用。
func (p *pool) invoke(ctx context.Context, req *Request) (*Response, error) {
	in, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer in.release()

	body, err := in.exec(ctx, req)
	if err != nil {
		return nil, err
	}
	return &Response{Revision: p.rev.ID, InstanceID: in.id, Body: body}, nil
}

// acquire 获取一个可用实例的并发槽，必要时冷启动新实例；饱和时排队，队列满则拒绝。
func (p *pool) acquire(ctx context.Context) (*instance, error) {
	in, full, err := p.tryGet()
	if err != nil {
		return nil, err
	}
	if !full {
		return in, nil
	}

	// 所有实例已满且达上限：进入有界排队。
	select {
	case p.queue <- struct{}{}:
		p.log("[pool %s] request queued (queued=%d/%d)", p.rev.ID, len(p.queue), cap(p.queue))
		defer func() { <-p.queue }()
	default:
		p.log("[pool %s] request rejected: queue full (%d)", p.rev.ID, cap(p.queue))
		return nil, newError(ErrKindQueueFull,
			fmt.Sprintf("revision %s queue full (%d), all %d instances at max concurrency",
				p.rev.ID, cap(p.queue), p.rev.Config.MaxInstances))
	}

	t := time.NewTicker(time.Millisecond)
	defer t.Stop()
	for {
		in, full, err := p.tryGet()
		if err != nil {
			return nil, err
		}
		if !full {
			return in, nil
		}
		select {
		case <-ctx.Done():
			return nil, newError(ErrKindUpstreamCancel, "caller cancelled while queued")
		case <-t.C:
		}
	}
}

// tryGet 尝试在现有实例上占槽；无可用槽且未达实例上限时冷启动新实例。
// 返回值 full=true 表示已饱和（应排队或拒绝）。
func (p *pool) tryGet() (in *instance, full bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.draining {
		return nil, false, newError(ErrKindDraining, "revision "+string(p.rev.ID)+" is draining")
	}
	for _, cand := range p.instances {
		if cand.tryAcquire() == nil {
			return cand, false, nil
		}
	}
	if len(p.instances) < p.rev.Config.MaxInstances {
		p.nextID++
		n := newInstance(fmt.Sprintf("%s-i%d", p.rev.ID, p.nextID), p.rev, p.log, p.now)
		p.instances = append(p.instances, n)
		p.log("[pool %s] scale-up: instances=%d", p.rev.ID, len(p.instances))
		if err := n.tryAcquire(); err != nil { // 新实例必成功
			panic(err)
		}
		return n, false, nil
	}
	return nil, true, nil
}

// tick 回收空闲超时的实例（可缩到零）。
func (p *pool) tick() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	keep := p.instances[:0]
	for _, in := range p.instances {
		in.mu.Lock()
		idle := in.inflight == 0 && now.Sub(in.lastIdle) >= p.rev.Config.IdleTimeout
		in.mu.Unlock()
		if idle {
			in.kill()
			p.log("[pool %s] scale-down: reclaim %s, instances=%d", p.rev.ID, in.id, len(p.instances)-1)
		} else {
			keep = append(keep, in)
		}
	}
	p.instances = keep
	if len(p.instances) == 0 {
		p.log("[pool %s] scaled to zero", p.rev.ID)
	}
}

// drain 整池排空：不再接收新流量，等待在途请求完成或排空超时后回收全部实例。
func (p *pool) drain() {
	p.mu.Lock()
	if p.draining {
		p.mu.Unlock()
		return
	}
	p.draining = true
	ins := append([]*instance(nil), p.instances...)
	p.mu.Unlock()

	p.log("[pool %s] draining %d instance(s)", p.rev.ID, len(ins))
	var wg sync.WaitGroup
	for _, in := range ins {
		wg.Add(1)
		go func(in *instance) {
			defer wg.Done()
			dctx, cancel := context.WithTimeout(context.Background(), p.rev.Config.DrainTimeout)
			defer cancel()
			in.startDrain(dctx)
			<-in.drained // 在途归零（正常完成或排空超时取消后释放）
			in.kill()
		}(in)
	}
	wg.Wait()

	p.mu.Lock()
	p.instances = nil
	p.mu.Unlock()
	p.log("[pool %s] drained to zero", p.rev.ID)
}

func (p *pool) stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := PoolStats{Queued: len(p.queue)}
	for _, in := range p.instances {
		in.mu.Lock()
		s.Instances++
		s.InFlight += in.inflight
		switch in.state {
		case stateActive:
			s.Active++
		case stateDraining:
			s.Draining++
		}
		in.mu.Unlock()
	}
	return s
}

func (p *pool) isDraining() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.draining
}

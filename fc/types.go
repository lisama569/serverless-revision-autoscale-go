package fc

import (
	"context"
	"time"
)

// RevisionID 是版本的不可变可寻址标识。
type RevisionID string

// Request 是一次函数调用。
type Request struct {
	Payload string
	// RouteKey 为业务路由键：非空时启用粘性路由，同一策略下同一键始终落到同一版本；
	// 为空时按权重比例分配。
	RouteKey string
}

// Response 是函数调用的结果，Attribution 记录请求归因信息。
type Response struct {
	Revision   RevisionID // 实际处理请求的版本
	InstanceID string     // 实际处理请求的实例
	Body       string
}

// Handler 是用户函数。ctx 取消语义：调用超时 / 排空超时 / 上游取消。
type Handler func(ctx context.Context, req *Request) (string, error)

// RevisionConfig 是版本的不可变配置。
type RevisionConfig struct {
	MaxConcurrency int           // 单实例最大并发
	MaxInstances   int           // 该版本实例上限
	QueueSize      int           // 该版本排队队列上限（超出即拒绝）
	InvokeTimeout  time.Duration // 单次调用超时
	DrainTimeout   time.Duration // 实例排空超时
	IdleTimeout    time.Duration // 无流量缩容到零的空闲时长
	MinInstances   int           // 保底常驻实例数（>0 时缩容不低于该值；默认可缩到零）
}

func (c RevisionConfig) withDefaults() RevisionConfig {
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 10
	}
	if c.MaxInstances <= 0 {
		c.MaxInstances = 4
	}
	if c.QueueSize < 0 {
		c.QueueSize = 0
	}
	if c.InvokeTimeout <= 0 {
		c.InvokeTimeout = 5 * time.Second
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 2 * time.Second
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 100 * time.Millisecond
	}
	if c.MinInstances < 0 {
		c.MinInstances = 0
	}
	if c.MinInstances > c.MaxInstances {
		c.MinInstances = c.MaxInstances
	}
	return c
}

// Revision 是一次发布产生的不可变版本。
type Revision struct {
	ID        RevisionID
	Function  string
	Handler   Handler
	Config    RevisionConfig
	CreatedAt time.Time
}

// TrafficPolicy 是流量权重分配，整体原子生效。
type TrafficPolicy struct {
	Weights map[RevisionID]int // 非负整数，至少一个为正，总和任意（按比例归一化）
	Epoch   uint64             // 产生该策略时的函数纪元
}

// RolloutPlan 描述一次渐进放量计划：从当前权重出发，经 Steps 个等距中间步，
// 每累计 RequestsPerStep 个已路由请求整体推进到下一步，最终权重精确等于 Target。
//
// 每一步都通过普通权重切换原子生效；计划走完后的权重与直接 UpdateTraffic(Target)
// 完全一致。放量节奏只取决于请求计数，与时间、并发、随机数无关，因而可精确复现。
type RolloutPlan struct {
	Target          map[RevisionID]int // 目标权重（与 UpdateTraffic 同样的校验规则）
	Steps           int                // 放量步数（>0）；含最终步，共产生 Steps 次权重切换
	RequestsPerStep int                // 每步之间路由的请求数（>0）
}

// RolloutPhase 标识渐进放量计划所处阶段。
type RolloutPhase string

const (
	RolloutIdle     RolloutPhase = "idle"     // 没有进行中的放量计划
	RolloutRunning  RolloutPhase = "running"  // 计划进行中
	RolloutComplete RolloutPhase = "complete" // 计划已走完（终态权重即目标权重）
	RolloutCanceled RolloutPhase = "canceled" // 被取消或被并发流量变更打断，停在自洽权重上
)

// RolloutStatus 是渐进放量计划的可观测快照。
type RolloutStatus struct {
	Phase           RolloutPhase
	Step            int                // 当前已生效的步序号（0..Steps）；0 表示尚未发生第一步切换
	Steps           int                // 计划总步数
	RequestsInStep  int                // 当前步已累计的路由请求数
	RequestsPerStep int                // 每步请求数
	CurrentWeights  map[RevisionID]int // 当前实际生效权重（始终自洽）
	TargetWeights   map[RevisionID]int // 计划目标权重
}

// Logger 用于测试与本地运行时打印关键状态变化。
type Logger func(format string, args ...any)

func noopLogger(string, ...any) {}

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
	// RoutingKey 是业务路由键：非空时按键做确定性粘性路由，
	// 同一键在流量策略不变的前提下始终落到同一版本；为空时按权重轮转分配。
	RoutingKey string
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
	// MinInstances 是保底常驻实例数：版本持有流量期间，即使空闲也保留
	// 这么多实例（避免冷启动）；超出部分空闲后照常回收。默认 0，行为与之前一致。
	// 版本失去流量进入排空时，保底实例一并回收。
	MinInstances int
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

// Logger 用于测试与本地运行时打印关键状态变化。
type Logger func(format string, args ...any)

func noopLogger(string, ...any) {}

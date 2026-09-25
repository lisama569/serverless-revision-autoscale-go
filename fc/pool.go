package fc

import "time"

// PoolStats 描述一个版本池的运行状态。
type PoolStats struct {
	Instances int // 存活实例数（含排空中）
	Active    int // 接收新流量的实例数
	Draining  int // 排空中的实例数
	InFlight  int // 在途请求数
	Queued    int // 排队中的请求数
}

// pool 是单个版本的实例池，负责按需伸缩、排队背压与排空。
type pool struct {
	rev *Revision
	log Logger
	now func() time.Time
}

func newPool(rev *Revision, log Logger, now func() time.Time) *pool {
	return &pool{rev: rev, log: log, now: now}
}

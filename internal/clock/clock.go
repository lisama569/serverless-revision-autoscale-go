// Package clock 提供时间抽象，便于在测试中用假时钟驱动伸缩与排空逻辑。
package clock

import "time"

// Clock 是控制面依赖的最小时间接口。
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTimer(d time.Duration) *Timer
}

// Timer 是可停止、可重置的定时器句柄。
type Timer struct {
	C    <-chan time.Time
	send chan time.Time // 仅假时钟使用：底层可发送通道

	// 真时钟使用
	timer *time.Timer

	// 假时钟使用
	clock   *FakeClock
	fireAt  time.Time
	stopped bool
}

// Stop 停止定时器；返回是否在触发前成功停止（语义与 time.Timer.Stop 一致）。
func (t *Timer) Stop() bool {
	if t.timer != nil {
		return t.timer.Stop()
	}
	return t.clock.stopTimer(t)
}

// Reset 将定时器重置为 d 后触发，返回是否在触发前重置。
func (t *Timer) Reset(d time.Duration) bool {
	if t.timer != nil {
		return t.timer.Reset(d)
	}
	return t.clock.resetTimer(t, d)
}

// RealClock 包装系统时钟。
type RealClock struct{}

// Now 返回当前系统时间。
func (RealClock) Now() time.Time { return time.Now() }

// After 返回 d 后的时间通道。
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTimer 创建真实定时器。
func (RealClock) NewTimer(d time.Duration) *Timer {
	tm := time.NewTimer(d)
	return &Timer{C: tm.C, send: nil, timer: tm}
}

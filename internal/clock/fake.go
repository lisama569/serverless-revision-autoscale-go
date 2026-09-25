package clock

import (
	"sort"
	"sync"
	"time"
)

type waiter struct {
	fireAt time.Time
	timer  *Timer
}

// FakeClock 是测试用可控时钟：时间仅在调用 Advance 时前进，
// 到期的定时器会在 Advance 内同步触发（先按到期时刻排序）。
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

// NewFakeClock 创建从 start 时刻开始的假时钟。
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now 返回假时钟当前时间。
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After 是 NewTimer(d).C 的便捷形式。
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	return c.NewTimer(d).C
}

// NewTimer 创建一个由假时钟调度的定时器。
func (c *FakeClock) NewTimer(d time.Duration) *Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.newTimerLocked(d)
}

func (c *FakeClock) newTimerLocked(d time.Duration) *Timer {
	ch := make(chan time.Time, 1)
	t := &Timer{C: ch, send: ch, fireAt: c.now.Add(d), clock: c}
	if d <= 0 {
		// 与 time.NewTimer(0) 语义一致：立即到期，不进入待调度集合。
		ch <- c.now
		t.stopped = true
		return t
	}
	c.waiters = append(c.waiters, &waiter{fireAt: t.fireAt, timer: t})
	return t
}

// stopTimer 从待触发集合移除并标记停止；已触发/已停止返回 false。
func (c *FakeClock) stopTimer(t *Timer) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.stopped {
		return false
	}
	for i, w := range c.waiters {
		if w.timer == t {
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			t.stopped = true
			return true
		}
	}
	// 已被 Advance 取出或已触发：按语义停止无效。
	t.stopped = true
	return false
}

// resetTimer 将定时器重新调度到 d 后；返回是否在触发/停止前重置成功。
func (c *FakeClock) resetTimer(t *Timer, d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	wasActive := false
	for i, w := range c.waiters {
		if w.timer == t {
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			wasActive = true
			break
		}
	}
	// 排空通道中可能存在的旧值。
	select {
	case <-t.C:
	default:
	}
	t.stopped = false
	t.fireAt = c.now.Add(d)
	c.waiters = append(c.waiters, &waiter{fireAt: t.fireAt, timer: t})
	return wasActive
}

// Advance 将时间前进 d，同步触发所有到期定时器（按到期时刻、注册顺序）。
// 若某定时器通道已有未读值则跳过，不重复投递。
func (c *FakeClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	end := c.now.Add(d)
	for {
		c.mu.Lock()
		sort.SliceStable(c.waiters, func(i, j int) bool {
			return c.waiters[i].fireAt.Before(c.waiters[j].fireAt)
		})
		var next *waiter
		if len(c.waiters) > 0 && !c.waiters[0].fireAt.After(end) {
			next = c.waiters[0]
			c.waiters = c.waiters[1:]
		}
		if next == nil {
			c.now = end
			c.mu.Unlock()
			return
		}
		c.now = next.fireAt
		timer := next.timer
		timer.stopped = true // 已从待触发集合取出
		c.mu.Unlock()
		// 非阻塞投递：调用方的 timer.C 均为容量 1。
		select {
		case timer.send <- c.now:
		default:
		}
	}
}

// WaitUntilScheduled 阻塞直到至少 n 个待触发定时器完成注册（真实时间内轮询）。
// 用于测试消除「定时器注册」与 Advance 之间的竞态。
func (c *FakeClock) WaitUntilScheduled(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := len(c.waiters)
		c.mu.Unlock()
		if got >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// DebugCountWaiters 返回待触发定时器数量（测试辅助）。
func (c *FakeClock) DebugCountWaiters() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

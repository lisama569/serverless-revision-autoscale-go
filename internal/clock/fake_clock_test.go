package clock

import (
	"testing"
	"time"
)

func TestFakeTimerStopAndFire(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	tm := c.NewTimer(50 * time.Millisecond)
	if !c.WaitUntilScheduled(1, time.Second) {
		t.Fatal("timer not scheduled")
	}
	c.Advance(50 * time.Millisecond)
	select {
	case <-tm.C:
	default:
		t.Fatal("timer did not fire")
	}
}

func TestFakeTimerZeroFiresImmediately(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	tm := c.NewTimer(0)
	select {
	case <-tm.C:
	default:
		t.Fatal("zero timer should fire immediately")
	}
	c.Advance(time.Second) // 不应有残留定时器
}

func TestFakeTimerStopPreventsFire(t *testing.T) {
	c := NewFakeClock(time.Unix(0, 0))
	tm := c.NewTimer(10 * time.Millisecond)
	if !c.WaitUntilScheduled(1, time.Second) {
		t.Fatal("not scheduled")
	}
	if !tm.Stop() {
		t.Fatal("Stop should report true")
	}
	c.Advance(time.Hour)
	select {
	case <-tm.C:
		t.Fatal("stopped timer fired")
	default:
	}
}

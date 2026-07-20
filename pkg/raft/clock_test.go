package raft

import (
	"testing"
	"time"
)

// TestRealClockNowIsMonotonic verifies that realClock.Now() returns
// monotonically non-decreasing values across successive calls.
func TestRealClockNowIsMonotonic(t *testing.T) {
	c := realClock{}
	t1 := c.Now()
	t2 := c.Now()
	if t2.Before(t1) {
		t.Fatalf("clock went backwards: t1=%v t2=%v", t1, t2)
	}
}

// TestRealTickerDeliversTick verifies that newRealTicker delivers at least one
// tick within twice the requested interval.
func TestRealTickerDeliversTick(t *testing.T) {
	const interval = 10 * time.Millisecond
	tk := newRealTicker(interval)
	defer tk.Stop()

	select {
	case got := <-tk.C():
		if got.IsZero() {
			t.Fatal("tick time is zero")
		}
	case <-time.After(2 * interval):
		t.Fatal("no tick received within 2x interval")
	}
}

// TestRealTickerReset verifies that Reset causes the ticker to fire again.
func TestRealTickerReset(t *testing.T) {
	const interval = 10 * time.Millisecond
	tk := newRealTicker(interval)
	defer tk.Stop()

	// Drain one tick.
	select {
	case <-tk.C():
	case <-time.After(2 * interval):
		t.Fatal("no first tick")
	}

	tk.Reset(interval)

	// Expect another tick after reset.
	select {
	case <-tk.C():
	case <-time.After(2 * interval):
		t.Fatal("no tick after Reset")
	}
}

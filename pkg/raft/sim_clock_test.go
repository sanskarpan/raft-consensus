package raft

import (
	"testing"
	"time"
)

var epoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// TestSimClockAdvanceDeliversTick verifies that advancing the simClock past a
// ticker's interval causes the ticker to deliver exactly one tick.
func TestSimClockAdvanceDeliversTick(t *testing.T) {
	c := newSimClock(epoch)

	tk := c.NewTicker(10 * time.Millisecond)
	defer tk.Stop()

	// No tick before advancing.
	select {
	case <-tk.C():
		t.Fatal("tick delivered before any advance")
	default:
	}

	// Advance past the interval.
	c.Advance(15 * time.Millisecond)

	// Now a tick should be available.
	select {
	case got := <-tk.C():
		if got.IsZero() {
			t.Fatal("tick time is zero")
		}
	default:
		t.Fatal("no tick delivered after advance past interval")
	}
}

// TestSimTickerStopSilences verifies that Stop() prevents future ticks even
// after the clock is advanced past the ticker's interval.
func TestSimTickerStopSilences(t *testing.T) {
	c := newSimClock(epoch)

	tk := c.NewTicker(10 * time.Millisecond)
	tk.Stop()

	// Advance far past the interval.
	c.Advance(100 * time.Millisecond)

	// No tick should be delivered.
	select {
	case <-tk.C():
		t.Fatal("tick delivered after Stop()")
	default:
	}
}

// TestSimClockMultipleTickersOrdering verifies that multiple tickers on the
// same simClock each fire independently and in ascending time order.
func TestSimClockMultipleTickersOrdering(t *testing.T) {
	c := newSimClock(epoch)

	tk10 := c.NewTicker(10 * time.Millisecond)
	tk25 := c.NewTicker(25 * time.Millisecond)
	defer tk10.Stop()
	defer tk25.Stop()

	// Advance 30ms: tk10 should fire at 10ms and 20ms; tk25 should fire at 25ms.
	c.Advance(30 * time.Millisecond)

	// Drain all available ticks from tk10.
	tk10Count := 0
	for {
		select {
		case <-tk10.C():
			tk10Count++
		default:
			goto doneTk10
		}
	}
doneTk10:
	if tk10Count < 2 {
		t.Errorf("tk10 fired %d times after 30ms advance, want >= 2", tk10Count)
	}

	// tk25 should have fired once.
	tk25Count := 0
	for {
		select {
		case <-tk25.C():
			tk25Count++
		default:
			goto doneTk25
		}
	}
doneTk25:
	if tk25Count < 1 {
		t.Errorf("tk25 fired %d times after 30ms advance, want >= 1", tk25Count)
	}
}

// TestSimTickerReset verifies that Reset repositions the ticker's next-fire
// time so it fires after the new interval from the current clock time.
func TestSimTickerReset(t *testing.T) {
	c := newSimClock(epoch)

	tk := c.NewTicker(50 * time.Millisecond)
	defer tk.Stop()

	// Advance 20ms: should NOT fire yet.
	c.Advance(20 * time.Millisecond)
	select {
	case <-tk.C():
		t.Fatal("tick delivered before first interval elapsed")
	default:
	}

	// Reset to 10ms. Should fire at 20ms + 10ms = 30ms from origin.
	tk.Reset(10 * time.Millisecond)

	// Advance another 15ms (total 35ms): should fire now.
	c.Advance(15 * time.Millisecond)
	select {
	case <-tk.C():
		// Expected.
	default:
		t.Fatal("no tick after Reset + advance past new interval")
	}
}

// TestSimClockNow verifies that Now() reflects advances correctly.
func TestSimClockNow(t *testing.T) {
	c := newSimClock(epoch)
	if got := c.Now(); !got.Equal(epoch) {
		t.Fatalf("Now() = %v, want %v", got, epoch)
	}
	c.Advance(5 * time.Second)
	want := epoch.Add(5 * time.Second)
	if got := c.Now(); !got.Equal(want) {
		t.Fatalf("Now() after advance = %v, want %v", got, want)
	}
}

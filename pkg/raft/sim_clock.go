package raft

import (
	"sync"
	"time"
)

// simClock is a manually-advanceable deterministic clock for simulation tests.
// Advancing the clock wakes any simTickers whose interval has elapsed.
type simClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*simTicker
}

// newSimClock creates a new simClock starting at t.
func newSimClock(t time.Time) *simClock {
	return &simClock{now: t}
}

// Now returns the current simulated time. Implements Clock.
func (c *simClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d and delivers ticks to any simTickers
// whose next-fire time has been reached. Each ticker may fire multiple times
// if d spans multiple intervals.
func (c *simClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now

	// Collect pending ticks for each ticker. We release the lock before sending
	// on channels to avoid a deadlock if a goroutine reads from the ticker's
	// channel while also holding c.mu.
	type tickWork struct {
		tk    *simTicker
		times []time.Time
	}
	var work []tickWork

	for _, tk := range c.tickers {
		tk.mu.Lock()
		if tk.stopped {
			tk.mu.Unlock()
			continue
		}
		var times []time.Time
		for !tk.next.After(now) {
			times = append(times, tk.next)
			tk.next = tk.next.Add(tk.interval)
		}
		tk.mu.Unlock()
		if len(times) > 0 {
			work = append(work, tickWork{tk, times})
		}
	}
	c.mu.Unlock()

	// Send ticks outside the clock lock.
	for _, w := range work {
		for _, t := range w.times {
			w.tk.mu.Lock()
			stopped := w.tk.stopped
			w.tk.mu.Unlock()
			if stopped {
				break
			}
			// Non-blocking send: if the channel is full, drop the tick (matches
			// real time.Ticker behaviour which also drops ticks on a slow reader).
			select {
			case w.tk.ch <- t:
			default:
			}
		}
	}
}

// NewTicker creates a simTicker driven by this clock that fires every d when
// Advance is called past the interval boundary.
func (c *simClock) NewTicker(d time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	tk := &simTicker{
		ch:       make(chan time.Time, 16),
		interval: d,
		next:     c.now.Add(d),
		clock:    c,
	}
	c.tickers = append(c.tickers, tk)
	return tk
}

// removeTicker unregisters a stopped ticker so it no longer receives advances.
func (c *simClock) removeTicker(tk *simTicker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, t := range c.tickers {
		if t == tk {
			c.tickers = append(c.tickers[:i], c.tickers[i+1:]...)
			return
		}
	}
}

// simTicker is a deterministic ticker driven by simClock.Advance.
type simTicker struct {
	mu       sync.Mutex
	ch       chan time.Time
	interval time.Duration
	next     time.Time
	stopped  bool
	clock    *simClock // set by NewTicker to support Reset + removeTicker
}

// C returns the tick channel. Implements Ticker.
func (t *simTicker) C() <-chan time.Time { return t.ch }

// Stop halts the ticker so no further ticks are delivered. Implements Ticker.
func (t *simTicker) Stop() {
	t.mu.Lock()
	already := t.stopped
	t.stopped = true
	t.mu.Unlock()
	if !already && t.clock != nil {
		t.clock.removeTicker(t)
	}
}

// Reset repositions the ticker's next-fire time to clock.Now() + d and
// re-registers it if it was previously stopped. Implements Ticker.
func (t *simTicker) Reset(d time.Duration) {
	t.mu.Lock()
	wasStopped := t.stopped
	t.stopped = false
	t.interval = d
	if t.clock != nil {
		now := t.clock.Now()
		t.next = now.Add(d)
	}
	t.mu.Unlock()

	if wasStopped && t.clock != nil {
		// Re-register with the clock.
		t.clock.mu.Lock()
		t.clock.tickers = append(t.clock.tickers, t)
		t.clock.mu.Unlock()
	}
}

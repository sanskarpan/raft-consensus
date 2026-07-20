package raft

import "time"

// Clock abstracts time.Now() for deterministic simulation.
type Clock interface {
	Now() time.Time
}

// Ticker abstracts time.Ticker for deterministic simulation.
type Ticker interface {
	C() <-chan time.Time
	Stop()
	Reset(d time.Duration)
}

// realClock wraps the real wall clock.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// realTicker wraps time.Ticker.
type realTicker struct{ t *time.Ticker }

func newRealTicker(d time.Duration) Ticker { return &realTicker{time.NewTicker(d)} }

func (r *realTicker) C() <-chan time.Time       { return r.t.C }
func (r *realTicker) Stop()                     { r.t.Stop() }
func (r *realTicker) Reset(d time.Duration)     { r.t.Reset(d) }

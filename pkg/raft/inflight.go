package raft

// inflightWindow tracks in-flight AppendEntries batches for one follower.
// Each slot stores the "last index" of a sent-but-not-yet-acked batch.
// cap = config.MaxInflight (default 256).
//
// The ring-buffer uses two cursors:
//   head — next write position (post-increment on add)
//   tail — next read position / oldest in-flight entry (post-increment on ack)
//
// Invariant: size == (head - tail + cap) % cap, where cap is the slice length.
type inflightWindow struct {
	buf  []uint64
	head int // next write slot
	tail int // oldest in-flight slot (next read)
	size int
	cap  int
}

// newInflightWindow creates an empty inflightWindow with the given capacity.
func newInflightWindow(cap int) *inflightWindow {
	if cap <= 0 {
		cap = 1
	}
	return &inflightWindow{
		buf: make([]uint64, cap),
		cap: cap,
	}
}

// full reports whether the window has no free slots.
func (w *inflightWindow) full() bool { return w.size == w.cap }

// empty reports whether there are no in-flight batches.
func (w *inflightWindow) empty() bool { return w.size == 0 }

// count returns the number of in-flight batches.
func (w *inflightWindow) count() int { return w.size }

// add records a newly sent batch whose last log index is lastIdx.
// It panics if the window is full (caller must check full() first).
func (w *inflightWindow) add(lastIdx uint64) {
	if w.full() {
		panic("inflightWindow: add on full window")
	}
	w.buf[w.head] = lastIdx
	w.head = (w.head + 1) % w.cap
	w.size++
}

// ack removes all in-flight entries whose last index is <= matchIdx.
// These correspond to batches that the follower has acknowledged.
func (w *inflightWindow) ack(matchIdx uint64) {
	for w.size > 0 && w.buf[w.tail] <= matchIdx {
		w.tail = (w.tail + 1) % w.cap
		w.size--
	}
}

// reset clears all in-flight entries (used on rejection or snapshot).
func (w *inflightWindow) reset() {
	w.head = 0
	w.tail = 0
	w.size = 0
}

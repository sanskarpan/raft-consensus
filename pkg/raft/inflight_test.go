package raft

import (
	"testing"
)

// TestInflightWindowBasic verifies the fundamental ring-buffer semantics.
func TestInflightWindowBasic(t *testing.T) {
	w := newInflightWindow(4)

	if !w.empty() {
		t.Fatal("new window should be empty")
	}
	if w.full() {
		t.Fatal("new window should not be full")
	}
	if w.count() != 0 {
		t.Fatalf("count = %d, want 0", w.count())
	}

	// Add slots until full.
	w.add(10)
	w.add(20)
	w.add(30)
	w.add(40)

	if !w.full() {
		t.Fatal("window should be full after 4 adds into cap-4 window")
	}
	if w.count() != 4 {
		t.Fatalf("count = %d, want 4", w.count())
	}
	if w.empty() {
		t.Fatal("full window should not be empty")
	}
}

// TestInflightWindowAddPanicsWhenFull verifies that add() panics on overflow.
func TestInflightWindowAddPanicsWhenFull(t *testing.T) {
	w := newInflightWindow(2)
	w.add(1)
	w.add(2)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when adding to a full window")
		}
	}()
	w.add(3) // must panic
}

// TestInflightWindowAck verifies that ack() removes entries <= matchIdx.
func TestInflightWindowAck(t *testing.T) {
	w := newInflightWindow(8)
	w.add(5)
	w.add(10)
	w.add(15)
	w.add(20)

	// Ack up to 10: removes entries with lastIdx <= 10 (i.e., 5 and 10).
	w.ack(10)

	if w.count() != 2 {
		t.Fatalf("after ack(10) count = %d, want 2", w.count())
	}

	// Ack beyond current max: all entries removed.
	w.ack(100)
	if !w.empty() {
		t.Fatalf("after ack(100) window should be empty, count = %d", w.count())
	}
}

// TestInflightWindowReset clears all entries.
func TestInflightWindowReset(t *testing.T) {
	w := newInflightWindow(4)
	w.add(1)
	w.add(2)
	w.reset()
	if !w.empty() {
		t.Fatal("window should be empty after reset")
	}
	if w.count() != 0 {
		t.Fatalf("count = %d after reset, want 0", w.count())
	}
}

// TestInflightWindowWrap verifies correct behavior when the ring buffer wraps.
func TestInflightWindowWrap(t *testing.T) {
	w := newInflightWindow(4)

	// Fill then partially ack to advance tail.
	w.add(1)
	w.add(2)
	w.add(3)
	w.add(4)
	w.ack(2) // frees slots 1 and 2, tail advances

	// Now add 2 more — these wrap around the ring.
	w.add(5)
	w.add(6)

	if w.count() != 4 {
		t.Fatalf("count = %d after wrap, want 4", w.count())
	}
	if !w.full() {
		t.Fatal("window should be full")
	}

	// Ack everything.
	w.ack(6)
	if !w.empty() {
		t.Fatal("window should be empty after acking all")
	}
}

// TestInflightWindowAckNoOp verifies that ack() on an empty window is safe.
func TestInflightWindowAckNoOp(t *testing.T) {
	w := newInflightWindow(4)
	w.ack(999) // must not panic
	if !w.empty() {
		t.Fatal("empty window should stay empty after ack")
	}
}

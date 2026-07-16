package raft

import (
	"context"
	"testing"
	"time"
)

// singleNodeConfig returns a one-voter configuration for the given id.
func singleNodeConfig(id string) Configuration {
	return Configuration{
		Servers: []Server{{ID: ServerID(id), Address: ServerAddress("mem://" + id)}},
	}
}

// setApplied simulates the apply path advancing applyIndex and notifying
// WaitApplied waiters, exactly as applyCommitted does (set r.applyIndex under
// r.mu, then notifyApplied outside r.mu).
func setApplied(r *raft, idx uint64) {
	r.mu.Lock()
	r.applyIndex = idx
	r.mu.Unlock()
	r.notifyApplied(idx)
}

// TestWaitAppliedReturnsImmediatelyIfAlreadyApplied verifies the fast path.
func TestWaitAppliedReturnsImmediatelyIfAlreadyApplied(t *testing.T) {
	r, _, _ := makeRaftNode("n1", singleNodeConfig("n1"))
	setApplied(r, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := r.WaitApplied(ctx, 5); err != nil {
		t.Fatalf("WaitApplied for already-applied index: %v", err)
	}
	if err := r.WaitApplied(ctx, 10); err != nil {
		t.Fatalf("WaitApplied for exactly-applied index: %v", err)
	}
}

// TestWaitAppliedUnblocksOnApply verifies that a blocked waiter is woken (without
// busy-polling) when applyIndex advances to or past the target.
func TestWaitAppliedUnblocksOnApply(t *testing.T) {
	r, _, _ := makeRaftNode("n1", singleNodeConfig("n1"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.WaitApplied(ctx, 7)
	}()

	// Give the waiter time to register and block on the cond.
	time.Sleep(20 * time.Millisecond)

	// Advance below the target first — must NOT unblock.
	setApplied(r, 6)
	select {
	case err := <-errCh:
		t.Fatalf("WaitApplied returned early at applyIndex=6: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	// Reach the target — must unblock.
	setApplied(r, 7)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("WaitApplied returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitApplied did not unblock after applyIndex reached target")
	}
}

// TestWaitAppliedUnblocksWhenApplyOvershoots verifies a single advance past the
// target wakes the waiter.
func TestWaitAppliedUnblocksWhenApplyOvershoots(t *testing.T) {
	r, _, _ := makeRaftNode("n1", singleNodeConfig("n1"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.WaitApplied(ctx, 5)
	}()
	time.Sleep(20 * time.Millisecond)

	setApplied(r, 100) // overshoot

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("WaitApplied returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitApplied did not unblock on overshoot")
	}
}

// TestWaitAppliedRespectsContext verifies the waiter returns ctx.Err() when the
// context is cancelled before the target index is applied — and does not hang.
func TestWaitAppliedRespectsContext(t *testing.T) {
	r, _, _ := makeRaftNode("n1", singleNodeConfig("n1"))

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.WaitApplied(ctx, 42)
	}()
	time.Sleep(20 * time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitApplied did not return after context cancellation")
	}
}

// TestWaitAppliedContextDeadline verifies a deadline-based context returns
// DeadlineExceeded when the index is never applied.
func TestWaitAppliedContextDeadline(t *testing.T) {
	r, _, _ := makeRaftNode("n1", singleNodeConfig("n1"))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := r.WaitApplied(ctx, 99)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("WaitApplied took too long to observe deadline: %v", elapsed)
	}
}

// TestWaitAppliedConcurrentWaiters verifies multiple waiters for different
// indices are all woken correctly by broadcast.
func TestWaitAppliedConcurrentWaiters(t *testing.T) {
	r, _, _ := makeRaftNode("n1", singleNodeConfig("n1"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	const n = 8
	results := make(chan error, n)
	for i := 1; i <= n; i++ {
		target := uint64(i)
		go func() {
			results <- r.WaitApplied(ctx, target)
		}()
	}
	time.Sleep(30 * time.Millisecond)

	setApplied(r, n) // wake all of them at once

	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("waiter returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("not all waiters unblocked (%d/%d done)", i, n)
		}
	}
}

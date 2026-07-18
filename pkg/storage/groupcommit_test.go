package storage

import (
	"sync"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// TestGroupCommitCoalesces verifies that many concurrent syncs for a single
// pending write share far fewer fsyncs than callers (group commit, #201).
func TestGroupCommitCoalesces(t *testing.T) {
	w, err := NewWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// One real entry so a current segment exists to fsync.
	if err := w.Append([]*raft.LogEntry{{Index: 1, Term: 1, Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}

	// Model a single un-synced pending write and have many goroutines request a
	// durable sync of it concurrently.
	target := w.writeSeq.Add(1)
	before := w.syncCount.Load()

	const callers = 30
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.groupSync(target); err != nil {
				t.Errorf("groupSync: %v", err)
			}
		}()
	}
	wg.Wait()

	n := w.syncCount.Load() - before
	if n == 0 {
		t.Fatal("expected at least one fsync")
	}
	if n > 6 {
		t.Fatalf("group commit failed to coalesce: %d fsyncs for %d concurrent syncs of one write", n, callers)
	}
	// The write must be durably marked synced for all callers.
	if w.syncedSeq < target {
		t.Fatalf("syncedSeq=%d < target=%d after group sync", w.syncedSeq, target)
	}
}

// TestGroupCommitStillDurableAndReadable verifies serial Appends remain durable
// and readable through the group-commit path.
func TestGroupCommitStillDurableAndReadable(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 50; i++ {
		if err := w.Append([]*raft.LogEntry{{Index: i, Term: 1, Data: []byte("v")}}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	w.Close()

	// Reopen and verify all 50 entries recovered.
	w2, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if last, _ := w2.LastIndex(); last != 50 {
		t.Fatalf("LastIndex after reopen = %d, want 50", last)
	}
}

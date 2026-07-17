package storage

import (
	"path/filepath"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// C1: Append must fsync the segment to disk before returning, otherwise a
// committed entry can be lost on power failure.
func TestAppendFsyncsBeforeReturning(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	before := w.syncCount.Load()
	if err := w.Append([]*raft.LogEntry{{Index: 1, Term: 1, Data: []byte("a")}}); err != nil {
		t.Fatal(err)
	}
	if got := w.syncCount.Load(); got <= before {
		t.Fatalf("Append did not fsync: syncCount before=%d after=%d", before, got)
	}
}

// C1: A durable store must surface fsync failures. If fsync fails, Append must
// return an error rather than silently reporting success (false durability).
func TestAppendPropagatesSyncError(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.failSync.Store(true)
	if err := w.Append([]*raft.LogEntry{{Index: 1, Term: 1, Data: []byte("a")}}); err == nil {
		t.Fatal("expected Append to return an error when fsync fails")
	}
}

// C2: StableStore.Sync must actually flush the underlying database, not be a
// no-op. Observable proxy: syncing a closed store must error (it talks to the db).
func TestStableStoreSyncActuallySyncs(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStableStore(filepath.Join(dir, "stable.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync on an open store should succeed: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err == nil {
		t.Fatal("expected Sync to error after Close (proving it flushes the real db)")
	}
}

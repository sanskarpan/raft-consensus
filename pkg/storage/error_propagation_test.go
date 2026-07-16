package storage

import (
	"testing"

	"github.com/raft-consensus/pkg/raft"
)

// M16: snapshot sink Cancel must surface a Close failure instead of silently
// discarding it. We provoke a Close error by closing the underlying file first,
// so the sink's own Close returns "file already closed".
func TestSnapshotSinkCancelReturnsCloseError(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := store.Create(raft.SnapshotVersionMax, 1, 1, raft.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	fs := sink.(*fileSnapshotSink)

	// Close the file out from under the sink so Cancel's Close() errors.
	if err := fs.file.Close(); err != nil {
		t.Fatal(err)
	}

	if err := sink.Cancel(); err == nil {
		t.Fatal("Cancel returned nil despite an underlying Close error; want non-nil")
	}
}

// M16: WAL.Close must surface a Close failure. We close a segment file out from
// under the WAL so that WAL.Close's file.Close() returns an error, which must
// be propagated rather than swallowed.
func TestWALCloseReturnsFirstError(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]*raft.LogEntry{
		{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte("x")},
	}); err != nil {
		t.Fatal(err)
	}

	// Close the current segment's file directly; WAL.Close will then get an
	// "already closed" error when it tries to close it again.
	if err := w.currentSegment.file.Close(); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err == nil {
		t.Fatal("WAL.Close returned nil despite an underlying Close error; want non-nil")
	}
}

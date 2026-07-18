package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// TestSnapshotCompressionRoundTrip verifies a gzip-compressed snapshot restores
// byte-identically after a fresh store reload, and is smaller on disk.
func TestSnapshotCompressionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	store.SetCompression(true)

	payload := bytes.Repeat([]byte("compress this snapshot payload "), 8000) // ~248 KiB, compressible
	sink, err := store.Create(1, 10, 5, raft.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("sink.Close: %v", err)
	}
	id := sink.ID()

	// On-disk file must be substantially smaller than the raw payload.
	fi, err := os.Stat(filepath.Join(dir, "snapshots", id+".snap"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() >= int64(len(payload)) {
		t.Fatalf("compressed snapshot is %d bytes, not smaller than payload %d", fi.Size(), len(payload))
	}

	// Reopen the store from disk (loads compression from the sidecar) and read.
	store2, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	snap, _, err := store2.Open(id)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	r := snap.Reader()
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("decompressed snapshot mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestSnapshotUncompressedStillWorks guards the default (no compression) path.
func TestSnapshotUncompressedStillWorks(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewFileSnapshotStore(dir, 3)
	payload := []byte("plain snapshot data")
	sink, _ := store.Create(1, 3, 2, raft.Configuration{})
	sink.Write(payload)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	snap, _, err := store.Open(sink.ID())
	if err != nil {
		t.Fatal(err)
	}
	r := snap.Reader()
	got, _ := io.ReadAll(r)
	r.Close()
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

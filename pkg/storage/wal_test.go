package storage

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/raft-consensus/pkg/raft"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// --- encodeRecord / nextRecord round-trip --------------------------------

func TestEncodeRecordRoundTrip(t *testing.T) {
	cases := []logRecord{
		{term: 1, index: 1, recordTy: 0, data: []byte("hello")},
		{term: 99, index: 42, recordTy: 1, data: []byte{}},
		{term: 1<<32 - 1, index: 1<<63 - 1, recordTy: 2, data: make([]byte, 1024)},
	}

	for _, rec := range cases {
		data, err := encodeRecord(&rec)
		if err != nil {
			t.Fatalf("encodeRecord: %v", err)
		}

		// Write to a temp file and read back.
		tmp, err := os.CreateTemp("", "rec-*.bin")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmp.Name())

		if _, err := tmp.Write(data); err != nil {
			t.Fatal(err)
		}
		if _, err := tmp.Seek(0, 0); err != nil {
			t.Fatal(err)
		}

		r, err := newSegmentReader(tmp)
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.nextRecord()
		if err != nil {
			t.Fatalf("nextRecord: %v", err)
		}

		if got.term != rec.term {
			t.Errorf("term: got %d, want %d", got.term, rec.term)
		}
		if got.index != rec.index {
			t.Errorf("index: got %d, want %d", got.index, rec.index)
		}
		if got.recordTy != rec.recordTy {
			t.Errorf("recordTy: got %d, want %d", got.recordTy, rec.recordTy)
		}
		if string(got.data) != string(rec.data) {
			t.Errorf("data: got %q, want %q", got.data, rec.data)
		}
	}
}

func TestEncodeRecordHeaderSize(t *testing.T) {
	// Verify no buffer overflow: header must be recordHeaderSize bytes.
	rec := &logRecord{term: 1, index: 1, recordTy: 0, data: []byte("data")}
	encoded, err := encodeRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	wantLen := recordHeaderSize + len(rec.data)
	if len(encoded) != wantLen {
		t.Errorf("encoded length = %d, want %d", len(encoded), wantLen)
	}
}

// --- WAL basic operations ------------------------------------------------

func TestWALAppendAndGet(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	entries := []*raft.LogEntry{
		{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte("a")},
		{Term: 1, Index: 2, Type: raft.EntryNormal, Data: []byte("bb")},
		{Term: 2, Index: 3, Type: raft.EntryNormal, Data: []byte("ccc")},
	}

	if err := w.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	for _, want := range entries {
		got, err := w.Get(want.Index)
		if err != nil {
			t.Fatalf("Get(%d): %v", want.Index, err)
		}
		if got.Term != want.Term {
			t.Errorf("Get(%d).Term = %d, want %d", want.Index, got.Term, want.Term)
		}
		if string(got.Data) != string(want.Data) {
			t.Errorf("Get(%d).Data = %q, want %q", want.Index, got.Data, want.Data)
		}
	}
}

func TestWALGetNotFound(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	_, err = w.Get(99)
	if err == nil {
		t.Error("expected error for missing index, got nil")
	}
}

func TestWALLastIndex(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	last, err := w.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if last != 0 {
		t.Errorf("empty WAL lastIndex = %d, want 0", last)
	}

	entries := []*raft.LogEntry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 1, Index: 3},
	}
	w.Append(entries)

	last, _ = w.LastIndex()
	if last != 3 {
		t.Errorf("lastIndex = %d, want 3", last)
	}
}

func TestWALFirstIndex(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	first, _ := w.FirstIndex()
	// Empty WAL: first should be 0 (the base index of the initial segment).
	_ = first

	entries := []*raft.LogEntry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
	}
	w.Append(entries)

	first, _ = w.FirstIndex()
	if first != 0 {
		t.Errorf("firstIndex = %d, want 0", first)
	}
}

func TestWALIterate(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 1; i <= 5; i++ {
		w.Append([]*raft.LogEntry{{Term: 1, Index: uint64(i), Data: []byte{byte(i)}}})
	}

	var collected []uint64
	w.Iterate(2, 4, func(e *raft.LogEntry) bool {
		collected = append(collected, e.Index)
		return true
	})

	if len(collected) != 3 {
		t.Errorf("collected %d entries, want 3; got %v", len(collected), collected)
	}
	for i, idx := range collected {
		if idx != uint64(i+2) {
			t.Errorf("collected[%d] = %d, want %d", i, idx, i+2)
		}
	}
}

func TestWALDeleteRange(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 1; i <= 5; i++ {
		w.Append([]*raft.LogEntry{{Term: 1, Index: uint64(i)}})
	}

	// Delete is segment-level; just verify it doesn't panic.
	if err := w.DeleteRange(1, 3); err != nil {
		t.Errorf("DeleteRange: %v", err)
	}
}

func TestWALCompact(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 1; i <= 5; i++ {
		w.Append([]*raft.LogEntry{{Term: 1, Index: uint64(i)}})
	}

	// Should not deadlock.
	if err := w.Compact(3); err != nil {
		t.Errorf("Compact: %v", err)
	}
}

func TestWALReopen(t *testing.T) {
	dir := tempDir(t)

	// Write entries.
	{
		w, err := NewWAL(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		entries := []*raft.LogEntry{
			{Term: 1, Index: 1, Data: []byte("first")},
			{Term: 1, Index: 2, Data: []byte("second")},
			{Term: 2, Index: 3, Data: []byte("third")},
		}
		if err := w.Append(entries); err != nil {
			t.Fatal(err)
		}
		w.Close()
	}

	// Reopen and verify entries are still readable.
	{
		w, err := NewWAL(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()

		last, _ := w.LastIndex()
		if last != 3 {
			t.Errorf("after reopen, lastIndex = %d, want 3", last)
		}

		e, err := w.Get(1)
		if err != nil {
			t.Fatalf("Get(1) after reopen: %v", err)
		}
		if string(e.Data) != "first" {
			t.Errorf("Get(1).Data = %q, want %q", e.Data, "first")
		}

		e, err = w.Get(3)
		if err != nil {
			t.Fatalf("Get(3) after reopen: %v", err)
		}
		if string(e.Data) != "third" {
			t.Errorf("Get(3).Data = %q, want %q", e.Data, "third")
		}
	}
}

func TestWALSync(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.Append([]*raft.LogEntry{{Term: 1, Index: 1, Data: []byte("x")}})

	if err := w.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
}

// --- StableStore ---------------------------------------------------------

func TestStableStoreSetGet(t *testing.T) {
	dir := tempDir(t)
	s, err := NewStableStore(filepath.Join(dir, "stable.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	v, err := s.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(v) != "value1" {
		t.Errorf("Get = %q, want %q", v, "value1")
	}

	missing, err := s.Get([]byte("no-such-key"))
	if err != nil {
		t.Fatalf("Get(missing): %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for missing key, got %q", missing)
	}
}

func TestStableStoreDelete(t *testing.T) {
	dir := tempDir(t)
	s, err := NewStableStore(filepath.Join(dir, "stable.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.Set([]byte("k"), []byte("v"))
	s.Delete([]byte("k"))

	v, err := s.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Errorf("expected nil after delete, got %q", v)
	}
}

func TestStableStoreIterate(t *testing.T) {
	dir := tempDir(t)
	s, err := NewStableStore(filepath.Join(dir, "stable.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, k := range []string{"prefix_a", "prefix_b", "other"} {
		s.Set([]byte(k), []byte(k+"-val"))
	}

	var keys []string
	s.Iterate([]byte("prefix_"), func(k, _ []byte) bool {
		keys = append(keys, string(k))
		return true
	})

	if len(keys) != 2 {
		t.Errorf("Iterate found %d keys with prefix, want 2: %v", len(keys), keys)
	}
}

// --- FileSnapshotStore ---------------------------------------------------

func TestFileSnapshotStoreCreateAndList(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}

	config := raft.Configuration{
		Servers: []raft.Server{{ID: "n1", Address: "localhost:8001"}},
	}

	sink, err := store.Create(raft.SnapshotVersionMax, 10, 2, config)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, err := sink.Write([]byte("snapshot-data")); err != nil {
		t.Fatalf("sink.Write: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("sink.Close: %v", err)
	}

	snaps, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 {
		t.Errorf("List() returned %d snapshots, want 1", len(snaps))
	}
	if snaps[0].Index != 10 {
		t.Errorf("snapshot.Index = %d, want 10", snaps[0].Index)
	}
}

func TestFileSnapshotStorePruning(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 2)
	if err != nil {
		t.Fatal(err)
	}

	config := raft.Configuration{}

	// Create 3 snapshots; only 2 should be retained.
	for i := 1; i <= 3; i++ {
		sink, err := store.Create(raft.SnapshotVersionMax, uint64(i*10), 1, config)
		if err != nil {
			t.Fatalf("Create #%d: %v", i, err)
		}
		sink.Write([]byte("data"))
		sink.Close()
	}

	snaps, _ := store.List()
	if len(snaps) > 2 {
		t.Errorf("after 3 creates with retainCount=2, got %d snapshots", len(snaps))
	}
}

func TestFileSnapshotChecksumRoundtrip(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("snapshot-checksum-data-12345")
	config := raft.Configuration{}

	sink, err := store.Create(raft.SnapshotVersionMax, 42, 3, config)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := sink.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	snaps, _ := store.List()
	if len(snaps) == 0 {
		t.Fatal("no snapshots after create")
	}

	snap, meta, err := store.Open(snaps[0].ID)
	if err != nil {
		t.Fatalf("Open (should verify CRC32 and succeed): %v", err)
	}
	if meta.Index != 42 {
		t.Errorf("meta.Index = %d, want 42", meta.Index)
	}

	got, err := io.ReadAll(snap.Reader())
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("Reader returned %q, want %q", got, payload)
	}
}

func TestFileSnapshotChecksumDetectsCorruption(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}

	config := raft.Configuration{}
	sink, _ := store.Create(raft.SnapshotVersionMax, 7, 1, config)
	sink.Write([]byte("important-data"))
	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	snaps, _ := store.List()
	if len(snaps) == 0 {
		t.Fatal("no snapshots")
	}

	// Corrupt one byte in the middle of the file.
	snapPath := dir + "/snapshots/" + snaps[0].ID + ".snap"
	f, err := os.OpenFile(snapPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open snap file: %v", err)
	}
	f.WriteAt([]byte{0xFF}, 3) // flip a byte in the data section
	f.Close()

	_, _, err = store.Open(snaps[0].ID)
	if err == nil {
		t.Error("expected CRC32 mismatch error, got nil")
	}
}

func TestFileSnapshotStoreDelete(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 5)
	if err != nil {
		t.Fatal(err)
	}

	config := raft.Configuration{}
	sink, _ := store.Create(raft.SnapshotVersionMax, 5, 1, config)
	sink.Write([]byte("data"))
	sink.Close()

	snaps, _ := store.List()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}

	if err := store.Delete(snaps[0].ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	snaps, _ = store.List()
	if len(snaps) != 0 {
		t.Errorf("after Delete, expected 0 snapshots, got %d", len(snaps))
	}
}

// TestSnapshotMetaSidecarWritten verifies that sink.Close() writes a .meta
// sidecar file and that the metadata survives a store reload (simulating restart).
func TestSnapshotMetaSidecarWritten(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}

	cfg := raft.Configuration{
		Servers: []raft.Server{
			{ID: "node1", Address: "localhost:7001"},
			{ID: "node2", Address: "localhost:7002"},
		},
	}
	sink, err := store.Create(raft.SnapshotVersionMax, 77, 5, cfg)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sink.Write([]byte("fsm-state"))
	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	snaps, _ := store.List()
	if len(snaps) == 0 {
		t.Fatal("no snapshots listed")
	}
	id := snaps[0].ID

	// Verify sidecar file exists on disk.
	sidecarPath := filepath.Join(dir, "snapshots", id+".meta")
	if _, err := os.Stat(sidecarPath); os.IsNotExist(err) {
		t.Fatalf("sidecar file %q was not created", sidecarPath)
	}

	// Simulate restart: create a new store pointing at the same directory.
	store2, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatalf("reload store: %v", err)
	}
	snaps2, _ := store2.List()
	if len(snaps2) == 0 {
		t.Fatal("no snapshots after reload")
	}
	m := snaps2[0]
	if m.Index != 77 {
		t.Errorf("Index = %d after reload, want 77", m.Index)
	}
	if m.Term != 5 {
		t.Errorf("Term = %d after reload, want 5", m.Term)
	}
	if len(m.Configuration.Servers) != 2 {
		t.Errorf("Configuration.Servers length = %d after reload, want 2", len(m.Configuration.Servers))
	} else {
		if m.Configuration.Servers[0].ID != "node1" {
			t.Errorf("Servers[0].ID = %q, want node1", m.Configuration.Servers[0].ID)
		}
	}
}

// TestSnapshotMetaFallbackFromFilename verifies that when no sidecar exists
// (legacy snapshot), readSnapshotMeta parses term/index from the filename.
func TestSnapshotMetaFallbackFromFilename(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Create a snapshot normally, then delete its sidecar to simulate legacy.
	cfg := raft.Configuration{}
	sink, _ := store.Create(raft.SnapshotVersionMax, 42, 3, cfg)
	sink.Write([]byte("legacy"))
	sink.Close()

	snaps, _ := store.List()
	if len(snaps) == 0 {
		t.Fatal("no snapshots")
	}
	id := snaps[0].ID
	sidecarPath := filepath.Join(dir, "snapshots", id+".meta")
	os.Remove(sidecarPath) // remove sidecar → simulate legacy

	// Reload.
	store2, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	snaps2, _ := store2.List()
	if len(snaps2) == 0 {
		t.Fatal("no snapshots after reload (fallback path)")
	}
	m := snaps2[0]
	if m.Index != 42 {
		t.Errorf("Index = %d (fallback), want 42", m.Index)
	}
	if m.Term != 3 {
		t.Errorf("Term = %d (fallback), want 3", m.Term)
	}
}

// TestSnapshotDeleteRemovesSidecar verifies that Delete() also removes the sidecar.
func TestSnapshotDeleteRemovesSidecar(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}

	sink, _ := store.Create(raft.SnapshotVersionMax, 10, 2, raft.Configuration{})
	sink.Write([]byte("data"))
	sink.Close()

	snaps, _ := store.List()
	if len(snaps) == 0 {
		t.Fatal("no snapshots")
	}
	id := snaps[0].ID
	sidecarPath := filepath.Join(dir, "snapshots", id+".meta")

	if err := store.Delete(id); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := os.Stat(sidecarPath); !os.IsNotExist(err) {
		t.Errorf("sidecar %q still exists after Delete", sidecarPath)
	}
}

package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/raft-consensus/pkg/raft"
)

// forceRotate rotates the WAL onto a fresh segment starting at lastIndex+1,
// so tests can build a multi-segment WAL without having to write 64 MiB.
// It mirrors what rotateSegment does when a segment fills up.
func forceRotate(t *testing.T, w *WAL) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.currentSegment.file.Sync(); err != nil {
		t.Fatalf("sync before rotate: %v", err)
	}
	if err := w.createNewSegment(w.lastIndex + 1); err != nil {
		t.Fatalf("rotate: %v", err)
	}
}

func countSegmentFiles(t *testing.T, dir string) int {
	t.Helper()
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, f := range files {
		if filepath.Ext(f.Name()) == segmentFileExt {
			n++
		}
	}
	return n
}

// M8: with more than one segment, getEntry must serve each entry from the
// segment that actually owns it. The pre-fix scan (largest baseIndex <= idx,
// last-match-wins, no upper bound) would read entries from the wrong segment
// file and return corrupt/wrong data or the wrong record.
func TestGetEntrySelectsOwningSegment(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Segment 1: indices 1..3.
	if err := w.Append([]*raft.LogEntry{
		{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte("one")},
		{Term: 1, Index: 2, Type: raft.EntryNormal, Data: []byte("two")},
		{Term: 1, Index: 3, Type: raft.EntryNormal, Data: []byte("three")},
	}); err != nil {
		t.Fatal(err)
	}

	forceRotate(t, w)

	// Segment 2 (baseIndex 4): indices 4..5.
	if err := w.Append([]*raft.LogEntry{
		{Term: 2, Index: 4, Type: raft.EntryNormal, Data: []byte("four")},
		{Term: 2, Index: 5, Type: raft.EntryNormal, Data: []byte("five")},
	}); err != nil {
		t.Fatal(err)
	}

	if got := countSegmentFiles(t, dir); got != 2 {
		t.Fatalf("expected 2 segment files, got %d", got)
	}

	want := map[uint64]struct {
		term uint64
		data string
	}{
		1: {1, "one"}, 2: {1, "two"}, 3: {1, "three"},
		4: {2, "four"}, 5: {2, "five"},
	}
	for idx, exp := range want {
		got, err := w.Get(idx)
		if err != nil {
			t.Fatalf("Get(%d): %v", idx, err)
		}
		if got.Term != exp.term || string(got.Data) != exp.data {
			t.Errorf("Get(%d) = {term:%d data:%q}, want {term:%d data:%q}",
				idx, got.Term, got.Data, exp.term, exp.data)
		}
	}
}

// M8: an index entry whose owning segment was removed must not be served from
// some unrelated segment that merely has a smaller baseIndex. A stale index
// entry pointing past the live segments must resolve to ErrNotFound.
func TestGetEntryRejectsStaleSegmentOwnership(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append([]*raft.LogEntry{
		{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte("a")},
	}); err != nil {
		t.Fatal(err)
	}
	forceRotate(t, w)
	if err := w.Append([]*raft.LogEntry{
		{Term: 1, Index: 2, Type: raft.EntryNormal, Data: []byte("b")},
	}); err != nil {
		t.Fatal(err)
	}

	// Inject a stale index entry for index 99 that claims to be owned by a
	// baseIndex that no live segment matches. The exact-ownership lookup must
	// reject it rather than reading from segment 1 (the pre-fix behavior).
	w.index.mu.Lock()
	w.index.indexes[99] = &indexEntry{term: 1, baseIndex: 12345, offset: 0}
	if w.index.lastIndex < 99 {
		w.index.lastIndex = 99
	}
	w.index.mu.Unlock()
	w.mu.Lock()
	w.lastIndex = 99
	w.mu.Unlock()

	if _, err := w.Get(99); err != ErrNotFound {
		t.Fatalf("Get(99) = %v, want ErrNotFound", err)
	}
}

// M9: DeleteRange must not os.Remove a non-current segment file that only
// partially overlaps [min,max]; doing so destroys the live entries in that
// segment which fall outside the range. Here segment 1 owns 1..4; deleting
// only [1,2] must keep 3 and 4 readable.
func TestDeleteRangePreservesPartiallyOverlappingSegment(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Segment 1: indices 1..4.
	if err := w.Append([]*raft.LogEntry{
		{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte("i1")},
		{Term: 1, Index: 2, Type: raft.EntryNormal, Data: []byte("i2")},
		{Term: 1, Index: 3, Type: raft.EntryNormal, Data: []byte("i3")},
		{Term: 1, Index: 4, Type: raft.EntryNormal, Data: []byte("i4")},
	}); err != nil {
		t.Fatal(err)
	}

	forceRotate(t, w)

	// Segment 2 (current): indices 5..6.
	if err := w.Append([]*raft.LogEntry{
		{Term: 2, Index: 5, Type: raft.EntryNormal, Data: []byte("i5")},
		{Term: 2, Index: 6, Type: raft.EntryNormal, Data: []byte("i6")},
	}); err != nil {
		t.Fatal(err)
	}

	// Delete only [1,2], a partial overlap with segment 1 (which owns 1..4).
	if err := w.DeleteRange(1, 2); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	// Segment file 1 must NOT have been removed, because it still holds 3 and 4.
	if got := countSegmentFiles(t, dir); got != 2 {
		t.Fatalf("expected 2 segment files after partial delete, got %d", got)
	}

	// The surviving entries in the partially-overlapping segment must remain.
	for _, idx := range []uint64{3, 4} {
		got, err := w.Get(idx)
		if err != nil {
			t.Fatalf("Get(%d) after partial DeleteRange: %v (entry destroyed!)", idx, err)
		}
		if string(got.Data) != string([]byte{'i', byte('0' + idx)}) {
			t.Errorf("Get(%d).Data = %q", idx, got.Data)
		}
	}

	// Deleted entries must be gone.
	for _, idx := range []uint64{1, 2} {
		if _, err := w.Get(idx); err != ErrNotFound {
			t.Errorf("Get(%d) after delete = %v, want ErrNotFound", idx, err)
		}
	}
}

// M9: a non-current segment that lies ENTIRELY within [min,max] should still
// have its file removed (the optimisation is preserved).
func TestDeleteRangeRemovesFullyContainedSegment(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append([]*raft.LogEntry{
		{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte("a")},
		{Term: 1, Index: 2, Type: raft.EntryNormal, Data: []byte("b")},
	}); err != nil {
		t.Fatal(err)
	}
	forceRotate(t, w)
	if err := w.Append([]*raft.LogEntry{
		{Term: 2, Index: 3, Type: raft.EntryNormal, Data: []byte("c")},
	}); err != nil {
		t.Fatal(err)
	}

	if got := countSegmentFiles(t, dir); got != 2 {
		t.Fatalf("expected 2 segment files, got %d", got)
	}

	// [1,2] entirely covers segment 1.
	if err := w.DeleteRange(1, 2); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	if got := countSegmentFiles(t, dir); got != 1 {
		t.Fatalf("expected 1 segment file after full-segment delete, got %d", got)
	}
	if _, err := w.Get(3); err != nil {
		t.Fatalf("Get(3) after delete: %v", err)
	}
}

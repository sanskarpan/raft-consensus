package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// writeGoodEntriesAndCorruptTail creates a WAL with entries 1..3, closes it,
// then appends `tail` bytes to the segment file to simulate a torn/corrupt
// trailing record left by a crash mid-append.
func writeGoodEntriesAndCorruptTail(t *testing.T, tail []byte) string {
	t.Helper()
	dir := t.TempDir()
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := w.Append([]*raft.LogEntry{{Index: i, Term: 1, Data: []byte{byte(i)}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	segPath := filepath.Join(dir, fmt.Sprintf("%020d.wal", 0))
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(tail); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return dir
}

func assertRecoveredThreeEntries(t *testing.T, dir string) {
	t.Helper()
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatalf("NewWAL should recover past a corrupt tail record, got error: %v", err)
	}
	defer w.Close()

	last, _ := w.LastIndex()
	if last != 3 {
		t.Fatalf("lastIndex=%d, want 3 (good entries must survive)", last)
	}
	for i := uint64(1); i <= 3; i++ {
		e, err := w.Get(i)
		if err != nil {
			t.Fatalf("Get(%d) after recovery: %v", i, err)
		}
		if e.Index != i {
			t.Fatalf("entry %d has index %d", i, e.Index)
		}
	}
}

// C13: a full-header record with a bad CRC at the tail (torn write) must be
// truncated and the preceding good entries recovered, not fail the whole WAL.
func TestWALRecoversFromCorruptTailRecord(t *testing.T) {
	tail := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(tail[4:8], 9) // length=9 => dataLen 0; CRC left zero (mismatches)
	dir := writeGoodEntriesAndCorruptTail(t, tail)
	assertRecoveredThreeEntries(t, dir)
}

// C13: a corrupt length field must not trigger a huge allocation; recovery must
// reject the oversized record (bounded by remaining file bytes) and recover.
func TestWALRejectsOversizedRecordLength(t *testing.T) {
	// Full header claiming 1MiB of data, but only 1 trailing byte is present.
	// Pre-fix: the reader allocates 1MiB and then hits ErrUnexpectedEOF, failing
	// the whole WAL. Post-fix: the length is rejected (exceeds remaining file
	// bytes) before allocating, and the good prefix is recovered.
	tail := make([]byte, recordHeaderSize+1)
	binary.BigEndian.PutUint32(tail[4:8], 1*1024*1024+9)
	dir := writeGoodEntriesAndCorruptTail(t, tail)
	assertRecoveredThreeEntries(t, dir)
}

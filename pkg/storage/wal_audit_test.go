package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// H-C2: after a segment rotation the WAL directory must be fsync'd so the newly
// created segment's directory entry is durable. We can't easily crash-inject
// power loss in a unit test, but we can prove entries survive a rotate + reopen
// (functional correctness) and that createNewSegment goes through fsyncDir
// (which would fail on a bad directory). This guards the durability path.
func TestWALRotateThenReopenSurvivesEntries(t *testing.T) {
	dir := t.TempDir()
	// Tiny segment size so we rotate quickly.
	w, err := NewWAL(dir, &WALOptions{SegmentSize: 1})
	if err != nil {
		t.Fatal(err)
	}

	// Append enough entries to force at least one rotation. segmentSize const is
	// large, so instead force rotation directly by driving many appends and then
	// manually rotating.
	for i := uint64(1); i <= 5; i++ {
		if err := w.Append([]*raft.LogEntry{{Index: i, Term: 1, Data: []byte{byte(i)}}}); err != nil {
			t.Fatal(err)
		}
	}

	// Force a rotation via the internal path to exercise createNewSegment's dir
	// fsync explicitly.
	w.mu.Lock()
	if err := w.rotateSegment(); err != nil {
		w.mu.Unlock()
		t.Fatalf("rotateSegment: %v", err)
	}
	nSegments := len(w.segments)
	w.mu.Unlock()
	if nSegments < 2 {
		t.Fatalf("expected >=2 segments after rotate, got %d", nSegments)
	}

	// Append more into the new segment.
	for i := uint64(6); i <= 8; i++ {
		if err := w.Append([]*raft.LogEntry{{Index: i, Term: 1, Data: []byte{byte(i)}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and assert all entries across both segments survived.
	w2, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	last, _ := w2.LastIndex()
	if last != 8 {
		t.Fatalf("lastIndex=%d after reopen, want 8", last)
	}
	for i := uint64(1); i <= 8; i++ {
		e, err := w2.Get(i)
		if err != nil {
			t.Fatalf("Get(%d) after rotate+reopen: %v", i, err)
		}
		if e.Index != i {
			t.Fatalf("entry %d has index %d", i, e.Index)
		}
	}
}

// H-C2 regression: createNewSegment and segment rotation must fsync the WAL
// directory so the new segment's directory entry is durable. We install a
// counting/failing fsyncDirFn seam and assert (a) the directory fsync is
// invoked on segment creation/rotation, and (b) a fsync error is surfaced. A
// pre-fix WAL never called fsyncDir on these paths, so the counter would stay
// at zero and the error would be swallowed.
func TestCreateNewSegmentFsyncsDir(t *testing.T) {
	dir := t.TempDir()

	var calls int
	restore := fsyncDirFn
	fsyncDirFn = func(p string) error { calls++; return restore(p) }
	defer func() { fsyncDirFn = restore }()

	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	createCalls := calls // includes the initial segment creation in openSegments
	if createCalls == 0 {
		t.Fatal("expected NewWAL to fsync the directory on initial segment creation")
	}

	// Force a rotation and assert it fsyncs the directory again.
	w.mu.Lock()
	if err := w.rotateSegment(); err != nil {
		w.mu.Unlock()
		t.Fatal(err)
	}
	w.mu.Unlock()
	if calls <= createCalls {
		t.Fatalf("expected segment rotation to fsync the directory (calls %d -> %d)", createCalls, calls)
	}
	w.Close()

	// The error path must be surfaced, not swallowed.
	fsyncDirFn = func(string) error { return errSentinelFsync }
	w2, err := NewWAL(t.TempDir(), nil)
	if err == nil {
		w2.Close()
		t.Fatal("expected NewWAL to fail when the directory fsync fails")
	}
}

var errSentinelFsync = fmt.Errorf("injected dir fsync failure")

// M-R6: valid records [1..5] with record 3 corrupted (a bad CRC) must cause
// NewWAL to REFUSE to open (mid-segment corruption), not silently truncate and
// drop records 3-5 like a torn tail.
func TestWALRefusesOnMidSegmentCorruption(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := w.Append([]*raft.LogEntry{{Index: i, Term: 1, Data: []byte("data")}}); err != nil {
			t.Fatal(err)
		}
	}
	// Capture record 3's offset before closing.
	w.index.mu.RLock()
	rec3 := w.index.indexes[3]
	w.index.mu.RUnlock()
	if rec3 == nil {
		t.Fatal("missing index entry for record 3")
	}
	off3 := rec3.offset
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the CRC of record 3 in place (flip the stored CRC bytes at off3).
	segPath := filepath.Join(dir, fmt.Sprintf("%020d.wal", 0))
	f, err := os.OpenFile(segPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var crc [4]byte
	if _, err := f.ReadAt(crc[:], off3); err != nil {
		t.Fatal(err)
	}
	crc[0] ^= 0xFF // corrupt the checksum -> record 3 fails CRC, 4 & 5 remain valid
	if _, err := f.WriteAt(crc[:], off3); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// NewWAL must refuse to open: records 4 and 5 are still valid after the
	// corruption, so this is genuine mid-file corruption, not a torn tail.
	if _, err := NewWAL(dir, nil); err == nil {
		t.Fatal("expected NewWAL to refuse to open on mid-segment corruption, but it succeeded (records 3-5 silently dropped)")
	}
}

// M-R6 boundary: a genuinely torn TAIL (garbage appended after the last good
// record, nothing valid afterward) must still recover, not be misclassified as
// mid-segment corruption.
func TestWALTornTailStillRecovers(t *testing.T) {
	// Append junk that never decodes into a valid record after the good prefix.
	tail := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(tail[4:8], 9) // dataLen 0, CRC left zero => mismatch
	dir := writeGoodEntriesAndCorruptTail(t, tail)
	assertRecoveredThreeEntries(t, dir)
}

// L6(a): StableStore's LogStore methods must return an error, not be silent
// no-ops that appear to succeed.
func TestStableStoreLogMethodsError(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStableStore(filepath.Join(dir, "stable.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Append([]*raft.LogEntry{{Index: 1, Term: 1}}); err == nil {
		t.Fatal("StableStore.Append must return an error, not silently succeed")
	}
	if _, err := s.GetEntry(1); err == nil {
		t.Fatal("StableStore.GetEntry must return an error, not silently succeed")
	}
	if err := s.DeleteRange(1, 5); err == nil {
		t.Fatal("StableStore.DeleteRange must return an error, not silently succeed")
	}
}

// shortWriter accepts only `limit` bytes per Write and reports the truncated
// count with a nil error, exactly the silent short-write scenario L6 guards.
type shortWriter struct{ limit int }

func (s shortWriter) Write(p []byte) (int, error) {
	if len(p) > s.limit {
		return s.limit, nil // short write, NO error
	}
	return len(p), nil
}

// L6(b): a short write (n < len(data), nil error) from the underlying writer
// must be surfaced as an error rather than silently accepted. Pre-fix, the raw
// file.Write result was used without checking n against len(data).
func TestWriteRecordFullDetectsShortWrite(t *testing.T) {
	data := make([]byte, 100)
	if _, err := writeRecordFull(shortWriter{limit: 40}, data); err == nil {
		t.Fatal("writeRecordFull must return an error on a short write")
	}
	if _, err := writeRecordFull(shortWriter{limit: 100}, data); err != nil {
		t.Fatalf("writeRecordFull must succeed on a full write: %v", err)
	}
}

// H-P2: concurrent reads via getEntry (ReadAt on the shared fd) must be
// race-free and return correct data. Run under -race to catch fd-sharing races.
func TestWALConcurrentReadsCorrect(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const n = 200
	for i := uint64(1); i <= n; i++ {
		data := []byte(fmt.Sprintf("entry-%d", i))
		if err := w.Append([]*raft.LogEntry{{Index: i, Term: 1, Data: data}}); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < 100; r++ {
				idx := uint64((r % n) + 1)
				e, err := w.Get(idx)
				if err != nil {
					errs <- fmt.Errorf("Get(%d): %w", idx, err)
					return
				}
				want := fmt.Sprintf("entry-%d", idx)
				if string(e.Data) != want || e.Index != idx {
					errs <- fmt.Errorf("Get(%d)=%q/%d want %q", idx, e.Data, e.Index, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

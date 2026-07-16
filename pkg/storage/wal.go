package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raft-consensus/pkg/raft"
	bolt "go.etcd.io/bbolt"
)

var (
	ErrInvalidLogEntry = errors.New("invalid log entry")
	ErrCorruptLog      = errors.New("corrupt log")
	ErrNotFound        = errors.New("not found")
)

const (
	segmentFileExt = ".wal"
	segmentSize    = 64 * 1024 * 1024

	magicNumber = 0x58eb6b0d

	// recordHeaderSize is the fixed header size for each WAL record:
	//   [0:4]   = CRC32 checksum
	//   [4:8]   = payload length (= entryDataLen + 9)
	//   [8]     = entry type
	//   [9:17]  = term
	//   [17:25] = index
	recordHeaderSize = 25
)

type WAL struct {
	mu       sync.RWMutex
	path     string
	segments []*segment
	index    *logIndex

	meta *bolt.DB

	currentSegment *segment
	lastIndex      uint64
	lastTerm       uint64

	writer *segmentWriter

	// syncCount counts successful fsyncs of the current segment. Used by tests
	// to assert that Append durably flushes before returning (C1).
	syncCount atomic.Uint64
	// failSync, when set, makes fsyncCurrentLocked return an error. Test hook
	// for verifying that Append surfaces fsync failures instead of falsely
	// reporting durability (C1).
	failSync atomic.Bool
}

type segment struct {
	mu           sync.RWMutex
	path         string
	file         *os.File
	size         int64
	baseIndex    uint64
	readerOffset int64

	indexes map[uint64]int64

	reader *segmentReader
}

type segmentWriter struct {
	mu      sync.Mutex
	buf     []byte
	pending []*logRecord
	crc     uint32
}

type logRecord struct {
	term     uint64
	index    uint64
	data     []byte
	recordTy uint8
}

type segmentReader struct {
	mu    sync.Mutex
	file  *os.File
	buf   []byte
	pos   int
	index int
}

type logIndex struct {
	mu         sync.RWMutex
	indexes    map[uint64]*indexEntry
	lastIndex  uint64
	lastOffset int64
}

type indexEntry struct {
	term uint64
	// baseIndex identifies the segment that owns this entry (its segment's
	// baseIndex). getEntry selects the segment by exact ownership rather than
	// scanning for the largest baseIndex <= idx (M8).
	baseIndex uint64
	offset    int64
	pos       int
	length    int
}

type WALOptions struct {
	SegmentSize int
}

func NewWAL(path string, opts *WALOptions) (*WAL, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	meta, err := bolt.Open(filepath.Join(path, "meta.db"), 0600, nil)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		path:  path,
		index: newLogIndex(),
		meta:  meta,
	}

	if err := wal.openSegments(); err != nil {
		wal.Close()
		return nil, err
	}

	return wal, nil
}

func (w *WAL) openSegments() error {
	files, err := os.ReadDir(w.path)
	if err != nil {
		return err
	}

	var segmentFiles []string
	for _, f := range files {
		if filepath.Ext(f.Name()) == segmentFileExt {
			segmentFiles = append(segmentFiles, f.Name())
		}
	}

	if len(segmentFiles) == 0 {
		return w.createNewSegment(0)
	}

	for i, name := range segmentFiles {
		path := filepath.Join(w.path, name)
		seg, err := openSegment(path)
		if err != nil {
			return err
		}
		w.segments = append(w.segments, seg)

		isLast := i == len(segmentFiles)-1
		if err := w.rebuildIndex(seg, isLast); err != nil {
			return err
		}
	}

	w.currentSegment = w.segments[len(w.segments)-1]
	w.lastIndex = w.index.lastIndex
	if w.lastIndex > 0 {
		entry, err := w.Get(w.lastIndex)
		if err != nil {
			return err
		}
		w.lastTerm = entry.Term
	}

	return nil
}

func (w *WAL) rebuildIndex(seg *segment, isLast bool) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	// Seek to beginning of the segment file to rebuild index from scratch.
	if _, err := seg.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader, err := newSegmentReader(seg.file)
	if err != nil {
		return err
	}

	for {
		// Record the byte offset before reading this record.
		offset, err := seg.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		rec, err := reader.nextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			// C13: a torn or corrupt record. On the last (current) segment this
			// is the expected result of a crash mid-append: truncate at the last
			// good offset and recover everything before it. On an earlier
			// segment it indicates genuine corruption we must not silently drop.
			if isLast {
				if terr := seg.file.Truncate(offset); terr != nil {
					return terr
				}
				if _, terr := seg.file.Seek(offset, io.SeekStart); terr != nil {
					return terr
				}
				seg.size = offset
				break
			}
			return err
		}

		w.index.addEntry(int64(rec.index), int64(rec.term), int64(seg.baseIndex), offset, 0)
	}

	return nil
}

func (w *WAL) createNewSegment(baseIndex uint64) error {
	seg, err := createSegment(w.path, baseIndex)
	if err != nil {
		return err
	}

	w.segments = append(w.segments, seg)
	w.currentSegment = seg
	return nil
}

func createSegment(path string, baseIndex uint64) (*segment, error) {
	filename := fmt.Sprintf("%020d%s", baseIndex, segmentFileExt)
	path = filepath.Join(path, filename)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	seg := &segment{
		path:      path,
		file:      file,
		baseIndex: baseIndex,
		indexes:   make(map[uint64]int64),
	}

	return seg, nil
}

func openSegment(path string) (*segment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	filename := filepath.Base(path)
	baseIndex := parseBaseIndex(filename)

	seg := &segment{
		path:         path,
		file:         file,
		size:         info.Size(),
		baseIndex:    baseIndex,
		indexes:      make(map[uint64]int64),
		readerOffset: 0,
	}

	return seg, nil
}

func parseBaseIndex(name string) uint64 {
	var idx uint64
	fmt.Sscanf(name[:len(name)-len(segmentFileExt)], "%020d", &idx)
	return idx
}

func (w *WAL) Append(entries []*raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range entries {
		if err := w.appendEntry(entry); err != nil {
			return err
		}
	}

	// C1: fsync the batch to stable storage before returning so that a
	// committed entry cannot be lost on power failure. rotateSegment already
	// fsyncs a segment it rotates away from; this covers the final segment.
	return w.fsyncCurrentLocked()
}

// fsyncCurrentLocked forces the current segment's data to durable storage.
// Caller must hold w.mu.
func (w *WAL) fsyncCurrentLocked() error {
	if w.failSync.Load() {
		return errors.New("wal: simulated fsync failure")
	}
	if w.currentSegment != nil {
		w.currentSegment.mu.Lock()
		err := w.currentSegment.file.Sync()
		w.currentSegment.mu.Unlock()
		if err != nil {
			return err
		}
	}
	w.syncCount.Add(1)
	return nil
}

func (w *WAL) appendEntry(entry *raft.LogEntry) error {
	rec := &logRecord{
		term:     entry.Term,
		index:    entry.Index,
		data:     entry.Data,
		recordTy: uint8(entry.Type),
	}

	data, err := encodeRecord(rec)
	if err != nil {
		return err
	}

	w.currentSegment.mu.Lock()
	defer w.currentSegment.mu.Unlock()

	// Get offset before writing for index tracking.
	offset, err := w.currentSegment.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	n, err := w.currentSegment.file.Write(data)
	if err != nil {
		return err
	}

	w.currentSegment.size += int64(n)
	w.lastIndex = entry.Index
	w.lastTerm = entry.Term

	// Update in-memory index with correct offset.
	w.index.addEntry(int64(entry.Index), int64(entry.Term), int64(w.currentSegment.baseIndex), offset, n)

	if w.currentSegment.size >= segmentSize {
		return w.rotateSegment()
	}

	return nil
}

func (w *WAL) rotateSegment() error {
	if err := w.currentSegment.file.Sync(); err != nil {
		return err
	}

	newBaseIndex := w.lastIndex + 1
	return w.createNewSegment(newBaseIndex)
}

func (w *WAL) Get(idx uint64) (*raft.LogEntry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if idx == 0 || idx > w.lastIndex {
		return nil, ErrNotFound
	}

	entry, err := w.getEntry(idx)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (w *WAL) getEntry(idx uint64) (*raft.LogEntry, error) {
	w.index.mu.RLock()
	ientry, ok := w.index.indexes[idx]
	w.index.mu.RUnlock()

	if !ok {
		return nil, ErrNotFound
	}

	// M8: select the segment that actually owns this entry using the baseIndex
	// recorded on the index entry, rather than scanning for the largest
	// segment baseIndex <= idx (last-match-wins with no upper bound). The old
	// scan could return a segment whose baseIndex <= idx even when idx belongs
	// to an earlier deleted/compacted range or to no live segment at all, and
	// had no upper-bound check against the next segment's baseIndex. We locate
	// the segment whose baseIndex == the owning baseIndex, and additionally
	// require idx to fall below the next segment's baseIndex (if any).
	seg := w.segmentByBaseIndex(ientry.baseIndex)
	if seg == nil {
		return nil, ErrNotFound
	}

	// Upper-bound check: idx must belong to this segment's index range, i.e.
	// seg.baseIndex <= idx < baseIndex of the next segment. This guards against
	// a stale index entry pointing at a segment that no longer owns idx.
	if idx < seg.baseIndex || idx >= w.upperBoundForSegment(seg) {
		return nil, ErrNotFound
	}

	// Open a fresh read-only file descriptor for this read.
	// Multiple goroutines call getEntry concurrently (e.g. replicateTo spawns
	// one goroutine per follower, each holding r.mu.RLock).  Sharing a single
	// file handle among concurrent readers causes seek-position races that
	// corrupt reads.  Opening a private fd per call eliminates the problem
	// without adding extra locking.  w.mu.RLock() is held by the caller so
	// the segment cannot be deleted while we read it.
	seg.mu.RLock()
	segPath := seg.file.Name()
	seg.mu.RUnlock()

	readFile, err := os.Open(segPath)
	if err != nil {
		return nil, err
	}
	defer readFile.Close()

	reader, err := newSegmentReader(readFile)
	if err != nil {
		return nil, err
	}

	if err := reader.seek(ientry.offset); err != nil {
		return nil, err
	}

	rec, err := reader.nextRecord()
	if err != nil {
		return nil, err
	}

	return &raft.LogEntry{
		Term:  rec.term,
		Index: rec.index,
		Type:  raft.EntryType(rec.recordTy),
		Data:  rec.data,
	}, nil
}

// segmentByBaseIndex returns the live segment whose baseIndex exactly matches
// base, or nil if no such segment exists (e.g. it was deleted/compacted away).
// Caller must hold w.mu (at least RLock).
func (w *WAL) segmentByBaseIndex(base uint64) *segment {
	for _, seg := range w.segments {
		if seg.baseIndex == base {
			return seg
		}
	}
	return nil
}

// upperBoundForSegment returns the exclusive upper bound of indices owned by
// the given segment: the baseIndex of the next segment, or math.MaxUint64 if it
// is the last segment. Segments are stored in ascending baseIndex order.
// Caller must hold w.mu (at least RLock).
func (w *WAL) upperBoundForSegment(seg *segment) uint64 {
	next := ^uint64(0)
	for _, s := range w.segments {
		if s.baseIndex > seg.baseIndex && s.baseIndex < next {
			next = s.baseIndex
		}
	}
	return next
}

func (w *WAL) Iterate(start, stop uint64, f func(*raft.LogEntry) bool) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	for idx := start; idx <= stop && idx <= w.lastIndex; idx++ {
		entry, err := w.getEntry(idx)
		if err != nil {
			return err
		}
		if !f(entry) {
			break
		}
	}

	return nil
}

func (w *WAL) FirstIndex() (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.firstIndexLocked(), nil
}

func (w *WAL) firstIndexLocked() uint64 {
	if len(w.segments) == 0 {
		return 0
	}
	return w.segments[0].baseIndex
}

func (w *WAL) LastIndex() (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.lastIndex, nil
}

func (w *WAL) DeleteRange(min, max uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.deleteRangeLocked(min, max)
}

// deleteRangeLocked removes log entries in [min, max] from the WAL.
// For non-current segments that fall entirely within the range, the segment
// file is deleted.  For the current write segment the file is only physically
// truncated when max >= w.lastIndex (tail truncation); for head compaction
// (max < w.lastIndex) only the in-memory index is updated so that valid
// entries after max are not destroyed.
// The in-memory index is always updated to remove all entries in [min, max]
// and w.lastIndex is updated accordingly.
// Caller must hold w.mu.
func (w *WAL) deleteRangeLocked(min, max uint64) error {
	if min > max {
		return nil
	}

	for i := len(w.segments) - 1; i >= 0; i-- {
		seg := w.segments[i]

		if seg.baseIndex > max {
			continue
		}

		if seg.baseIndex+w.segmentEntryCount(seg) < min {
			continue
		}

		// Never close/delete the current write segment.
		if seg == w.currentSegment {
			// For a tail-truncation (max covers everything from min to the end
			// of the log), physically truncate the file so that the deleted
			// entries do not re-appear after a restart.  For a head-compaction
			// (only a prefix of the log is deleted, entries after max must be
			// kept), leave the file intact; only the in-memory index is updated.
			if max >= w.lastIndex {
				if err := w.truncateCurrentSegmentAt(min); err != nil {
					return err
				}
			}
			continue
		}

		// M9: only physically delete the segment FILE when the segment lies
		// entirely within [min, max]. A segment that only partially overlaps
		// the range still holds live entries outside [min, max]; deleting its
		// file would destroy them. In that case fall back to logical deletion
		// (index-only), leaving the file — and its surviving entries — intact.
		lo, hi, hasEntries := w.segmentIndexBounds(seg)
		if !hasEntries || (lo >= min && hi <= max) {
			seg.file.Close()
			if err := os.Remove(seg.path); err != nil {
				return err
			}

			copy(w.segments[i:], w.segments[i+1:])
			w.segments[len(w.segments)-1] = nil
			w.segments = w.segments[:len(w.segments)-1]
		}
		// Otherwise: partial overlap — keep the file; the in-memory index below
		// removes the [min, max] entries logically so they are no longer served.
	}

	// Remove deleted entries from the in-memory index.
	w.index.mu.Lock()
	for idx := min; idx <= max; idx++ {
		delete(w.index.indexes, idx)
	}
	// Recompute lastIndex after deletion.
	w.index.lastIndex = 0
	for idx := range w.index.indexes {
		if idx > w.index.lastIndex {
			w.index.lastIndex = idx
		}
	}
	w.index.mu.Unlock()

	w.index.mu.RLock()
	w.lastIndex = w.index.lastIndex
	w.index.mu.RUnlock()

	// After non-current segment deletions, ensure currentSegment is valid.
	if len(w.segments) == 0 {
		return w.createNewSegment(w.lastIndex + 1)
	}
	w.currentSegment = w.segments[len(w.segments)-1]

	return nil
}

// truncateCurrentSegmentAt physically truncates the current write segment at
// the byte offset of the first WAL record whose log index is >= min.
// This prevents deleted entries from reappearing after a crash/restart.
// Caller must hold w.mu (write lock).
func (w *WAL) truncateCurrentSegmentAt(min uint64) error {
	w.index.mu.RLock()
	var truncOffset int64 = -1
	for idx, entry := range w.index.indexes {
		if idx >= min {
			if truncOffset < 0 || entry.offset < truncOffset {
				truncOffset = entry.offset
			}
		}
	}
	w.index.mu.RUnlock()

	if truncOffset < 0 {
		// No entries in range; nothing to truncate.
		return nil
	}

	seg := w.currentSegment
	seg.mu.Lock()
	defer seg.mu.Unlock()

	// Truncate the underlying file and reposition the write cursor so that
	// the next appendEntry starts at the correct offset.
	// M16: capture and return the first failure instead of silently ignoring
	// Truncate/Seek errors, which would leave the file in an inconsistent
	// state (deleted entries could reappear after restart).
	if err := seg.file.Truncate(truncOffset); err != nil {
		return err
	}
	if _, err := seg.file.Seek(truncOffset, io.SeekStart); err != nil {
		return err
	}
	seg.size = truncOffset
	return nil
}

func (w *WAL) Compact(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if index == 0 {
		return nil
	}

	firstIndex := w.firstIndexLocked()

	if index < firstIndex {
		return nil
	}

	return w.deleteRangeLocked(firstIndex, index)
}

// segmentIndexBounds returns the lowest and highest live log indices that the
// given segment currently owns, plus whether it owns any entries at all.
// Ownership is determined by the baseIndex recorded on each index entry so that
// entries are attributed to their true segment (M8/M9). Caller must hold w.mu.
func (w *WAL) segmentIndexBounds(seg *segment) (lo, hi uint64, ok bool) {
	w.index.mu.RLock()
	defer w.index.mu.RUnlock()

	for idx, ie := range w.index.indexes {
		if ie.baseIndex != seg.baseIndex {
			continue
		}
		if !ok {
			lo, hi, ok = idx, idx, true
			continue
		}
		if idx < lo {
			lo = idx
		}
		if idx > hi {
			hi = idx
		}
	}
	return lo, hi, ok
}

// segmentEntryCount returns how many entries the segment has based on its index map.
func (w *WAL) segmentEntryCount(seg *segment) uint64 {
	w.index.mu.RLock()
	defer w.index.mu.RUnlock()

	count := uint64(0)
	for idx := range w.index.indexes {
		if idx >= seg.baseIndex {
			count++
		}
	}
	return count
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// M16: capture and return the first non-nil error instead of silently
	// dropping Close failures (a failed Close can hide a lost write / fsync).
	var firstErr error
	for _, seg := range w.segments {
		if seg.file != nil {
			if err := seg.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	if w.meta != nil {
		if err := w.meta.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// encodeRecord serialises a log record into the on-disk format:
//
//	[0:4]   CRC32 of everything after the checksum field
//	[4:8]   payload length = len(data) + 9
//	[8]     entry type
//	[9:17]  term
//	[17:25] index
//	[25:]   entry data
func encodeRecord(rec *logRecord) ([]byte, error) {
	header := make([]byte, recordHeaderSize)

	dataLen := uint32(len(rec.data))
	binary.BigEndian.PutUint32(header[4:8], dataLen+9) // 9 = type(1) + term(8)
	header[8] = rec.recordTy
	binary.BigEndian.PutUint64(header[9:17], rec.term)
	binary.BigEndian.PutUint64(header[17:25], rec.index)

	// CRC covers everything from byte 4 onward (header[4:] + data).
	crcPayload := make([]byte, 0, (recordHeaderSize-4)+len(rec.data))
	crcPayload = append(crcPayload, header[4:]...)
	crcPayload = append(crcPayload, rec.data...)
	crc := crc32.ChecksumIEEE(crcPayload)
	binary.BigEndian.PutUint32(header[0:4], crc)

	result := make([]byte, recordHeaderSize+len(rec.data))
	copy(result, header)
	copy(result[recordHeaderSize:], rec.data)

	return result, nil
}

func newLogIndex() *logIndex {
	return &logIndex{
		indexes: make(map[uint64]*indexEntry),
	}
}

func (idx *logIndex) addEntry(index, term, baseIndex, offset int64, length int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.indexes[uint64(index)] = &indexEntry{
		term:      uint64(term),
		baseIndex: uint64(baseIndex),
		offset:    offset,
		length:    length,
	}
	if uint64(index) > idx.lastIndex {
		idx.lastIndex = uint64(index)
		idx.lastOffset = offset
	}
}

func newSegmentReader(file *os.File) (*segmentReader, error) {
	return &segmentReader{
		file: file,
		buf:  make([]byte, 4096),
	}, nil
}

// nextRecord reads and decodes the next record from the current file position.
func (r *segmentReader) nextRecord() (*logRecord, error) {
	header := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(r.file, header); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}

	storedCRC := binary.BigEndian.Uint32(header[0:4])
	length := binary.BigEndian.Uint32(header[4:8])
	recType := header[8]
	term := binary.BigEndian.Uint64(header[9:17])
	index := binary.BigEndian.Uint64(header[17:25])

	dataLen := int(length) - 9 // subtract type(1) + term(8)
	if dataLen < 0 {
		return nil, ErrCorruptLog
	}

	// C13: bound the allocation by the number of bytes actually remaining in the
	// file. A corrupt/torn length field must never drive a huge allocation
	// (allocation DoS on recovery); if it exceeds what the file can hold, the
	// record cannot be valid and is treated as corrupt.
	if remaining, err := r.remainingBytes(); err == nil && int64(dataLen) > remaining {
		return nil, ErrCorruptLog
	}

	data := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(r.file, data); err != nil {
			return nil, err
		}
	}

	// Verify CRC.
	crcPayload := make([]byte, 0, (recordHeaderSize-4)+dataLen)
	crcPayload = append(crcPayload, header[4:]...)
	crcPayload = append(crcPayload, data...)
	computed := crc32.ChecksumIEEE(crcPayload)
	if computed != storedCRC {
		return nil, ErrCorruptLog
	}

	return &logRecord{
		term:     term,
		index:    index,
		data:     data,
		recordTy: recType,
	}, nil
}

func (r *segmentReader) seek(offset int64) error {
	_, err := r.file.Seek(offset, io.SeekStart)
	return err
}

// remainingBytes reports how many bytes are left in the file after the current
// read position. Used to bound record-data allocation during recovery (C13).
func (r *segmentReader) remainingBytes() (int64, error) {
	cur, err := r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	info, err := r.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size() - cur, nil
}

func (w *WAL) Sync() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.currentSegment != nil {
		return w.currentSegment.file.Sync()
	}
	return nil
}

// StableStore is a BoltDB-backed persistent key-value store used for Raft
// stable state (current term, voted-for).
type StableStore struct {
	db *bolt.DB
}

func NewStableStore(path string) (*StableStore, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}

	store := &StableStore{db: db}

	if err := store.initialize(); err != nil {
		store.Close()
		return nil, err
	}

	return store, nil
}

func (s *StableStore) initialize() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("stable"))
		return err
	})
}

func (s *StableStore) Set(key []byte, value []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("stable")).Put(key, value)
	})
}

func (s *StableStore) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("stable")).Get(key)
		if v != nil {
			value = make([]byte, len(v))
			copy(value, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *StableStore) Delete(key []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("stable")).Delete(key)
	})
}

func (s *StableStore) Iterate(prefix []byte, f func(key, value []byte) bool) error {
	return s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte("stable")).Cursor()
		for k, v := cursor.Seek(prefix); k != nil; k, v = cursor.Next() {
			if len(k) < len(prefix) || string(k[:len(prefix)]) != string(prefix) {
				break
			}
			if !f(k, v) {
				break
			}
		}
		return nil
	})
}

// Sync forces any buffered database writes to durable storage. bbolt fsyncs on
// every Update transaction by default, but callers rely on Sync() as an
// explicit durability barrier (e.g. before granting a vote), so it must
// actually flush rather than being a no-op (C2).
func (s *StableStore) Sync() error {
	return s.db.Sync()
}

func (s *StableStore) Close() error {
	return s.db.Close()
}

func (s *StableStore) FirstIndex() (uint64, error) {
	return 0, nil
}

func (s *StableStore) LastIndex() (uint64, error) {
	return 0, nil
}

func (s *StableStore) Append(entries []*raft.LogEntry) error {
	return nil
}

func (s *StableStore) GetEntry(idx uint64) (*raft.LogEntry, error) {
	return nil, nil
}

func (s *StableStore) DeleteRange(min, max uint64) error {
	return nil
}

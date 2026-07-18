package storage

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// snapMetaExt is the extension of the JSON sidecar file that persists SnapshotMeta.
const snapMetaExt = ".meta"

// snapFooterMagic is the 4-byte sentinel at position [-8:-4] of a checksummed snapshot file.
var snapFooterMagic = [4]byte{'S', 'N', 'A', 'P'}

// snapFooterSize is the total size of the appended footer (magic + CRC32).
const snapFooterSize = 8

const (
	snapshotDir = "snapshots"
	snapshotExt = ".snap"
	tmpSuffix   = ".tmp"
)

type FileSnapshotStore struct {
	mu          sync.RWMutex
	path        string
	snapshots   []*raft.SnapshotMeta
	retainCount int

	// checksummed records, per snapshot ID, whether the durable meta declares
	// the snapshot to carry a CRC32 footer. When true, Open MUST find and
	// verify a valid footer and reject the snapshot otherwise (M15). When the
	// flag is absent (legacy snapshot with no sidecar) verification is skipped
	// only for genuinely legacy files.
	checksummed map[string]bool

	// compression is the store's setting for NEWLY created snapshots (gzip when
	// true). snapCompression records the on-disk compression per existing
	// snapshot ID (from the sidecar) so Open can decompress correctly.
	compression     bool
	snapCompression map[string]string
}

// SetCompression enables gzip compression for snapshots created after this call.
// Existing snapshots are read using their recorded compression, so it is safe to
// toggle at any time.
func (s *FileSnapshotStore) SetCompression(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compression = v
}

// sidecarMeta is the storage-local JSON shape persisted in the .meta sidecar.
// It embeds the raft.SnapshotMeta fields plus a Checksummed flag that records
// whether the snapshot was written with a CRC32 footer (M15). The raft package
// type cannot be modified from here, so we serialize a superset.
type sidecarMeta struct {
	Index         uint64             `json:"Index"`
	Term          uint64             `json:"Term"`
	Configuration raft.Configuration `json:"Configuration"`
	ID            string             `json:"ID"`
	Version       uint64             `json:"Version"`
	Checksummed   bool               `json:"Checksummed"`
	// Compression records how the snapshot payload is compressed on disk
	// ("" = none, "gzip"). Empty for legacy snapshots.
	Compression string `json:"Compression,omitempty"`
}

func toSidecarMeta(m *raft.SnapshotMeta, checksummed bool, compression string) *sidecarMeta {
	return &sidecarMeta{
		Index:         m.Index,
		Term:          m.Term,
		Configuration: m.Configuration,
		ID:            m.ID,
		Version:       m.Version,
		Checksummed:   checksummed,
		Compression:   compression,
	}
}

const snapCompressionGzip = "gzip"

func (s *sidecarMeta) toSnapshotMeta() *raft.SnapshotMeta {
	return &raft.SnapshotMeta{
		Index:         s.Index,
		Term:          s.Term,
		Configuration: s.Configuration,
		ID:            s.ID,
		Version:       s.Version,
	}
}

func NewFileSnapshotStore(path string, retainCount int) (*FileSnapshotStore, error) {
	if err := os.MkdirAll(filepath.Join(path, snapshotDir), 0755); err != nil {
		return nil, err
	}

	store := &FileSnapshotStore{
		path:        path,
		retainCount: retainCount,
		checksummed: make(map[string]bool),
	}

	if err := store.loadSnapshots(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *FileSnapshotStore) loadSnapshots() error {
	dir, err := os.Open(filepath.Join(s.path, snapshotDir))
	if err != nil {
		return err
	}
	defer dir.Close()

	files, err := dir.Readdirnames(0)
	if err != nil {
		return err
	}

	var snapshots []*raft.SnapshotMeta
	checksummed := make(map[string]bool)
	compression := make(map[string]string)
	for _, name := range files {
		if !strings.HasSuffix(name, snapshotExt) {
			continue
		}

		meta, ck, comp, err := s.readSnapshotMeta(name)
		if err != nil {
			continue
		}
		snapshots = append(snapshots, meta)
		checksummed[meta.ID] = ck
		compression[meta.ID] = comp
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Index > snapshots[j].Index
	})

	s.snapshots = snapshots
	s.checksummed = checksummed
	s.snapCompression = compression
	return nil
}

// readSnapshotMeta reconstructs SnapshotMeta for a snapshot file and reports
// whether the durable meta declares the snapshot to be checksummed (M15).
// It first tries to read a JSON sidecar (.meta) file written by sink.Close();
// a valid sidecar authoritatively records Checksummed. If no sidecar exists
// (genuinely legacy snapshot) it falls back to parsing term and index from the
// filename ({term}-{index}.snap) with an empty Configuration and checksummed=false.
func (s *FileSnapshotStore) readSnapshotMeta(name string) (*raft.SnapshotMeta, bool, string, error) {
	id := strings.TrimSuffix(name, snapshotExt)

	// --- try sidecar first ---
	sidecarPath := filepath.Join(s.path, snapshotDir, id+snapMetaExt)
	data, err := os.ReadFile(sidecarPath)
	if err == nil {
		var sm sidecarMeta
		if jsonErr := json.Unmarshal(data, &sm); jsonErr == nil {
			m := sm.toSnapshotMeta()
			m.ID = id
			return m, sm.Checksummed, sm.Compression, nil
		}
	}

	// --- fallback: parse "{term}-{index}" from filename (legacy) ---
	meta := &raft.SnapshotMeta{ID: id}
	parts := strings.SplitN(id, "-", 2)
	if len(parts) == 2 {
		term, errT := strconv.ParseUint(parts[0], 10, 64)
		index, errI := strconv.ParseUint(parts[1], 10, 64)
		if errT == nil && errI == nil {
			meta.Term = term
			meta.Index = index
		}
	}

	// Verify the .snap file is readable (fail fast on corrupt/missing file).
	f, err := os.Open(filepath.Join(s.path, snapshotDir, name))
	if err != nil {
		return nil, false, "", err
	}
	f.Close()

	return meta, false, "", nil
}

func (s *FileSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration) (raft.SnapshotSink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.snapshots) >= s.retainCount {
		// Use the lock-free helper since we already hold the mutex.
		if err := s.pruneLocked(); err != nil {
			return nil, err
		}
	}

	id := fmt.Sprintf("%d-%d", term, index)
	tmpPath := filepath.Join(s.path, snapshotDir, id+tmpSuffix)

	file, err := os.Create(tmpPath)
	if err != nil {
		return nil, err
	}

	hasher := crc32.NewIEEE()
	sink := &fileSnapshotSink{
		file:    file,
		id:      id,
		tmpPath: tmpPath,
		meta: &raft.SnapshotMeta{
			ID:            id,
			Index:         index,
			Term:          term,
			Configuration: configuration,
			Version:       uint64(version),
		},
		store:  s,
		hasher: hasher,
	}
	// The CRC footer must cover the exact ON-DISK bytes, so hash whatever lands
	// in the file (compressed bytes when gzip is enabled).
	tee := io.MultiWriter(file, hasher)
	if s.compression {
		sink.gz = gzip.NewWriter(tee)
		sink.w = sink.gz
	} else {
		sink.w = tee
	}

	return sink, nil
}

func (s *fileSnapshotSink) Write(p []byte) (int, error) {
	// s.w is file+hasher (or gzip over it); it writes p in full, hashing the
	// resulting on-disk bytes for the CRC footer.
	return s.w.Write(p)
}

func (s *fileSnapshotSink) Close() error {
	// Flush any buffered compressed data to the file+hasher before the footer.
	compression := ""
	if s.gz != nil {
		if err := s.gz.Close(); err != nil {
			s.file.Close()
			return fmt.Errorf("snapshot: close gzip: %w", err)
		}
		compression = snapCompressionGzip
	}

	// Append 8-byte footer: [SNAP magic (4)] [CRC32/IEEE (4 big-endian)]
	var footer [snapFooterSize]byte
	copy(footer[:4], snapFooterMagic[:])
	binary.BigEndian.PutUint32(footer[4:], s.hasher.Sum32())
	if _, err := s.file.Write(footer[:]); err != nil {
		s.file.Close()
		return fmt.Errorf("snapshot: write footer: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		s.file.Close()
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}

	dir := filepath.Join(s.store.path, snapshotDir)

	// C14: persist the .meta sidecar (Index/Term/Version/Configuration)
	// durably and atomically BEFORE the .snap file becomes visible. Otherwise a
	// crash between the .snap rename and the sidecar write leaves a visible
	// snapshot whose membership configuration falls back to empty — corrupting
	// cluster membership on restore. Writing meta first means a visible .snap
	// always has a durable meta; an orphan .meta (no .snap) is simply ignored.
	// The sink always appends a CRC32 footer above, so the snapshot is
	// checksummed; record that fact durably so Open can require and verify the
	// footer (M15) rather than silently skipping when the trailing bytes fail
	// to match the magic (e.g. footer truncated by a torn write).
	sidecarData, err := json.Marshal(toSidecarMeta(s.meta, true, compression))
	if err != nil {
		return fmt.Errorf("snapshot: marshal meta: %w", err)
	}
	metaTmp := filepath.Join(dir, s.id+snapMetaExt+tmpSuffix)
	if err := writeFileSync(metaTmp, sidecarData); err != nil {
		return fmt.Errorf("snapshot: write meta sidecar: %w", err)
	}
	if err := os.Rename(metaTmp, filepath.Join(dir, s.id+snapMetaExt)); err != nil {
		return err
	}

	// Now make the data file visible.
	finalPath := filepath.Join(dir, s.id+snapshotExt)
	if err := os.Rename(s.tmpPath, finalPath); err != nil {
		return err
	}

	// C14: fsync the directory so both renames survive a crash. Without this the
	// data file may be durable while its directory entry is not.
	if err := fsyncDir(dir); err != nil {
		return err
	}

	s.store.mu.Lock()
	s.store.snapshots = append(s.store.snapshots, s.meta)
	if s.store.checksummed == nil {
		s.store.checksummed = make(map[string]bool)
	}
	s.store.checksummed[s.meta.ID] = true
	if s.store.snapCompression == nil {
		s.store.snapCompression = make(map[string]string)
	}
	s.store.snapCompression[s.meta.ID] = compression
	// M10: keep the list sorted newest-first so pruneLocked (which deletes from
	// the tail) removes the OLDEST snapshot, never a newer one.
	sort.Slice(s.store.snapshots, func(i, j int) bool {
		return s.store.snapshots[i].Index > s.store.snapshots[j].Index
	})
	s.store.mu.Unlock()

	return nil
}

// writeFileSync writes data to path and fsyncs it before returning, so the
// file's contents are durable (used for atomic sidecar writes).
func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// fsyncDir fsyncs a directory so that renames/creations within it are durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *fileSnapshotSink) Cancel() error {
	// M16: capture and return the first non-nil error rather than discarding
	// both the Close and Remove results (a failed cleanup could leak the temp
	// file or hide a filesystem error).
	var firstErr error
	if err := s.file.Close(); err != nil {
		firstErr = err
	}
	if err := os.Remove(s.tmpPath); err != nil && !os.IsNotExist(err) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (s *fileSnapshotSink) ID() string {
	return s.id
}

func (s *FileSnapshotStore) Open(id string) (raft.Snapshot, *raft.SnapshotMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapPath := filepath.Join(s.path, snapshotDir, id+snapshotExt)
	file, err := os.Open(snapPath)
	if err != nil {
		return nil, nil, err
	}

	var meta *raft.SnapshotMeta
	for _, m := range s.snapshots {
		if m.ID == id {
			meta = m
			break
		}
	}

	if meta == nil {
		file.Close()
		return nil, nil, fmt.Errorf("snapshot not found")
	}

	// M15: if the durable meta declares this snapshot to be checksummed, a valid
	// CRC32 footer MUST be present and verify; a missing/corrupt footer is a
	// hard error rather than being silently treated as a "legacy" unchecked
	// snapshot. Only genuinely legacy snapshots (no sidecar declaring a
	// checksum) skip verification.
	mustVerify := s.checksummed[id]
	dataSize, err := verifysnapChecksum(file, mustVerify)
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("snapshot %s corrupt: %w", id, err)
	}

	// Seek back to the beginning so Reader() delivers data from offset 0.
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, nil, err
	}

	return &fileSnapshot{
		file:        file,
		meta:        meta,
		dataSize:    dataSize, // 0 means legacy (full file is data)
		compression: s.snapCompression[id],
	}, meta, nil
}

// verifysnapChecksum reads the 8-byte footer of f, checks the magic, and
// verifies the CRC32 of all preceding bytes.  It returns the byte count of
// the pure data section (excluding the footer).
//
// mustVerify (M15) reflects whether the durable meta declares this snapshot to
// be checksummed. When true, a missing/short/wrong-magic footer is a hard
// error — verification is NOT silently skipped, because that path would let a
// corrupt or truncated snapshot masquerade as a legacy unchecked one. When
// false (genuinely legacy, no sidecar), a file with no footer returns 0, nil
// (no verification).
func verifysnapChecksum(f *os.File, mustVerify bool) (dataSize int64, err error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	total := fi.Size()

	if total < snapFooterSize {
		if mustVerify {
			return 0, fmt.Errorf("checksummed snapshot too small for footer (%d bytes)", total)
		}
		// Too small to have a footer — treat as legacy.
		return 0, nil
	}

	// Read the last 8 bytes.
	var footer [snapFooterSize]byte
	if _, err := f.ReadAt(footer[:], total-snapFooterSize); err != nil {
		return 0, err
	}

	// Check magic.
	if [4]byte(footer[:4]) != snapFooterMagic {
		if mustVerify {
			return 0, fmt.Errorf("checksummed snapshot missing footer magic")
		}
		// No magic — legacy snapshot, skip verification.
		return 0, nil
	}

	// Verify CRC32 of data portion.
	expected := binary.BigEndian.Uint32(footer[4:])
	dataLen := total - snapFooterSize

	hasher := crc32.NewIEEE()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if _, err := io.CopyN(hasher, f, dataLen); err != nil {
		return 0, err
	}
	if hasher.Sum32() != expected {
		return 0, fmt.Errorf("CRC32 mismatch (got %08x, want %08x)", hasher.Sum32(), expected)
	}

	return dataLen, nil
}

func (s *FileSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*raft.SnapshotMeta, len(s.snapshots))
	copy(result, s.snapshots)
	return result, nil
}

func (s *FileSnapshotStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteLocked(id)
}

// pruneLocked removes the oldest snapshots to make room for a new one,
// keeping at most (retainCount - 1) so that after the new snapshot is
// closed the total stays at retainCount.
// Caller must hold s.mu.
func (s *FileSnapshotStore) pruneLocked() error {
	// Snapshots are sorted newest-first. Delete from the tail (oldest) until
	// we have retainCount-1 entries so there is room for the new one.
	for len(s.snapshots) >= s.retainCount {
		oldest := s.snapshots[len(s.snapshots)-1]
		if err := s.deleteLocked(oldest.ID); err != nil {
			return err
		}
	}
	return nil
}

// deleteLocked removes a snapshot (and its sidecar) by ID without acquiring the mutex.
// Caller must hold s.mu.
func (s *FileSnapshotStore) deleteLocked(id string) error {
	snapPath := filepath.Join(s.path, snapshotDir, id+snapshotExt)
	if err := os.Remove(snapPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	// Remove sidecar; a missing sidecar is fine (legacy snapshot has none), but
	// L6: a real removal error (e.g. permissions, I/O) must be surfaced rather
	// than swallowed, otherwise a stale sidecar could silently outlive its
	// snapshot and be resurrected by loadSnapshots on restart.
	sidecarPath := filepath.Join(s.path, snapshotDir, id+snapMetaExt)
	if err := os.Remove(sidecarPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	for i, m := range s.snapshots {
		if m.ID == id {
			s.snapshots = append(s.snapshots[:i], s.snapshots[i+1:]...)
			break
		}
	}
	delete(s.checksummed, id)

	return nil
}

type fileSnapshotSink struct {
	file    *os.File
	id      string
	tmpPath string
	meta    *raft.SnapshotMeta
	store   *FileSnapshotStore
	hasher  interface {
		Write([]byte) (int, error)
		Sum32() uint32
	}
	// w is the writer the caller's bytes flow into: either the raw
	// file+hasher tee, or a gzip.Writer over it. gz is non-nil when compressed.
	w  io.Writer
	gz *gzip.Writer
}

type fileSnapshot struct {
	file        *os.File
	meta        *raft.SnapshotMeta
	dataSize    int64  // 0 = legacy (whole file is data); >0 = data bytes before footer
	compression string // "" = none, "gzip"
}

func (s *fileSnapshot) Index() uint64 {
	return s.meta.Index
}

func (s *fileSnapshot) Term() uint64 {
	return s.meta.Term
}

func (s *fileSnapshot) Reader() io.ReadCloser {
	// The on-disk data portion (excluding the CRC32 footer when present).
	var raw io.Reader = s.file
	if s.dataSize > 0 {
		raw = io.LimitReader(s.file, s.dataSize)
	}
	if s.compression == snapCompressionGzip {
		gz, err := gzip.NewReader(raw)
		if err != nil {
			// Surface the error on the first Read rather than returning nil.
			return errReadCloser{err: err, c: s.file}
		}
		return gzipReadCloser{gz: gz, f: s.file}
	}
	if s.dataSize > 0 {
		return struct {
			io.Reader
			io.Closer
		}{raw, s.file}
	}
	return s.file
}

// gzipReadCloser decompresses the snapshot payload and closes both the gzip
// reader and the underlying file.
type gzipReadCloser struct {
	gz *gzip.Reader
	f  *os.File
}

func (g gzipReadCloser) Read(p []byte) (int, error) { return g.gz.Read(p) }
func (g gzipReadCloser) Close() error {
	gerr := g.gz.Close()
	ferr := g.f.Close()
	if gerr != nil {
		return gerr
	}
	return ferr
}

// errReadCloser returns err on every Read but still closes the file.
type errReadCloser struct {
	err error
	c   io.Closer
}

func (e errReadCloser) Read([]byte) (int, error) { return 0, e.err }
func (e errReadCloser) Close() error             { return e.c.Close() }

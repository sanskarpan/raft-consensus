package raft

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// testSnapshotStore is an in-memory snapshot store that actually persists data
// (unlike memSnapshotStore / capStore whose List() returns nil) so that
// LatestSnapshot can find entries.
type testSnapshotStore struct {
	mu    sync.RWMutex
	snaps []*testSnap // ordered: newest at index 0
}

type testSnap struct {
	meta SnapshotMeta
	data []byte
}

type testSink struct {
	store *testSnapshotStore
	meta  SnapshotMeta
	buf   bytes.Buffer
}

func (s *testSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *testSink) ID() string                  { return s.meta.ID }
func (s *testSink) Cancel() error               { return nil }
func (s *testSink) Close() error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	entry := &testSnap{
		meta: s.meta,
		data: append([]byte(nil), s.buf.Bytes()...),
	}
	// prepend so newest is first
	s.store.snaps = append([]*testSnap{entry}, s.store.snaps...)
	return nil
}

type testSnapReader struct {
	data []byte
}

func (s *testSnapReader) Index() uint64         { return 0 }
func (s *testSnapReader) Term() uint64          { return 0 }
func (s *testSnapReader) Reader() io.ReadCloser { return io.NopCloser(bytes.NewReader(s.data)) }

func (m *testSnapshotStore) Create(version SnapshotVersion, index, term uint64, config Configuration) (SnapshotSink, error) {
	id := fmt.Sprintf("%d-%d", term, index)
	return &testSink{
		store: m,
		meta: SnapshotMeta{
			ID:    id,
			Index: index,
			Term:  term,
		},
	}, nil
}

func (m *testSnapshotStore) Open(id string) (Snapshot, *SnapshotMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.snaps {
		if s.meta.ID == id {
			cp := s.meta
			return &testSnapReader{data: s.data}, &cp, nil
		}
	}
	return nil, nil, fmt.Errorf("snapshot %s not found", id)
}

func (m *testSnapshotStore) List() ([]*SnapshotMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*SnapshotMeta, len(m.snaps))
	for i, s := range m.snaps {
		cp := s.meta
		out[i] = &cp
	}
	return out, nil
}

func (m *testSnapshotStore) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, s := range m.snaps {
		if s.meta.ID == id {
			m.snaps = append(m.snaps[:i], m.snaps[i+1:]...)
			return nil
		}
	}
	return nil
}

// snapFSM is a minimal FSM whose Snapshot() returns non-empty data so we can
// assert that LatestSnapshot streams actual bytes.
type snapFSM struct {
	mu      sync.Mutex
	applied [][]byte
}

func (f *snapFSM) Apply(entry []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, append([]byte(nil), entry...))
	return entry, nil
}

func (f *snapFSM) Snapshot() (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Serialize all applied entries separated by newlines so data is non-empty.
	var buf bytes.Buffer
	for _, e := range f.applied {
		buf.Write(e)
		buf.WriteByte('\n')
	}
	if buf.Len() == 0 {
		buf.WriteString("empty")
	}
	data := append([]byte(nil), buf.Bytes()...)
	return &testSnapReader{data: data}, nil
}

func (f *snapFSM) Restore(r io.Reader) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = nil
	return nil
}

// newSingleNodeRaft builds a single-node raft using a testSnapshotStore so that
// Snapshot() creates entries visible to LatestSnapshot(). Returns the raft node
// and a cleanup func.
func newSingleNodeRaft(t *testing.T) (*raft, func()) {
	t.Helper()
	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	trans := newChanTransport("n1")
	fsm := &snapFSM{}
	ss := &testSnapshotStore{}
	rc := &Config{
		LocalID:              "n1",
		ElectionTick:         5,
		HeartbeatTick:        1,
		InitialConfiguration: cfg,
	}
	r, err := newRaft(rc, "n1", newMemLogStore(), newMemStableStore(), ss, fsm, trans)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	trans.appendEntriesFn = func(req *AppendEntriesRequest) *AppendEntriesResponse {
		return r.HandleAppendEntriesRPC(req)
	}
	trans.requestVoteFn = func(req *RequestVoteRequest) *RequestVoteResponse {
		return r.HandleRequestVoteRPC(req)
	}
	trans.installSnapshotFn = func(req *InstallSnapshotRequest) *InstallSnapshotResponse {
		return r.HandleInstallSnapshotRPC(req)
	}
	if err := r.Start(); err != nil {
		t.Fatalf("r.Start: %v", err)
	}
	return r, func() { _ = r.Shutdown() }
}

// becomeLeaderWithTimeout waits up to 5 s for r to reach StateLeader.
func becomeLeaderWithTimeout(t *testing.T, r *raft) error {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.State() == StateLeader {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for leader; state=%s", r.State())
}

// testCtx returns a context that expires when the test times out (capped at 10s).
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestLatestSnapshotNoSnapshots(t *testing.T) {
	r, cleanup := newSingleNodeRaft(t)
	defer cleanup()
	_, _, rc, err := r.LatestSnapshot()
	if err == nil {
		rc.Close()
		t.Fatal("expected error when no snapshots available")
	}
}

func TestLatestSnapshotAfterSnapshot(t *testing.T) {
	r, cleanup := newSingleNodeRaft(t)
	defer cleanup()

	if err := becomeLeaderWithTimeout(t, r); err != nil {
		t.Fatalf("become leader: %v", err)
	}

	ctx := testCtx(t)
	if _, err := r.Apply(ctx, []byte("set:k:v")); err != nil {
		t.Fatalf("apply: %v", err)
	}

	if err := r.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	idx, term, rc, err := r.LatestSnapshot()
	if err != nil {
		t.Fatalf("LatestSnapshot: %v", err)
	}
	defer rc.Close()
	if idx == 0 {
		t.Errorf("expected idx > 0, got %d", idx)
	}
	if term == 0 {
		t.Errorf("expected term > 0, got %d", term)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read snapshot data: %v", err)
	}
	if len(data) == 0 {
		t.Errorf("expected non-empty snapshot data")
	}
	_ = bytes.NewReader(data)
}

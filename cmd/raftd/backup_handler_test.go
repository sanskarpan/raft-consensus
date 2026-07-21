package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

// stubRaftWith returns a new stubRaft with optional fields set via a mutator.
func stubRaftWith(fn func(*stubRaft)) *stubRaft {
	r := &stubRaft{}
	if fn != nil {
		fn(r)
	}
	return r
}

// backupTestFields extends stubRaft with fields needed by backup handler tests.
// We embed them directly on stubRaft via the extra fields added below.
// newBackupServer creates a minimal Server wired up with a stubRaft for testing
// the snapshot download/upload/restore handlers.
func newBackupServer(t *testing.T, stub *stubRaft) *Server {
	t.Helper()
	if stub == nil {
		stub = &stubRaft{}
	}
	s := &Server{
		config:  &Config{AdminToken: "tok"},
		logger:  zap.NewNop(),
		limiter: newWriteLimiter(1000),
		raftNode: &backupStubRaft{
			stubRaft: stub,
		},
	}
	return s
}

// backupStubRaft wraps stubRaft and adds backup-specific fields/methods so that
// we can test the three new handlers without modifying the shared stubRaft.
type backupStubRaft struct {
	*stubRaft
	latestSnapshotErr  error
	latestSnapshotIdx  uint64
	latestSnapshotTerm uint64
	latestSnapshotData io.ReadCloser
	restoreFn          func(context.Context, io.Reader) error
}

func (r *backupStubRaft) LatestSnapshot() (uint64, uint64, io.ReadCloser, error) {
	if r.latestSnapshotErr != nil {
		return 0, 0, nil, r.latestSnapshotErr
	}
	return r.latestSnapshotIdx, r.latestSnapshotTerm, r.latestSnapshotData, nil
}

func (r *backupStubRaft) Restore(ctx context.Context, reader io.Reader) error {
	if r.restoreFn != nil {
		return r.restoreFn(ctx, reader)
	}
	return nil
}

// ---------------------------------------------------------------------------
// handleSnapshotDownload tests
// ---------------------------------------------------------------------------

func TestSnapshotDownloadNoSnapshot(t *testing.T) {
	stub := &backupStubRaft{
		stubRaft:          &stubRaft{},
		latestSnapshotErr: fmt.Errorf("no snapshots"),
	}
	s := &Server{
		config:   &Config{AdminToken: "tok"},
		logger:   zap.NewNop(),
		limiter:  newWriteLimiter(1000),
		raftNode: stub,
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/snapshot/download", nil)
	rr := httptest.NewRecorder()
	s.handleSnapshotDownload(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestSnapshotDownloadStreamsData(t *testing.T) {
	data := []byte("snapshot data bytes")
	stub := &backupStubRaft{
		stubRaft:           &stubRaft{},
		latestSnapshotIdx:  42,
		latestSnapshotTerm: 3,
		latestSnapshotData: io.NopCloser(bytes.NewReader(data)),
	}
	s := &Server{
		config:   &Config{AdminToken: "tok"},
		logger:   zap.NewNop(),
		limiter:  newWriteLimiter(1000),
		raftNode: stub,
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/snapshot/download", nil)
	rr := httptest.NewRecorder()
	s.handleSnapshotDownload(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if got := rr.Body.Bytes(); !bytes.Equal(got, data) {
		t.Fatalf("body mismatch: got %q, want %q", got, data)
	}
	if rr.Header().Get("X-Snapshot-Index") != "42" {
		t.Fatalf("X-Snapshot-Index header = %q, want 42", rr.Header().Get("X-Snapshot-Index"))
	}
	if rr.Header().Get("X-Snapshot-Term") != "3" {
		t.Fatalf("X-Snapshot-Term header = %q, want 3", rr.Header().Get("X-Snapshot-Term"))
	}
}

func TestSnapshotDownloadMethodNotAllowed(t *testing.T) {
	s := &Server{
		config:   &Config{AdminToken: "tok"},
		logger:   zap.NewNop(),
		raftNode: &backupStubRaft{stubRaft: &stubRaft{}},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/snapshot/download", nil)
	rr := httptest.NewRecorder()
	s.handleSnapshotDownload(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// handleRestore tests
// ---------------------------------------------------------------------------

func TestRestoreEndpointCallsRestore(t *testing.T) {
	var called bool
	stub := &backupStubRaft{
		stubRaft: &stubRaft{},
		restoreFn: func(ctx context.Context, reader io.Reader) error {
			called = true
			return nil
		},
	}
	s := &Server{
		config:   &Config{AdminToken: "tok"},
		logger:   zap.NewNop(),
		raftNode: stub,
	}

	req := httptest.NewRequest(http.MethodPut, "/admin/restore", bytes.NewReader([]byte("data")))
	rr := httptest.NewRecorder()
	s.handleRestore(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if !called {
		t.Fatal("Restore was not called on the Raft node")
	}
}

func TestRestoreEndpointMethodNotAllowed(t *testing.T) {
	s := &Server{
		config:   &Config{AdminToken: "tok"},
		logger:   zap.NewNop(),
		raftNode: &backupStubRaft{stubRaft: &stubRaft{}},
	}
	req := httptest.NewRequest(http.MethodGet, "/admin/restore", nil)
	rr := httptest.NewRecorder()
	s.handleRestore(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

func TestRestoreEndpointPropagatesError(t *testing.T) {
	stub := &backupStubRaft{
		stubRaft: &stubRaft{},
		restoreFn: func(ctx context.Context, reader io.Reader) error {
			return fmt.Errorf("restore failed: disk full")
		},
	}
	s := &Server{
		config:   &Config{AdminToken: "tok"},
		logger:   zap.NewNop(),
		raftNode: stub,
	}

	req := httptest.NewRequest(http.MethodPut, "/admin/restore", bytes.NewReader([]byte("data")))
	rr := httptest.NewRecorder()
	s.handleRestore(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", rr.Code, rr.Body.String())
	}
}

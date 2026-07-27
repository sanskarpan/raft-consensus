package main

// Tests for security-hardening features:
//   - secureHeadersMiddleware sets X-Content-Type-Options, X-Frame-Options,
//     and Content-Security-Policy on every response
//   - security headers coexist with CORS headers added by corsMiddleware
//   - OtlpInsecure config field defaults to false (TLS enabled by default)
//   - handleSnapshotUpload success and error paths

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/backup"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// TestSecureHeadersPresent — all three defensive headers appear on a response
// ---------------------------------------------------------------------------

func TestSecureHeadersPresent(t *testing.T) {
	s := bareServer("")

	// Wrap a simple handler with secureHeadersMiddleware.
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := s.secureHeadersMiddleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	want := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Content-Security-Policy": "default-src 'none'",
	}
	for header, expected := range want {
		if got := rr.Header().Get(header); got != expected {
			t.Errorf("header %q = %q, want %q", header, got, expected)
		}
	}
}

// ---------------------------------------------------------------------------
// TestSecureHeadersDoNotOverrideCORSOrigin — security headers and CORS headers
// coexist when both middlewares are in the chain.
// ---------------------------------------------------------------------------

func TestSecureHeadersDoNotOverrideCORSOrigin(t *testing.T) {
	s := bareServer("")
	// Enable a specific CORS origin in the config so corsMiddleware emits
	// Access-Control-Allow-Origin.
	s.config.CORSOrigins = "https://example.com"

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// Replicate the initHTTP chain: secureHeaders wraps corsMiddleware wraps mux.
	handler := s.secureHeadersMiddleware(s.corsMiddleware(inner))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// Security headers must be present.
	if got := rr.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := rr.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Errorf("X-Frame-Options = %q, want DENY", got)
	}
	if got := rr.Header().Get("Content-Security-Policy"); got != "default-src 'none'" {
		t.Errorf("Content-Security-Policy = %q, want default-src 'none'", got)
	}

	// CORS header must also be present (not clobbered by security middleware).
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q, want https://example.com", got)
	}
}

// ---------------------------------------------------------------------------
// TestOtlpInsecureConfigDefault — empty Config zero-value has OtlpInsecure=false
// ---------------------------------------------------------------------------

func TestOtlpInsecureConfigDefault(t *testing.T) {
	cfg := Config{}
	if cfg.OtlpInsecure {
		t.Error("Config{}.OtlpInsecure should default to false (TLS enabled), got true")
	}
}

// ---------------------------------------------------------------------------
// handleSnapshotUpload tests
// ---------------------------------------------------------------------------

// stubUploader is a minimal backup.Uploader that records the last upload call.
type stubUploader struct {
	uploadErr  error
	uploadName string
}

func (u *stubUploader) Upload(_ context.Context, name string, _ io.Reader) error {
	u.uploadName = name
	return u.uploadErr
}

func (u *stubUploader) Download(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}

func (u *stubUploader) List(_ context.Context) ([]string, error) { return nil, nil }

var _ backup.Uploader = (*stubUploader)(nil)

func TestSnapshotUploadMethodNotAllowed(t *testing.T) {
	s := &Server{
		config:   &Config{AdminToken: "tok"},
		logger:   zap.NewNop(),
		raftNode: &backupStubRaft{stubRaft: &stubRaft{}},
		uploader: &backup.NoOpUploader{Logger: zap.NewNop()},
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/snapshot/upload", nil)
	rr := httptest.NewRecorder()
	s.handleSnapshotUpload(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

func TestSnapshotUploadSuccess(t *testing.T) {
	data := []byte("snap data")
	stub := &backupStubRaft{
		stubRaft:           &stubRaft{},
		latestSnapshotIdx:  7,
		latestSnapshotTerm: 2,
		latestSnapshotData: io.NopCloser(bytes.NewReader(data)),
	}
	uploader := &stubUploader{}
	s := &Server{
		config:   &Config{AdminToken: "tok"},
		logger:   zap.NewNop(),
		raftNode: stub,
		uploader: uploader,
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/snapshot/upload", nil)
	rr := httptest.NewRecorder()
	s.handleSnapshotUpload(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if uploader.uploadName == "" {
		t.Error("Upload was not called on the uploader")
	}
}

func TestSnapshotUploadNoSnapshot(t *testing.T) {
	// Snapshot() succeeds but LatestSnapshot() returns an error — exercises
	// the "no snapshot after force" error path.
	stub := &backupStubRaft{
		stubRaft:          &stubRaft{},
		latestSnapshotErr: io.EOF,
	}
	s := &Server{
		config:   &Config{AdminToken: "tok"},
		logger:   zap.NewNop(),
		raftNode: stub,
		uploader: &backup.NoOpUploader{Logger: zap.NewNop()},
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/snapshot/upload", nil)
	rr := httptest.NewRecorder()
	s.handleSnapshotUpload(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", rr.Code, rr.Body.String())
	}
}

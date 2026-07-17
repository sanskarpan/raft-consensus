package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raft-consensus/pkg/fsm"
	"github.com/raft-consensus/pkg/raft"
)

// ---------------------------------------------------------------------------
// H-O1 — /ready requires a known leader AND Healthy(); /health stays liveness.
// ---------------------------------------------------------------------------

func TestReadyRequiresLeaderAndHealth(t *testing.T) {
	newReq := func() (*Server, *httptest.ResponseRecorder) {
		s := bareServer("tok")
		return s, httptest.NewRecorder()
	}

	// Case 1: follower with a known leader and healthy -> 200.
	s, w := newReq()
	s.raftNode = &leaderStub{leader: "leaderX"}
	s.handleReady(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("healthy follower with leader: /ready = %d, want 200", w.Code)
	}

	// Case 2: no known leader -> 503.
	s, w = newReq()
	s.raftNode = &leaderStub{leader: ""}
	s.handleReady(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("no-leader follower: /ready = %d, want 503", w.Code)
	}

	// Case 3: leader known but node unhealthy (fatal storage error) -> 503.
	s, w = newReq()
	s.raftNode = &leaderStub{leader: "leaderX", unhealthy: true}
	s.handleReady(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unhealthy node: /ready = %d, want 503", w.Code)
	}

	// Case 4: draining node -> 503 (M-R4 interaction).
	s, w = newReq()
	s.raftNode = &leaderStub{leader: "leaderX"}
	s.draining.Store(true)
	s.handleReady(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining node: /ready = %d, want 503", w.Code)
	}
}

func TestHealthIsPureLiveness(t *testing.T) {
	// /health returns 200 even with no leader and an unhealthy node.
	s := bareServer("tok")
	s.raftNode = &leaderStub{leader: "", unhealthy: true}
	w := httptest.NewRecorder()
	s.handleHealth(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/health = %d, want 200 (pure liveness)", w.Code)
	}
}

// ---------------------------------------------------------------------------
// H-R4 — watch handler clears the per-request write deadline so a stream can
// run past the 60s WriteTimeout.
// ---------------------------------------------------------------------------

// deadlineRecorder is a ResponseWriter+Flusher that records SetWriteDeadline
// calls so we can assert the watch handler cleared the deadline (H-R4).
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlineSet   int32
	clearedToZero int32
}

func (d *deadlineRecorder) Flush() {}

func (d *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	atomic.AddInt32(&d.deadlineSet, 1)
	if t.IsZero() {
		atomic.StoreInt32(&d.clearedToZero, 1)
	}
	return nil
}

func TestWatchClearsWriteDeadline(t *testing.T) {
	s := watchAuthServer(t)

	rec := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel quickly so the handler returns instead of blocking forever.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	r := httptest.NewRequest(http.MethodGet, "/v1/watch?key=k", nil).WithContext(ctx)

	s.handleV1Watch(rec, r)

	if atomic.LoadInt32(&rec.deadlineSet) == 0 {
		t.Fatal("watch handler did not call SetWriteDeadline")
	}
	if atomic.LoadInt32(&rec.clearedToZero) == 0 {
		t.Fatal("watch handler did not clear the write deadline (zero time)")
	}
}

// ---------------------------------------------------------------------------
// M-R3 — Shutdown cancels open watches (so http.Shutdown does not block) and
// the stream emits a clean event: shutdown.
// ---------------------------------------------------------------------------

func TestShutdownCancelsWatchesWithShutdownEvent(t *testing.T) {
	s := watchAuthServer(t)

	rec := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	r := httptest.NewRequest(http.MethodGet, "/v1/watch?key=k", nil)

	done := make(chan struct{})
	go func() {
		s.handleV1Watch(rec, r)
		close(done)
	}()

	// Wait until the watch has registered itself.
	deadline := time.After(2 * time.Second)
	for {
		s.watchMu.Lock()
		n := len(s.watchCancels)
		s.watchMu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("watch never registered")
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}

	// Simulate the start of Shutdown: flip draining and cancel all watches.
	s.draining.Store(true)
	s.cancelAllWatches()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watch handler did not return after cancelAllWatches")
	}

	if !strings.Contains(rec.Body.String(), "event: shutdown") {
		t.Fatalf("watch did not emit shutdown event; body=%q", rec.Body.String())
	}

	// Registry drained.
	s.watchMu.Lock()
	n := len(s.watchCancels)
	s.watchMu.Unlock()
	if n != 0 {
		t.Fatalf("watchCancels not drained: %d entries remain", n)
	}
}

// ---------------------------------------------------------------------------
// M-R4 — write endpoints return 503 once draining; leader transfer preserved.
// ---------------------------------------------------------------------------

func TestDrainingRejectsWrites(t *testing.T) {
	s := watchAuthServer(t) // this node is leader
	s.draining.Store(true)

	cases := []struct {
		method, path, body string
	}{
		{http.MethodPost, "/command", "{}"},
		{http.MethodPut, "/v1/kv/foo", "bar"},
		{http.MethodDelete, "/v1/kv/foo", ""},
		{http.MethodPost, "/v1/txn", `{"compare":[],"success":[],"failure":[]}`},
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
		r.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(w, r)
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("%s %s while draining: status=%d, want 503", c.method, c.path, w.Code)
		}
	}
}

func TestNotDrainingAllowsWrites(t *testing.T) {
	// Sanity: with draining off, a PUT to the leader is not rejected with 503.
	s := watchAuthServer(t)
	s.raftNode = &leaderStub{leader: raft.ServerID(s.config.NodeID), applyResult: mustEncodeKV(t)}
	r := httptest.NewRequest(http.MethodPut, "/v1/kv/foo", strings.NewReader("bar"))
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, r)
	if w.Code == http.StatusServiceUnavailable {
		t.Fatalf("PUT rejected with 503 while not draining")
	}
}

func mustEncodeKV(t *testing.T) []byte {
	t.Helper()
	// Apply a put command to a throwaway store to obtain a valid result blob
	// that DecodeKeyValueResult can parse.
	data, err := fsm.EncodeCommand("put", "foo", "bar")
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	kv := fsm.NewKVStore()
	res, err := kv.Apply(data)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	return res
}

// ---------------------------------------------------------------------------
// M-S1 — constant-time token comparison.
// ---------------------------------------------------------------------------

func TestConstantTimeEqual(t *testing.T) {
	if !constantTimeEqual("secret", "secret") {
		t.Error("equal tokens compared as unequal")
	}
	if constantTimeEqual("secret", "secreu") {
		t.Error("different tokens compared as equal")
	}
	if constantTimeEqual("secret", "secre") {
		t.Error("different-length tokens compared as equal")
	}
	if constantTimeEqual("", "x") {
		t.Error("empty vs non-empty compared as equal")
	}
}

func TestAuthMiddlewareConstantTimeTokens(t *testing.T) {
	// Single token path.
	s := bareServer("s3cr3t")
	h := s.authMiddleware(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer s3cr3t")
	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("valid single token rejected: %d", w.Code)
	}

	// Map token path with roles.
	s2 := bareServer("")
	s2.config.AdminTokens = map[string]string{"rtok": "read", "wtok": "write"}
	var gotRole string
	h2 := s2.authMiddleware(func(w http.ResponseWriter, req *http.Request) {
		gotRole, _ = req.Context().Value(roleContextKey{}).(string)
		w.WriteHeader(http.StatusOK)
	})
	r2 := httptest.NewRequest(http.MethodGet, "/", nil)
	r2.Header.Set("Authorization", "Bearer rtok")
	w2 := httptest.NewRecorder()
	h2(w2, r2)
	if w2.Code != http.StatusOK || gotRole != "read" {
		t.Fatalf("map token: status=%d role=%q, want 200/read", w2.Code, gotRole)
	}

	// Wrong token rejected.
	r3 := httptest.NewRequest(http.MethodGet, "/", nil)
	r3.Header.Set("Authorization", "Bearer nope")
	w3 := httptest.NewRecorder()
	h2(w3, r3)
	if w3.Code != http.StatusUnauthorized {
		t.Fatalf("invalid token accepted: %d", w3.Code)
	}
}

// ---------------------------------------------------------------------------
// M-O3 — /metrics is gated behind auth when tokens are configured; open in dev.
// ---------------------------------------------------------------------------

func TestMetricsGatedWhenTokensConfigured(t *testing.T) {
	s := bareServer("tok") // token configured => gated
	if !s.metricsAuthEnabled() {
		t.Fatal("metricsAuthEnabled() = false with admin token set")
	}
	mux := s.buildMux()

	// Without a token: 401/403.
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusUnauthorized && w.Code != http.StatusForbidden {
		t.Fatalf("/metrics without token (gated): status=%d, want 401/403", w.Code)
	}

	// With a valid token: 200.
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.Header.Set("Authorization", "Bearer tok")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, r)
	if w2.Code != http.StatusOK {
		t.Fatalf("/metrics with token: status=%d, want 200", w2.Code)
	}
}

func TestMetricsOpenInDevMode(t *testing.T) {
	s := bareServer("") // no tokens => dev mode, open
	if s.metricsAuthEnabled() {
		t.Fatal("metricsAuthEnabled() = true with no tokens (dev mode should be open)")
	}
	mux := s.buildMux()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/metrics in dev mode: status=%d, want 200 (open)", w.Code)
	}
}

func TestMetricsAuthConfigForcesGate(t *testing.T) {
	s := bareServer("") // no tokens, but metrics_auth explicitly on
	s.config.MetricsAuth = true
	if !s.metricsAuthEnabled() {
		t.Fatal("metrics_auth=true did not enable gating")
	}
}

// ---------------------------------------------------------------------------
// M-O2 — request-ID correlation middleware.
// ---------------------------------------------------------------------------

func TestRequestIDGeneratedAndEchoed(t *testing.T) {
	s := bareServer("tok")
	var seen string
	h := s.requestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = requestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	// No incoming ID: one is generated and echoed.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if seen == "" {
		t.Fatal("request ID not present in context")
	}
	if got := w.Header().Get(requestIDHeader); got != seen {
		t.Fatalf("echoed request ID %q != context %q", got, seen)
	}

	// Incoming ID is preserved.
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(requestIDHeader, "client-supplied-id")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r)
	if seen != "client-supplied-id" {
		t.Fatalf("incoming request ID overwritten: %q", seen)
	}
	if got := w2.Header().Get(requestIDHeader); got != "client-supplied-id" {
		t.Fatalf("echoed request ID = %q, want client-supplied-id", got)
	}
}

func TestForwardToLeaderPropagatesRequestID(t *testing.T) {
	var gotID string
	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = r.Header.Get(requestIDHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer leaderSrv.Close()
	addr := strings.TrimPrefix(leaderSrv.URL, "http://")

	s := bareServer("secret")
	s.config.Cluster = []ClusterMember{{ID: "leader", HTTPAddress: addr}}
	s.raftNode = &leaderStub{leader: "leader"}

	r := httptest.NewRequest(http.MethodPost, "/command", strings.NewReader("{}"))
	ctx := context.WithValue(r.Context(), requestIDKey{}, "corr-123")
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()
	if err := s.forwardToLeader(w, r, "leader", "/command"); err != nil {
		t.Fatalf("forwardToLeader: %v", err)
	}
	if gotID != "corr-123" {
		t.Fatalf("leader saw request ID %q, want corr-123", gotID)
	}
}

// ---------------------------------------------------------------------------
// M-O1 — watch connection gauge tracks register/deregister.
// ---------------------------------------------------------------------------

func TestRegisterWatchTracksCount(t *testing.T) {
	s := bareServer("tok")
	_, dereg1 := s.registerWatch(func() {})
	_, dereg2 := s.registerWatch(func() {})

	s.watchMu.Lock()
	n := len(s.watchCancels)
	s.watchMu.Unlock()
	if n != 2 {
		t.Fatalf("watchCancels count = %d, want 2", n)
	}

	dereg1()
	dereg2()
	s.watchMu.Lock()
	n = len(s.watchCancels)
	s.watchMu.Unlock()
	if n != 0 {
		t.Fatalf("watchCancels count after deregister = %d, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// L5 — idle zero-count per-IP watch counters are swept.
// ---------------------------------------------------------------------------

func TestSweepEvictsIdleWatchCounters(t *testing.T) {
	s := bareServer("tok")

	// Acquire and release a watch slot so a zero-count counter is left behind.
	release, ok := s.acquireWatchSlot("5.5.5.5")
	if !ok {
		t.Fatal("acquireWatchSlot failed")
	}
	release()

	if _, present := s.watchPerIP.Load("5.5.5.5"); !present {
		t.Fatal("counter not created")
	}

	// Directly exercise the eviction logic the sweep runs on each tick.
	s.watchPerIP.Range(func(key, val any) bool {
		if atomic.LoadInt64(val.(*int64)) == 0 {
			s.watchPerIP.Delete(key)
		}
		return true
	})

	if _, present := s.watchPerIP.Load("5.5.5.5"); present {
		t.Fatal("idle zero-count watch counter was not evicted")
	}

	// A non-zero counter must survive.
	ctr := s.watchCounterFor("6.6.6.6")
	atomic.AddInt64(ctr, 1)
	s.watchPerIP.Range(func(key, val any) bool {
		if atomic.LoadInt64(val.(*int64)) == 0 {
			s.watchPerIP.Delete(key)
		}
		return true
	})
	if _, present := s.watchPerIP.Load("6.6.6.6"); !present {
		t.Fatal("active watch counter was wrongly evicted")
	}
}

// ---------------------------------------------------------------------------
// H-S1 — allowed members wired to the transport when TLS is configured.
// ---------------------------------------------------------------------------

// memberStubTransport is a raft.Transport that records SetAllowedMembers (H-S1).
type memberStubTransport struct {
	got []string
}

func (m *memberStubTransport) AppendEntries(context.Context, raft.ServerID, *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	return nil, nil
}
func (m *memberStubTransport) RequestVote(context.Context, raft.ServerID, *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	return nil, nil
}
func (m *memberStubTransport) InstallSnapshot(context.Context, raft.ServerID, *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	return nil, nil
}
func (m *memberStubTransport) TimeoutNow(context.Context, raft.ServerID) error { return nil }
func (m *memberStubTransport) SetLocalID(raft.ServerID)                        {}
func (m *memberStubTransport) Close() error                                    { return nil }
func (m *memberStubTransport) SetAllowedMembers(ids []string)                  { m.got = ids }

var _ raft.Transport = (*memberStubTransport)(nil)

func TestApplyAllowedMembers(t *testing.T) {
	s := bareServer("tok")
	s.config.Cluster = []ClusterMember{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}

	stub := &memberStubTransport{}
	s.applyAllowedMembers(stub)

	if len(stub.got) != 3 {
		t.Fatalf("SetAllowedMembers got %d ids, want 3", len(stub.got))
	}
	want := map[string]bool{"n1": true, "n2": true, "n3": true}
	for _, id := range stub.got {
		if !want[id] {
			t.Errorf("unexpected member id %q", id)
		}
	}
}

func TestTLSConfiguredGatesAllowedMembers(t *testing.T) {
	s := bareServer("tok")
	if s.tlsConfigured() {
		t.Fatal("tlsConfigured() = true with no TLS material")
	}
	s.config.TLSCert = "cert.pem"
	if !s.tlsConfigured() {
		t.Fatal("tlsConfigured() = false with TLSCert set")
	}
}

package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/fsm"
	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// ---------------------------------------------------------------------------
// leaderStub — a raft.Raft fake with a configurable Leader() and ReadIndex().
// ---------------------------------------------------------------------------

type leaderStub struct {
	leader      raft.ServerID
	readIndex   uint64
	readErr     error
	applied     uint64
	appliedCall int64
	unhealthy   bool // when true, Healthy() reports false (H-R2/H-O1)
	applyErr    error
	applyResult []byte
}

func (r *leaderStub) State() raft.RaftState {
	if r.leader == "self" {
		return raft.StateLeader
	}
	return raft.StateFollower
}
func (r *leaderStub) Leader() raft.ServerID             { return r.leader }
func (r *leaderStub) Term() uint64                      { return 1 }
func (r *leaderStub) LastIndex() uint64                 { return 0 }
func (r *leaderStub) LastTerm() uint64                  { return 1 }
func (r *leaderStub) AppliedIndex() uint64              { atomic.AddInt64(&r.appliedCall, 1); return r.applied }
func (r *leaderStub) Configuration() raft.Configuration { return raft.Configuration{} }

// WaitApplied returns nil immediately when the stub's applied index already
// satisfies idx, otherwise blocks until the context is canceled. It counts as
// an "applied" observation so tests can assert waitApplied consulted the node.
func (r *leaderStub) WaitApplied(ctx context.Context, idx uint64) error {
	atomic.AddInt64(&r.appliedCall, 1)
	if r.applied >= idx {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}
func (r *leaderStub) ReadIndex(_ context.Context) (uint64, error) {
	return r.readIndex, r.readErr
}
func (r *leaderStub) Apply(_ context.Context, _ []byte) ([]byte, error) {
	return r.applyResult, r.applyErr
}

// Healthy satisfies the healthChecker interface used by /ready (H-R2/H-O1).
func (r *leaderStub) Healthy() bool                                { return !r.unhealthy }
func (r *leaderStub) Snapshot() error                              { return nil }
func (r *leaderStub) Restore(_ context.Context, _ io.Reader) error { return nil }
func (r *leaderStub) Start() error                                 { return nil }
func (r *leaderStub) Shutdown() error                              { return nil }
func (r *leaderStub) RequestLeadership(_ context.Context) error    { return nil }
func (r *leaderStub) ReplaceServer(_ context.Context, _, _ raft.ServerID, _ raft.ServerAddress) error {
	return nil
}
func (r *leaderStub) AddServer(_ context.Context, _ raft.ServerID, _ raft.ServerAddress) error {
	return nil
}
func (r *leaderStub) AddLearner(_ context.Context, _ raft.ServerID, _ raft.ServerAddress) error {
	return nil
}
func (r *leaderStub) PromoteLearner(_ context.Context, _ raft.ServerID) error { return nil }
func (r *leaderStub) RemoveServer(_ context.Context, _ raft.ServerID) error   { return nil }
func (r *leaderStub) Demote(_ context.Context, _ raft.ServerID) error         { return nil }

var _ raft.Raft = (*leaderStub)(nil)

// ---------------------------------------------------------------------------
// H12 — loadConfig cluster validation
// ---------------------------------------------------------------------------

func TestValidateClusterRejectsEmpty(t *testing.T) {
	cfg := &Config{NodeID: "n1"}
	if err := validateCluster(cfg); err == nil {
		t.Fatal("empty cluster accepted, want error")
	}
}

func TestValidateClusterRequiresLocalMember(t *testing.T) {
	cfg := &Config{
		NodeID:  "n1",
		Cluster: []ClusterMember{{ID: "n2", Address: "a:1"}, {ID: "n3", Address: "b:2"}},
	}
	if err := validateCluster(cfg); err == nil {
		t.Fatal("cluster without local node_id accepted, want error")
	}
}

func TestValidateClusterRejectsDuplicateIDs(t *testing.T) {
	cfg := &Config{
		NodeID:  "n1",
		Cluster: []ClusterMember{{ID: "n1", Address: "a:1"}, {ID: "n1", Address: "b:2"}},
	}
	if err := validateCluster(cfg); err == nil {
		t.Fatal("duplicate member IDs accepted, want error")
	}
}

func TestValidateClusterAcceptsValid(t *testing.T) {
	cfg := &Config{
		NodeID:  "n1",
		Cluster: []ClusterMember{{ID: "n1", Address: "a:1"}, {ID: "n2", Address: "b:2"}},
	}
	if err := validateCluster(cfg); err != nil {
		t.Fatalf("valid cluster rejected: %v", err)
	}
}

// ---------------------------------------------------------------------------
// H12 — unauthenticated pprof must not bind to a non-loopback address
// ---------------------------------------------------------------------------

func TestDebugAddrIsLoopback(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:6060": true,
		"localhost:6060": true,
		"[::1]:6060":     true,
		":6060":          true,
		"0.0.0.0:6060":   false,
		"10.0.0.5:6060":  false,
		"192.168.1.1:60": false,
	}
	for addr, want := range cases {
		if got := debugAddrIsLoopback(addr); got != want {
			t.Errorf("debugAddrIsLoopback(%q) = %v, want %v", addr, got, want)
		}
	}
}

func TestStartRefusesUnauthPprofOnPublicAddr(t *testing.T) {
	s := bareServer("") // no tokens
	s.config.AllowNoAuth = true
	s.config.HTTPAddr = "127.0.0.1:0"
	s.config.DebugAddr = "0.0.0.0:6060"
	s.raftNode = &leaderStub{leader: "self"}
	s.kv = fsm.NewKVStore()
	s.watchMgr = newWatchMgrForTest(t, s.kv)
	s.initHTTP()

	// Start() should error because pprof would be exposed unauthenticated on a
	// public address. We stub raft.Start via leaderStub (no-op).
	if err := s.Start(); err == nil {
		s.Shutdown()
		t.Fatal("Start accepted unauthenticated pprof on 0.0.0.0, want error")
	}
}

// ---------------------------------------------------------------------------
// M13 — CORS defaults to deny; ?token= query auth dropped
// ---------------------------------------------------------------------------

func TestCORSDefaultsDeny(t *testing.T) {
	s := bareServer("tok") // CORSOrigins empty => deny
	h := s.corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Origin", "https://evil.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO = %q, want empty (deny) for un-allowlisted origin", got)
	}
}

func TestCORSAllowlistEchoesOrigin(t *testing.T) {
	s := bareServer("tok")
	s.config.CORSOrigins = "https://ui.example, https://admin.example"
	h := s.corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Origin", "https://admin.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://admin.example" {
		t.Errorf("ACAO = %q, want https://admin.example", got)
	}
}

func TestQueryParamTokenAuthRejected(t *testing.T) {
	s := bareServer("secret")
	handler := s.authMiddleware(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// Token supplied only via ?token= — must NOT authenticate (M13).
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(http.MethodGet, "/admin/cluster?token=secret", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("?token= query auth accepted: status=%d, want 401", w.Code)
	}
	// Header auth still works.
	r2 := httptest.NewRequest(http.MethodGet, "/admin/cluster", nil)
	r2.Header.Set("Authorization", "Bearer secret")
	w2 := httptest.NewRecorder()
	handler(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("header auth rejected: status=%d, want 200", w2.Code)
	}
}

// ---------------------------------------------------------------------------
// M12 — internal error strings must not leak to clients
// ---------------------------------------------------------------------------

func TestWriteErrorDoesNotLeakInternalDetail(t *testing.T) {
	s := bareServer("tok")
	w := httptest.NewRecorder()
	s.writeError(w, io.ErrUnexpectedEOF) // arbitrary internal error
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body) //nolint:errcheck
	if strings.Contains(body["error"], io.ErrUnexpectedEOF.Error()) {
		t.Errorf("response leaked internal error detail: %q", body["error"])
	}
	if body["error"] != "internal error" {
		t.Errorf("error = %q, want generic 'internal error'", body["error"])
	}
}

// ---------------------------------------------------------------------------
// M14 — watch connection caps
// ---------------------------------------------------------------------------

func TestAcquireWatchSlotGlobalCap(t *testing.T) {
	s := bareServer("tok")
	s.config.MaxWatchConnections = 2
	s.config.MaxWatchConnectionsPerIP = 100

	r1, ok1 := s.acquireWatchSlot("1.1.1.1")
	r2, ok2 := s.acquireWatchSlot("2.2.2.2")
	_, ok3 := s.acquireWatchSlot("3.3.3.3")
	if !ok1 || !ok2 {
		t.Fatal("first two watch slots should be granted")
	}
	if ok3 {
		t.Fatal("third watch slot exceeded global cap but was granted")
	}
	// Releasing one frees a slot.
	r1()
	_, ok4 := s.acquireWatchSlot("4.4.4.4")
	if !ok4 {
		t.Fatal("slot should be available after release")
	}
	r2()
}

func TestAcquireWatchSlotPerIPCap(t *testing.T) {
	s := bareServer("tok")
	s.config.MaxWatchConnections = 100
	s.config.MaxWatchConnectionsPerIP = 1

	_, ok1 := s.acquireWatchSlot("9.9.9.9")
	_, ok2 := s.acquireWatchSlot("9.9.9.9")
	if !ok1 {
		t.Fatal("first slot for IP should be granted")
	}
	if ok2 {
		t.Fatal("per-IP cap exceeded but slot granted")
	}
	// A different IP is unaffected.
	if _, ok := s.acquireWatchSlot("8.8.8.8"); !ok {
		t.Fatal("different IP incorrectly limited")
	}
}

// ---------------------------------------------------------------------------
// L7 — malformed numeric inputs return 400
// ---------------------------------------------------------------------------

func watchAuthServer(t *testing.T) *Server {
	t.Helper()
	s := bareServer("secret")
	// Make this node the leader so PUT/DELETE reach the local Apply path
	// (rather than forwarding) — NodeID is "test" from bareServer.
	s.raftNode = &leaderStub{leader: raft.ServerID(s.config.NodeID)}
	s.kv = fsm.NewKVStore()
	s.watchMgr = newWatchMgrForTest(t, s.kv)
	s.initHTTP()
	return s
}

func TestWatchRejectsMalformedRevision(t *testing.T) {
	s := watchAuthServer(t)
	r := httptest.NewRequest(http.MethodGet, "/v1/watch?key=k&revision=notanumber", nil)
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed revision: status=%d, want 400", w.Code)
	}
}

func TestPutRejectsMalformedSeqNum(t *testing.T) {
	s := watchAuthServer(t)
	r := httptest.NewRequest(http.MethodPut, "/v1/kv/foo", strings.NewReader("bar"))
	r.Header.Set("Authorization", "Bearer secret")
	r.Header.Set("X-Seq-Num", "abc")
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed X-Seq-Num on PUT: status=%d, want 400", w.Code)
	}
}

// ---------------------------------------------------------------------------
// L8 — Content-Type disambiguation and size limits
// ---------------------------------------------------------------------------

func TestPutRejectsInvalidJSONWhenJSONContentType(t *testing.T) {
	s := watchAuthServer(t)
	r := httptest.NewRequest(http.MethodPut, "/v1/kv/foo", strings.NewReader("{not json"))
	r.Header.Set("Authorization", "Bearer secret")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON body with JSON content-type: status=%d, want 400", w.Code)
	}
}

func TestPutRejectsOversizedValue(t *testing.T) {
	s := watchAuthServer(t)
	// Raise the raw body cap so the value-size limit (not MaxBytesReader) trips.
	s.config.MaxRequestBodyBytes = int64(maxValueSize) * 4
	big := strings.Repeat("x", maxValueSize+1)
	r := httptest.NewRequest(http.MethodPut, "/v1/kv/foo", strings.NewReader(big))
	r.Header.Set("Authorization", "Bearer secret")
	// No JSON content-type => raw body is the value.
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized value: status=%d, want 400", w.Code)
	}
}

func TestIsJSONContentType(t *testing.T) {
	if !isJSONContentType("application/json") {
		t.Error("application/json not recognized")
	}
	if !isJSONContentType("application/json; charset=utf-8") {
		t.Error("application/json with charset not recognized")
	}
	if isJSONContentType("text/plain") {
		t.Error("text/plain wrongly recognized as JSON")
	}
	if isJSONContentType("") {
		t.Error("empty content-type wrongly recognized as JSON")
	}
}

// ---------------------------------------------------------------------------
// H6 — follower forwarding uses https when TLS configured and validates addr
// ---------------------------------------------------------------------------

func TestLeaderSchemeFollowsTLS(t *testing.T) {
	s := bareServer("tok")
	if s.leaderScheme() != "http" {
		t.Errorf("scheme without TLS = %q, want http", s.leaderScheme())
	}
	s.config.HTTPSCert = "server.crt"
	if s.leaderScheme() != "https" {
		t.Errorf("scheme with HTTPSCert = %q, want https", s.leaderScheme())
	}
}

func TestForwardToLeaderRejectsInvalidAddress(t *testing.T) {
	s := bareServer("tok")
	// Cluster member with a bogus (no host:port) address for the leader.
	s.config.Cluster = []ClusterMember{{ID: "leader", HTTPAddress: "not-a-valid-address"}}
	s.raftNode = &leaderStub{leader: "leader"}

	r := httptest.NewRequest(http.MethodPost, "/command", strings.NewReader("{}"))
	w := httptest.NewRecorder()
	err := s.forwardToLeader(w, r, "leader", "/command")
	if err == nil {
		t.Fatal("forwardToLeader accepted an invalid leader address")
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 for invalid leader address", w.Code)
	}
	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body) //nolint:errcheck
	if strings.Contains(body["error"], "not-a-valid-address") {
		t.Errorf("response leaked internal address detail: %q", body["error"])
	}
}

// TestForwardToLeaderForwardsToConfiguredAddr verifies the forward hop actually
// reaches the leader's configured HTTP address and copies the Authorization
// header through (H6). A plaintext leader server is used with TLS off.
func TestForwardToLeaderForwardsToConfiguredAddr(t *testing.T) {
	var gotAuth string
	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusTeapot)
	}))
	defer leaderSrv.Close()

	addr := strings.TrimPrefix(leaderSrv.URL, "http://")

	s := bareServer("secret") // TLS off => http scheme
	s.config.Cluster = []ClusterMember{{ID: "leader", HTTPAddress: addr}}
	s.raftNode = &leaderStub{leader: "leader"}

	r := httptest.NewRequest(http.MethodPost, "/command", strings.NewReader("{}"))
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	if err := s.forwardToLeader(w, r, "leader", "/command"); err != nil {
		t.Fatalf("forwardToLeader: %v", err)
	}
	if w.Code != http.StatusTeapot {
		t.Errorf("status = %d, want 418 (proxied from leader)", w.Code)
	}
	if gotAuth != "Bearer secret" {
		t.Errorf("leader saw Authorization = %q, want 'Bearer secret'", gotAuth)
	}
}

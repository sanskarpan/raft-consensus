package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raft-consensus/pkg/raft"
)

// clusterHandler writes a minimal leader ClusterInfo for /admin/cluster.
func writeCluster(w http.ResponseWriter, leader string, term uint64, servers []raft.Server) {
	info := ClusterInfo{
		NodeID: leader, State: "Leader", Leader: leader, Term: term,
		Config: raft.Configuration{Servers: servers},
	}
	b, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

// ---------------------------------------------------------------------------
// H5 — mutating writes must carry idempotency keys (X-Client-ID + X-Seq-Num)
// ---------------------------------------------------------------------------

// TestSubmitCommandCarriesIdempotencyKey verifies that SubmitCommand attaches
// both idempotency headers to its write, and that the same seq is reused when
// the request is retried against a second node (never retrying a write without
// a key). Pre-fix, SubmitCommand set no headers at all.
func TestSubmitCommandCarriesIdempotencyKey(t *testing.T) {
	type hdr struct {
		clientID string
		seqNum   string
	}
	var mu sync.Mutex
	var seen []hdr

	// srv1: cluster ok, but /command always fails so the client retries srv2.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/cluster" {
			writeCluster(w, "n1", 1, nil)
			return
		}
		mu.Lock()
		seen = append(seen, hdr{r.Header.Get(headerClientID), r.Header.Get(headerSeqNum)})
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/cluster" {
			writeCluster(w, "n1", 1, nil)
			return
		}
		if r.URL.Path == "/command" {
			mu.Lock()
			seen = append(seen, hdr{r.Header.Get(headerClientID), r.Header.Get(headerSeqNum)})
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":"ok"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv2.Close()

	addr1 := strings.TrimPrefix(srv1.URL, "http://")
	addr2 := strings.TrimPrefix(srv2.URL, "http://")
	c := NewClient(WithAddresses([]string{addr1, addr2}))

	if _, err := c.SubmitCommand("k", "v"); err != nil {
		t.Fatalf("SubmitCommand: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("no /command requests observed")
	}
	for i, h := range seen {
		if h.clientID == "" {
			t.Errorf("request %d missing %s header", i, headerClientID)
		}
		if h.seqNum == "" {
			t.Errorf("request %d missing %s header", i, headerSeqNum)
		}
	}
	// All retries of the same logical write must share one seq number.
	first := seen[0].seqNum
	for i, h := range seen {
		if h.seqNum != first {
			t.Errorf("request %d seq %q != first seq %q (retry changed the idempotency key)", i, h.seqNum, first)
		}
	}
}

// TestTxnCarriesIdempotencyKey verifies that Txn attaches idempotency headers
// (pre-fix doPost sent none) and reuses one seq across retries.
func TestTxnCarriesIdempotencyKey(t *testing.T) {
	var mu sync.Mutex
	var seqs []string
	fail := true

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/txn" {
			mu.Lock()
			seqs = append(seqs, r.Header.Get(headerSeqNum))
			cid := r.Header.Get(headerClientID)
			shouldFail := fail
			fail = false // succeed on the retry
			mu.Unlock()
			if cid == "" {
				t.Errorf("txn missing %s header", headerClientID)
			}
			if shouldFail {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"succeeded":true,"revision":7}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient(WithAddresses([]string{addr}))

	resp, err := c.Txn(&ClientTxnRequest{})
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("expected txn to succeed")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seqs) < 2 {
		t.Fatalf("expected at least 2 txn attempts (one failure + one retry), got %d", len(seqs))
	}
	for i, s := range seqs {
		if s == "" {
			t.Errorf("attempt %d missing %s header", i, headerSeqNum)
		}
		if s != seqs[0] {
			t.Errorf("attempt %d seq %q != first %q (retry changed idempotency key)", i, s, seqs[0])
		}
	}
}

// ---------------------------------------------------------------------------
// H7 — quorum reads must require a majority to agree on the same value
// ---------------------------------------------------------------------------

// TestQuorumReadRequiresAgreement verifies that when a majority of nodes
// disagree, the quorum read errors instead of returning the first node's
// value. Pre-fix it counted bare HTTP 200s and returned the first reply.
func TestQuorumReadRequiresAgreement(t *testing.T) {
	servers := []raft.Server{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}

	makeSrv := func(val string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/admin/cluster" {
				writeCluster(w, "n1", 1, servers)
				return
			}
			if r.URL.Path == "/command" {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(val))
				return
			}
			http.NotFound(w, r)
		}))
	}

	// Three distinct values → no value reaches quorum (2 of 3).
	s1, s2, s3 := makeSrv("A"), makeSrv("B"), makeSrv("C")
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	addr := func(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }
	c := NewClient(WithAddresses([]string{addr(s1), addr(s2), addr(s3)}))

	if _, err := c.GetValueWithConsistency("k", ReadQuorum); err == nil {
		t.Fatal("expected error when a majority disagree, got nil")
	}
}

// TestQuorumReadReturnsAgreedValue verifies a value that a majority agree on
// is returned even if a minority disagrees.
func TestQuorumReadReturnsAgreedValue(t *testing.T) {
	servers := []raft.Server{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}

	makeSrv := func(val string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/admin/cluster" {
				writeCluster(w, "n1", 1, servers)
				return
			}
			if r.URL.Path == "/command" {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(val))
				return
			}
			http.NotFound(w, r)
		}))
	}

	// Two agree on "same", one disagrees → quorum of 2 reached.
	s1, s2, s3 := makeSrv("same"), makeSrv("same"), makeSrv("other")
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	addr := func(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }
	c := NewClient(WithAddresses([]string{addr(s1), addr(s2), addr(s3)}))

	val, err := c.GetValueWithConsistency("k", ReadQuorum)
	if err != nil {
		t.Fatalf("expected quorum success, got %v", err)
	}
	if val != "same" {
		t.Errorf("got %q, want %q", val, "same")
	}
}

// ---------------------------------------------------------------------------
// L5 — maxLeaseDuration must be initialized; Client fields must be race-safe
// ---------------------------------------------------------------------------

// TestMaxLeaseDurationInitialized verifies the lease window is non-zero by
// default (pre-fix it was always 0, so the lease was always expired) and that
// WithMaxLeaseDuration overrides it.
func TestMaxLeaseDurationInitialized(t *testing.T) {
	c := NewClient()
	if c.maxLeaseDuration <= 0 {
		t.Fatalf("maxLeaseDuration = %v, want > 0", c.maxLeaseDuration)
	}

	c2 := NewClient(WithMaxLeaseDuration(3 * time.Second))
	if c2.maxLeaseDuration != 3*time.Second {
		t.Errorf("WithMaxLeaseDuration = %v, want 3s", c2.maxLeaseDuration)
	}
}

// TestLeaseGetSetsExpiry verifies that after a successful lease read the
// expiry is pushed into the future (only possible with a non-zero duration).
func TestLeaseGetSetsExpiry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/cluster" {
			writeCluster(w, "n1", 1, []raft.Server{{ID: "n1"}})
			return
		}
		if r.URL.Path == "/command" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("v"))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient(WithAddresses([]string{addr}), WithMaxLeaseDuration(2*time.Second))

	if _, err := c.GetValueWithConsistency("k", ReadLease); err != nil {
		t.Fatalf("lease read: %v", err)
	}
	c.mu.Lock()
	exp := c.leaseExpiry
	c.mu.Unlock()
	if !exp.After(time.Now()) {
		t.Errorf("leaseExpiry %v not in the future; lease window was not applied", exp)
	}
}

// TestClientFieldsRaceSafe drives foreground reads/writes concurrently with a
// Watch goroutine touching leader-tracking fields. Run with -race; pre-fix the
// unlocked currentAddr/leader/leaseExpiry fields data-raced.
func TestClientFieldsRaceSafe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/admin/cluster":
			writeCluster(w, "n1", 1, []raft.Server{{ID: "n1"}})
		case r.URL.Path == "/command":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("v"))
		case strings.HasPrefix(r.URL.Path, "/v1/watch"):
			// Stream one SSE event then keep the connection open briefly.
			fl, _ := w.(http.Flusher)
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("data: {\"revision\":1,\"events\":[]}\n\n"))
			if fl != nil {
				fl.Flush()
			}
			time.Sleep(50 * time.Millisecond)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient(WithAddresses([]string{addr}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Background watch goroutine (reads c.addrs, exercises reconnect paths).
	ch, _ := c.Watch(ctx, "k")
	go func() {
		for range ch {
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = c.GetClusterInfo()
				_, _ = c.GetValueWithConsistency("k", ReadLease)
				_ = c.getCurrentAddr()
			}
		}()
	}
	wg.Wait()
	cancel()
}

package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// clusterInfoJSON returns a JSON-encoded ClusterInfo with the given leader
// and an empty Servers list in the configuration.
func clusterInfoJSON(nodeID, state, leader string, term uint64) string {
	info := ClusterInfo{
		NodeID:    nodeID,
		State:     state,
		Leader:    leader,
		Term:      term,
		CommitIdx: 5,
		Config:    raft.Configuration{Servers: []raft.Server{}},
	}
	b, _ := json.Marshal(info)
	return string(b)
}

// TestClientGetLeaderSuccess verifies that GetLeader() returns the leader ID
// when the server responds with a valid cluster JSON.
func TestClientGetLeaderSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/cluster" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(clusterInfoJSON("n1", "Leader", "n1", 1)))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	// Strip the "http://" prefix so the client prepends it correctly.
	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient(WithAddresses([]string{addr}))

	leader, err := c.GetLeader()
	if err != nil {
		t.Fatalf("GetLeader() error: %v", err)
	}
	if leader != "n1" {
		t.Errorf("GetLeader() = %q, want %q", leader, "n1")
	}
}

// TestClientGetLeaderAllFailed verifies that GetClusterInfo() returns an error
// when all servers respond with 500.
func TestClientGetLeaderAllFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient(WithAddresses([]string{addr}))

	_, err := c.GetClusterInfo()
	if err == nil {
		t.Fatal("expected error when all servers return 500, got nil")
	}
}

// TestClientSubmitCommandRetryOnFirstFailure verifies that SubmitCommand
// succeeds when the first server is unhealthy but the second responds correctly.
//
// The client.SubmitCommand flow is:
//  1. GetClusterInfo() — returns info with currentAddr set to a server.
//  2. sendCommand to currentAddr — fails.
//  3. Retry all addrs in c.addrs; second one succeeds.
//
// We achieve this by having two mock servers:
//   - Server 1: cluster info returns Server 2 as "current" and the command endpoint fails.
//   - Server 2: cluster info succeeds and command endpoint returns a valid result.
func TestClientSubmitCommandRetryOnFirstFailure(t *testing.T) {
	// We need to know server2's address before building its handler, so we use
	// a late-binding approach: capture the pointer via a closure.
	var srv2Addr string

	// Server 1: cluster endpoint returns srv2Addr as current server,
	//           command endpoint always fails.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/cluster" {
			// Report srv2 as the leader so currentAddr is set to srv1 then retried.
			info := ClusterInfo{
				NodeID:    "n1",
				State:     "Leader",
				Leader:    "n1",
				Term:      1,
				CommitIdx: 0,
				Config:    raft.Configuration{Servers: []raft.Server{}},
			}
			b, _ := json.Marshal(info)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}
		// /command always fails on srv1.
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv1.Close()

	// Server 2: both cluster and command succeed.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/cluster" {
			info := ClusterInfo{
				NodeID:    "n2",
				State:     "Follower",
				Leader:    "n1",
				Term:      1,
				CommitIdx: 0,
				Config:    raft.Configuration{Servers: []raft.Server{}},
			}
			b, _ := json.Marshal(info)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}
		if r.URL.Path == "/command" {
			// Return a valid KvResult JSON.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":"testval"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv2.Close()
	srv2Addr = strings.TrimPrefix(srv2.URL, "http://")

	addr1 := strings.TrimPrefix(srv1.URL, "http://")
	// Provide both addresses so the retry loop in SubmitCommand finds srv2.
	c := NewClient(WithAddresses([]string{addr1, srv2Addr}))

	_, err := c.SubmitCommand("key1", "testval")
	if err != nil {
		t.Fatalf("SubmitCommand() error: %v", err)
	}
}

// TestClientHealthCheck verifies that HealthCheck() returns nil when at least
// one server is healthy.
func TestClientHealthCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient(WithAddresses([]string{addr}))

	if err := c.HealthCheck(); err != nil {
		t.Errorf("HealthCheck() error: %v", err)
	}
}

// TestClientHealthCheckAllDown verifies that HealthCheck() returns an error
// when all servers are unhealthy.
func TestClientHealthCheckAllDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient(WithAddresses([]string{addr}))

	if err := c.HealthCheck(); err == nil {
		t.Fatal("expected error when all servers are down, got nil")
	}
}

// TestClientWithConsistencyStale verifies that GetValueWithConsistency with
// ReadStale succeeds when at least one server responds to /command.
func TestClientWithConsistencyStale(t *testing.T) {
	// Both servers respond successfully to /command.
	makeHandler := func(value string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/command" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"value":"` + value + `"}`))
				return
			}
			if r.URL.Path == "/admin/cluster" {
				info := ClusterInfo{
					NodeID: "n1",
					State:  "Leader",
					Leader: "n1",
					Term:   1,
					Config: raft.Configuration{Servers: []raft.Server{}},
				}
				b, _ := json.Marshal(info)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(b)
				return
			}
			http.NotFound(w, r)
		}
	}

	srv1 := httptest.NewServer(makeHandler("val1"))
	defer srv1.Close()
	srv2 := httptest.NewServer(makeHandler("val2"))
	defer srv2.Close()

	addr1 := strings.TrimPrefix(srv1.URL, "http://")
	addr2 := strings.TrimPrefix(srv2.URL, "http://")

	c := NewClient(WithAddresses([]string{addr1, addr2}))

	val, err := c.GetValueWithConsistency("mykey", ReadStale)
	if err != nil {
		t.Fatalf("GetValueWithConsistency(ReadStale) error: %v", err)
	}
	if val == "" {
		t.Error("GetValueWithConsistency(ReadStale) returned empty value")
	}
}

// TestClientWithConsistencyQuorum verifies that GetValueWithConsistency with
// ReadQuorum succeeds when quorum=1 (0 servers + 1 = quorum of 1 with an
// empty Servers list means quorum = 0/2+1 = 1).
func TestClientWithConsistencyQuorum(t *testing.T) {
	makeHandler := func() http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/command" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"value":"quorumval"}`))
				return
			}
			if r.URL.Path == "/admin/cluster" {
				// Empty Servers list → quorum = 0/2+1 = 1
				info := ClusterInfo{
					NodeID: "n1",
					State:  "Leader",
					Leader: "n1",
					Term:   1,
					Config: raft.Configuration{Servers: []raft.Server{}},
				}
				b, _ := json.Marshal(info)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(b)
				return
			}
			http.NotFound(w, r)
		}
	}

	srv1 := httptest.NewServer(makeHandler())
	defer srv1.Close()
	srv2 := httptest.NewServer(makeHandler())
	defer srv2.Close()

	addr1 := strings.TrimPrefix(srv1.URL, "http://")
	addr2 := strings.TrimPrefix(srv2.URL, "http://")

	c := NewClient(WithAddresses([]string{addr1, addr2}))

	val, err := c.GetValueWithConsistency("mykey", ReadQuorum)
	if err != nil {
		t.Fatalf("GetValueWithConsistency(ReadQuorum) error: %v", err)
	}
	if val == "" {
		t.Error("GetValueWithConsistency(ReadQuorum) returned empty value")
	}
}

// ---------------------------------------------------------------------------
// 5.1.8 — Cluster failure scenarios
// ---------------------------------------------------------------------------

// TestClientLeaderChangeMidOperation verifies that when the leader changes
// between GetClusterInfo and the command call, the client retries and succeeds
// against a different node that is now the leader.
func TestClientLeaderChangeMidOperation(t *testing.T) {
	// callCount tracks how many times /admin/cluster has been called on srv1.
	var callCount int

	// srv1 reports itself as leader on the first call, then returns 503 on
	// subsequent /command calls — simulating a leader that crashes mid-operation.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/cluster" {
			callCount++
			info := ClusterInfo{
				NodeID: "n1", State: "Leader", Leader: "n1", Term: 1,
				Config: raft.Configuration{Servers: []raft.Server{}},
			}
			b, _ := json.Marshal(info)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}
		// /command always fails on srv1 (leader crashed).
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv1.Close()

	// srv2 is a follower that successfully handles /command after the
	// client retries it.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/cluster" {
			info := ClusterInfo{
				NodeID: "n2", State: "Follower", Leader: "n1", Term: 1,
				Config: raft.Configuration{Servers: []raft.Server{}},
			}
			b, _ := json.Marshal(info)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}
		if r.URL.Path == "/command" {
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

	_, err := c.SubmitCommand("key", "val")
	if err != nil {
		t.Fatalf("SubmitCommand after leader change: %v", err)
	}
}

// TestClientQuorumLoss verifies that when all nodes in the cluster are
// unreachable, GetClusterInfo returns an error rather than hanging or
// returning stale data.
func TestClientQuorumLoss(t *testing.T) {
	// All three servers return connection-reset responses (simulating a
	// total cluster outage — quorum lost).
	makeDown := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Immediately close the connection to simulate a crashed node.
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, _ := hj.Hijack()
				conn.Close()
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
	}

	srv1, srv2, srv3 := makeDown(), makeDown(), makeDown()
	defer srv1.Close()
	defer srv2.Close()
	defer srv3.Close()

	addr := func(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }
	c := NewClient(
		WithAddresses([]string{addr(srv1), addr(srv2), addr(srv3)}),
		WithTimeout(500*time.Millisecond),
	)
	c.httpClient.Timeout = 500 * time.Millisecond

	_, err := c.GetClusterInfo()
	if err == nil {
		t.Fatal("expected error when all nodes are down (quorum lost), got nil")
	}
	t.Logf("correctly returned error on quorum loss: %v", err)
}

// TestClientPartialFailoverSucceeds verifies that the client can still
// complete operations when a minority of nodes are down (quorum maintained).
func TestClientPartialFailoverSucceeds(t *testing.T) {
	// srv1 is down.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv1.Close()

	// srv2 is healthy and acts as leader.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/cluster" {
			info := ClusterInfo{
				NodeID: "n2", State: "Leader", Leader: "n2", Term: 2,
				Config: raft.Configuration{Servers: []raft.Server{
					{ID: "n1"}, {ID: "n2"}, {ID: "n3"},
				}},
			}
			b, _ := json.Marshal(info)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}
		if r.URL.Path == "/command" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"value":"written"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv2.Close()

	addr1 := strings.TrimPrefix(srv1.URL, "http://")
	addr2 := strings.TrimPrefix(srv2.URL, "http://")
	c := NewClient(WithAddresses([]string{addr1, addr2}))

	_, err := c.SubmitCommand("k", "v")
	if err != nil {
		t.Fatalf("SubmitCommand with minority failure: %v", err)
	}
}

// TestClientNodeRecovery verifies that after a node comes back online, the
// client can successfully retrieve cluster info from it.
func TestClientNodeRecovery(t *testing.T) {
	// Simulate a node that is initially down and then recovers.
	recovering := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !recovering {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/admin/cluster" {
			info := ClusterInfo{
				NodeID: "n1", State: "Leader", Leader: "n1", Term: 3,
				Config: raft.Configuration{Servers: []raft.Server{}},
			}
			b, _ := json.Marshal(info)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient(WithAddresses([]string{addr}))

	// First call — node is down.
	_, err := c.GetClusterInfo()
	if err == nil {
		t.Fatal("expected error when node is down, got nil")
	}

	// Node recovers.
	recovering = true

	// Second call — node is back up.
	info, err := c.GetClusterInfo()
	if err != nil {
		t.Fatalf("GetClusterInfo after recovery: %v", err)
	}
	if info.State != "Leader" {
		t.Errorf("expected Leader after recovery, got %q", info.State)
	}
}

// TestClientTimeout verifies that a call to GetClusterInfo times out when the
// server takes longer than the configured client timeout.
func TestClientTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sleep for 2 seconds — longer than the client timeout.
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(clusterInfoJSON("n1", "Leader", "n1", 1)))
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient(
		WithAddresses([]string{addr}),
		WithTimeout(100*time.Millisecond),
	)

	// Override the underlying http.Client timeout to match.
	c.httpClient.Timeout = 100 * time.Millisecond

	_, err := c.GetClusterInfo()
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	t.Logf("correctly timed out: %v", err)
}

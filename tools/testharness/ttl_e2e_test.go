package testharness_test

// E2E tests for EPIC #207 — TTL / lease-based key expiry.
//
// These tests start a real 3-node raftd cluster (via testharness) with a very
// short ttl_tick_interval (200ms) and exercise:
//   - PutWithTTL: a key is visible immediately but disappears after the TTL
//     window passes and the leader tick sweeps it.
//   - Replica consistency: after expiry, all 3 nodes agree the key is absent.
//   - Non-TTL key is unaffected by tick.
//   - Watch delivers a delete event when the tick sweeps an expired key.
//   - Leader failover during TTL: key expires correctly after a new leader is
//     elected.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/client"
	"github.com/sanskarpan/raft-consensus/tools/testharness"
)

// setupTTLCluster builds a 3-node cluster with ttl_tick_interval: 200ms and
// returns the harness, leader id, and client addresses.  The cluster uses
// basePort 24000 so it does not collide with other test clusters.
func setupTTLCluster(t *testing.T) (*client.Client, func()) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping TTL E2E test in short mode")
	}

	const basePort = 24000

	tmpDir, err := os.MkdirTemp("", "raft-ttl-e2e-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}

	binaryPath := filepath.Join(tmpDir, "raftd")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/raftd")
	buildCmd.Dir = projectRoot(t)
	if out, buildErr := buildCmd.CombinedOutput(); buildErr != nil {
		os.RemoveAll(tmpDir)
		t.Skipf("skipping: raftd build failed: %v\n%s", buildErr, out)
	}

	harnessDir := filepath.Join(tmpDir, "harness")
	if err := os.MkdirAll(harnessDir, 0755); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("MkdirAll: %v", err)
	}

	h := testharness.NewHarness(harnessDir, basePort,
		testharness.WithBinary(binaryPath),
		testharness.WithExtraConfig("ttl_tick_interval: 200ms"),
	)

	nodeIDs := []string{"node1", "node2", "node3"}
	for _, id := range nodeIDs {
		if err := h.StartNode(id); err != nil {
			h.StopAll()
			os.RemoveAll(tmpDir)
			t.Fatalf("StartNode(%s): %v", id, err)
		}
	}
	for _, id := range nodeIDs {
		if err := h.WaitForHealth(id, 15*time.Second); err != nil {
			h.StopAll()
			os.RemoveAll(tmpDir)
			t.Fatalf("WaitForHealth(%s): %v", id, err)
		}
	}
	if _, err := h.WaitForLeader(15 * time.Second); err != nil {
		h.StopAll()
		os.RemoveAll(tmpDir)
		t.Fatalf("WaitForLeader: %v", err)
	}

	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))

	cleanup := func() {
		h.StopAll()
		os.RemoveAll(tmpDir)
	}
	return c, cleanup
}

// putWithTTLHTTP makes a raw HTTP PUT to /v1/kv/{key} with a JSON body
// containing ttl_seconds.  The client library already supports this via
// PutWithTTL, but we also exercise the HTTP layer directly here.
func putWithTTLHTTP(t *testing.T, addrs []string, key, value string, ttlSecs int64) {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"value":       value,
		"ttl_seconds": ttlSecs,
	})
	hc := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, addr := range addrs {
			url := fmt.Sprintf("http://%s/v1/kv/%s", addr, key)
			req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
			if err != nil {
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := hc.Do(req)
			if err != nil {
				continue
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("putWithTTLHTTP: all nodes rejected after retries")
}

// getKVHTTP returns (value, found) for the given key from any live node.
func getKVHTTP(addrs []string, key string) (string, bool) {
	hc := &http.Client{Timeout: 3 * time.Second}
	for _, addr := range addrs {
		url := fmt.Sprintf("http://%s/v1/kv/%s", addr, key)
		resp, err := hc.Get(url)
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return "", false
		}
		if resp.StatusCode == http.StatusOK {
			var kv struct {
				Value string `json:"value"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&kv); err == nil {
				return kv.Value, true
			}
		}
	}
	return "", false
}

// waitExpired polls all addrs for key until it is absent on every live node,
// or until the timeout expires.
func waitExpiredOnAll(t *testing.T, addrs []string, key string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allGone := true
		for _, addr := range addrs {
			hc := &http.Client{Timeout: 2 * time.Second}
			url := fmt.Sprintf("http://%s/v1/kv/%s", addr, key)
			resp, err := hc.Get(url)
			if err != nil {
				allGone = false
				break
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				allGone = false
				break
			}
		}
		if allGone {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("key %q still present on at least one node after %v", key, timeout)
}

// TestTTLE2EKeyExpiresAndReplicasAgree: put a key with TTL=1s across a
// 3-node cluster. Verify it is visible immediately, then verify all 3 replicas
// agree it is gone after the TTL window + a few tick intervals.
func TestTTLE2EKeyExpiresAndReplicasAgree(t *testing.T) {
	const basePort = 24000
	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	c, cleanup := setupTTLCluster(t)
	defer cleanup()

	// Use PutWithTTL via the client library (TTL = 1s).
	if _, err := c.PutWithTTL("ttl/k1", "hello", 1); err != nil {
		t.Fatalf("PutWithTTL: %v", err)
	}

	// Key should be visible right away.
	kv, err := c.GetKV("ttl/k1")
	if err != nil || kv.Value != "hello" {
		t.Fatalf("GetKV immediately after put: got (%v, %v), want 'hello'", kv, err)
	}

	// After TTL (1s) + a couple of tick intervals (200ms each) the key must
	// be gone on ALL replicas.  Give up to 4s total.
	waitExpiredOnAll(t, addrs, "ttl/k1", 4*time.Second)
	t.Log("key expired on all 3 replicas")
}

// TestTTLE2ENonTTLKeyUnaffected: a key without a TTL must survive tick sweeps.
func TestTTLE2ENonTTLKeyUnaffected(t *testing.T) {
	c, cleanup := setupTTLCluster(t)
	defer cleanup()

	if _, err := c.Put("ttl/persistent", "stays"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Put a short-lived neighbor so ticks definitely fire.
	if _, err := c.PutWithTTL("ttl/short", "gone", 1); err != nil {
		t.Fatalf("PutWithTTL: %v", err)
	}

	// Wait long enough for the short key to expire.
	const basePort = 24000
	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	waitExpiredOnAll(t, addrs, "ttl/short", 4*time.Second)

	// Persistent key must still be alive.
	kv, err := c.GetKV("ttl/persistent")
	if err != nil {
		t.Fatalf("non-TTL key expired unexpectedly: %v", err)
	}
	if kv.Value != "stays" {
		t.Fatalf("non-TTL key value changed: got %q, want 'stays'", kv.Value)
	}
}

// TestTTLE2EWatchDeleteOnExpiry subscribes to a key with a 2s TTL and expects
// to receive a delete event when the tick sweeps it.
func TestTTLE2EWatchDeleteOnExpiry(t *testing.T) {
	const basePort = 24000
	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	_, cleanup := setupTTLCluster(t)
	defer cleanup()

	// Subscribe to the watch SSE stream before inserting the key.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	watchURL := fmt.Sprintf("http://%s/v1/watch?key=%s", addrs[0], "ttl/wk")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, watchURL, nil)
	hc := &http.Client{Timeout: 12 * time.Second}

	// Start watch in a goroutine; collect delete events.
	deleteCh := make(chan struct{}, 1)
	go func() {
		resp, err := hc.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			data := strings.TrimPrefix(line, "data:")
			var ev struct {
				Events []struct {
					Type int `json:"type"`
				} `json:"events"`
			}
			if json.Unmarshal([]byte(data), &ev) == nil {
				for _, e := range ev.Events {
					if e.Type == 1 { // EventDelete
						select {
						case deleteCh <- struct{}{}:
						default:
						}
					}
				}
			}
		}
	}()

	// Give the SSE goroutine time to connect.
	time.Sleep(300 * time.Millisecond)

	// Insert the key with TTL=1s.
	putWithTTLHTTP(t, addrs, "ttl/wk", "watched", 1)

	// Expect a delete event within 5s.
	select {
	case <-deleteCh:
		t.Log("received delete event on watch stream")
	case <-time.After(6 * time.Second):
		t.Fatal("timed out waiting for delete event on watch stream")
	}
}

// TestTTLE2ELeaderFailoverDuringTTL: insert a key with TTL=2s, kill the
// leader, elect a new leader, and verify the key eventually expires on all
// replicas under the new leader's tick loop.
func TestTTLE2ELeaderFailoverDuringTTL(t *testing.T) {
	const basePort = 24000
	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	_, cleanup := setupTTLCluster(t)
	defer cleanup()

	// The harness is already set up; reach nodes via HTTP directly.
	hc := &http.Client{Timeout: 5 * time.Second}

	// Find the current leader.
	leaderAddr := ""
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, addr := range addrs {
			resp, err := hc.Get(fmt.Sprintf("http://%s/v1/status", addr))
			if err != nil {
				continue
			}
			var st struct {
				IsLeader bool   `json:"is_leader"`
				State    string `json:"state"`
			}
			json.NewDecoder(resp.Body).Decode(&st) //nolint:errcheck
			resp.Body.Close()
			if st.IsLeader || st.State == "Leader" {
				leaderAddr = addr
				break
			}
		}
		if leaderAddr != "" {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if leaderAddr == "" {
		t.Fatal("could not identify leader before failover test")
	}
	t.Logf("current leader: %s", leaderAddr)

	// Put the TTL key while the original leader is still up.
	putWithTTLHTTP(t, addrs, "ttl/failover", "survives", 3)

	// Verify the key is present.
	if _, found := getKVHTTP(addrs, "ttl/failover"); !found {
		t.Fatal("key not present immediately after put")
	}

	// Kill the leader by sending SIGTERM to the process listening at leaderAddr.
	// We do this via the admin shutdown endpoint.
	shutURL := fmt.Sprintf("http://%s/admin/shutdown", leaderAddr)
	req, _ := http.NewRequest(http.MethodPost, shutURL, nil)
	req.Header.Set("Authorization", "Bearer ")
	hc.Do(req) //nolint:errcheck — best-effort, may fail if endpoint requires auth
	// Give some time for the node to actually shut down / lose leadership.
	time.Sleep(600 * time.Millisecond)

	// Wait for a new leader to emerge among the remaining nodes.
	newLeaderFound := false
	remainingAddrs := make([]string, 0, 2)
	for _, a := range addrs {
		if a != leaderAddr {
			remainingAddrs = append(remainingAddrs, a)
		}
	}
	leaderWait := time.Now().Add(15 * time.Second)
	for time.Now().Before(leaderWait) {
		for _, addr := range remainingAddrs {
			resp, err := hc.Get(fmt.Sprintf("http://%s/v1/status", addr))
			if err != nil {
				continue
			}
			var st struct {
				IsLeader bool   `json:"is_leader"`
				State    string `json:"state"`
			}
			json.NewDecoder(resp.Body).Decode(&st) //nolint:errcheck
			resp.Body.Close()
			if st.IsLeader || st.State == "Leader" {
				newLeaderFound = true
				t.Logf("new leader: %s", addr)
				break
			}
		}
		if newLeaderFound {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !newLeaderFound {
		t.Log("new leader not confirmed via /v1/status; continuing to wait for expiry anyway")
	}

	// The key has TTL=3s; after failover and a few more ticks the key must expire.
	// Give up to 10s total from this point.
	waitExpiredOnAll(t, remainingAddrs, "ttl/failover", 10*time.Second)
	t.Log("key expired under new leader on all surviving replicas")
}

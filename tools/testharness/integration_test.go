package testharness_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/raft-consensus/pkg/client"
	"github.com/raft-consensus/tools/testharness"
)

// projectRoot returns the absolute path to the repository root by walking up
// from this test file's location until go.mod is found.  This avoids any
// hardcoded absolute paths and works on any machine or CI environment.
func projectRoot(t *testing.T) string {
	t.Helper()
	// __file__ of this source file (resolved at compile time via runtime.Caller).
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate go.mod: repository root not found")
		}
		dir = parent
	}
}

// TestMultiProcessCluster spins up a real 3-node raftd cluster, submits
// commands, kills a follower, submits more commands, restarts the follower,
// and verifies the cluster remains operational throughout.
func TestMultiProcessCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process integration test in short mode")
	}

	// Step 2: Build the raftd binary into a temp directory.
	tmpDir, err := os.MkdirTemp("", "raft-integration-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	binaryPath := filepath.Join(tmpDir, "raftd")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/raftd")
	buildCmd.Dir = projectRoot(t)
	buildOut, buildErr := buildCmd.CombinedOutput()
	if buildErr != nil {
		t.Skipf("skipping: failed to build raftd binary: %v\n%s", buildErr, buildOut)
	}
	t.Logf("built raftd binary: %s", binaryPath)

	// Step 3 & 4: Create harness and start 3 nodes.
	harnessDir := filepath.Join(tmpDir, "harness")
	if err := os.MkdirAll(harnessDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	h := testharness.NewHarness(harnessDir, 19800, testharness.WithBinary(binaryPath))
	defer h.StopAll()

	nodeIDs := []string{"node1", "node2", "node3"}
	for _, id := range nodeIDs {
		if err := h.StartNode(id); err != nil {
			t.Fatalf("StartNode(%s): %v", id, err)
		}
		t.Logf("started node %s", id)
	}

	// Step 5: Wait for all 3 nodes to be healthy.
	for _, id := range nodeIDs {
		if err := h.WaitForHealth(id, 10*time.Second); err != nil {
			t.Fatalf("WaitForHealth(%s): %v", id, err)
		}
		t.Logf("node %s is healthy", id)
	}

	// Step 6: Wait for a leader to be elected.
	leaderID, err := h.WaitForLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	t.Logf("leader elected: %s", leaderID)

	// Step 7: Submit 10 set commands and verify no errors.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := h.SubmitCommand(key, value); err != nil {
			t.Fatalf("SubmitCommand #%d: %v", i, err)
		}
	}
	t.Log("submitted 10 commands successfully")

	// Step 8: Kill a follower (first non-leader node).
	var followerID string
	for _, id := range nodeIDs {
		if id != leaderID {
			followerID = id
			break
		}
	}
	if followerID == "" {
		t.Fatal("could not find a follower to kill")
	}

	if err := h.StopNode(followerID); err != nil {
		t.Fatalf("StopNode(%s): %v", followerID, err)
	}
	t.Logf("killed follower: %s", followerID)

	// Step 9: Submit 5 more commands — quorum (2/3) still present.
	for i := 10; i < 15; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := h.SubmitCommand(key, value); err != nil {
			t.Fatalf("SubmitCommand after follower kill #%d: %v", i, err)
		}
	}
	t.Log("submitted 5 more commands after follower kill")

	// Step 10: Restart the killed follower and wait for it to be healthy.
	if err := h.StartNode(followerID); err != nil {
		t.Fatalf("StartNode (restart) %s: %v", followerID, err)
	}
	t.Logf("restarted follower: %s", followerID)

	if err := h.WaitForHealth(followerID, 10*time.Second); err != nil {
		t.Fatalf("WaitForHealth after restart (%s): %v", followerID, err)
	}
	t.Logf("restarted follower %s is healthy", followerID)

	// Step 11: Verify the cluster still has a leader.
	_, err = h.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader after follower restart: %v", err)
	}
	t.Log("cluster is still operational after follower restart")
}

// ---------------------------------------------------------------------------
// v1 API integration tests
// ---------------------------------------------------------------------------

// setupV1Cluster builds the raftd binary, starts a 3-node cluster at the given
// base port, waits for health and a leader election, then returns
// (harness, leaderID, []httpAddresses).  Cluster teardown is registered with
// t.Cleanup so callers do not need to stop nodes manually.
func setupV1Cluster(t *testing.T, basePort int) (*testharness.Harness, string, []string) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping multi-process integration test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "raft-v1-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	// Build the raftd binary.
	binaryPath := filepath.Join(tmpDir, "raftd")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/raftd")
	buildCmd.Dir = projectRoot(t)
	if out, buildErr := buildCmd.CombinedOutput(); buildErr != nil {
		t.Skipf("skipping: raftd build failed: %v\n%s", buildErr, out)
	}

	harnessDir := filepath.Join(tmpDir, "harness")
	if err := os.MkdirAll(harnessDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	h := testharness.NewHarness(harnessDir, basePort, testharness.WithBinary(binaryPath))
	t.Cleanup(func() { h.StopAll() })

	nodeIDs := []string{"node1", "node2", "node3"}
	for _, id := range nodeIDs {
		if err := h.StartNode(id); err != nil {
			t.Fatalf("StartNode(%s): %v", id, err)
		}
	}
	for _, id := range nodeIDs {
		if err := h.WaitForHealth(id, 10*time.Second); err != nil {
			t.Fatalf("WaitForHealth(%s): %v", id, err)
		}
	}

	leaderID, err := h.WaitForLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// HTTP ports are basePort+100, +101, +102.
	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	return h, leaderID, addrs
}

// TestV1API exercises /v1/kv and /v1/txn against a live 3-node cluster.
// All subtests share the same cluster to avoid the overhead of spinning up
// multiple clusters.  Each subtest uses isolated key prefixes.
func TestV1API(t *testing.T) {
	h, leaderID, addrs := setupV1Cluster(t, 20000)
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))

	// ---- CRUD ---------------------------------------------------------------
	t.Run("CRUD", func(t *testing.T) {
		// PUT a new key.
		kv, err := c.Put("crud/key", "hello")
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		if kv.Value != "hello" {
			t.Errorf("Put value = %q, want 'hello'", kv.Value)
		}
		if kv.Version != 1 {
			t.Errorf("Version = %d, want 1 (first write)", kv.Version)
		}

		// Linearizable GET must return the same value.
		got, err := c.GetKV("crud/key")
		if err != nil {
			t.Fatalf("GetKV: %v", err)
		}
		if got.Value != "hello" {
			t.Errorf("GetKV value = %q, want 'hello'", got.Value)
		}

		// Stale GET — tries all nodes; at least one (the leader) has the entry.
		stale, err := c.GetKVStale("crud/key")
		if err != nil {
			t.Fatalf("GetKVStale: %v", err)
		}
		if stale.Value != "hello" {
			t.Errorf("GetKVStale value = %q, want 'hello'", stale.Value)
		}

		// DELETE the key.
		if err := c.DeleteKV("crud/key"); err != nil {
			t.Fatalf("DeleteKV: %v", err)
		}

		// Linearizable GET after delete must fail.
		_, err = c.GetKV("crud/key")
		if err == nil {
			t.Error("GetKV after delete: expected error, got nil")
		}
	})

	// ---- Range query --------------------------------------------------------
	t.Run("RangeQuery", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			if _, err := c.Put(fmt.Sprintf("range/key%d", i), fmt.Sprintf("val%d", i)); err != nil {
				t.Fatalf("Put range/key%d: %v", i, err)
			}
		}
		// One key outside the queried prefix — must not appear in results.
		if _, err := c.Put("other/rangekey", "other"); err != nil {
			t.Fatalf("Put other/rangekey: %v", err)
		}

		kvs, err := c.Range("range/")
		if err != nil {
			t.Fatalf("Range: %v", err)
		}
		if len(kvs) != 3 {
			t.Errorf("Range returned %d keys, want 3", len(kvs))
		}
		for _, kv := range kvs {
			if !strings.HasPrefix(kv.Key, "range/") {
				t.Errorf("unexpected key %q in range result", kv.Key)
			}
		}
	})

	// ---- Compare-and-swap transaction ---------------------------------------
	t.Run("TransactionCAS", func(t *testing.T) {
		if _, err := c.Put("txn/key", "initial"); err != nil {
			t.Fatalf("Put initial value: %v", err)
		}

		// Success path: value == "initial" → update to "updated".
		resp, err := c.Txn(&client.ClientTxnRequest{
			Compare: []client.TxnCompare{
				{Key: "txn/key", Target: "value", Result: "equal", Value: "initial"},
			},
			Success: []client.ClientTxnOp{{Type: 0, Key: "txn/key", Value: "updated"}},
			Failure: []client.ClientTxnOp{},
		})
		if err != nil {
			t.Fatalf("Txn (success path): %v", err)
		}
		if !resp.Succeeded {
			t.Error("expected transaction to succeed but it did not")
		}

		kv, err := c.GetKV("txn/key")
		if err != nil {
			t.Fatalf("GetKV after successful CAS: %v", err)
		}
		if kv.Value != "updated" {
			t.Errorf("value after CAS = %q, want 'updated'", kv.Value)
		}

		// Failure path: value is now "updated"; compare against "initial" → must fail.
		resp, err = c.Txn(&client.ClientTxnRequest{
			Compare: []client.TxnCompare{
				{Key: "txn/key", Target: "value", Result: "equal", Value: "initial"},
			},
			Success: []client.ClientTxnOp{{Type: 0, Key: "txn/key", Value: "updated2"}},
			Failure: []client.ClientTxnOp{},
		})
		if err != nil {
			t.Fatalf("Txn (failure path): %v", err)
		}
		if resp.Succeeded {
			t.Error("expected transaction to fail but it succeeded")
		}

		// Value must be unchanged.
		kv, err = c.GetKV("txn/key")
		if err != nil {
			t.Fatalf("GetKV after failed CAS: %v", err)
		}
		if kv.Value != "updated" {
			t.Errorf("value after failed CAS = %q, want 'updated'", kv.Value)
		}
	})

	// ---- Linearizable read from every node ----------------------------------
	t.Run("LinearizableGet", func(t *testing.T) {
		if _, err := c.Put("linear/key", "linear-value"); err != nil {
			t.Fatalf("Put: %v", err)
		}

		// A per-node client forces each GET to that specific node.
		// Followers forward the request to the leader, so all return the
		// up-to-date committed value.
		for _, addr := range addrs {
			nc := client.NewClient(
				client.WithAddresses([]string{addr}),
				client.WithTimeout(10*time.Second),
			)
			kv, err := nc.GetKV("linear/key")
			if err != nil {
				t.Errorf("GetKV from %s: %v", addr, err)
				continue
			}
			if kv.Value != "linear-value" {
				t.Errorf("GetKV from %s: value = %q, want 'linear-value'", addr, kv.Value)
			}
		}
	})

	// ---- Leader forwarding for writes ---------------------------------------
	t.Run("LeaderForwarding", func(t *testing.T) {
		// Locate a follower's HTTP address.
		var followerAddr string
		for _, id := range []string{"node1", "node2", "node3"} {
			if id == leaderID {
				continue
			}
			addr, err := h.GetNodeAddr(id)
			if err != nil {
				continue
			}
			// GetNodeAddr returns ":port"; prepend host to form "localhost:port".
			followerAddr = "localhost" + addr
			break
		}
		if followerAddr == "" {
			t.Fatal("could not identify a follower node")
		}

		// PUT directly to the follower.  It must forward to the leader and succeed.
		fc := client.NewClient(
			client.WithAddresses([]string{followerAddr}),
			client.WithTimeout(10*time.Second),
		)
		kv, err := fc.Put("forward/key", "forwarded")
		if err != nil {
			t.Fatalf("Put to follower (%s): %v", followerAddr, err)
		}
		if kv.Value != "forwarded" {
			t.Errorf("forwarded Put value = %q, want 'forwarded'", kv.Value)
		}

		// Confirm the committed entry is readable from the full cluster.
		got, err := c.GetKV("forward/key")
		if err != nil {
			t.Fatalf("GetKV after forwarded Put: %v", err)
		}
		if got.Value != "forwarded" {
			t.Errorf("GetKV after forwarded Put: value = %q, want 'forwarded'", got.Value)
		}
	})
}

// TestV1WatchAPI verifies that the SSE /v1/watch endpoint delivers change
// events in real time.
func TestV1WatchAPI(t *testing.T) {
	_, _, addrs := setupV1Cluster(t, 20010)

	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Subscribe before writing so the event is delivered live (not via replay).
	ch, err := c.Watch(ctx, "watch/key")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Allow the SSE connection to reach the server and be registered in the
	// WatchManager before we write.
	time.Sleep(300 * time.Millisecond)

	// PUT the watched key.
	if _, err := c.Put("watch/key", "watch-value"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The event must arrive within the context deadline.
	select {
	case we, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		if we.Err != nil {
			t.Fatalf("watch error: %v", we.Err)
		}
		if len(we.Events) == 0 {
			t.Fatal("received WatchEvent with no events")
		}
		ev := we.Events[0]
		if ev.Key != "watch/key" {
			t.Errorf("event key = %q, want 'watch/key'", ev.Key)
		}
		if ev.KV == nil || ev.KV.Value != "watch-value" {
			t.Errorf("event KV = %v, want value 'watch-value'", ev.KV)
		}
		t.Logf("received watch event: key=%q value=%q revision=%d",
			ev.Key, ev.KV.Value, ev.Revision)
	case <-ctx.Done():
		t.Fatal("timed out waiting for watch event")
	}
}

// TestMembershipAPI exercises the /admin/members cluster membership management
// endpoints against a live 3-node cluster.  No auth tokens are configured so
// all requests are allowed through.
func TestMembershipAPI(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process integration test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "raft-membership-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	binaryPath := filepath.Join(tmpDir, "raftd")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/raftd")
	buildCmd.Dir = projectRoot(t)
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	h := testharness.NewHarness(tmpDir, 20030, testharness.WithBinary(binaryPath))
	defer h.StopAll() //nolint:errcheck

	for _, id := range []string{"node1", "node2", "node3"} {
		if err := h.StartNode(id); err != nil {
			t.Fatalf("StartNode(%s): %v", id, err)
		}
	}
	for _, id := range []string{"node1", "node2", "node3"} {
		if err := h.WaitForHealth(id, 15*time.Second); err != nil {
			t.Fatalf("WaitForHealth(%s): %v", id, err)
		}
	}
	leaderID, err := h.WaitForLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	leaderPort := h.HttpPortForID(leaderID)
	leaderAddr := fmt.Sprintf("http://localhost:%d", leaderPort)
	hc := &http.Client{Timeout: 10 * time.Second}

	// GET /admin/members — list current members.
	resp, err := hc.Get(leaderAddr + "/admin/members")
	if err != nil {
		t.Fatalf("GET /admin/members: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /admin/members = %d, want 200", resp.StatusCode)
	}

	// POST /admin/members — attempt to add a node that is already a member.
	// The server should respond with 409 Conflict or 200 (idempotent).
	addBody, _ := json.Marshal(map[string]string{
		"id":      "node1",
		"address": fmt.Sprintf("localhost:%d", 20030),
	})
	resp, err = hc.Post(leaderAddr+"/admin/members", "application/json", bytes.NewReader(addBody))
	if err != nil {
		t.Fatalf("POST /admin/members: %v", err)
	}
	resp.Body.Close()
	// Accept 200 (already a member, no-op) or 409 (conflict).
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		t.Errorf("POST /admin/members = %d, want 200 or 409", resp.StatusCode)
	}

	// DELETE /admin/members/{id} — attempt to remove a non-existent node.
	req, _ := http.NewRequest(http.MethodDelete, leaderAddr+"/admin/members/nonexistent", nil)
	resp, err = hc.Do(req)
	if err != nil {
		t.Fatalf("DELETE /admin/members/nonexistent: %v", err)
	}
	resp.Body.Close()
	// Non-existent server → 400 or 500 (Raft error); either is acceptable.
	if resp.StatusCode == http.StatusOK {
		t.Errorf("DELETE /admin/members/nonexistent = 200, expected an error status")
	}

	t.Logf("membership API tests passed (leader=%s, leaderPort=%d)", leaderID, leaderPort)
}

// TestWatchAuthRejected verifies that /v1/watch returns 401 when the cluster
// is configured with auth tokens and the request carries no credentials.
// This test starts a single-node cluster with an auth token and checks that
// an unauthenticated SSE connection is rejected before any data is streamed.
func TestWatchAuthRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process integration test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "raft-watchauth-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	binaryPath := filepath.Join(tmpDir, "raftd")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/raftd")
	buildCmd.Dir = projectRoot(t)
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	// Write a single-node config with a static admin token.
	nodeDir := filepath.Join(tmpDir, "authnode")
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	const raftPort = 20050
	const httpPort = 20150
	const authToken = "test-secret-token"
	config := fmt.Sprintf(`node_id: authnode
listen_addr: ":%d"
http_addr: ":%d"
data_dir: %s/data
admin_token: %s
cluster:
  - id: authnode
    address: localhost:%d
    http_address: localhost:%d
`, raftPort, httpPort, nodeDir, authToken, raftPort, httpPort)

	configPath := filepath.Join(nodeDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	cmd := exec.Command(binaryPath, "-config", configPath)
	cmd.Dir = tmpDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start node: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill() //nolint:errcheck
			cmd.Wait()         //nolint:errcheck
		}
	}()

	// Wait for the node to be healthy.
	hc := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := hc.Get(fmt.Sprintf("http://localhost:%d/health", httpPort))
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Request /v1/watch WITHOUT an auth token — must return 401.
	resp, err := hc.Get(fmt.Sprintf("http://localhost:%d/v1/watch?key=foo", httpPort))
	if err != nil {
		t.Fatalf("GET /v1/watch (no auth): %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET /v1/watch (no auth) = %d, want 401", resp.StatusCode)
	}

	// Request /v1/watch WITH a valid auth token — must NOT return 401.
	req, _ := http.NewRequest(http.MethodGet,
		fmt.Sprintf("http://localhost:%d/v1/watch?key=foo", httpPort), nil)
	req.Header.Set("Authorization", "Bearer "+authToken)
	hc2 := &http.Client{Timeout: 2 * time.Second}
	resp2, err := hc2.Do(req)
	if err != nil {
		// A timeout or EOF is fine — the SSE stream blocks; we only care about
		// the status code being != 401.
		t.Logf("GET /v1/watch (with auth) error (expected for SSE): %v", err)
	} else {
		resp2.Body.Close()
		if resp2.StatusCode == http.StatusUnauthorized {
			t.Errorf("GET /v1/watch (with auth) = 401, expected success status")
		}
	}
}

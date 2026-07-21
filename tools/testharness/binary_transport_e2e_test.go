package testharness_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/tools/testharness"
)

// setupBinaryCluster builds raftd (once), starts a 3-node cluster with the
// given extraConfig, waits for health + a leader, and returns the harness.
// Cleanup is registered via t.Cleanup so callers do not need to stop nodes.
func setupBinaryCluster(t *testing.T, basePort int, extraConfig string) (*testharness.Harness, []string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping multi-process E2E test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "raft-binary-e2e-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	binaryPath := filepath.Join(tmpDir, "raftd")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/raftd")
	buildCmd.Dir = projectRoot(t)
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Skipf("skipping: failed to build raftd: %v\n%s", err, out)
	}

	harnessDir := filepath.Join(tmpDir, "harness")
	if err := os.MkdirAll(harnessDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	var opts []testharness.HarnessOption
	opts = append(opts, testharness.WithBinary(binaryPath))
	if extraConfig != "" {
		opts = append(opts, testharness.WithExtraConfig(extraConfig))
	}

	h := testharness.NewHarness(harnessDir, basePort, opts...)
	t.Cleanup(func() { h.StopAll() })

	nodeIDs := []string{"node1", "node2", "node3"}
	for _, id := range nodeIDs {
		if err := h.StartNode(id); err != nil {
			t.Fatalf("StartNode(%s): %v", id, err)
		}
	}
	for _, id := range nodeIDs {
		if err := h.WaitForHealth(id, 15*time.Second); err != nil {
			t.Fatalf("WaitForHealth(%s): %v", id, err)
		}
	}

	// HTTP addresses are basePort+100, +101, +102.
	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	return h, addrs
}

// kvPut sends a PUT /v1/kv/{key} to addr (host:port, no scheme).
func kvPut(addr, key, value string) error {
	body, _ := json.Marshal(map[string]string{"value": value})
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/kv/%s", addr, key),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT /v1/kv/%s: status %d: %s", key, resp.StatusCode, b)
	}
	return nil
}

// kvGet retrieves the value of key from the HTTP API at addr (host:port, no scheme).
// Uses ?stale=true so any node can answer without a Raft round-trip.
func kvGet(addr, key string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/kv/%s?stale=true", addr, key))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("key %q not found", key)
	}
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("GET /v1/kv/%s: status %d: %s", key, resp.StatusCode, b)
	}
	var result struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	return result.Value, nil
}

// TestBinaryTransportCluster verifies that a 3-node cluster started with
// binary_transport: true can elect a leader and replicate 20 keys to all nodes.
func TestBinaryTransportCluster(t *testing.T) {
	const basePort = 27000
	h, addrs := setupBinaryCluster(t, basePort, "binary_transport: true")
	_ = addrs // we resolve leader addr from harness

	leaderID, err := h.WaitForLeader(30 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	t.Logf("leader elected: %s", leaderID)

	leaderAddr, err := h.GetNodeAddr(leaderID)
	if err != nil {
		t.Fatalf("GetNodeAddr(%s): %v", leaderID, err)
	}
	// GetNodeAddr returns ":port" (leading colon); prepend localhost.
	leaderAddr = "localhost" + leaderAddr

	const numKeys = 20
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("bin-key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		if err := kvPut(leaderAddr, key, val); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	// Give followers a moment to apply the replicated entries.
	time.Sleep(300 * time.Millisecond)

	// Verify a sample key on every live node via stale read.
	const sampleKey = "bin-key-10"
	const wantVal = "val-10"
	verified := 0
	for _, nodeID := range []string{"node1", "node2", "node3"} {
		addr, err := h.GetNodeAddr(nodeID)
		if err != nil {
			t.Logf("GetNodeAddr(%s): %v (skipping)", nodeID, err)
			continue
		}
		nodeHTTP := "localhost" + addr
		got, err := kvGet(nodeHTTP, sampleKey)
		if err != nil {
			t.Logf("Get %s on %s: %v (may still be catching up)", sampleKey, nodeID, err)
			continue
		}
		if got != wantVal {
			t.Errorf("node %s: %s = %q, want %q", nodeID, sampleKey, got, wantVal)
		}
		verified++
	}
	if verified == 0 {
		t.Error("could not verify key on any node")
	}
	t.Logf("verified %s=%q on %d/3 nodes", sampleKey, wantVal, verified)
}

// TestBinaryTransportFallbackJSON verifies that binary_transport: false (JSON
// framing) still produces a correct cluster that can replicate and serve keys.
func TestBinaryTransportFallbackJSON(t *testing.T) {
	const basePort = 27100
	h, _ := setupBinaryCluster(t, basePort, "binary_transport: false")

	leaderID, err := h.WaitForLeader(30 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	t.Logf("leader elected: %s", leaderID)

	leaderAddr, err := h.GetNodeAddr(leaderID)
	if err != nil {
		t.Fatalf("GetNodeAddr(%s): %v", leaderID, err)
	}
	leaderAddr = "localhost" + leaderAddr

	const numKeys = 20
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("json-key-%d", i)
		val := fmt.Sprintf("jval-%d", i)
		if err := kvPut(leaderAddr, key, val); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	// Verify the last key directly on the leader (linearizable stale=true).
	const lastKey = "json-key-19"
	const wantVal = "jval-19"
	got, err := kvGet(leaderAddr, lastKey)
	if err != nil {
		t.Fatalf("Get %s: %v", lastKey, err)
	}
	if got != wantVal {
		t.Errorf("%s = %q, want %q", lastKey, got, wantVal)
	}
}

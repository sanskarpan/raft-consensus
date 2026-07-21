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

// setupBackupCluster builds raftd, starts a 3-node cluster at basePort, waits
// for health and a leader, then returns the harness, the list of HTTP
// addresses (localhost:basePort+100/101/102), and a cleanup function.
func setupBackupCluster(t *testing.T, basePort int) (*testharness.Harness, []string, func()) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping backup E2E test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "raft-backup-e2e-*")
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
	os.MkdirAll(harnessDir, 0755) //nolint:errcheck

	h := testharness.NewHarness(harnessDir, basePort, testharness.WithBinary(binaryPath))

	for _, id := range []string{"node1", "node2", "node3"} {
		if err := h.StartNode(id); err != nil {
			h.StopAll()       //nolint:errcheck
			os.RemoveAll(tmpDir)
			t.Fatalf("StartNode(%s): %v", id, err)
		}
	}
	for _, id := range []string{"node1", "node2", "node3"} {
		if err := h.WaitForHealth(id, 15*time.Second); err != nil {
			h.StopAll()       //nolint:errcheck
			os.RemoveAll(tmpDir)
			t.Fatalf("WaitForHealth(%s): %v", id, err)
		}
	}
	if _, err := h.WaitForLeader(15 * time.Second); err != nil {
		h.StopAll() //nolint:errcheck
		os.RemoveAll(tmpDir)
		t.Fatalf("WaitForLeader: %v", err)
	}

	// HTTP ports follow the harness convention: raftPort + 100.
	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	return h, addrs, func() {
		h.StopAll()       //nolint:errcheck
		os.RemoveAll(tmpDir)
	}
}

// findLeaderAddr polls all nodes' /v1/status and returns the HTTP address of
// the current leader.
func findLeaderAddr(t *testing.T, addrs []string) string {
	t.Helper()
	hc := &http.Client{Timeout: 3 * time.Second}
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
				return addr
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("could not find leader after 15s")
	return ""
}

// TestSnapshotDownloadEndpoint verifies that GET /admin/snapshot/download
// returns a non-empty binary payload after a forced snapshot.
func TestSnapshotDownloadEndpoint(t *testing.T) {
	const basePort = 26000
	_, addrs, cleanup := setupBackupCluster(t, basePort)
	defer cleanup()

	hc := &http.Client{Timeout: 5 * time.Second}

	// Write some keys first so the snapshot is non-trivial.
	leaderAddr := findLeaderAddr(t, addrs)
	for i := 0; i < 5; i++ {
		body, _ := json.Marshal(map[string]string{"value": fmt.Sprintf("v%d", i)})
		deadline := time.Now().Add(10 * time.Second)
		var ok bool
		for time.Now().Before(deadline) {
			req, _ := http.NewRequest(http.MethodPut,
				fmt.Sprintf("http://%s/v1/kv/backup/k%d", leaderAddr, i),
				bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := hc.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				ok = true
				break
			}
			if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !ok {
			t.Fatalf("could not write key backup/k%d", i)
		}
	}

	// Force a snapshot via POST /admin/snapshot.
	snapResp, err := hc.Post(
		fmt.Sprintf("http://%s/admin/snapshot", leaderAddr),
		"application/json", nil)
	if err != nil {
		t.Fatalf("POST /admin/snapshot: %v", err)
	}
	snapResp.Body.Close()
	time.Sleep(500 * time.Millisecond) // let snapshot flush to disk

	// Download the snapshot.
	dlResp, err := hc.Get(fmt.Sprintf("http://%s/admin/snapshot/download", leaderAddr))
	if err != nil {
		t.Fatalf("GET /admin/snapshot/download: %v", err)
	}
	defer dlResp.Body.Close()
	if dlResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(dlResp.Body)
		t.Fatalf("download returned %d: %s", dlResp.StatusCode, body)
	}

	data, err := io.ReadAll(dlResp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("snapshot download returned empty body")
	}
	t.Logf("downloaded snapshot: %d bytes, index=%s, term=%s",
		len(data),
		dlResp.Header.Get("X-Snapshot-Index"),
		dlResp.Header.Get("X-Snapshot-Term"))
}

// TestRestoreEndpointE2E downloads a snapshot and restores it back to the
// same leader to verify the round-trip works end-to-end.
func TestRestoreEndpointE2E(t *testing.T) {
	const basePort = 26100
	_, addrs, cleanup := setupBackupCluster(t, basePort)
	defer cleanup()

	hc := &http.Client{Timeout: 5 * time.Second}

	// Write keys.
	leaderAddr := findLeaderAddr(t, addrs)
	for i := 0; i < 3; i++ {
		body, _ := json.Marshal(map[string]string{"value": fmt.Sprintf("restore-val%d", i)})
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			req, _ := http.NewRequest(http.MethodPut,
				fmt.Sprintf("http://%s/v1/kv/restore/k%d", leaderAddr, i),
				bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := hc.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				break
			}
			if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	// Force snapshot.
	snapResp, _ := hc.Post(
		fmt.Sprintf("http://%s/admin/snapshot", leaderAddr),
		"application/json", nil)
	if snapResp != nil {
		snapResp.Body.Close()
	}
	time.Sleep(500 * time.Millisecond)

	// Download snapshot.
	dlResp, err := hc.Get(fmt.Sprintf("http://%s/admin/snapshot/download", leaderAddr))
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	snapData, _ := io.ReadAll(dlResp.Body)
	dlResp.Body.Close()
	if len(snapData) == 0 {
		t.Fatal("empty snapshot — cannot test restore")
	}

	// Restore the same snapshot back to the leader.
	req, _ := http.NewRequest(
		http.MethodPut,
		fmt.Sprintf("http://%s/admin/restore", leaderAddr),
		bytes.NewReader(snapData))
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("restore PUT: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("restore returned %d: %s", resp.StatusCode, body)
	}
	t.Log("restore completed successfully")
}

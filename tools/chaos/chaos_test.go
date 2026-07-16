// Package chaos contains process-level chaos tests for the Raft cluster.
// These tests kill and restart nodes while writes are in flight to verify
// that the cluster maintains consistency.
//
// Run with:
//
//	go test -v -timeout 120s ./tools/chaos/
package chaos

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/raft-consensus/pkg/client"
	"github.com/raft-consensus/tools/testharness"
)

func projectRoot(t *testing.T) string {
	t.Helper()
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
			t.Fatal("could not locate go.mod")
		}
		dir = parent
	}
}

func buildBinary(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	binaryPath := filepath.Join(tmpDir, "raftd")
	cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/raftd")
	cmd.Dir = projectRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("skipping: failed to build raftd: %v\n%s", err, out)
	}
	return binaryPath
}

// TestLeaderFailover writes keys continuously, kills the leader mid-flight,
// and verifies that all successfully acknowledged writes are readable after
// a new leader is elected.
func TestLeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	binaryPath := buildBinary(t)
	harnessDir := t.TempDir()
	const basePort = 21000
	h := testharness.NewHarness(harnessDir, basePort, testharness.WithBinary(binaryPath))
	defer h.StopAll() //nolint:errcheck

	for _, id := range []string{"node1", "node2", "node3"} {
		if err := h.StartNode(id); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}

	// Wait for all nodes to be healthy.
	for _, id := range []string{"node1", "node2", "node3"} {
		if err := h.WaitForHealth(id, 20*time.Second); err != nil {
			t.Fatalf("health %s: %v", id, err)
		}
	}

	leaderID, err := h.WaitForLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("no leader: %v", err)
	}
	t.Logf("initial leader: %s", leaderID)

	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(5*time.Second))

	// Write 20 keys before the kill.
	const preKillKeys = 20
	for i := 0; i < preKillKeys; i++ {
		key := fmt.Sprintf("chaos/pre/%d", i)
		if _, err := c.Put(key, fmt.Sprintf("val-%d", i)); err != nil {
			t.Errorf("pre-kill put %s: %v", key, err)
		}
	}

	// Kill the current leader.
	t.Logf("killing leader %s", leaderID)
	if err := h.StopNode(leaderID); err != nil {
		t.Fatalf("stop leader: %v", err)
	}

	// Wait for re-election among the remaining two nodes.
	newLeaderID, err := h.WaitForLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("no new leader after kill: %v", err)
	}
	t.Logf("new leader: %s", newLeaderID)

	// Write 20 more keys after the kill.
	const postKillKeys = 20
	for i := 0; i < postKillKeys; i++ {
		key := fmt.Sprintf("chaos/post/%d", i)
		if _, err := c.Put(key, fmt.Sprintf("val-%d", i)); err != nil {
			t.Errorf("post-kill put %s: %v", key, err)
		}
	}

	// All pre-kill keys must still be readable.
	for i := 0; i < preKillKeys; i++ {
		key := fmt.Sprintf("chaos/pre/%d", i)
		kv, err := c.GetKV(key)
		if err != nil {
			t.Errorf("read pre-kill key %s: %v", key, err)
			continue
		}
		want := fmt.Sprintf("val-%d", i)
		if kv.Value != want {
			t.Errorf("key %s = %q, want %q", key, kv.Value, want)
		}
	}

	// Restart the killed node and verify it catches up.
	t.Logf("restarting %s", leaderID)
	if err := h.StartNode(leaderID); err != nil {
		t.Fatalf("restart %s: %v", leaderID, err)
	}
	if err := h.WaitForHealth(leaderID, 15*time.Second); err != nil {
		t.Fatalf("health after restart: %v", err)
	}

	// Give the restarted node time to replicate.
	time.Sleep(2 * time.Second)

	// All post-kill keys must be readable on all nodes.
	for i := 0; i < postKillKeys; i++ {
		key := fmt.Sprintf("chaos/post/%d", i)
		kv, err := c.GetKV(key)
		if err != nil {
			t.Errorf("read post-kill key %s: %v", key, err)
			continue
		}
		want := fmt.Sprintf("val-%d", i)
		if kv.Value != want {
			t.Errorf("key %s = %q, want %q", key, kv.Value, want)
		}
	}
}

// TestFollowerRestart verifies that a restarted follower catches up
// from its persisted WAL and then from leader replication.
func TestFollowerRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	binaryPath := buildBinary(t)
	harnessDir := t.TempDir()
	const basePort = 21100
	h := testharness.NewHarness(harnessDir, basePort, testharness.WithBinary(binaryPath))
	defer h.StopAll() //nolint:errcheck

	for _, id := range []string{"node1", "node2", "node3"} {
		if err := h.StartNode(id); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}

	for _, id := range []string{"node1", "node2", "node3"} {
		if err := h.WaitForHealth(id, 20*time.Second); err != nil {
			t.Fatalf("health %s: %v", id, err)
		}
	}

	leaderID, err := h.WaitForLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("no leader: %v", err)
	}

	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(5*time.Second))

	// Pick a follower.
	followerID := ""
	for _, id := range []string{"node1", "node2", "node3"} {
		if id != leaderID {
			followerID = id
			break
		}
	}
	t.Logf("leader=%s follower to restart=%s", leaderID, followerID)

	// Write 10 keys while follower is alive.
	for i := 0; i < 10; i++ {
		if _, err := c.Put(fmt.Sprintf("restart/%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Errorf("put %d: %v", i, err)
		}
	}

	// Stop the follower.
	t.Logf("stopping follower %s", followerID)
	if err := h.StopNode(followerID); err != nil {
		t.Fatalf("stop follower: %v", err)
	}

	// Write more while follower is down (quorum = leader + remaining follower).
	for i := 10; i < 20; i++ {
		if _, err := c.Put(fmt.Sprintf("restart/%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Errorf("put while down %d: %v", i, err)
		}
	}

	// Restart the follower.
	t.Logf("restarting follower %s", followerID)
	if err := h.StartNode(followerID); err != nil {
		t.Fatalf("restart follower: %v", err)
	}
	if err := h.WaitForHealth(followerID, 15*time.Second); err != nil {
		t.Fatalf("health after restart: %v", err)
	}

	// Give the follower time to catch up via log replication.
	time.Sleep(3 * time.Second)

	// All 20 keys must be readable (linearizable = leader read).
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("restart/%d", i)
		kv, err := c.GetKV(key)
		if err != nil {
			t.Errorf("read %s: %v", key, err)
			continue
		}
		want := fmt.Sprintf("v%d", i)
		if kv.Value != want {
			t.Errorf("key %s = %q, want %q", key, kv.Value, want)
		}
	}
}

package upgrade

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/client"
	"github.com/sanskarpan/raft-consensus/tools/testharness"
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

func buildBinary(t *testing.T, output string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", output, "./cmd/raftd")
	cmd.Dir = projectRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("skipping: failed to build raftd: %v\n%s", err, out)
	}
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", timeout, msg)
}

func TestFullClusterUpgrade(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping upgrade test in short mode")
	}

	harnessDir := t.TempDir()
	const basePort = 21800

	v1Binary := filepath.Join(harnessDir, "raftd-v1")
	buildBinary(t, v1Binary)

	h := testharness.NewHarness(harnessDir, basePort, testharness.WithBinary(v1Binary))
	defer h.StopAll()

	nodeIDs := []string{"node1", "node2", "node3"}
	for _, id := range nodeIDs {
		if err := h.StartNode(id); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	for _, id := range nodeIDs {
		if err := h.WaitForHealth(id, 20*time.Second); err != nil {
			t.Fatalf("health %s: %v", id, err)
		}
	}
	if _, err := h.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("no leader: %v", err)
	}

	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))

	for i := range 10 {
		if _, err := c.Put(fmt.Sprintf("pre-upgrade/%d", i), fmt.Sprintf("old-%d", i)); err != nil {
			t.Errorf("pre-upgrade put %d: %v", i, err)
		}
	}

	t.Log("stopping all nodes for upgrade...")
	if err := h.StopAll(); err != nil {
		t.Fatalf("stop all: %v", err)
	}

	v2Binary := filepath.Join(harnessDir, "raftd-v2")
	buildBinary(t, v2Binary)

	t.Log("starting all nodes with new binary...")
	h2 := testharness.NewHarness(harnessDir, basePort, testharness.WithBinary(v2Binary))
	defer h2.StopAll()
	for _, id := range nodeIDs {
		if err := h2.StartNode(id); err != nil {
			t.Fatalf("start %s (v2): %v", id, err)
		}
	}
	for _, id := range nodeIDs {
		if err := h2.WaitForHealth(id, 30*time.Second); err != nil {
			t.Fatalf("health %s (v2): %v", id, err)
		}
	}
	if _, err := h2.WaitForLeader(30 * time.Second); err != nil {
		t.Fatalf("no leader after upgrade: %v", err)
	}

	allPreReadable := func() bool {
		for i := range 10 {
			kv, err := c.GetKV(fmt.Sprintf("pre-upgrade/%d", i))
			if err != nil || kv.Value != fmt.Sprintf("old-%d", i) {
				return false
			}
		}
		return true
	}
	eventually(t, 20*time.Second, allPreReadable, "all pre-upgrade keys readable after full restart")

	for i := 10; i < 20; i++ {
		if _, err := c.Put(fmt.Sprintf("post-upgrade/%d", i), fmt.Sprintf("new-%d", i)); err != nil {
			t.Errorf("post-upgrade put %d: %v", i, err)
		}
	}
	allPostReadable := func() bool {
		for i := 10; i < 20; i++ {
			kv, err := c.GetKV(fmt.Sprintf("post-upgrade/%d", i))
			if err != nil || kv.Value != fmt.Sprintf("new-%d", i) {
				return false
			}
		}
		return true
	}
	eventually(t, 15*time.Second, allPostReadable, "all post-upgrade keys readable")
}

func TestFullClusterRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rollback test in short mode")
	}

	harnessDir := t.TempDir()
	const basePort = 25000

	v1Binary := filepath.Join(harnessDir, "raftd-v1")
	buildBinary(t, v1Binary)

	h := testharness.NewHarness(harnessDir, basePort, testharness.WithBinary(v1Binary))
	defer h.StopAll()

	nodeIDs := []string{"node1", "node2", "node3"}
	for _, id := range nodeIDs {
		if err := h.StartNode(id); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	for _, id := range nodeIDs {
		if err := h.WaitForHealth(id, 20*time.Second); err != nil {
			t.Fatalf("health %s: %v", id, err)
		}
	}
	if _, err := h.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("no leader: %v", err)
	}

	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))

	for i := range 10 {
		if _, err := c.Put(fmt.Sprintf("rollback/%d", i), fmt.Sprintf("val-%d", i)); err != nil {
			t.Errorf("put %d: %v", i, err)
		}
	}

	t.Log("stopping all nodes for rollback...")
	if err := h.StopAll(); err != nil {
		t.Fatalf("stop all: %v", err)
	}

	rollbackBinary := filepath.Join(harnessDir, "raftd-rollback")
	buildBinary(t, rollbackBinary)

	t.Log("restarting all nodes (simulated rollback)...")
	h2 := testharness.NewHarness(harnessDir, basePort, testharness.WithBinary(rollbackBinary))
	defer h2.StopAll()
	for _, id := range nodeIDs {
		if err := h2.StartNode(id); err != nil {
			t.Fatalf("start %s (rollback): %v", id, err)
		}
	}
	for _, id := range nodeIDs {
		if err := h2.WaitForHealth(id, 30*time.Second); err != nil {
			t.Fatalf("health %s (rollback): %v", id, err)
		}
	}
	if _, err := h2.WaitForLeader(30 * time.Second); err != nil {
		t.Fatalf("no leader after rollback: %v", err)
	}

	allReadable := func() bool {
		for i := range 10 {
			kv, err := c.GetKV(fmt.Sprintf("rollback/%d", i))
			if err != nil || kv.Value != fmt.Sprintf("val-%d", i) {
				return false
			}
		}
		return true
	}
	eventually(t, 20*time.Second, allReadable, "all keys readable after rollback")
}

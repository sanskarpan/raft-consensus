package soaktest_test

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/client"
	"github.com/sanskarpan/raft-consensus/tools/testharness"
)

var soakDuration = flag.Duration("soak.duration", 10*time.Second, "duration of the soak test")
var soakConcurrency = flag.Int("soak.concurrency", 10, "number of concurrent writers")

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

func buildRaftd(t *testing.T) string {
	t.Helper()
	return buildRaftdCached(t)
}

var (
	raftdBuildOnce sync.Once
	raftdBinary    string
	raftdBuildErr  error
)

func buildRaftdCached(t *testing.T) string {
	t.Helper()
	raftdBuildOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "raftd-soak-*")
		if err != nil {
			raftdBuildErr = fmt.Errorf("MkdirTemp: %w", err)
			return
		}
		binaryPath := filepath.Join(tmpDir, "raftd")
		cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/raftd")
		cmd.Dir = projectRoot(t)
		out, err := cmd.CombinedOutput()
		if err != nil {
			os.RemoveAll(tmpDir)
			raftdBuildErr = fmt.Errorf("build failed: %w\n%s", err, out)
			return
		}
		raftdBinary = binaryPath
	})
	if raftdBuildErr != nil {
		t.Skipf("skipping: %v", raftdBuildErr)
	}
	return raftdBinary
}

type result struct {
	latency time.Duration
	err     error
}

func setupSoakCluster(t *testing.T, basePort int) (*testharness.Harness, *client.Client) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "raft-soak-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	binaryPath := buildRaftdCached(t)
	harnessDir := filepath.Join(tmpDir, "harness")
	if err := os.MkdirAll(harnessDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	h := testharness.NewHarness(harnessDir, basePort, testharness.WithBinary(binaryPath))
	t.Cleanup(func() { h.StopAll() })

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
	if _, err := h.WaitForLeader(20 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	addrs := []string{
		fmt.Sprintf("localhost:%d", basePort+100),
		fmt.Sprintf("localhost:%d", basePort+101),
		fmt.Sprintf("localhost:%d", basePort+102),
	}
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))
	return h, c
}

func TestSoakSustainedWrite(t *testing.T) {
	_, c := setupSoakCluster(t, 28000)
	dur := *soakDuration
	conc := *soakConcurrency

	t.Logf("soak: %s with %d concurrent writers", dur, conc)

	var ops atomic.Uint64
	results := make(chan result, 100000)
	done := make(chan struct{})

	var writeWG sync.WaitGroup
	for w := 0; w < conc; w++ {
		writeWG.Add(1)
		go func(writerID int) {
			defer writeWG.Done()
			prefix := fmt.Sprintf("soak/w%d/", writerID)
			seq := 0
			for {
				select {
				case <-done:
					return
				default:
				}
				key := fmt.Sprintf("%s%08d", prefix, seq)
				val := fmt.Sprintf("val-%d-%d", writerID, seq)
				start := time.Now()
				_, err := c.Put(key, val)
				select {
				case <-done:
					return
				default:
				}
				results <- result{time.Since(start), err}
				ops.Add(1)
				seq++
			}
		}(w)
	}

	// Collect results while writers are still running so the channel
	// never blocks the producers.
	var (
		latencyMu sync.Mutex
		latencies []float64
		errCount  int
	)
	collectDone := make(chan struct{})
	go func() {
		for r := range results {
			latencyMu.Lock()
			if r.err != nil {
				errCount++
			} else {
				latencies = append(latencies, r.latency.Seconds()*1000)
			}
			latencyMu.Unlock()
		}
		close(collectDone)
	}()

	time.Sleep(dur)
	close(done)
	writeWG.Wait()
	close(results)
	<-collectDone

	totalOps := ops.Load()
	latencyMu.Lock()
	sort.Float64s(latencies)
	latencyMu.Unlock()

	throughput := float64(totalOps) / dur.Seconds()
	t.Logf("total ops: %d, errors: %d, throughput: %.0f ops/sec", totalOps, errCount, throughput)

	if len(latencies) > 0 {
		p50 := latencies[int(float64(len(latencies))*0.5)]
		p90 := latencies[int(float64(len(latencies))*0.9)]
		p99 := latencies[int(float64(len(latencies))*0.99)]
		t.Logf("latency ms: p50=%.2f p90=%.2f p99=%.2f", p50, p90, p99)
	}

	if throughput < 10 {
		t.Errorf("throughput too low: %.0f ops/sec (need >= 10)", throughput)
	}
	if errCount > 0 {
		t.Errorf("soak: %d write errors", errCount)
	}
}

func TestSoakLeaderFailover(t *testing.T) {
	h, c := setupSoakCluster(t, 28200)
	dur := *soakDuration

	leaderID, err := h.WaitForLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	t.Logf("initial leader: %s", leaderID)

	var keyIndex int
	var mu sync.Mutex
	written := make(map[string]string)

	writeOK := func(key, val string) bool {
		_, err := c.Put(key, val)
		if err == nil {
			mu.Lock()
			written[key] = val
			mu.Unlock()
			return true
		}
		return false
	}

	// Write keys until we have a baseline.
	t.Log("writing baseline keys...")
	baselineKeys := 20
	for i := 0; i < baselineKeys; i++ {
		key := fmt.Sprintf("soak-fl/init-%04d", i)
		val := fmt.Sprintf("ival-%04d", i)
		for attempt := 0; attempt < 10; attempt++ {
			if writeOK(key, val) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	// Simultaneously write keys and kill the leader halfway.
	type measurement struct {
		latency time.Duration
		ok      bool
	}
	measurements := make(chan measurement, 100000)
	writeDone := make(chan struct{})
	var writeWG sync.WaitGroup
	writeWG.Add(1)
	go func() {
		defer writeWG.Done()
		for {
			select {
			case <-writeDone:
				return
			default:
			}
			mu.Lock()
			i := keyIndex
			keyIndex++
			mu.Unlock()
			key := fmt.Sprintf("soak-fl/k-%06d", i)
			val := fmt.Sprintf("v-%06d", i)
			start := time.Now()
			_, err := c.Put(key, val)
			select {
			case <-writeDone:
				return
			default:
			}
			measurements <- measurement{time.Since(start), err == nil}
			if err == nil {
				mu.Lock()
				written[key] = val
				mu.Unlock()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Collect measurements concurrently so the channel never blocks.
	var (
		mLatencies []float64
		mOk, mFail int
		mMu        sync.Mutex
	)
	collectDone := make(chan struct{})
	go func() {
		for m := range measurements {
			mMu.Lock()
			if m.ok {
				mOk++
				mLatencies = append(mLatencies, m.latency.Seconds()*1000)
			} else {
				mFail++
			}
			mMu.Unlock()
		}
		close(collectDone)
	}()

	time.Sleep(dur / 2)

	t.Logf("killing leader %s...", leaderID)
	if err := h.StopNode(leaderID); err != nil {
		t.Fatalf("StopNode(%s): %v", leaderID, err)
	}

	newLeader, err := h.WaitForLeader(30 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader after kill: %v", err)
	}
	t.Logf("new leader: %s (failover: %s lost)", newLeader, leaderID)

	time.Sleep(dur / 2)
	close(writeDone)
	writeWG.Wait()
	close(measurements)
	<-collectDone

	t.Logf("writes: %d ok, %d failed", mOk, mFail)

	if len(mLatencies) > 0 {
		sort.Float64s(mLatencies)
		p50 := mLatencies[int(float64(len(mLatencies))*0.5)]
		p95 := mLatencies[int(float64(len(mLatencies))*0.95)]
		t.Logf("write latency ms: p50=%.2f p95=%.2f", p50, p95)
	}

	// Verify all written keys are readable.
	t.Log("verifying data integrity...")
	var missing, corrupt int
	mu.Lock()
	for key, want := range written {
		kv, err := c.GetKV(key)
		if err != nil {
			missing++
			continue
		}
		if kv.Value != want {
			corrupt++
		}
	}
	mu.Unlock()
	if missing > 0 {
		t.Errorf("data loss: %d keys missing after leader failover", missing)
	}
	if corrupt > 0 {
		t.Errorf("data corruption: %d keys have wrong values", corrupt)
	}
	t.Logf("data integrity: %d/%d keys verified (missing=%d, corrupt=%d)", len(written)-missing-corrupt, len(written), missing, corrupt)
}

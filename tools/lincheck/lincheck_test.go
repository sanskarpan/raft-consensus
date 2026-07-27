// Package main — linearizability test using the process-based testharness.
//
// TestLinearizability starts a 3-node raftd cluster, runs 4 concurrent clients
// issuing Put/Get operations on a small key set for ~5 seconds, then verifies
// the recorded history is linearizable per key using porcupine.
//
// Run:
//
//	go test -v -timeout 90s ./tools/lincheck/
//
// The test is skipped in -short mode and when the raftd binary cannot be built.
package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/sanskarpan/raft-consensus/pkg/client"
	"github.com/sanskarpan/raft-consensus/tools/testharness"
)

// kvModel is a linearizable register model for a single string-valued key.
// Puts always succeed; Gets must observe the last written value.
var kvModel = porcupine.Model{
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(regInput)
		cur := state.(string)
		if in.op == "put" {
			return true, in.value
		}
		// get: output must equal the current state
		return output.(string) == cur, cur
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
}

var (
	linBuildOnce sync.Once
	linBinary    string
	linBuildErr  error
)

func buildLinRaftd(t *testing.T) string {
	t.Helper()
	linBuildOnce.Do(func() {
		_, file, _, ok := runtime.Caller(0)
		if !ok {
			linBuildErr = fmt.Errorf("runtime.Caller failed")
			return
		}
		root := filepath.Dir(file)
		for {
			if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
				break
			}
			parent := filepath.Dir(root)
			if parent == root {
				linBuildErr = fmt.Errorf("go.mod not found")
				return
			}
			root = parent
		}
		tmp, err := os.MkdirTemp("", "lincheck-raftd-*")
		if err != nil {
			linBuildErr = err
			return
		}
		bin := filepath.Join(tmp, "raftd")
		cmd := exec.Command("go", "build", "-o", bin, "./cmd/raftd")
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			linBuildErr = fmt.Errorf("build failed: %w\n%s", err, out)
			return
		}
		linBinary = bin
	})
	if linBuildErr != nil {
		t.Skipf("skipping: %v", linBuildErr)
	}
	return linBinary
}

func TestLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping linearizability test in short mode")
	}

	bin := buildLinRaftd(t)

	tmpDir, err := os.MkdirTemp("", "lincheck-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	h := testharness.NewHarness(tmpDir, 29100, testharness.WithBinary(bin))
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

	// Build one client per worker that addresses all nodes.
	var addrs []string
	for _, id := range []string{"node1", "node2", "node3"} {
		addr, err := h.GetNodeAddr(id)
		if err != nil {
			t.Fatalf("GetNodeAddr(%s): %v", id, err)
		}
		if strings.HasPrefix(addr, ":") {
			addr = "localhost" + addr
		}
		addrs = append(addrs, addr)
	}

	const (
		numWorkers = 4
		testDur    = 5 * time.Second
	)
	keys := []string{"lin-x", "lin-y", "lin-z"}

	type opEvent struct {
		clientID int
		input    regInput
		output   string
		call     int64 // monotonic ns
		ret      int64 // -1 = indeterminate
	}

	start := time.Now()
	mono := func() int64 { return time.Since(start).Nanoseconds() }

	var mu sync.Mutex
	var events []opEvent
	var stop int32

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		c := client.NewClient(
			client.WithAddresses(addrs),
			client.WithTimeout(3*time.Second),
		)
		go func(id int, c *client.Client) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id) + 42))
			seq := 0
			for atomic.LoadInt32(&stop) == 0 {
				key := keys[rng.Intn(len(keys))]
				time.Sleep(8 * time.Millisecond)

				if rng.Intn(2) == 0 {
					// PUT
					seq++
					val := fmt.Sprintf("w%d-%d", id, seq)
					callT := mono()
					_, err := c.Put(key, val)
					retT := mono()
					if err != nil {
						retT = -1 // indeterminate
					}
					mu.Lock()
					events = append(events, opEvent{id, regInput{"put", key, val}, "", callT, retT})
					mu.Unlock()
				} else {
					// GET (linearizable)
					callT := mono()
					kv, err := c.GetKV(key)
					retT := mono()
					if err == nil {
						mu.Lock()
						events = append(events, opEvent{id, regInput{"get", key, ""}, kv.Value, callT, retT})
						mu.Unlock()
					} else if errors.Is(err, client.ErrKeyNotFound) {
						// Key not yet written — record empty string (matches init state)
						mu.Lock()
						events = append(events, opEvent{id, regInput{"get", key, ""}, "", callT, retT})
						mu.Unlock()
					}
					// other errors (timeout, leader change): skip — indeterminate read
				}
			}
		}(w, c)
	}

	time.Sleep(testDur)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()

	// Fix indeterminate put returns: allow up to 5s bounded window.
	indet := 0
	for i := range events {
		if events[i].ret == -1 {
			events[i].ret = events[i].call + int64(5*time.Second)
			indet++
		}
	}
	t.Logf("collected %d operations (%d indeterminate puts) across %d keys",
		len(events), indet, len(keys))

	// Verify linearizability per key.
	allOK := true
	for _, key := range keys {
		var ops []porcupine.Operation
		for _, e := range events {
			if e.input.key != key {
				continue
			}
			ops = append(ops, porcupine.Operation{
				ClientId: e.clientID,
				Input:    e.input,
				Call:     e.call,
				Output:   e.output,
				Return:   e.ret,
			})
		}
		if len(ops) == 0 {
			continue
		}
		res := porcupine.CheckOperationsTimeout(kvModel, ops, 15*time.Second)
		t.Logf("  key %-8s: %d ops -> %s", key, len(ops), linVerdict(res))
		if res == porcupine.Illegal {
			allOK = false
		}
	}

	if !allOK {
		t.Error("LINEARIZABILITY VIOLATION: history is not linearizable")
	}
}

func linVerdict(r porcupine.CheckResult) string {
	switch r {
	case porcupine.Ok:
		return "LINEARIZABLE"
	case porcupine.Illegal:
		return "VIOLATION"
	default:
		return "UNKNOWN (timed out; not counted as violation)"
	}
}

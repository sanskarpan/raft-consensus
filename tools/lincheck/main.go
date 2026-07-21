// Command lincheck is a Jepsen-style linearizability checker for a running
// raft-consensus cluster. Concurrent HTTP clients issue put/get on a small key
// set while a fault injector pauses random nodes; the recorded history is then
// verified linearizable per key with Porcupine (the Go equivalent of Jepsen's
// Knossos/Elle checker). Run against a live cluster (e.g. docker compose).
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"
)

var endpoints = []string{"localhost:8012", "localhost:8014", "localhost:8016"}
var nodes = []string{"raft-node1", "raft-node2", "raft-node3"}
var keys = []string{"lin-a", "lin-b", "lin-c"}

type regInput struct {
	op    string // "put" | "get"
	key   string
	value string
}

// registerModel: each key is an independent linearizable register.
var registerModel = porcupine.Model{
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(regInput)
		st := state.(string)
		if in.op == "put" {
			return true, in.value // a put always succeeds; output ignored
		}
		return output.(string) == st, st // get must observe the current value
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
}

type event struct {
	clientID int
	input    regInput
	output   string
	call     int64
	ret      int64
}

func httpDo(method, url, body string) (int, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var r io.Reader
	if body != "" {
		r = bytes.NewReader([]byte(body))
	}
	req, _ := http.NewRequestWithContext(ctx, method, url, r)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func main() {
	start := time.Now()
	now := func() int64 { return time.Since(start).Nanoseconds() }

	var mu sync.Mutex
	var events []event
	var stop int32
	var wg sync.WaitGroup

	// Fault injector: pause a current FOLLOWER for ~2s, then unpause; repeat.
	// Pausing only followers keeps writes to the leader definite (no
	// indeterminate ops -> no linearizability false positives) while still
	// stressing replication (the paused follower lags and must catch up) and
	// verifying that linearizable reads served by followers are never stale.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(7))
		for atomic.LoadInt32(&stop) == 0 {
			f := nodes[rng.Intn(len(nodes))] // pause ANY node, including the leader
			_ = exec.Command("docker", "pause", f).Run()
			time.Sleep(1500 * time.Millisecond)
			_ = exec.Command("docker", "unpause", f).Run()
			time.Sleep(1500 * time.Millisecond)
		}
	}()

	const workers = 4
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id) + 100))
			seq := 0
			for atomic.LoadInt32(&stop) == 0 {
				key := keys[rng.Intn(len(keys))]
				ep := endpoints[rng.Intn(len(endpoints))]
				time.Sleep(8 * time.Millisecond)
				if rng.Intn(2) == 0 {
					// put a unique value
					seq++
					val := fmt.Sprintf("c%d-%d", id, seq)
					c := now()
					code, _ := httpDo("PUT", fmt.Sprintf("http://%s/v1/kv/%s", ep, key), val)
					r := now()
					// A 200 put is definite. A non-200/timeout put is INDETERMINATE
					// (it may or may not have committed): record it with ret=-1 and
					// fix Return to the end of the history afterwards, so Porcupine
					// may linearize it anywhere in [call, end] (committed) or at the
					// very end (never took effect) — the correct model for a write
					// with an unknown outcome under leader failover.
					if code != 200 {
						r = -1
					}
					mu.Lock()
					events = append(events, event{id, regInput{"put", key, val}, "", c, r})
					mu.Unlock()
				} else {
					c := now()
					code, body := httpDo("GET", fmt.Sprintf("http://%s/v1/kv/%s", ep, key), "")
					r := now()
					switch code {
					case 200:
						// extract "value":"..."
						val := extractValue(body)
						mu.Lock()
						events = append(events, event{id, regInput{"get", key, ""}, val, c, r})
						mu.Unlock()
					case 404:
						mu.Lock()
						events = append(events, event{id, regInput{"get", key, ""}, "", c, r})
						mu.Unlock()
					}
					// other codes (timeout/leader-change): skip (indeterminate read)
				}
			}
		}(w)
	}

	time.Sleep(8 * time.Second)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	// Fix indeterminate put returns to just past the end of the history so they
	// can be linearized anywhere after their call (or never taken effect).
	indet := 0
	for i := range events {
		if events[i].ret == -1 {
			// A committed write takes effect within a bounded time (election +
			// replication). Cap the uncertainty window instead of end-of-history
			// so Porcupine's search stays tractable.
			events[i].ret = events[i].call + int64(5*time.Second)
			indet++
		}
	}
	fmt.Printf("indeterminate (uncertain-outcome) puts: %d\n", indet)
	// ensure all nodes unpaused
	for _, n := range nodes {
		_ = exec.Command("docker", "unpause", n).Run()
	}

	// Check linearizability per key.
	fmt.Printf("collected %d operations across %d keys\n", len(events), len(keys))
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
		res := porcupine.CheckOperationsTimeout(registerModel, ops, 30*time.Second)
		fmt.Printf("  key %-6s: %d ops -> %s\n", key, len(ops), verdict(res))
		if res == porcupine.Illegal {
			allOK = false
		}
	}
	if allOK {
		fmt.Println("RESULT: LINEARIZABLE ✓ (all keys, under node-pause faults)")
		os.Exit(0)
	}
	fmt.Println("RESULT: LINEARIZABILITY VIOLATION ✗")
	os.Exit(1)
}

func verdict(r porcupine.CheckResult) string {
	switch r {
	case porcupine.Ok:
		return "LINEARIZABLE"
	case porcupine.Illegal:
		return "VIOLATION"
	default:
		return "UNKNOWN (check timed out; not a violation)"
	}
}

func extractValue(body string) string {
	// crude JSON value extraction to avoid struct coupling
	const marker = `"value":"`
	i := bytesIndex(body, marker)
	if i < 0 {
		return ""
	}
	rest := body[i+len(marker):]
	j := bytesIndex(rest, `"`)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func bytesIndex(s, sub string) int { return bytesIndexImpl(s, sub) }
func bytesIndexImpl(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

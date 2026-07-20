package raft

import (
	"sync/atomic"
	"testing"
	"time"
)

// recordingClock counts the number of Now() calls to verify the raft core
// uses the injected clock instead of time.Now() directly.
type recordingClock struct {
	calls int64
	base  time.Time
}

func (c *recordingClock) Now() time.Time {
	atomic.AddInt64(&c.calls, 1)
	return c.base.Add(time.Duration(atomic.LoadInt64(&c.calls)) * time.Millisecond)
}

func (c *recordingClock) count() int64 {
	return atomic.LoadInt64(&c.calls)
}

// TestClockThreadedIntoRaft verifies that when Config.Clock is set the raft
// node uses it (call count > 0) and does not panic. This is a pure
// "no regression" check: existing tests validate functional correctness;
// this one only checks the injection path.
func TestClockThreadedIntoRaft(t *testing.T) {
	clk := &recordingClock{base: time.Now()}

	ids := []string{"n1", "n2", "n3"}
	cfg := Configuration{}
	for _, id := range ids {
		cfg.Servers = append(cfg.Servers, Server{ID: ServerID(id)})
	}

	var nodes []*raft
	var transports []*chanTransport

	for _, id := range ids {
		trans := newChanTransport(ServerID(id))
		fsm := &echoFSM{}
		nodeCfg := &Config{
			LocalID:              ServerID(id),
			ElectionTick:         5,
			HeartbeatTick:        1,
			InitialConfiguration: cfg,
			Clock:                clk,
		}
		r, err := newRaft(nodeCfg, ServerID(id),
			newMemLogStore(), newMemStableStore(), &memSnapshotStore{}, fsm, trans)
		if err != nil {
			t.Fatalf("newRaft: %v", err)
		}
		trans.appendEntriesFn = func(req *AppendEntriesRequest) *AppendEntriesResponse {
			return r.HandleAppendEntriesRPC(req)
		}
		trans.requestVoteFn = func(req *RequestVoteRequest) *RequestVoteResponse {
			return r.HandleRequestVoteRPC(req)
		}
		trans.installSnapshotFn = func(req *InstallSnapshotRequest) *InstallSnapshotResponse {
			return r.HandleInstallSnapshotRPC(req)
		}
		nodes = append(nodes, r)
		transports = append(transports, trans)
	}

	for i, tr := range transports {
		for j, other := range transports {
			if i != j {
				tr.connect(other)
			}
		}
	}

	for _, r := range nodes {
		if err := r.Start(); err != nil {
			t.Fatal(err)
		}
		defer r.Shutdown()
	}

	// Wait for leader election (up to 5 seconds using real time.After).
	deadline := time.Now().Add(5 * time.Second)
	elected := false
	for time.Now().Before(deadline) {
		for _, r := range nodes {
			if r.State() == StateLeader {
				elected = true
				break
			}
		}
		if elected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !elected {
		t.Fatal("no leader elected within 5s")
	}

	// The injected clock should have been called at least once.
	if clk.count() == 0 {
		t.Fatal("injected clock was never called — Clock is not threaded through raft.go")
	}
}

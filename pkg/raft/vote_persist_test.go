package raft

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// failingStableStore wraps memStableStore and can be toggled to fail Sync, to
// simulate a durable-storage failure while persisting a vote.
type failingStableStore struct {
	*memStableStore
	failSync atomic.Bool
}

func (s *failingStableStore) Sync() error {
	if s.failSync.Load() {
		return fmt.Errorf("injected sync failure")
	}
	return s.memStableStore.Sync()
}

func newVoteTestNode(t *testing.T, stable StableStore) *raft {
	t.Helper()
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}
	trans := newChanTransport("n1")
	rc := &Config{
		LocalID:              "n1",
		ElectionTick:         5,
		HeartbeatTick:        1,
		InitialConfiguration: cfg,
	}
	r, err := newRaft(rc, "n1", newMemLogStore(), stable, &memSnapshotStore{}, &echoFSM{}, trans)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	return r
}

// TestVoteGrantDeniedWhenPersistFails verifies that a node does NOT report
// VoteGranted=true if it cannot durably persist the vote. Granting a vote whose
// record is lost would let the node vote again for a different candidate in the
// same term after a crash — violating election safety (≤ 1 leader per term).
func TestVoteGrantDeniedWhenPersistFails(t *testing.T) {
	stable := &failingStableStore{memStableStore: newMemStableStore()}
	r := newVoteTestNode(t, stable)

	// Persisting the vote will fail.
	stable.failSync.Store(true)
	resp := r.handleRequestVote(&RequestVoteRequest{
		Term:         1,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if resp.VoteGranted {
		t.Fatal("vote must NOT be granted when it cannot be persisted")
	}
	if r.votedFor == "n2" {
		t.Fatalf("votedFor must not record n2 durably on persist failure, got %q", r.votedFor)
	}

	// With persistence healthy, the same candidate IS granted the vote.
	stable.failSync.Store(false)
	resp2 := r.handleRequestVote(&RequestVoteRequest{
		Term:         1,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if !resp2.VoteGranted {
		t.Fatalf("vote should be granted once persistence works: %+v", resp2)
	}
	if r.votedFor != "n2" {
		t.Fatalf("votedFor = %q, want n2", r.votedFor)
	}
}

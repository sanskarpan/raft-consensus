package raft

import "testing"

// M7: On a prevLog term mismatch, the follower returns ConflictTerm and the
// first index it holds for that term, so the leader can back up past the whole
// conflicting term in one step instead of decrementing nextIndex one at a time.
func TestAppendEntriesReturnsConflictTerm(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}
	r, _, _ := makeRaftNode("n1", cfg)

	r.mu.Lock()
	r.state = StateFollower
	r.term = 5
	// Follower log: indices 1..4 all at term 2 (a stale term the leader lacks).
	for i := uint64(1); i <= 4; i++ {
		_ = r.log.Append([]*LogEntry{{Term: 2, Index: i, Type: EntryNormal, Data: []byte{byte(i)}}})
	}
	r.lastIndex = 4
	r.lastTerm = 2
	r.mu.Unlock()

	// Leader probes at index 4 expecting term 3 — mismatch (follower has term 2).
	resp := r.HandleAppendEntriesRPC(&AppendEntriesRequest{
		Term: 5, LeaderID: "n2", PrevLogIndex: 4, PrevLogTerm: 3,
	})
	if resp.Success {
		t.Fatal("expected rejection on term mismatch")
	}
	if resp.ConflictTerm != 2 {
		t.Fatalf("ConflictTerm=%d, want 2", resp.ConflictTerm)
	}
	// First index the follower holds for term 2 is 1, so the leader is told to
	// back up all the way to 1 in a single round.
	if resp.Index != 1 {
		t.Fatalf("conflict Index=%d, want 1 (first index of the conflicting term)", resp.Index)
	}
}

package raft

import (
	"testing"
)

func newFollowerNode(t *testing.T, checkQuorum bool) *raft {
	t.Helper()
	cfg := Configuration{Servers: []Server{{ID: "self"}, {ID: "leaderX"}, {ID: "cand"}}}
	trans := newChanTransport("self")
	rc := &Config{
		LocalID:              "self",
		ElectionTick:         5,
		HeartbeatTick:        1,
		InitialConfiguration: cfg,
		CheckQuorum:          checkQuorum,
	}
	r, err := newRaft(rc, "self", newMemLogStore(), newMemStableStore(),
		&memSnapshotStore{}, &echoFSM{}, trans)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r.configuration = cfg
	r.state = StateFollower
	r.term = 5
	r.leaderID = "leaderX" // we know a current leader
	r.electionTicks = 3    // and heard from it recently (timer not expired)
	return r
}

func upToDateVote(candidate string, term uint64, transfer bool) *RequestVoteRequest {
	return &RequestVoteRequest{
		Term:           term,
		CandidateID:    ServerID(candidate),
		LastLogIndex:   0,
		LastLogTerm:    0,
		PreVote:        false,
		LeaderTransfer: transfer,
	}
}

// TestDisruptiveVoteRejectedWhenLeaderRecentlyHeard verifies that with
// CheckQuorum enabled, a node that recently heard from a leader rejects a real
// RequestVote from a higher term WITHOUT adopting that term (§4.2.3).
func TestDisruptiveVoteRejectedWhenLeaderRecentlyHeard(t *testing.T) {
	r := newFollowerNode(t, true)

	resp := r.handleRequestVote(upToDateVote("cand", 6, false))

	if resp.VoteGranted {
		t.Fatal("disruptive RequestVote must be rejected while a leader was heard recently")
	}
	if r.term != 5 {
		t.Fatalf("term = %d, want 5 (must NOT adopt the disruptive candidate's term)", r.term)
	}
	if r.state != StateFollower {
		t.Fatalf("state = %v, want Follower (unchanged)", r.state)
	}
}

// TestLeaderTransferVoteBypassesGuard verifies a RequestVote marked as a
// leadership transfer is honored even when a leader was heard recently.
func TestLeaderTransferVoteBypassesGuard(t *testing.T) {
	r := newFollowerNode(t, true)

	resp := r.handleRequestVote(upToDateVote("cand", 6, true))

	if !resp.VoteGranted {
		t.Fatal("leadership-transfer RequestVote must bypass the disruptive-server guard")
	}
	if r.term != 6 {
		t.Fatalf("term = %d, want 6 (transfer vote adopts the new term)", r.term)
	}
	if r.votedFor != "cand" {
		t.Fatalf("votedFor = %q, want cand", r.votedFor)
	}
}

// TestDisruptiveGuardOffByDefault verifies the guard is inactive without
// CheckQuorum, preserving existing behavior (the vote is granted normally).
func TestDisruptiveGuardOffByDefault(t *testing.T) {
	r := newFollowerNode(t, false)

	resp := r.handleRequestVote(upToDateVote("cand", 6, false))

	if !resp.VoteGranted {
		t.Fatal("with CheckQuorum disabled, a valid higher-term vote must be granted")
	}
	if r.term != 6 {
		t.Fatalf("term = %d, want 6", r.term)
	}
}

// TestDisruptiveGuardInactiveAfterLeaderLost verifies that once the election
// timer has expired (no recent leader contact), a legitimate election is not
// blocked even with CheckQuorum on.
func TestDisruptiveGuardInactiveAfterLeaderLost(t *testing.T) {
	r := newFollowerNode(t, true)
	r.electionTicks = 0 // leader contact lost (timer expired)

	resp := r.handleRequestVote(upToDateVote("cand", 6, false))

	if !resp.VoteGranted {
		t.Fatal("after losing leader contact, a legitimate vote must be granted")
	}
	if r.term != 6 {
		t.Fatalf("term = %d, want 6", r.term)
	}
}

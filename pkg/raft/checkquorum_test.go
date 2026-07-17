package raft

import (
	"testing"
	"time"
)

func newLeaderNode(t *testing.T, checkQuorum bool, servers ...Server) *raft {
	t.Helper()
	cfg := Configuration{Servers: servers}
	trans := newChanTransport(servers[0].ID)
	rc := &Config{
		LocalID:              servers[0].ID,
		ElectionTick:         5,
		HeartbeatTick:        1,
		InitialConfiguration: cfg,
		CheckQuorum:          checkQuorum,
	}
	r, err := newRaft(rc, servers[0].ID, newMemLogStore(), newMemStableStore(),
		&memSnapshotStore{}, &echoFSM{}, trans)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	// loadConfiguration runs in Start(), which this unit test bypasses; set the
	// active configuration directly so QuorumSize reflects the voter set.
	r.configuration = cfg
	r.state = StateLeader
	r.term = 5
	r.leaderID = servers[0].ID
	return r
}

// TestCheckQuorumStepsDownOnLostQuorum verifies a leader with CheckQuorum enabled
// steps down when it has not heard from a quorum of voters within an election
// timeout.
func TestCheckQuorumStepsDownOnLostQuorum(t *testing.T) {
	r := newLeaderNode(t, true,
		Server{ID: "n1"}, Server{ID: "n2"}, Server{ID: "n3"})

	// No fresh acks from n2/n3 (leader is isolated). Only self counts → 1 < 2.
	r.checkQuorum()

	if r.state != StateFollower {
		t.Fatalf("state = %v, want Follower (leader must step down on quorum loss)", r.state)
	}
	if r.leaderID == "n1" {
		t.Fatalf("leaderID should be cleared after step-down, got %q", r.leaderID)
	}
}

// TestCheckQuorumStaysLeaderWithContact verifies the leader keeps leadership
// while a quorum of voters has acked recently.
func TestCheckQuorumStaysLeaderWithContact(t *testing.T) {
	r := newLeaderNode(t, true,
		Server{ID: "n1"}, Server{ID: "n2"}, Server{ID: "n3"})

	// A fresh ack from n2: self(1) + n2(1) = 2 = quorum(3) → stays leader.
	r.heartbeatAcks["n2"] = time.Now()
	r.checkQuorum()

	if r.state != StateLeader {
		t.Fatalf("state = %v, want Leader (quorum contact present)", r.state)
	}
}

// TestCheckQuorumStaleAcksStepDown verifies acks older than an election timeout
// do not count toward quorum.
func TestCheckQuorumStaleAcksStepDown(t *testing.T) {
	r := newLeaderNode(t, true,
		Server{ID: "n1"}, Server{ID: "n2"}, Server{ID: "n3"})

	// n2 acked, but long ago (well past the election timeout window).
	r.heartbeatAcks["n2"] = time.Now().Add(-10 * r.electionTimeout())
	r.checkQuorum()

	if r.state != StateFollower {
		t.Fatalf("state = %v, want Follower (stale acks must not count)", r.state)
	}
}

// TestCheckQuorumDisabledNeverStepsDown verifies the default (disabled) behavior
// is unchanged: an isolated leader keeps its state.
func TestCheckQuorumDisabledNeverStepsDown(t *testing.T) {
	r := newLeaderNode(t, false,
		Server{ID: "n1"}, Server{ID: "n2"}, Server{ID: "n3"})

	r.checkQuorum()

	if r.state != StateLeader {
		t.Fatalf("state = %v, want Leader (CheckQuorum disabled)", r.state)
	}
}

// TestCheckQuorumSingleNodeStaysLeader verifies a single-voter cluster (quorum 1)
// never steps down even with CheckQuorum on.
func TestCheckQuorumSingleNodeStaysLeader(t *testing.T) {
	r := newLeaderNode(t, true, Server{ID: "n1"})

	r.checkQuorum()

	if r.state != StateLeader {
		t.Fatalf("state = %v, want Leader (single-node always has quorum)", r.state)
	}
}

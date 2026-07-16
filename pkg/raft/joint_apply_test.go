package raft

import (
	"context"
	"testing"
	"time"
)

// TestHT1_JointConsensusApplyEndToEnd exercises applyConfigurationEntry through a
// full joint-consensus transition end to end (H-T1):
//
//   - Start a 3-node cluster (n1,n2,n3) and elect a leader.
//   - Add a brand-new voter n4 via AddServer (joint C_old,new -> C_new).
//   - Add a learner n5, catch it up, then PromoteLearner (another joint round).
//
// It asserts that after each transition the committed configuration reflects the
// change and that writes still commit (i.e. both old and new quorums are honored
// during the joint phase and the new quorum afterwards).
func TestHT1_JointConsensusApplyEndToEnd(t *testing.T) {
	ids := []string{"n1", "n2", "n3", "n4", "n5"}
	initial := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}

	nodes := map[ServerID]*raft{}
	trans := map[ServerID]*chanTransport{}
	for _, id := range ids {
		r, tr, _ := makeRaftNode(id, initial)
		nodes[ServerID(id)] = r
		trans[ServerID(id)] = tr
	}
	// Fully connect every transport (so joining nodes can be reached).
	for i := range ids {
		for j := range ids {
			if i != j {
				trans[ServerID(ids[i])].connect(trans[ServerID(ids[j])])
			}
		}
	}

	// Start the initial three voters. n4/n5 start later once added.
	for _, id := range []string{"n1", "n2", "n3"} {
		if err := nodes[ServerID(id)].Start(); err != nil {
			t.Fatal(err)
		}
		defer nodes[ServerID(id)].Shutdown()
	}

	var leader *raft
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && leader == nil {
		for _, id := range []string{"n1", "n2", "n3"} {
			if nodes[ServerID(id)].State() == StateLeader {
				leader = nodes[ServerID(id)]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}

	apply := func(payload string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := leader.Apply(ctx, []byte(payload))
		return err
	}
	if err := apply("pre-change"); err != nil {
		t.Fatalf("apply before change: %v", err)
	}

	// ---- Add a brand-new voter n4 (joint transition) --------------------------
	// Start n4 first so it can receive replication and (during the joint phase)
	// participate in the NEW-config quorum.
	if err := nodes["n4"].Start(); err != nil {
		t.Fatal(err)
	}
	defer nodes["n4"].Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := leader.AddServer(ctx, "n4", "n4-addr")
	cancel()
	if err != nil {
		t.Fatalf("AddServer(n4): %v", err)
	}

	// Wait until the committed configuration on the leader contains n4 as a voter
	// (this only happens once the joint entry AND the commit-joint entry both
	// commit and apply — the full applyConfigurationEntry path).
	if !waitConfigVoter(leader, "n4", 5*time.Second) {
		t.Fatalf("n4 never became a committed voter (joint apply transition failed)")
	}
	// The joint phase must have ended (jointConfig cleared).
	leader.mu.RLock()
	stillJoint := leader.jointConfig != nil
	leader.mu.RUnlock()
	if stillJoint {
		t.Fatal("leader still in joint consensus after transition completed")
	}

	// A write must still commit under the NEW 4-voter quorum.
	if err := apply("after-n4"); err != nil {
		t.Fatalf("apply after adding n4: %v", err)
	}

	// ---- Add a learner n5, catch it up, then promote it -----------------------
	if err := nodes["n5"].Start(); err != nil {
		t.Fatal(err)
	}
	defer nodes["n5"].Shutdown()

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	err = leader.AddLearner(ctx, "n5", "n5-addr")
	cancel()
	if err != nil {
		t.Fatalf("AddLearner(n5): %v", err)
	}
	if !waitConfigContains(leader, "n5", true, 5*time.Second) {
		t.Fatal("n5 never became a committed learner")
	}

	// Drive a few writes so the learner catches up to near the leader's tail.
	for i := 0; i < 5; i++ {
		if err := apply("catchup"); err != nil {
			t.Fatalf("apply during learner catch-up: %v", err)
		}
	}

	// Wait for n5's matchIndex to catch up so PromoteLearner passes the
	// readiness gate.
	promoteDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(promoteDeadline) {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		err = leader.PromoteLearner(ctx, "n5")
		cancel()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("PromoteLearner(n5) never succeeded: %v", err)
	}

	// n5 must become a committed VOTER (learner=false) through the joint apply.
	if !waitConfigVoter(leader, "n5", 5*time.Second) {
		t.Fatal("n5 never became a committed voter after promotion")
	}

	// Final quorum check: with 5 voters, a write must still commit.
	if err := apply("final"); err != nil {
		t.Fatalf("apply under final 5-voter config: %v", err)
	}

	// The committed configuration must contain exactly the 5 voters, no learners.
	cfg := leader.Configuration()
	if cfg.VoteCount() != 5 {
		t.Fatalf("expected 5 voters in final config, got %d (%+v)", cfg.VoteCount(), cfg.Servers)
	}
	if len(cfg.Learners()) != 0 {
		t.Fatalf("expected no learners in final config, got %d", len(cfg.Learners()))
	}
}

func waitConfigVoter(r *raft, id ServerID, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cfg := r.Configuration()
		if cfg.IsVoter(id) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func waitConfigContains(r *raft, id ServerID, learner bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cfg := r.Configuration()
		if s := cfg.GetServer(id); s != nil && s.Learner == learner {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

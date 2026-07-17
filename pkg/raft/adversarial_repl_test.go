package raft

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// leaderIndex returns the index of a node currently in StateLeader, or -1.
func leaderIndex(nodes []*raft) int {
	for i, r := range nodes {
		if r.State() == StateLeader {
			return i
		}
	}
	return -1
}

func applyBurst(r *raft, n int) int {
	acked := 0
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
		if _, err := r.Apply(ctx, []byte("x")); err == nil {
			acked++
		}
		cancel()
	}
	return acked
}

// TestAdversarialReplicationConsistency runs a randomized fault campaign that
// repeatedly isolates the leader (forcing failover + a rejoined-node catch-up),
// stressing the M-P1/M-P2 continuous drain path and conflict backup. It asserts
// that after healing, all replicas converge to the SAME committed state and no
// acknowledged write is lost (linearizability-lite: every committed entry is
// present on every node).
func TestAdversarialReplicationConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("adversarial soak")
	}
	nodes, transports, fsms := makeCluster(t)
	for _, r := range nodes {
		if err := r.Start(); err != nil {
			t.Fatal(err)
		}
		defer r.Shutdown()
	}
	findLeader(t, nodes, 5*time.Second)

	rng := rand.New(rand.NewSource(42))
	acked := 0
	const rounds = 15

	for round := 0; round < rounds; round++ {
		li := leaderIndex(nodes)
		if li < 0 {
			// no leader right now; give the cluster a moment to elect one
			eventually(t, 5*time.Second, func() bool { return leaderIndex(nodes) >= 0 }, "no leader mid-campaign")
			li = leaderIndex(nodes)
		}
		// Apply some entries to the current leader.
		acked += applyBurst(nodes[li], 1+rng.Intn(6))

		// Isolate the leader -> the other two must elect a new leader.
		atomic.StoreInt32(&transports[li].drop, 1)

		// Wait for a NEW leader among the reachable majority.
		var newLeader *raft
		eventually(t, 5*time.Second, func() bool {
			for i, r := range nodes {
				if i != li && r.State() == StateLeader {
					newLeader = r
					return true
				}
			}
			return false
		}, "no new leader after isolating the old one")

		// Commit more entries on the majority; the isolated old leader misses them.
		acked += applyBurst(newLeader, 3+rng.Intn(6))

		// Heal the old leader; it must rejoin and catch up (drain + conflict backup).
		atomic.StoreInt32(&transports[li].drop, 0)
		time.Sleep(time.Duration(30+rng.Intn(80)) * time.Millisecond)
	}

	// Heal everything and land a final marker on a stable leader.
	for _, tr := range transports {
		atomic.StoreInt32(&tr.drop, 0)
	}
	eventually(t, 8*time.Second, func() bool {
		li := leaderIndex(nodes)
		if li < 0 {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err := nodes[li].Apply(ctx, []byte("final-marker"))
		cancel()
		if err == nil {
			acked++
			return true
		}
		return false
	}, "could not land a final marker after healing")

	// All three FSMs must converge to the same count, and cover every acked write.
	eventually(t, 15*time.Second, func() bool {
		c0, c1, c2 := fsms[0].count(), fsms[1].count(), fsms[2].count()
		return c0 == c1 && c1 == c2 && c0 >= acked
	}, "replicas did not converge to a consistent committed state")

	c0, c1, c2 := fsms[0].count(), fsms[1].count(), fsms[2].count()
	if c0 != c1 || c1 != c2 {
		t.Fatalf("DIVERGENCE: FSM counts differ across replicas: %d/%d/%d", c0, c1, c2)
	}
	if c0 < acked {
		t.Fatalf("LOST COMMITTED ENTRIES: converged=%d < acked=%d", c0, acked)
	}
	t.Logf("adversarial OK: %d acked writes, all 3 replicas converged to %d applied entries", acked, c0)
}

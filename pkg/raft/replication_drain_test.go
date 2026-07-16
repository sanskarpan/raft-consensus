package raft

import (
	"context"
	"testing"
	"time"
)

// M-P1/M-P2: a follower that is many batches behind must be caught up by the
// continuous drain loop (replicateTo keeps sending until caught up) rather than
// one batch per heartbeat tick. Apply >2x the 100-entry batch size and assert
// every follower's FSM catches up quickly.
func TestReplicationDrainsLargeBacklog(t *testing.T) {
	nodes, _, fsms := makeCluster(t)
	for _, r := range nodes {
		if err := r.Start(); err != nil {
			t.Fatal(err)
		}
		defer r.Shutdown()
	}
	leader := findLeader(t, nodes, 5*time.Second)

	const N = 250 // > 2 batches of 100
	for i := 0; i < N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if _, err := leader.Apply(ctx, []byte("v")); err != nil {
			cancel()
			t.Fatalf("apply %d: %v", i, err)
		}
		cancel()
	}

	// All three FSMs must reach N. With the drain loop this happens within a few
	// round-trips; without it (one batch per tick) it would take ~3 heartbeat
	// intervals per 100 entries. Poll with a generous bound.
	deadline := time.Now().Add(5 * time.Second)
	for {
		allCaughtUp := true
		for _, f := range fsms {
			if f.count() < N {
				allCaughtUp = false
				break
			}
		}
		if allCaughtUp {
			return // success
		}
		if time.Now().After(deadline) {
			counts := []int{fsms[0].count(), fsms[1].count(), fsms[2].count()}
			t.Fatalf("followers did not drain backlog of %d: counts=%v", N, counts)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

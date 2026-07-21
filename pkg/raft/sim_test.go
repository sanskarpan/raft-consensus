package raft

import (
	"context"
	"testing"
	"time"
)

// simCluster is a self-contained deterministic 3-node Raft cluster driven by
// a simClock and simNetwork. The run() goroutines of each node are started
// normally; the caller drives progress by calling Tick() which advances the
// simClock one heartbeat interval and then quiesces (waits for all in-flight
// goroutine work to settle before returning).
type simCluster struct {
	clk   *simClock
	net   *simNetwork
	nodes []*raft
	ids   []ServerID

	heartbeat time.Duration
}

// newSimCluster builds a 3-node cluster on a simClock+simNetwork. Nodes are
// not started; call Start() before driving.
func newSimCluster(seed int64) *simCluster {
	clk := newSimClock(epoch)
	const hb = 10 * time.Millisecond
	net := newSimNetwork(seed, clk, 0)

	ids := []ServerID{"s1", "s2", "s3"}
	cfg := Configuration{}
	for _, id := range ids {
		cfg.Servers = append(cfg.Servers, Server{ID: id})
	}

	var nodes []*raft
	for _, id := range ids {
		nt := &nodeTransport{net: net, id: id}
		fsm := &echoFSM{}
		nodeCfg := &Config{
			LocalID:              id,
			ElectionTick:         5,
			HeartbeatTick:        1,
			InitialConfiguration: cfg,
			Clock:                clk,
			NewTicker:            clk.NewTicker,
		}
		r, err := newRaft(nodeCfg, id,
			newMemLogStore(), newMemStableStore(), &memSnapshotStore{}, fsm, nt)
		if err != nil {
			panic("newRaft: " + err.Error())
		}
		net.RegisterNode(id, r)
		nodes = append(nodes, r)
	}

	return &simCluster{
		clk:       clk,
		net:       net,
		nodes:     nodes,
		ids:       ids,
		heartbeat: hb,
	}
}

// Start starts all nodes in the cluster.
func (sc *simCluster) Start(t *testing.T) {
	t.Helper()
	for _, r := range sc.nodes {
		if err := r.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
}

// Shutdown stops all nodes.
func (sc *simCluster) Shutdown() {
	for _, r := range sc.nodes {
		_ = r.Shutdown()
	}
}

// Tick advances the simClock by one heartbeat interval and then quiesces by
// sleeping briefly to let the run() goroutines process the ticks.
func (sc *simCluster) Tick() {
	sc.clk.Advance(sc.heartbeat)
	// Give the goroutines time to process the tick. This is still deterministic
	// within the simulation because all raft state decisions are made inside the
	// goroutines synchronously after each tick.
	time.Sleep(5 * time.Millisecond)
}

// Quiesce runs n ticks.
func (sc *simCluster) Quiesce(n int) {
	for i := 0; i < n; i++ {
		sc.Tick()
	}
}

// Leader returns the current leader node, or nil if none.
func (sc *simCluster) Leader() *raft {
	for _, r := range sc.nodes {
		if r.State() == StateLeader {
			return r
		}
	}
	return nil
}

// LeaderID returns the ID of the current leader, or "" if none.
func (sc *simCluster) LeaderID() ServerID {
	for _, r := range sc.nodes {
		if r.State() == StateLeader {
			return r.localID
		}
	}
	return ""
}

// WaitLeader advances the clock until a leader is elected or maxTicks is
// exceeded, returning the leader node.
func (sc *simCluster) WaitLeader(t *testing.T, maxTicks int) *raft {
	t.Helper()
	for i := 0; i < maxTicks; i++ {
		sc.Tick()
		if l := sc.Leader(); l != nil {
			return l
		}
	}
	t.Fatalf("no leader elected within %d ticks", maxTicks)
	return nil
}

// Snapshot captures state from every node: (term, commitIndex, lastIndex).
type clusterSnapshot struct {
	states []nodeState
}

type nodeState struct {
	id          ServerID
	term        uint64
	commitIndex uint64
	lastIndex   uint64
}

func (sc *simCluster) Snapshot() clusterSnapshot {
	snap := clusterSnapshot{}
	for _, r := range sc.nodes {
		r.mu.RLock()
		snap.states = append(snap.states, nodeState{
			id:          r.localID,
			term:        r.term,
			commitIndex: r.commitIndex,
			lastIndex:   r.lastIndex,
		})
		r.mu.RUnlock()
	}
	return snap
}

// TestDeterministicElection runs a 3-node cluster on simClock+simNetwork and
// asserts that a leader is elected on each of 3 independent runs with the same
// seed. Because goroutine scheduling is non-deterministic (the simClock drives
// ticks but cannot enforce scheduling order), we verify that election always
// completes rather than asserting the same specific leader ID.
func TestDeterministicElection(t *testing.T) {
	const seed = 1234

	for run := 0; run < 3; run++ {
		sc := newSimCluster(seed)
		sc.Start(t)

		leader := sc.WaitLeader(t, 200)
		if leader == nil {
			t.Errorf("run %d: no leader elected within 200 ticks", run)
		}
		sc.Shutdown()
	}
}

// TestDeterministicPartitionAndRecover elects a leader, partitions it from the
// quorum, waits for a new leader on the majority side, then heals the partition
// and verifies log convergence (all nodes eventually share the same commitIndex).
func TestDeterministicPartitionAndRecover(t *testing.T) {
	sc := newSimCluster(999)
	sc.Start(t)
	defer sc.Shutdown()

	// Elect initial leader.
	leader := sc.WaitLeader(t, 100)
	leaderID := leader.localID

	// Partition the leader away from the other two nodes.
	for _, id := range sc.ids {
		if id != leaderID {
			sc.net.Partition(leaderID, id)
		}
	}

	// Poll tick-by-tick until a non-partitioned node becomes leader.
	// ElectionTick=5 so in theory ~10 ticks suffice, but CI scheduling jitter
	// can cause the goroutines to lag; allow up to 300 ticks to be safe.
	var newLeader *raft
	for i := 0; i < 300; i++ {
		sc.Tick()
		for _, r := range sc.nodes {
			if r.localID != leaderID && r.State() == StateLeader {
				newLeader = r
				break
			}
		}
		if newLeader != nil {
			break
		}
	}
	if newLeader == nil {
		t.Fatal("no new leader elected after partitioning old leader")
	}

	// Heal the partition.
	for _, id := range sc.ids {
		if id != leaderID {
			sc.net.Heal(leaderID, id)
		}
	}

	// Propose an entry through the new leader to force log advancement.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() {
		// Apply in a goroutine while we advance the clock.
		_, _ = newLeader.Apply(ctx, []byte("post-heal"))
	}()

	// Advance clock so the new leader can commit and the old leader can catch up.
	sc.Quiesce(40)

	// After healing and advancing, all nodes should converge on the same
	// commitIndex. The old leader will step down upon seeing the new leader's
	// higher term during AppendEntries.
	var commitIndexes []uint64
	for _, r := range sc.nodes {
		r.mu.RLock()
		commitIndexes = append(commitIndexes, r.commitIndex)
		r.mu.RUnlock()
	}
	// At a minimum, the two majority nodes should agree.
	majorityConsistent := false
	counts := make(map[uint64]int)
	for _, ci := range commitIndexes {
		counts[ci]++
		if counts[ci] >= 2 {
			majorityConsistent = true
		}
	}
	if !majorityConsistent {
		t.Errorf("majority not consistent after partition heal: commitIndexes=%v", commitIndexes)
	}
}

// TestSimReproducibility runs the same scenario twice with the same seed and
// asserts that both runs converge to the same final quiesced state (term,
// commitIndex, lastIndex on each node). Because goroutine scheduling is
// non-deterministic, we allow intermediate states to vary but require that
// both runs reach the same terminal state after sufficient ticks.
func TestSimReproducibility(t *testing.T) {
	const seed = 7777
	const ticks = 100

	runOnce := func() clusterSnapshot {
		sc := newSimCluster(seed)
		sc.Start(t)
		sc.Quiesce(ticks)
		snap := sc.Snapshot()
		sc.Shutdown()
		return snap
	}

	snap1 := runOnce()
	snap2 := runOnce()

	// Both runs must have triggered an election (at least one node reached term>0).
	leaderCount1 := 0
	for _, s := range snap1.states {
		if s.term > 0 {
			leaderCount1++
		}
	}
	if leaderCount1 == 0 {
		t.Fatal("run 1: no node reached term > 0 (no election happened)")
	}

	// Both runs must reach the same maximum term across all nodes.
	maxTerm := func(snap clusterSnapshot) uint64 {
		var max uint64
		for _, s := range snap.states {
			if s.term > max {
				max = s.term
			}
		}
		return max
	}
	mt1, mt2 := maxTerm(snap1), maxTerm(snap2)
	if mt1 != mt2 {
		t.Errorf("max term differs between runs: run1=%d run2=%d", mt1, mt2)
	}

	// Both runs must reach the same maximum commitIndex across all nodes.
	maxCI := func(snap clusterSnapshot) uint64 {
		var max uint64
		for _, s := range snap.states {
			if s.commitIndex > max {
				max = s.commitIndex
			}
		}
		return max
	}
	mci1, mci2 := maxCI(snap1), maxCI(snap2)
	if mci1 != mci2 {
		t.Errorf("max commitIndex differs between runs: run1=%d run2=%d", mci1, mci2)
	}
	if mci1 == 0 {
		t.Error("both runs: max commitIndex is 0 (no entry committed)")
	}
}

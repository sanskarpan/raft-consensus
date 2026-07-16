package raft

import (
	"context"
	"testing"
	"time"
)

// singleConfig builds a one-server voting configuration.
func singleConfig(id string) Configuration {
	return Configuration{Servers: []Server{{ID: ServerID(id), Address: ServerAddress(id)}}}
}

// threeConfig builds a three-server voting configuration.
func threeConfig(a, b, c string) Configuration {
	return Configuration{Servers: []Server{
		{ID: ServerID(a), Address: ServerAddress(a)},
		{ID: ServerID(b), Address: ServerAddress(b)},
		{ID: ServerID(c), Address: ServerAddress(c)},
	}}
}

// ---- L3: GetServer / Voters / Learners must not alias the range copy --------

func TestGetServerReturnsRealElement(t *testing.T) {
	cfg := threeConfig("a", "b", "c")

	// Mutate through the returned pointer; it must affect the real element.
	s := cfg.GetServer("b")
	if s == nil {
		t.Fatal("GetServer returned nil for existing server")
	}
	s.Learner = true

	if !cfg.Servers[1].Learner {
		t.Fatalf("GetServer returned a copy: mutation did not reach cfg.Servers[1]")
	}

	// Two lookups of the same id must return the identical pointer.
	if cfg.GetServer("b") != &cfg.Servers[1] {
		t.Fatalf("GetServer did not return pointer to the real slice element")
	}
}

func TestVotersLearnersReturnRealElements(t *testing.T) {
	cfg := Configuration{Servers: []Server{
		{ID: "a"},
		{ID: "b", Learner: true},
		{ID: "c"},
	}}

	voters := cfg.Voters()
	if len(voters) != 2 {
		t.Fatalf("expected 2 voters, got %d", len(voters))
	}
	// Mutating a returned voter pointer must affect the real element.
	voters[0].Address = "changed"
	if cfg.GetServer(voters[0].ID).Address != "changed" {
		t.Fatalf("Voters() returned a copy, not a pointer to the real element")
	}

	learners := cfg.Learners()
	if len(learners) != 1 {
		t.Fatalf("expected 1 learner, got %d", len(learners))
	}
	learners[0].Address = "L"
	if cfg.GetServer(learners[0].ID).Address != "L" {
		t.Fatalf("Learners() returned a copy, not a pointer to the real element")
	}
}

// ---- L1: Validate requires ElectionTick sufficiently > HeartbeatTick --------

func TestValidateRejectsTightElectionTick(t *testing.T) {
	cfg := &Config{LocalID: "a", ElectionTick: 2, HeartbeatTick: 1}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected Validate to reject ElectionTick=2, HeartbeatTick=1")
	}
	// A comfortably larger ElectionTick is accepted.
	ok := &Config{LocalID: "a", ElectionTick: 5, HeartbeatTick: 1}
	if err := ok.Validate(); err != nil {
		t.Fatalf("expected Validate to accept ElectionTick=5, HeartbeatTick=1: %v", err)
	}
}

// ---- L2: startElectionWith takes an explicit alreadyPreVoted flag -----------

func TestStartElectionWithExplicitPreVoteFlag(t *testing.T) {
	// Use a multi-node config so pre-vote does not immediately short-circuit to a
	// real election; this isolates the alreadyPreVoted flag behaviour.
	r, _, _ := makeRaftNode("a", threeConfig("a", "b", "c"))
	r.config.PreVote = true
	r.mu.Lock()
	r.configuration = threeConfig("a", "b", "c")
	r.state = StateFollower
	// alreadyPreVoted=false with PreVote on must route through pre-vote.
	r.startElectionWith(false)
	inPreVote := r.inPreVote
	term := r.term
	r.mu.Unlock()
	if !inPreVote {
		t.Fatalf("startElectionWith(false) with PreVote should enter pre-vote phase")
	}
	if term != 0 {
		t.Fatalf("pre-vote must not bump the term; got term %d", term)
	}

	// alreadyPreVoted=true must proceed straight to a real election (single node
	// wins immediately and becomes leader), regardless of the shared inPreVote flag.
	r2, _, _ := makeRaftNode("b", singleConfig("b"))
	r2.config.PreVote = true
	r2.mu.Lock()
	r2.configuration = singleConfig("b")
	r2.state = StateFollower
	r2.inPreVote = false
	r2.startElectionWith(true)
	state := r2.state
	r2.mu.Unlock()
	if state != StateLeader {
		t.Fatalf("startElectionWith(true) in single-node cluster should become leader, got %s", state)
	}
}

// ---- H3: persistLog rejects non-contiguous appends --------------------------

func TestPersistLogRejectsNonContiguous(t *testing.T) {
	r, _, _ := makeRaftNode("a", singleConfig("a"))
	r.mu.Lock()
	defer r.mu.Unlock()

	// lastIndex starts at 0; a contiguous append at index 1 is fine.
	if err := r.persistLog([]*LogEntry{{Term: 1, Index: 1}}); err != nil {
		t.Fatalf("contiguous append failed: %v", err)
	}
	// A gap (index 3 when lastIndex is 1) must be rejected.
	if err := r.persistLog([]*LogEntry{{Term: 1, Index: 3}}); err == nil {
		t.Fatalf("expected non-contiguous append (index 3 after 1) to be rejected")
	}
	// lastIndex must be unchanged after the rejected append.
	if r.lastIndex != 1 {
		t.Fatalf("lastIndex changed on rejected append: got %d, want 1", r.lastIndex)
	}
	// A gap within a single batch must also be rejected.
	if err := r.persistLog([]*LogEntry{{Term: 1, Index: 2}, {Term: 1, Index: 4}}); err == nil {
		t.Fatalf("expected non-contiguous batch to be rejected")
	}
}

// ---- H3: commitIndex capped at last entry carried by the request ------------

func TestAppendEntriesCommitCappedAtRequestEntries(t *testing.T) {
	r, _, _ := makeRaftNode("f", threeConfig("f", "l", "x"))

	// Seed the follower log with entries 1..3 (from an earlier, longer leader).
	r.mu.Lock()
	r.configuration = threeConfig("f", "l", "x")
	_ = r.persistLog([]*LogEntry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 1, Index: 3},
	})
	r.term = 1
	r.state = StateFollower
	r.mu.Unlock()

	// New leader in term 1 sends a request that only covers up to index 1 (a
	// heartbeat carrying prevLogIndex=0 and a single entry at index 1) but
	// advertises LeaderCommit=3. commitIndex must NOT jump to 3 (the local log
	// tail) — only to what this request actually covers.
	resp := r.handleAppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderID:     "l",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*LogEntry{{Term: 1, Index: 1}},
		LeaderCommit: 3,
	})
	if !resp.Success {
		t.Fatalf("append entries should succeed")
	}
	r.mu.RLock()
	ci := r.commitIndex
	r.mu.RUnlock()
	if ci != 1 {
		t.Fatalf("commitIndex advanced past request coverage: got %d, want 1", ci)
	}
}

// ---- H2: stale AppendEntries response must not move matchIndex --------------

// blockingTransport lets a test intercept AppendEntries, park the caller until
// released, and control the response — so we can simulate a term change that
// lands while a replication RPC is in flight.
type blockingTransport struct {
	localID ServerID
	release chan struct{}
	entered chan struct{}
	resp    *AppendEntriesResponse
}

func (b *blockingTransport) AppendEntries(_ context.Context, _ ServerID, _ *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	return b.resp, nil
}
func (b *blockingTransport) RequestVote(_ context.Context, _ ServerID, _ *RequestVoteRequest) (*RequestVoteResponse, error) {
	return &RequestVoteResponse{}, nil
}
func (b *blockingTransport) InstallSnapshot(_ context.Context, _ ServerID, _ *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	return &InstallSnapshotResponse{}, nil
}
func (b *blockingTransport) TimeoutNow(_ context.Context, _ ServerID) error { return nil }
func (b *blockingTransport) SetLocalID(id ServerID)                         { b.localID = id }
func (b *blockingTransport) Close() error                                   { return nil }

func TestReplicateToDropsStaleResponse(t *testing.T) {
	bt := &blockingTransport{
		release: make(chan struct{}),
		entered: make(chan struct{}, 1),
		resp:    &AppendEntriesResponse{Term: 5, Success: true, Index: 2},
	}
	cfg := &Config{
		LocalID:              "a",
		ElectionTick:         5,
		HeartbeatTick:        1,
		InitialConfiguration: threeConfig("a", "b", "c"),
	}
	r, err := newRaft(cfg, "a", newMemLogStore(), newMemStableStore(), &memSnapshotStore{}, &echoFSM{}, bt)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}

	// Become leader in term 5 with entries to replicate.
	r.mu.Lock()
	r.configuration = threeConfig("a", "b", "c")
	r.state = StateLeader
	r.term = 5
	r.leaderID = "a"
	_ = r.persistLog([]*LogEntry{{Term: 5, Index: 1}, {Term: 5, Index: 2}})
	r.nextIndex["b"] = 1
	r.matchIndex["b"] = 0
	r.inflightReplication = map[ServerID]bool{}
	r.mu.Unlock()

	// Kick off a replication that will block inside AppendEntries.
	go r.replicateTo("b")
	<-bt.entered

	// While the RPC is in flight, simulate the leader stepping down and being
	// re-elected in a NEW term. The in-flight response describes the old term.
	r.mu.Lock()
	r.term = 6
	r.mu.Unlock()

	// Release the RPC; the (stale) success response arrives.
	close(bt.release)

	// Give the response handler time to run.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		r.mu.RLock()
		mi := r.matchIndex["b"]
		r.mu.RUnlock()
		if mi != 0 {
			t.Fatalf("stale response updated matchIndex to %d; expected it to be dropped", mi)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ---- H1: sendHeartbeat must not spawn unbounded replication goroutines ------

func TestReplicationInflightBounded(t *testing.T) {
	bt := &blockingTransport{
		release: make(chan struct{}),
		entered: make(chan struct{}, 1),
		resp:    &AppendEntriesResponse{Term: 1, Success: true, Index: 1},
	}
	cfg := &Config{
		LocalID:              "a",
		ElectionTick:         5,
		HeartbeatTick:        1,
		InitialConfiguration: threeConfig("a", "b", "c"),
	}
	r, err := newRaft(cfg, "a", newMemLogStore(), newMemStableStore(), &memSnapshotStore{}, &echoFSM{}, bt)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r.mu.Lock()
	r.configuration = threeConfig("a", "b", "c")
	r.state = StateLeader
	r.term = 1
	r.leaderID = "a"
	r.nextIndex = map[ServerID]uint64{"b": 1, "c": 1}
	r.matchIndex = map[ServerID]uint64{"b": 0, "c": 0}
	r.inflightReplication = map[ServerID]bool{}

	// First heartbeat spawns one goroutine per follower.
	r.sendHeartbeat()
	// A follower (b) is now in-flight (blocked in AppendEntries). Repeated
	// heartbeats must NOT spawn additional goroutines for the in-flight follower.
	r.mu.Unlock()

	// Wait until b's goroutine is parked in the transport.
	<-bt.entered

	r.mu.Lock()
	if !r.inflightReplication["b"] {
		r.mu.Unlock()
		t.Fatalf("expected follower b to be marked in-flight")
	}
	// Fire many more heartbeats while b is still blocked.
	for i := 0; i < 50; i++ {
		r.sendHeartbeat()
	}
	stillInflight := r.inflightReplication["b"]
	r.mu.Unlock()

	if !stillInflight {
		t.Fatalf("in-flight guard cleared unexpectedly")
	}
	// Release everything so goroutines can drain.
	close(bt.release)
}

// ---- M1: leadership transfer must not report success prematurely ------------

func TestLeadershipTransferNoPrematureSuccess(t *testing.T) {
	r, _, _ := makeRaftNode("a", threeConfig("a", "b", "c"))
	r.mu.Lock()
	r.configuration = threeConfig("a", "b", "c")
	r.state = StateLeader
	r.term = 2
	r.leaderID = "a"
	r.lastIndex = 5
	r.matchIndex = map[ServerID]uint64{"b": 5, "c": 5}
	lt := &leadershipTransfer{target: "b", complete: make(chan struct{})}
	r.leadershipTransfer = lt

	// Drive one transfer step: TimeoutNow is sent but leadership has NOT changed,
	// so the future must remain unresolved.
	r.doLeadershipTransfer()
	r.mu.Unlock()

	select {
	case <-lt.complete:
		t.Fatalf("leadership transfer reported completion before leadership changed")
	default:
	}

	// While transferring, new proposals must be rejected. handleProposal locks
	// r.mu itself, so call it without holding the lock.
	fut := &ApplyFuture{ch: make(chan struct{})}
	r.handleProposal(&proposalFuture{data: []byte("x"), future: fut})
	if err := fut.Error(); err == nil {
		t.Fatalf("expected proposal to be rejected during leadership transfer")
	}

	// Now simulate the new leader taking over: it won an election in a higher
	// term and sends AppendEntries, which steps us down to follower.
	r.handleAppendEntries(&AppendEntriesRequest{
		Term:     3,
		LeaderID: "b",
	})

	select {
	case <-lt.complete:
		if lt.err != nil {
			t.Fatalf("transfer should succeed once leadership changed, got %v", lt.err)
		}
	case <-time.After(time.Second):
		t.Fatalf("transfer future was not resolved after leadership changed")
	}
}

func TestLeadershipTransferTimeout(t *testing.T) {
	r, _, _ := makeRaftNode("a", threeConfig("a", "b", "c"))
	r.mu.Lock()
	r.configuration = threeConfig("a", "b", "c")
	r.state = StateLeader
	r.term = 2
	r.leaderID = "a"
	r.lastIndex = 5
	r.matchIndex = map[ServerID]uint64{"b": 5, "c": 5}
	lt := &leadershipTransfer{target: "b", complete: make(chan struct{})}
	r.leadershipTransfer = lt

	// First step arms the deadline and sends TimeoutNow.
	r.doLeadershipTransfer()
	// Force the deadline into the past.
	lt.deadline = time.Now().Add(-time.Second)
	// Next step must observe the timeout and fail the transfer.
	r.doLeadershipTransfer()
	r.mu.Unlock()

	select {
	case <-lt.complete:
		if lt.err != ErrTimeout {
			t.Fatalf("expected ErrTimeout, got %v", lt.err)
		}
	default:
		t.Fatalf("transfer future not resolved after timeout")
	}
}

// ---- M2: Shutdown resolves committed futures as success ---------------------

func TestShutdownResolvesCommittedFutures(t *testing.T) {
	r, _, _ := makeRaftNode("a", singleConfig("a"))
	r.mu.Lock()
	r.configuration = singleConfig("a")
	r.state = StateLeader
	r.term = 1

	// Entry 1 is committed; entry 2 is not.
	_ = r.persistLog([]*LogEntry{
		{Term: 1, Index: 1, Type: EntryNormal, Data: []byte("c")},
		{Term: 1, Index: 2, Type: EntryNormal, Data: []byte("u")},
	})
	r.commitIndex = 1
	r.applyIndex = 1 // already applied so drain does not re-apply

	committedFut := &ApplyFuture{ch: make(chan struct{})}
	uncommittedFut := &ApplyFuture{ch: make(chan struct{})}
	r.pendingFutures[1] = committedFut
	r.pendingFutures[2] = uncommittedFut
	r.mu.Unlock()

	// Invoke the shutdown drain path directly (this is what run() calls on stop).
	r.drainFuturesOnShutdown()

	// Committed future must resolve WITHOUT ErrNotStarted.
	select {
	case <-committedFut.ch:
		if committedFut.Error() == ErrNotStarted {
			t.Fatalf("committed future failed with ErrNotStarted on shutdown")
		}
		if committedFut.Error() != nil {
			t.Fatalf("committed future should resolve as success, got %v", committedFut.Error())
		}
	default:
		t.Fatalf("committed future not resolved on shutdown")
	}

	// Uncommitted future must fail with ErrNotStarted.
	select {
	case <-uncommittedFut.ch:
		if uncommittedFut.Error() != ErrNotStarted {
			t.Fatalf("uncommitted future should fail with ErrNotStarted, got %v", uncommittedFut.Error())
		}
	default:
		t.Fatalf("uncommitted future not resolved on shutdown")
	}
}

// ---- M3: triggerSnapshot records the real (applied) snapshot index ----------

func TestSnapshotIndexReflectsApplyIndex(t *testing.T) {
	r, _, _ := makeRaftNode("a", singleConfig("a"))
	r.mu.Lock()
	r.configuration = singleConfig("a")
	r.state = StateLeader
	r.term = 1
	r.lastIndex = 100
	r.applyIndex = 40 // FSM only applied up to 40
	r.snapshotIndex = 0
	r.mu.Unlock()

	// Take a snapshot directly through the run-loop handler.
	req := &reqSnapshotFuture{done: make(chan struct{})}
	r.processSnapshot(req)
	<-req.done
	if req.err != nil {
		t.Fatalf("processSnapshot error: %v", req.err)
	}

	r.mu.RLock()
	si := r.snapshotIndex
	r.mu.RUnlock()
	if si != 40 {
		t.Fatalf("snapshotIndex should equal applyIndex (40), got %d", si)
	}
}

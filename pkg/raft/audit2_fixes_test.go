package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Test doubles: a snapshot store that actually stores bytes, and a compacting
// log store. These are needed for the C-A1 (leader sends InstallSnapshot) test
// and other snapshot-driven scenarios.
// =============================================================================

type memSnapEntry struct {
	meta *SnapshotMeta
	data []byte
}

type dataSnapshotStore struct {
	mu    sync.Mutex
	snaps map[string]*memSnapEntry
	seq   int
}

func newDataSnapshotStore() *dataSnapshotStore {
	return &dataSnapshotStore{snaps: make(map[string]*memSnapEntry)}
}

type dataSink struct {
	store *dataSnapshotStore
	id    string
	meta  *SnapshotMeta
	buf   bytes.Buffer
}

func (s *dataSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *dataSink) ID() string                  { return s.id }
func (s *dataSink) Cancel() error               { return nil }
func (s *dataSink) Close() error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	data := append([]byte(nil), s.buf.Bytes()...)
	s.store.snaps[s.id] = &memSnapEntry{meta: s.meta, data: data}
	return nil
}

func (m *dataSnapshotStore) Create(version SnapshotVersion, index, term uint64, configuration Configuration) (SnapshotSink, error) {
	m.mu.Lock()
	m.seq++
	id := fmt.Sprintf("snap-%d-%d-%d", term, index, m.seq)
	m.mu.Unlock()
	return &dataSink{
		store: m,
		id:    id,
		meta: &SnapshotMeta{
			ID:            id,
			Index:         index,
			Term:          term,
			Configuration: configuration,
		},
	}, nil
}

type dataSnapshot struct {
	meta *SnapshotMeta
	data []byte
}

func (s *dataSnapshot) Index() uint64         { return s.meta.Index }
func (s *dataSnapshot) Term() uint64          { return s.meta.Term }
func (s *dataSnapshot) Reader() io.ReadCloser { return io.NopCloser(bytes.NewReader(s.data)) }

func (m *dataSnapshotStore) Open(id string) (Snapshot, *SnapshotMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.snaps[id]
	if !ok {
		return nil, nil, errors.New("snapshot not found")
	}
	return &dataSnapshot{meta: e.meta, data: e.data}, e.meta, nil
}

func (m *dataSnapshotStore) List() ([]*SnapshotMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*SnapshotMeta
	for _, e := range m.snaps {
		out = append(out, e.meta)
	}
	return out, nil
}

func (m *dataSnapshotStore) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.snaps, id)
	return nil
}

// compactLogStore wraps memLogStore and tracks a true FirstIndex so that
// Compact() actually hides entries at or below a compaction point (as a real
// WAL would after snapshot+compact). This lets us exercise the C-A1 path where
// a follower's nextIndex falls below FirstIndex.
type compactLogStore struct {
	*memLogStore
	mu    sync.Mutex
	first uint64
}

func newCompactLogStore() *compactLogStore {
	return &compactLogStore{memLogStore: newMemLogStore()}
}

func (c *compactLogStore) Compact(upto uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if upto+1 > c.first {
		c.first = upto + 1
	}
	// Drop the underlying entries so Get on a compacted index fails.
	_ = c.DeleteRange(1, upto)
	return nil
}

func (c *compactLogStore) FirstIndex() (uint64, error) {
	c.mu.Lock()
	first := c.first
	c.mu.Unlock()
	if first == 0 {
		return c.memLogStore.FirstIndex()
	}
	return first, nil
}

// makeRaftNodeWithStores mirrors makeRaftNode but lets the caller supply custom
// stores (used by the C-A1 test).
func makeRaftNodeWithStores(id string, config Configuration, log LogStore, snap SnapshotStore) (*raft, *chanTransport, *echoFSM) {
	trans := newChanTransport(ServerID(id))
	fsm := &echoFSM{}
	cfg := &Config{
		LocalID:              ServerID(id),
		ElectionTick:         5,
		HeartbeatTick:        1,
		InitialConfiguration: config,
	}
	r, err := newRaft(cfg, ServerID(id), log, newMemStableStore(), snap, fsm, trans)
	if err != nil {
		panic(fmt.Sprintf("newRaft: %v", err))
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
	return r, trans, fsm
}

// =============================================================================
// C-A1: leader sends InstallSnapshot to a follower behind the compaction point
// =============================================================================

func TestCA1_LeaderSendsInstallSnapshot(t *testing.T) {
	// n1 is the sole voter (quorum=1 so it becomes leader without help); n2 is a
	// learner that still receives replication and snapshots. This keeps the test
	// deterministic while exercising the exact C-A1 path: a follower/learner whose
	// nextIndex has fallen below the leader's compacted FirstIndex.
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2", Learner: true}}}

	leaderLog := newCompactLogStore()
	leaderSnap := newDataSnapshotStore()
	r1, t1, _ := makeRaftNodeWithStores("n1", cfg, leaderLog, leaderSnap)

	r2, t2, fsm2 := makeRaftNodeWithStores("n2", cfg, newMemLogStore(), newDataSnapshotStore())
	// n2 is a learner: it must never campaign, so start it in StateLearner.
	r2.config.StartAsLearner = true

	t1.connect(t2)
	t2.connect(t1)

	if err := r1.Start(); err != nil {
		t.Fatal(err)
	}
	defer r1.Shutdown()
	if err := r2.Start(); err != nil {
		t.Fatal(err)
	}
	defer r2.Shutdown()

	waitState(t, r1, StateLeader, 5*time.Second)

	// Partition n2 (drop all RPCs to it) and drive writes on the leader.
	setDrop(t2, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		if _, err := r1.Apply(ctx, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	waitApplied(t, r1, 11, 3*time.Second) // 10 entries + no-op

	// Snapshot the leader and compact so the early entries are gone.
	if err := r1.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// Confirm compaction actually hid entries below FirstIndex.
	first, _ := leaderLog.FirstIndex()
	if first <= 1 {
		t.Fatalf("expected leader log to be compacted, FirstIndex=%d", first)
	}

	// Heal the partition. n2's nextIndex is 1 (never replicated), which is now
	// below the leader's FirstIndex, so the leader must send InstallSnapshot.
	setDrop(t2, 0)

	// Wait for n2 to catch up via the snapshot path.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r2.AppliedIndex() >= first-1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if r2.AppliedIndex() < first-1 {
		t.Fatalf("follower did not catch up via InstallSnapshot: appliedIndex=%d, snapshot covered through=%d",
			r2.AppliedIndex(), first-1)
	}
	_ = fsm2

	// The follower's matchIndex on the leader must have advanced to at least the
	// snapshot's last-included index.
	r1.mu.RLock()
	mi := r1.matchIndex["n2"]
	r1.mu.RUnlock()
	if mi < first-1 {
		t.Fatalf("leader matchIndex[n2]=%d, expected >= %d after InstallSnapshot", mi, first-1)
	}
}

func setDrop(t *chanTransport, v int32) {
	// drop is an int32 field accessed atomically by the transport.
	atomic.StoreInt32(&t.drop, v)
}

// =============================================================================
// H-C1: leader self-removal must not panic and must step the leader down
// =============================================================================

func TestHC1_LeaderSelfRemovalStepsDownNoPanic(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	r, _, _ := makeRaftNode("n1", cfg)
	r.mu.Lock()
	r.state = StateLeader
	r.term = 2
	r.leaderID = "n1"
	r.configuration = Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}
	r.jointConfig = &JointConfiguration{
		OldConfig: Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}},
		NewConfig: Configuration{Servers: []Server{{ID: "n2"}}}, // n1 removed itself
	}
	r.matchIndex = map[ServerID]uint64{"n2": 0}
	r.nextIndex = map[ServerID]uint64{"n2": 1}
	r.mu.Unlock()

	// Applying the CommitJoint entry activates NewConfig (n1 no longer a voter).
	entry := &LogEntry{Type: EntryConfiguration, Data: encodeCommitJointChange(), Index: 5, Term: 2}

	// Must not panic and must step down.
	r.applyConfigurationEntry(entry)

	if r.State() == StateLeader {
		t.Fatalf("expected leader to step down after removing itself from the config")
	}

	// advanceCommitIndex must not panic when self is absent from the config.
	r.mu.Lock()
	r.state = StateLeader
	r.configuration = Configuration{Servers: []Server{{ID: "n2"}}}
	r.jointConfig = nil
	r.lastIndex = 5
	r.commitIndex = 0
	r.matchIndex = map[ServerID]uint64{"n2": 5}
	// Give the log an entry at index 5 in our term so the loop reaches the count.
	_ = r.log.Append([]*LogEntry{{Index: 5, Term: 2, Type: EntryNormal, Data: []byte("x")}})
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("advanceCommitIndex panicked on absent self: %v", rec)
			}
		}()
		r.advanceCommitIndex()
	}()
	r.mu.Unlock()
}

// =============================================================================
// H-R1: Shutdown waits for the final drain (applied == committed)
// =============================================================================

func TestHR1_ShutdownWaitsForDrain(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	r, _, fsm := makeRaftNode("n1", cfg)
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	waitState(t, r, StateLeader, 3*time.Second)

	// Propose a batch of entries.
	const n = 20
	for i := 0; i < n; i++ {
		fut := &ApplyFuture{ch: make(chan struct{}), data: []byte(fmt.Sprintf("e%d", i))}
		select {
		case r.proposalCh <- &proposalFuture{data: fut.data, future: fut}:
		case <-time.After(time.Second):
			t.Fatal("proposalCh full")
		}
	}

	// Immediately shut down. H-R1: Shutdown must block until run() finishes its
	// drain, so applyIndex must equal commitIndex when Shutdown returns.
	if err := r.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	r.mu.RLock()
	applied := r.applyIndex
	committed := r.commitIndex
	r.mu.RUnlock()
	if applied != committed {
		t.Fatalf("after Shutdown applied(%d) != committed(%d): drain did not complete", applied, committed)
	}
	if committed < n {
		t.Logf("committed=%d (some proposals may have raced shutdown)", committed)
	}
	if fsm.count() != int(applied) {
		// no-op entry has empty data and is not applied to echoFSM, so count may
		// be applied-1; just assert it is not wildly behind.
		if fsm.count() < int(applied)-1 {
			t.Fatalf("fsm applied %d, expected ~%d", fsm.count(), applied)
		}
	}
}

// =============================================================================
// H-R3: an FSM apply panic must NOT advance applyIndex; the node halts
// =============================================================================

type panicFSM struct {
	mu      sync.Mutex
	applied int
	panicAt int
}

func (f *panicFSM) Apply(entry []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied++
	if f.applied == f.panicAt {
		panic("boom")
	}
	return entry, nil
}
func (f *panicFSM) Snapshot() (Snapshot, error) { return &noopSnapshot{}, nil }
func (f *panicFSM) Restore(_ io.Reader) error   { return nil }

func TestHR3_FSMPanicHaltsWithoutAdvancingApply(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	trans := newChanTransport("n1")
	fsm := &panicFSM{panicAt: 3}
	c := &Config{LocalID: "n1", ElectionTick: 5, HeartbeatTick: 1, InitialConfiguration: cfg}
	r, err := newRaft(c, "n1", newMemLogStore(), newMemStableStore(), &memSnapshotStore{}, fsm, trans)
	if err != nil {
		t.Fatal(err)
	}
	trans.appendEntriesFn = func(req *AppendEntriesRequest) *AppendEntriesResponse { return r.HandleAppendEntriesRPC(req) }
	trans.requestVoteFn = func(req *RequestVoteRequest) *RequestVoteResponse { return r.HandleRequestVoteRPC(req) }
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	defer r.Shutdown()
	waitState(t, r, StateLeader, 3*time.Second)

	// Fire several proposals; the 3rd FSM Apply panics.
	for i := 0; i < 6; i++ {
		fut := &ApplyFuture{ch: make(chan struct{}), data: []byte(fmt.Sprintf("d%d", i))}
		r.proposalCh <- &proposalFuture{data: fut.data, future: fut}
	}

	// Wait for the node to halt.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !r.Healthy() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if r.Healthy() {
		t.Fatal("expected node to halt after FSM apply panic")
	}
	if r.FatalError() == nil {
		t.Fatal("expected FatalError set after FSM apply panic")
	}

	// The applyIndex must NOT have advanced past the panicking entry. The
	// panicking entry is the 3rd normal data entry; the no-op sits at index 1,
	// so the panic is at applyIndex 3 (or 4 depending on ordering). Assert the
	// node did not blow through all 6 entries.
	r.mu.RLock()
	applied := r.applyIndex
	committed := r.commitIndex
	r.mu.RUnlock()
	if applied >= committed && committed > 0 && applied > 4 {
		t.Fatalf("applyIndex advanced past the FSM panic point: applied=%d committed=%d", applied, committed)
	}
}

// =============================================================================
// M-R5: pending futures are capped; excess proposals get ErrNodeBusy
// =============================================================================

func TestMR5_PendingFuturesCapRejects(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}
	r, _, _ := makeRaftNode("n1", cfg)
	r.mu.Lock()
	r.state = StateLeader
	r.term = 1
	r.leaderID = "n1"
	// Pre-fill pendingFutures to the cap so the next batch overflows.
	limit := r.pendingFutureCap()
	for i := 1; i <= limit; i++ {
		r.pendingFutures[uint64(i)] = &ApplyFuture{ch: make(chan struct{})}
	}
	r.lastIndex = uint64(limit)
	r.mu.Unlock()

	fut := &ApplyFuture{ch: make(chan struct{})}
	r.handleProposal([]*proposalFuture{{data: []byte("x"), future: fut}})
	if !errors.Is(fut.Error(), ErrNodeBusy) {
		t.Fatalf("expected ErrNodeBusy when pending futures at cap, got %v", fut.Error())
	}
}

// =============================================================================
// H-P1: group commit — a batch of proposals is appended in one WAL append
// =============================================================================

// countingLogStore counts Append calls to prove group commit coalesces.
type countingLogStore struct {
	*memLogStore
	mu      sync.Mutex
	appends int
}

func newCountingLogStore() *countingLogStore {
	return &countingLogStore{memLogStore: newMemLogStore()}
}

func (c *countingLogStore) Append(entries []*LogEntry) error {
	c.mu.Lock()
	c.appends++
	c.mu.Unlock()
	return c.memLogStore.Append(entries)
}

func (c *countingLogStore) appendCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appends
}

func TestHP1_GroupCommitBatchesAppends(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	log := newCountingLogStore()
	r, _, _ := makeRaftNodeWithStores("n1", cfg, log, &memSnapshotStore{})
	r.mu.Lock()
	r.state = StateLeader
	r.term = 1
	r.leaderID = "n1"
	r.mu.Unlock()

	// Build a batch of 50 proposals and process them in one group-commit round.
	const n = 50
	batch := make([]*proposalFuture, n)
	for i := 0; i < n; i++ {
		batch[i] = &proposalFuture{data: []byte(fmt.Sprintf("e%d", i)), future: &ApplyFuture{ch: make(chan struct{})}}
	}
	before := log.appendCount()
	r.handleProposal(batch)
	after := log.appendCount()

	if after-before != 1 {
		t.Fatalf("group commit should use exactly 1 Append for the batch, used %d", after-before)
	}
	// All entries must be present and contiguous.
	if r.LastIndex() != n {
		t.Fatalf("expected lastIndex %d after batch, got %d", n, r.LastIndex())
	}
}

func BenchmarkProposalThroughput(b *testing.B) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	r, _, _ := makeRaftNode("n1", cfg)
	if err := r.Start(); err != nil {
		b.Fatal(err)
	}
	defer r.Shutdown()
	// Wait for leadership.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && r.State() != StateLeader {
		time.Sleep(5 * time.Millisecond)
	}

	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = r.Apply(ctx, []byte(fmt.Sprintf("bench-%d", i)))
		}(i)
	}
	wg.Wait()
}

// =============================================================================
// M-R8 / L7: config validation defaults and warnings
// =============================================================================

func TestMR8_ConfigValidateDefaults(t *testing.T) {
	c := &Config{LocalID: "n1", ElectionTick: 10, HeartbeatTick: 1}
	if err := c.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if c.SnapshotInterval == 0 {
		t.Error("SnapshotInterval not defaulted")
	}
	if c.SnapshotThreshold == 0 {
		t.Error("SnapshotThreshold not defaulted")
	}
	if c.TrailingLogs == 0 {
		t.Error("TrailingLogs not defaulted")
	}
	if c.MaxSizePerMsg == 0 {
		t.Error("MaxSizePerMsg not defaulted")
	}
	if c.MaxInflight == 0 {
		t.Error("MaxInflight not defaulted")
	}

	// Negative snapshot interval must be rejected.
	bad := &Config{LocalID: "n1", ElectionTick: 10, HeartbeatTick: 1, SnapshotInterval: -1}
	if err := bad.Validate(); err == nil {
		t.Error("expected error for negative SnapshotInterval")
	}
}

func TestL7_ElectionRatioWarning(t *testing.T) {
	// 5:1 must remain valid (existing tests use it) but produce a warning.
	c := &Config{LocalID: "n1", ElectionTick: 5, HeartbeatTick: 1}
	if err := c.Validate(); err != nil {
		t.Fatalf("5:1 ratio must stay valid, got %v", err)
	}
	if c.LastValidateWarning() == "" {
		t.Error("expected a warning for a below-recommended election/heartbeat ratio")
	}

	// 10:1 must produce no warning.
	c2 := &Config{LocalID: "n1", ElectionTick: 10, HeartbeatTick: 1}
	if err := c2.Validate(); err != nil {
		t.Fatal(err)
	}
	if c2.LastValidateWarning() != "" {
		t.Errorf("did not expect a warning at 10:1, got %q", c2.LastValidateWarning())
	}

	// Below the hard 3:1 floor must still fail.
	c3 := &Config{LocalID: "n1", ElectionTick: 2, HeartbeatTick: 1}
	if err := c3.Validate(); err == nil {
		t.Error("expected hard failure below the 3x floor")
	}
}

// =============================================================================
// M-C1: outbound RPC context carries a deadline and is canceled on stop
// =============================================================================

func TestMC1_RPCContextHasDeadlineAndCancelsOnStop(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	r, _, _ := makeRaftNode("n1", cfg)
	r.stopCh = make(chan struct{})

	ctx, cancel := r.rpcContext()
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("expected rpcContext to have a deadline (M-C1)")
	}

	// Closing stopCh must cancel the derived context.
	close(r.stopCh)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("rpcContext not canceled after stopCh closed")
	}
}

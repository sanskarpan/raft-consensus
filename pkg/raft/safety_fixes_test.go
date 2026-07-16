package raft

import (
	"context"
	"errors"
	"testing"
)

// metaOnlySnapStore is a SnapshotStore double that only serves metadata via
// List — used to simulate a restart where the log was compacted but a snapshot
// exists on disk.
type metaOnlySnapStore struct{ metas []*SnapshotMeta }

func (m *metaOnlySnapStore) Create(SnapshotVersion, uint64, uint64, Configuration) (SnapshotSink, error) {
	return nil, errors.New("not implemented")
}
func (m *metaOnlySnapStore) Open(string) (Snapshot, *SnapshotMeta, error) {
	return nil, nil, errors.New("not implemented")
}
func (m *metaOnlySnapStore) List() ([]*SnapshotMeta, error) { return m.metas, nil }
func (m *metaOnlySnapStore) Delete(string) error           { return nil }

// C5: On restart with a compacted (empty) log, snapshot metadata must
// initialize lastIndex/lastTerm/applyIndex/commitIndex; otherwise the node
// believes it has an empty log (lastTerm 0) and cannot win elections / re-applies.
func TestLoadSnapshotStateInitializesFromSnapshotOnRestart(t *testing.T) {
	cfg := &Config{
		LocalID:              "n1",
		ElectionTick:         5,
		HeartbeatTick:        1,
		InitialConfiguration: Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}},
	}
	snaps := &metaOnlySnapStore{metas: []*SnapshotMeta{{ID: "7-42", Index: 42, Term: 7}}}
	r, err := newRaft(cfg, "n1", newMemLogStore(), newMemStableStore(), snaps, &echoFSM{}, newChanTransport("n1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadSnapshotState(); err != nil {
		t.Fatal(err)
	}

	r.mu.Lock()
	li, lt, ai, ci := r.lastIndex, r.lastTerm, r.applyIndex, r.commitIndex
	r.mu.Unlock()
	if li != 42 || lt != 7 {
		t.Fatalf("lastIndex/lastTerm = %d/%d, want 42/7", li, lt)
	}
	if ai < 42 {
		t.Fatalf("applyIndex=%d, want >= 42", ai)
	}
	if ci < 42 {
		t.Fatalf("commitIndex=%d, want >= 42", ci)
	}
}

// C6: During joint consensus a candidate must win a majority in BOTH the old and
// new configurations. A majority in only one config must not win (that would
// permit two leaders in the same term across the transition).
func TestElectionRequiresBothMajoritiesInJointConsensus(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}
	r, _, _ := makeRaftNode("n1", cfg)

	old := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}
	newc := Configuration{Servers: []Server{{ID: "n3"}, {ID: "n4"}, {ID: "n5"}}}

	r.mu.Lock()
	r.jointConfig = &JointConfiguration{OldConfig: old, NewConfig: newc}

	// Votes from an old-config majority only: {n1, n2}.
	r.votes = map[ServerID]bool{"n1": true, "n2": true}
	oldOnly := r.hasVoteQuorum(r.votes)

	// Add votes making a new-config majority too: {n3, n4}.
	r.votes["n3"] = true
	r.votes["n4"] = true
	both := r.hasVoteQuorum(r.votes)
	r.mu.Unlock()

	if oldOnly {
		t.Fatal("won election with only an old-config majority during joint consensus (two-leader risk)")
	}
	if !both {
		t.Fatal("failed to win despite a majority in both configs")
	}
}

// C7: Raft permits at most one outstanding configuration change. A second
// membership change requested before the first commits must be rejected.
func TestRejectsConcurrentConfigChange(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}
	r, _, _ := makeRaftNode("n1", cfg)

	r.mu.Lock()
	r.state = StateLeader
	r.term = 1
	r.configuration = cfg
	r.mu.Unlock()

	if err := r.AddServer(context.Background(), "n4", "addr4"); err != nil {
		t.Fatalf("first AddServer should succeed: %v", err)
	}
	// The first change is appended but not yet committed. A second must fail.
	if err := r.RemoveServer(context.Background(), "n2"); !errors.Is(err, ErrConfigChangeInProgress) {
		t.Fatalf("second config change: got err=%v, want ErrConfigChangeInProgress", err)
	}

	// Once the first config entry commits, a new change is allowed again.
	r.mu.Lock()
	r.commitIndex = r.pendingConfigIndex
	r.mu.Unlock()
	if err := r.RemoveServer(context.Background(), "n2"); err != nil {
		t.Fatalf("config change after commit should succeed: %v", err)
	}
}

// C3: A stale/duplicate AppendEntries that conflicts at an index <= commitIndex
// must NOT truncate committed entries. Doing so violates State Machine Safety
// (a committed, applied command would be lost/overwritten).
func TestAppendEntriesDoesNotTruncateCommittedEntries(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}
	r, _, _ := makeRaftNode("n1", cfg)

	r.mu.Lock()
	r.state = StateFollower
	r.term = 1
	r.mu.Unlock()

	// Establish committed log [1,2,3] at term 1.
	setup := &AppendEntriesRequest{
		Term: 1, LeaderID: "n2", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*LogEntry{
			{Term: 1, Index: 1, Type: EntryNormal, Data: []byte("a")},
			{Term: 1, Index: 2, Type: EntryNormal, Data: []byte("b")},
			{Term: 1, Index: 3, Type: EntryNormal, Data: []byte("c")},
		},
		LeaderCommit: 3,
	}
	if resp := r.HandleAppendEntriesRPC(setup); !resp.Success {
		t.Fatal("setup append failed")
	}
	r.mu.Lock()
	ci := r.commitIndex
	r.mu.Unlock()
	if ci != 3 {
		t.Fatalf("commitIndex=%d after setup, want 3", ci)
	}

	// A delayed/duplicate AppendEntries carries a CONFLICTING entry at the
	// committed index 2 (different term). It must be rejected without deleting
	// the committed entries.
	stale := &AppendEntriesRequest{
		Term: 1, LeaderID: "n2", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*LogEntry{
			{Term: 2, Index: 2, Type: EntryNormal, Data: []byte("X")},
		},
		LeaderCommit: 3,
	}
	_ = r.HandleAppendEntriesRPC(stale)

	for idx, want := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
		e, err := r.log.Get(idx)
		if err != nil {
			t.Fatalf("committed entry %d was deleted: %v", idx, err)
		}
		if string(e.Data) != want {
			t.Fatalf("committed entry %d overwritten: data=%q want %q", idx, e.Data, want)
		}
	}
	if r.LastIndex() != 3 {
		t.Fatalf("lastIndex=%d, want 3 (committed suffix must survive)", r.LastIndex())
	}
}

// C4: A delayed/duplicate InstallSnapshot with a LastIncludedIndex below our
// current commitIndex must not roll commitIndex/applyIndex backward (which would
// re-apply already-applied entries — State Machine Safety violation).
func TestInstallSnapshotIsMonotonic(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}
	r, _, _ := makeRaftNode("n1", cfg)

	r.mu.Lock()
	r.state = StateFollower
	r.term = 1
	r.commitIndex = 100
	r.applyIndex = 100
	r.lastIndex = 100
	r.lastTerm = 1
	r.mu.Unlock()

	req := &InstallSnapshotRequest{
		Term: 1, LeaderID: "n2",
		LastIncludedIndex: 50, LastIncludedTerm: 1,
		Done: true, Data: []byte("stale-snapshot"),
	}
	r.HandleInstallSnapshotRPC(req)

	r.mu.Lock()
	ci, ai := r.commitIndex, r.applyIndex
	r.mu.Unlock()
	if ci < 100 {
		t.Fatalf("commitIndex regressed to %d (want >= 100)", ci)
	}
	if ai < 100 {
		t.Fatalf("applyIndex regressed to %d (want >= 100)", ai)
	}
}

// C5: After installing a snapshot that conflicts with the existing log, the
// stale conflicting entries must be discarded so the log does not still serve an
// entry that contradicts the snapshot.
func TestInstallSnapshotReconcilesLog(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}
	r, _, _ := makeRaftNode("n1", cfg)

	r.mu.Lock()
	r.state = StateFollower
	r.term = 1
	for i := uint64(1); i <= 5; i++ {
		_ = r.log.Append([]*LogEntry{{Term: 1, Index: i, Type: EntryNormal, Data: []byte{byte(i)}}})
	}
	r.lastIndex = 5
	r.lastTerm = 1
	r.commitIndex = 0
	r.applyIndex = 0
	r.mu.Unlock()

	// Snapshot at index 3, term 2 — conflicts with our term-1 log.
	req := &InstallSnapshotRequest{
		Term: 2, LeaderID: "n2",
		LastIncludedIndex: 3, LastIncludedTerm: 2,
		Done: true, Data: []byte("snap"),
	}
	r.HandleInstallSnapshotRPC(req)

	r.mu.Lock()
	li := r.lastIndex
	r.mu.Unlock()
	if li != 3 {
		t.Fatalf("lastIndex=%d after install, want 3", li)
	}
	if e, err := r.log.Get(3); err == nil && e.Term != req.LastIncludedTerm {
		t.Fatalf("stale conflicting log entry 3 (term %d) survived snapshot install", e.Term)
	}
}

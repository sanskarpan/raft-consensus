package raft

import (
	"context"
	"testing"
	"time"
)

// M4: Heartbeat-confirmed ReadIndex must NOT serve a read based on acks that
// were recorded BEFORE the read began. With only stale acks and no fresh
// heartbeat round (the node's run loop is not started here), ReadIndex must
// block until the context expires rather than returning a (possibly stale) index.
func TestReadIndexRequiresFreshConfirmation(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}
	r, _, _ := makeRaftNode("n1", cfg)

	r.mu.Lock()
	r.state = StateLeader
	r.term = 1
	r.configuration = cfg
	r.commitIndex = 7
	// Stale acks: recorded now, i.e. strictly before ReadIndex's `start`.
	r.heartbeatAcks = map[ServerID]time.Time{
		"n2": time.Now(),
		"n3": time.Now(),
	}
	r.stopCh = make(chan struct{})
	r.mu.Unlock()

	// Ensure `start` inside ReadIndex is strictly after the acks above.
	time.Sleep(2 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := r.ReadIndex(ctx); err == nil {
		t.Fatal("ReadIndex returned success on stale (pre-start) acks; a linearizable read must require fresh post-start quorum confirmation")
	}
}

// M4: Once a quorum of voters acknowledges a heartbeat AFTER the read begins,
// ReadIndex returns the captured commit index.
func TestReadIndexReturnsOnFreshQuorum(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}
	r, _, _ := makeRaftNode("n1", cfg)

	r.mu.Lock()
	r.state = StateLeader
	r.term = 1
	r.configuration = cfg
	r.commitIndex = 9
	r.heartbeatAcks = map[ServerID]time.Time{}
	r.stopCh = make(chan struct{})
	r.mu.Unlock()

	// Simulate a follower acking a heartbeat shortly AFTER the read starts.
	go func() {
		time.Sleep(20 * time.Millisecond)
		r.mu.Lock()
		r.heartbeatAcks["n2"] = time.Now()
		r.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	idx, err := r.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex should succeed once a voter acks after start: %v", err)
	}
	if idx != 9 {
		t.Fatalf("ReadIndex = %d, want 9 (commit index at read start)", idx)
	}
}

// M4: single-voter clusters serve reads immediately (self is the quorum).
func TestReadIndexSingleNodeImmediate(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	r, _, _ := makeRaftNode("n1", cfg)

	r.mu.Lock()
	r.state = StateLeader
	r.term = 1
	r.configuration = cfg
	r.commitIndex = 3
	r.stopCh = make(chan struct{})
	r.mu.Unlock()

	idx, err := r.ReadIndex(context.Background())
	if err != nil || idx != 3 {
		t.Fatalf("single-node ReadIndex = (%d,%v), want (3,nil)", idx, err)
	}
}

package raft

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// simNetwork is a deterministic in-process Transport for simulation tests.
// Messages are delivered (or dropped) according to a seeded schedule.
// Partitions can be injected and healed at any time.
type simNetwork struct {
	mu sync.Mutex

	localID ServerID

	// nodes maps node IDs to their raft instances for direct in-process delivery.
	nodes map[ServerID]*raft

	// seed is the original seed for reproducibility logging.
	seed int64
	rng  *rand.Rand

	// latency is the simulated per-hop latency (informational; the synchronous
	// simulation advances the simClock by this amount on each message delivery).
	latency time.Duration

	// partitioned holds (src,dst) pairs where messages are dropped.
	// A partition is bidirectional: both (a,b) and (b,a) are inserted.
	partitioned map[[2]ServerID]bool

	clock *simClock

	// inflight tracks the number of message deliveries currently executing.
	// Uses atomic.Int64 (not sync.WaitGroup) so Drain() can poll without
	// blocking forever if Add/Done calls are imbalanced.
	inflight atomic.Int64
}

// newSimNetwork creates a deterministic simNetwork with the given seed,
// simClock, and per-hop latency.
func newSimNetwork(seed int64, clock *simClock, latency time.Duration) *simNetwork {
	return &simNetwork{
		nodes:       make(map[ServerID]*raft),
		seed:        seed,
		rng:         rand.New(rand.NewSource(seed)),
		latency:     latency,
		partitioned: make(map[[2]ServerID]bool),
		clock:       clock,
	}
}

// RegisterNode registers a raft node with the network so it can receive RPCs.
func (n *simNetwork) RegisterNode(id ServerID, r *raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = r
}

// Partition drops all messages between a and b (bidirectional).
func (n *simNetwork) Partition(a, b ServerID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned[[2]ServerID{a, b}] = true
	n.partitioned[[2]ServerID{b, a}] = true
}

// Heal removes the partition between a and b (bidirectional).
func (n *simNetwork) Heal(a, b ServerID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.partitioned, [2]ServerID{a, b})
	delete(n.partitioned, [2]ServerID{b, a})
}

// isPartitioned reports whether messages from src to dst are currently dropped.
// Caller must hold n.mu.
func (n *simNetwork) isPartitioned(src, dst ServerID) bool {
	return n.partitioned[[2]ServerID{src, dst}]
}

// InFlight returns the number of currently executing message deliveries.
func (n *simNetwork) InFlight() int64 {
	return n.inflight.Load()
}

// Drain blocks until all in-flight message deliveries have completed.
// It uses runtime.Gosched() to yield the scheduler so that the delivering
// goroutines get CPU time to finish.
func (n *simNetwork) Drain() {
	for n.inflight.Load() > 0 {
		runtime.Gosched()
	}
}

// deliver runs fn (which calls a node's RPC handler) while holding the
// inflight counter so Drain() can observe it. The caller is responsible for
// any partition/existence checks before calling deliver.
func (n *simNetwork) deliver(fn func()) {
	n.inflight.Add(1)
	defer n.inflight.Add(-1)
	fn()
}

// AppendEntries implements Transport. It delivers directly to the target node's
// handler if not partitioned; otherwise returns an error.
func (n *simNetwork) AppendEntries(_ context.Context, target ServerID, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	n.mu.Lock()
	src := n.localID
	partitioned := n.isPartitioned(src, target)
	node, ok := n.nodes[target]
	n.mu.Unlock()

	if partitioned {
		return nil, fmt.Errorf("simNetwork: partition between %s and %s", src, target)
	}
	if !ok {
		return nil, fmt.Errorf("simNetwork: unknown target %s", target)
	}

	// Advance simulated latency.
	if n.latency > 0 && n.clock != nil {
		n.clock.Advance(n.latency)
	}

	var resp *AppendEntriesResponse
	n.deliver(func() {
		resp = node.HandleAppendEntriesRPC(req)
	})
	return resp, nil
}

// RequestVote implements Transport.
func (n *simNetwork) RequestVote(_ context.Context, target ServerID, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	n.mu.Lock()
	src := n.localID
	partitioned := n.isPartitioned(src, target)
	node, ok := n.nodes[target]
	n.mu.Unlock()

	if partitioned {
		return nil, fmt.Errorf("simNetwork: partition between %s and %s", src, target)
	}
	if !ok {
		return nil, fmt.Errorf("simNetwork: unknown target %s", target)
	}

	if n.latency > 0 && n.clock != nil {
		n.clock.Advance(n.latency)
	}

	var resp *RequestVoteResponse
	n.deliver(func() {
		resp = node.HandleRequestVoteRPC(req)
	})
	return resp, nil
}

// InstallSnapshot implements Transport.
func (n *simNetwork) InstallSnapshot(_ context.Context, target ServerID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	n.mu.Lock()
	src := n.localID
	partitioned := n.isPartitioned(src, target)
	node, ok := n.nodes[target]
	n.mu.Unlock()

	if partitioned {
		return nil, fmt.Errorf("simNetwork: partition between %s and %s", src, target)
	}
	if !ok {
		return nil, fmt.Errorf("simNetwork: unknown target %s", target)
	}

	if n.latency > 0 && n.clock != nil {
		n.clock.Advance(n.latency)
	}

	var resp *InstallSnapshotResponse
	n.deliver(func() {
		resp = node.HandleInstallSnapshotRPC(req)
	})
	return resp, nil
}

// TimeoutNow implements Transport.
func (n *simNetwork) TimeoutNow(_ context.Context, target ServerID) error {
	n.mu.Lock()
	src := n.localID
	partitioned := n.isPartitioned(src, target)
	n.mu.Unlock()

	if partitioned {
		return fmt.Errorf("simNetwork: partition between %s and %s", src, target)
	}
	return nil
}

// SetLocalID implements Transport. In the simNetwork the localID is the
// identity used for partition checks on outgoing messages.
func (n *simNetwork) SetLocalID(id ServerID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.localID = id
}

// Close implements Transport (no-op for in-process network).
func (n *simNetwork) Close() error { return nil }

// nodeTransport wraps simNetwork and presents it as a per-node Transport
// (with a fixed localID for partition lookups).
type nodeTransport struct {
	net *simNetwork
	id  ServerID
}

func (t *nodeTransport) AppendEntries(ctx context.Context, target ServerID, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	t.net.mu.Lock()
	t.net.localID = t.id
	t.net.mu.Unlock()
	return t.net.AppendEntries(ctx, target, req)
}

func (t *nodeTransport) RequestVote(ctx context.Context, target ServerID, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	t.net.mu.Lock()
	t.net.localID = t.id
	t.net.mu.Unlock()
	return t.net.RequestVote(ctx, target, req)
}

func (t *nodeTransport) InstallSnapshot(ctx context.Context, target ServerID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	t.net.mu.Lock()
	t.net.localID = t.id
	t.net.mu.Unlock()
	return t.net.InstallSnapshot(ctx, target, req)
}

func (t *nodeTransport) TimeoutNow(ctx context.Context, target ServerID) error {
	t.net.mu.Lock()
	t.net.localID = t.id
	t.net.mu.Unlock()
	return t.net.TimeoutNow(ctx, target)
}

func (t *nodeTransport) SetLocalID(id ServerID) { t.id = id }
func (t *nodeTransport) Close() error            { return nil }

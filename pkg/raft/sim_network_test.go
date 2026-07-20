package raft

import (
	"context"
	"testing"
	"time"
)

// TestSimNetworkDeliversMessages verifies that AppendEntries is delivered to a
// registered node and returns a valid response.
func TestSimNetworkDeliversMessages(t *testing.T) {
	clk := newSimClock(epoch)
	net := newSimNetwork(42, clk, 0)

	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}

	r1, _, _ := makeRaftNodeWithClock("n1", cfg, clk)
	r2, _, _ := makeRaftNodeWithClock("n2", cfg, clk)

	net.RegisterNode("n1", r1)
	net.RegisterNode("n2", r2)

	// Send an AppendEntries from n1 to n2 (unauthenticated, just checking delivery).
	req := &AppendEntriesRequest{
		Term:     0,
		LeaderID: "n1",
	}
	resp, err := net.AppendEntries(context.Background(), "n2", req)
	if err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}
	if resp == nil {
		t.Fatal("got nil response")
	}
}

// TestSimNetworkPartitionDrops verifies that after Partition(a, b) messages
// between those two nodes return an error.
func TestSimNetworkPartitionDrops(t *testing.T) {
	clk := newSimClock(epoch)
	net := newSimNetwork(42, clk, 0)

	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}
	r1, _, _ := makeRaftNodeWithClock("n1", cfg, clk)
	r2, _, _ := makeRaftNodeWithClock("n2", cfg, clk)

	net.RegisterNode("n1", r1)
	net.RegisterNode("n2", r2)
	net.SetLocalID("n1")

	net.Partition("n1", "n2")

	req := &AppendEntriesRequest{Term: 0, LeaderID: "n1"}
	_, err := net.AppendEntries(context.Background(), "n2", req)
	if err == nil {
		t.Fatal("expected error after partition, got nil")
	}
}

// TestSimNetworkHealRestores verifies that healing a partition allows messages
// to flow again.
func TestSimNetworkHealRestores(t *testing.T) {
	clk := newSimClock(epoch)
	net := newSimNetwork(42, clk, 0)

	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}
	r1, _, _ := makeRaftNodeWithClock("n1", cfg, clk)
	r2, _, _ := makeRaftNodeWithClock("n2", cfg, clk)

	net.RegisterNode("n1", r1)
	net.RegisterNode("n2", r2)
	net.SetLocalID("n1")

	net.Partition("n1", "n2")
	net.Heal("n1", "n2")

	req := &AppendEntriesRequest{Term: 0, LeaderID: "n1"}
	resp, err := net.AppendEntries(context.Background(), "n2", req)
	if err != nil {
		t.Fatalf("AppendEntries after heal failed: %v", err)
	}
	if resp == nil {
		t.Fatal("got nil response after heal")
	}
}

// makeRaftNodeWithClock is like makeRaftNode but injects a simClock.
func makeRaftNodeWithClock(id string, config Configuration, clk *simClock) (*raft, *chanTransport, *echoFSM) {
	trans := newChanTransport(ServerID(id))
	fsm := &echoFSM{}
	cfg := &Config{
		LocalID:              ServerID(id),
		ElectionTick:         5,
		HeartbeatTick:        1,
		InitialConfiguration: config,
		Clock:                clk,
		NewTicker:            clk.NewTicker,
	}
	r, err := newRaft(cfg, ServerID(id),
		newMemLogStore(),
		newMemStableStore(),
		&memSnapshotStore{},
		fsm,
		trans,
	)
	if err != nil {
		panic("newRaft: " + err.Error())
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

// TestSimNetworkLatency verifies that the simNetwork can be configured with a
// base latency (added to simClock) and messages are still delivered after that
// latency is advanced.
func TestSimNetworkLatency(t *testing.T) {
	clk := newSimClock(epoch)
	const latency = 5 * time.Millisecond
	net := newSimNetwork(42, clk, latency)

	cfg := Configuration{Servers: []Server{{ID: "n1"}, {ID: "n2"}}}
	r1, _, _ := makeRaftNodeWithClock("n1", cfg, clk)
	r2, _, _ := makeRaftNodeWithClock("n2", cfg, clk)

	net.RegisterNode("n1", r1)
	net.RegisterNode("n2", r2)
	net.SetLocalID("n1")

	// With latency=5ms and no clock advance, messages are still delivered
	// synchronously in simNetwork (synchronous simulation mode).
	req := &AppendEntriesRequest{Term: 0, LeaderID: "n1"}
	resp, err := net.AppendEntries(context.Background(), "n2", req)
	if err != nil {
		t.Fatalf("AppendEntries with latency failed: %v", err)
	}
	if resp == nil {
		t.Fatal("nil response")
	}
}

package raft

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/sanskarpan/raft-consensus/pkg/metrics"
)

func counterValue(t *testing.T, c interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		t.Fatalf("metric write: %v", err)
	}
	if m.Counter != nil {
		return m.Counter.GetValue()
	}
	return 0
}

func histSampleCount(t *testing.T, h interface{ Write(*dto.Metric) error }) uint64 {
	t.Helper()
	m := &dto.Metric{}
	if err := h.Write(m); err != nil {
		t.Fatalf("metric write: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestLeaderChangeAndCommitLatencyMetrics verifies that becoming leader bumps
// raft_leader_changes_total and that a committed proposal records a
// raft_proposal_commit_latency_seconds observation.
func TestLeaderChangeAndCommitLatencyMetrics(t *testing.T) {
	leaderChangesBefore := counterValue(t, metrics.LeaderChangesCounter)
	latencyCountBefore := histSampleCount(t, metrics.ProposalCommitLatencyHistogram)

	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	r, _, _ := makeRaftNode("n1", cfg)
	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()

	waitState(t, r, StateLeader, 5*time.Second)

	if got := counterValue(t, metrics.LeaderChangesCounter); got <= leaderChangesBefore {
		t.Fatalf("leader_changes_total did not increase on election: before=%v after=%v",
			leaderChangesBefore, got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := r.Apply(ctx, []byte("hello")); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got := histSampleCount(t, metrics.ProposalCommitLatencyHistogram); got <= latencyCountBefore {
		t.Fatalf("proposal_commit_latency has no new observation: before=%d after=%d",
			latencyCountBefore, got)
	}
}

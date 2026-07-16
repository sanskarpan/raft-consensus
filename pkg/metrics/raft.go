package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TermGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_term",
		Help: "Current term",
	})

	CommitIndexGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_commit_index",
		Help: "Last committed index",
	})

	AppliedIndexGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_applied_index",
		Help: "Last applied index",
	})

	LeaderIDGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_leader_id",
		Help: "Current leader ID (1 if has leader)",
	})

	ElectionCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_elections_total",
		Help: "Total number of elections",
	})

	VoteGrantedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_votes_granted_total",
		Help: "Total number of votes granted",
	})

	AppendEntriesSentCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "raft_append_entries_sent_total",
			Help: "Total number of AppendEntries messages sent",
		},
		[]string{"target", "success"},
	)

	AppendEntriesLatencyHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raft_append_entries_latency_seconds",
			Help:    "Latency of AppendEntries RPC",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"target"},
	)

	RequestVoteLatencyHistogram = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "raft_request_vote_latency_seconds",
			Help:    "Latency of RequestVote RPC",
			Buckets: prometheus.DefBuckets,
		},
	)

	SnapshotCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_snapshots_total",
		Help: "Total number of snapshots taken",
	})

	SnapshotRestoreCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_snapshots_restored_total",
		Help: "Total number of snapshots restored",
	})

	FSMApplyLatencyHistogram = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "raft_fsm_apply_latency_seconds",
			Help:    "Latency of FSM apply",
			Buckets: prometheus.DefBuckets,
		},
	)

	ReplicationLagGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "raft_replication_lag",
			Help: "Replication lag per follower",
		},
		[]string{"target"},
	)
)

func RecordTerm(term uint64) {
	TermGauge.Set(float64(term))
}

func RecordCommitIndex(index uint64) {
	CommitIndexGauge.Set(float64(index))
}

func RecordAppliedIndex(index uint64) {
	AppliedIndexGauge.Set(float64(index))
}

func RecordLeaderID(hasLeader bool) {
	if hasLeader {
		LeaderIDGauge.Set(1)
	} else {
		LeaderIDGauge.Set(0)
	}
}

func RecordElection() {
	ElectionCounter.Inc()
}

func RecordVoteGranted() {
	VoteGrantedCounter.Inc()
}

func RecordAppendEntriesSent(target string, success bool) {
	AppendEntriesSentCounter.WithLabelValues(target, boolToString(success)).Inc()
}

func RecordAppendEntriesLatency(target string, seconds float64) {
	AppendEntriesLatencyHistogram.WithLabelValues(target).Observe(seconds)
}

func RecordRequestVoteLatency(seconds float64) {
	RequestVoteLatencyHistogram.Observe(seconds)
}

func RecordSnapshot() {
	SnapshotCounter.Inc()
}

func RecordSnapshotRestore() {
	SnapshotRestoreCounter.Inc()
}

func RecordFSMApplyLatency(seconds float64) {
	FSMApplyLatencyHistogram.Observe(seconds)
}

func RecordReplicationLag(target string, lag uint64) {
	ReplicationLagGauge.WithLabelValues(target).Set(float64(lag))
}

func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

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

	// WALFsyncLatencyHistogram records the time spent in each WAL fsync — a
	// classic Raft write-latency source (C-O1).
	WALFsyncLatencyHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "raft_wal_fsync_seconds",
		Help:    "Latency of WAL fsync",
		Buckets: prometheus.DefBuckets,
	})

	// ApplyLagGauge is commitIndex-appliedIndex: how far the FSM trails the log
	// (M-O1). The primary saturation signal for the apply pipeline.
	ApplyLagGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_apply_lag",
		Help: "commitIndex - appliedIndex (entries committed but not yet applied)",
	})

	// ProposalsCounter counts proposals by outcome (M-O1: throughput + errors).
	ProposalsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "raft_proposals_total",
		Help: "Total client proposals by outcome",
	}, []string{"outcome"})

	// RejectionsCounter counts RPC/request rejections by kind (M-O1: error rate).
	RejectionsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "raft_rejections_total",
		Help: "Total rejections by kind (append_entries, vote, not_leader, forward)",
	}, []string{"kind"})

	// InstallSnapshotLatencyHistogram records outbound InstallSnapshot latency (M-O1).
	InstallSnapshotLatencyHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "raft_install_snapshot_latency_seconds",
		Help:    "Latency of outbound InstallSnapshot RPC",
		Buckets: prometheus.DefBuckets,
	})

	// SnapshotSizeBytesGauge exports the byte size of the most recent snapshot
	// transferred through InstallSnapshot (sent by a leader or received by a
	// follower). Consumed by the dashboard's snapshot-size card.
	SnapshotSizeBytesGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_snapshot_size_bytes",
		Help: "Byte size of the most recent snapshot transferred via InstallSnapshot",
	})

	// WatchConnectionsGauge exports the number of open watch (SSE) streams (M-O1).
	WatchConnectionsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_watch_connections",
		Help: "Open watch (SSE) connections",
	})

	// HTTPRequestDuration is a per-route client-request latency histogram for the
	// HTTP API, wired via promhttp.InstrumentHandlerDuration (M-O1).
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "raft_http_request_duration_seconds",
		Help:    "HTTP API request latency by handler and method",
		Buckets: prometheus.DefBuckets,
	}, []string{"handler", "method", "code"})
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

func RecordWALFsync(seconds float64) {
	WALFsyncLatencyHistogram.Observe(seconds)
}

func RecordApplyLag(commitIndex, appliedIndex uint64) {
	if commitIndex >= appliedIndex {
		ApplyLagGauge.Set(float64(commitIndex - appliedIndex))
	}
}

func RecordProposal(outcome string) {
	ProposalsCounter.WithLabelValues(outcome).Inc()
}

func RecordRejection(kind string) {
	RejectionsCounter.WithLabelValues(kind).Inc()
}

func RecordInstallSnapshotLatency(seconds float64) {
	InstallSnapshotLatencyHistogram.Observe(seconds)
}

// RecordSnapshotSize sets the gauge for the most recent snapshot transferred.
func RecordSnapshotSize(bytes int) {
	SnapshotSizeBytesGauge.Set(float64(bytes))
}

func SetWatchConnections(n int) {
	WatchConnectionsGauge.Set(float64(n))
}

func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

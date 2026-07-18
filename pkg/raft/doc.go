// Package raft implements the Raft consensus algorithm (leader election, log
// replication, and safety) with production extensions: pre-vote, heartbeat-
// confirmed ReadIndex reads (§6.4), joint-consensus membership changes,
// learner replicas, leadership transfer, snapshotting/compaction, and an
// optional CheckQuorum leader step-down with disruptive-server vote rejection
// (§4.2.3).
//
// The core type is the unexported raft node driven through the exported
// interfaces in types.go (LogStore, StableStore, SnapshotStore, Transport, FSM).
// Consumers embed it via a higher layer such as cmd/raftd; see the repository
// README and docs/ for usage.
package raft

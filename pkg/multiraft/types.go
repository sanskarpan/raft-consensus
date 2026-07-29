package multiraft

import "github.com/sanskarpan/raft-consensus/pkg/raft"

// GroupID identifies a Raft shard. uint64 is small, cheap to compare, and
// embeds cleanly in error messages and log fields.
type GroupID = uint64

// GroupConfig holds the configuration for a single Raft shard.
type GroupConfig struct {
	// ID is the unique identifier for this group.
	ID GroupID

	// KeyRangeStart is the inclusive lower bound of the key range owned by this
	// group. An empty string means the beginning of the keyspace.
	KeyRangeStart string

	// KeyRangeEnd is the exclusive upper bound of the key range owned by this
	// group. An empty string means +infinity (beyond all keys), following the
	// same convention as etcd range end.
	KeyRangeEnd string

	// DataDir is the base directory for this group's WAL, stable store, and
	// snapshot files. When empty it defaults to
	//   baseDataDir/group-{ID}
	// so each group always gets its own isolated subdirectory.
	DataDir string

	// Members lists the per-group cluster members. Each member's Address is the
	// transport (Raft TCP/gRPC) address for this group, which differs from the
	// top-level Cluster[].Address used by the single-group configuration.
	Members []raft.Server

	// RaftConfig carries group-level Raft tunables. RaftConfig.LocalID must be
	// set to the local node's ServerID for this group.
	// RaftConfig.InitialConfiguration is built from Members automatically inside
	// newGroupState if left empty.
	RaftConfig raft.Config
}

// GroupInfo is the read-only summary of a shard's current state, returned by
// the GET /admin/groups endpoint.
type GroupInfo struct {
	ID            GroupID       `json:"id"`
	KeyRangeStart string        `json:"key_range_start"`
	KeyRangeEnd   string        `json:"key_range_end"`
	IsLeader      bool          `json:"is_leader"`
	LeaderID      raft.ServerID `json:"leader_id"`
	Term          uint64        `json:"term"`
	AppliedIndex  uint64        `json:"applied_index"`
}

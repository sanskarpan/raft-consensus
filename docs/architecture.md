# Architecture

This document describes the design and internal structure of the Go-based Raft consensus implementation at `github.com/raft-consensus`.

## Overview

The implementation provides a production-quality Raft consensus engine with:

- Full leader election with pre-vote support
- Log replication with per-follower progress tracking
- Snapshotting via a pluggable FSM snapshot interface
- Joint-consensus membership changes
- A TCP transport (primary) and a gRPC transport (alternative)
- A built-in in-memory KV store FSM for demonstration
- Prometheus metrics exposure via the HTTP server

The entry point for embedding the engine is `raft.NewRaft(...)`, which returns a `raft.Raft` interface. The server binary (`cmd/raftd`) wraps this interface behind an HTTP API.

---

## Module Structure

```
github.com/raft-consensus
├── cmd/raftd/           Server binary: HTTP API, config loading, signal handling
├── pkg/raft/            Core Raft state machine
│   ├── raft.go          State machine implementation
│   ├── types.go         Public types, interfaces (Raft, FSM, LogStore, …)
│   ├── joint.go         Joint-consensus configuration types and helpers
│   └── snapshot.go      Snapshot restore helpers
├── pkg/storage/         Persistence layer
│   ├── wal.go           Write-Ahead Log (segment files, CRC32, index)
│   └── wal.go (cont.)   StableStore (bbolt), FileSnapshotStore
├── pkg/transport/       Network transports
│   ├── tcp.go           JSON-over-TCP transport
│   └── grpc.go          gRPC transport
├── pkg/fsm/             Finite state machine implementations
│   └── kv.go            In-memory key-value store
├── pkg/metrics/         Prometheus metrics registration
│   └── raft.go          Gauge, counter, histogram definitions
├── pkg/client/          HTTP client library for cluster interaction
│   └── client.go
├── pkg/tracing/         OpenTelemetry tracing helpers
├── proto/               gRPC protobuf definitions
├── tools/testharness/   Process-based integration test harness
├── charts/              Helm chart for Kubernetes deployment
├── scripts/certs/       TLS certificate generation
│   └── generate.sh
└── docs/                Architecture and operations documentation
```

---

## Core State Machine (`pkg/raft/`)

### States and Transitions

The state machine (`raft` struct in `raft.go`) cycles through five states:

| State         | Description                                              |
|---------------|----------------------------------------------------------|
| `StateFollower`   | Default; receives heartbeats and log entries from leader |
| `StateCandidate`  | Starts elections after election timeout expires          |
| `StateLeader`     | Sends heartbeats, accepts client proposals               |
| `StateLearner`    | Non-voting member; receives log replication only         |
| `StateShutdown`   | Terminal state after `Shutdown()` is called              |

Transitions:

```
Follower   --election timeout--> Candidate
Candidate  --quorum votes--> Leader
Candidate  --sees higher term or leader--> Follower
Leader     --sees higher term--> Follower
Any        --Shutdown()--> Shutdown
```

### Key Fields on the `raft` struct

```go
type raft struct {
    localID     ServerID
    state       RaftState
    term        uint64
    votedFor    ServerID

    log         LogStore       // WAL-backed log
    stable      StableStore    // bbolt-backed durable KV
    snapshot    SnapshotStore  // file-based snapshot store
    fsm         FSM            // pluggable state machine

    lastIndex   uint64         // last appended log index
    commitIndex uint64         // highest committed index
    applyIndex  uint64         // highest index applied to FSM

    nextIndex   map[ServerID]uint64  // per-follower: next index to send
    matchIndex  map[ServerID]uint64  // per-follower: highest known replicated index

    pendingFutures map[uint64]*ApplyFuture  // index -> waiting client future
    proposalCh     chan proposalFuture       // client proposals enter via this channel
    electionTicks  int                       // ticks until election timeout
}
```

### Timers

A single `time.Ticker` drives all timer logic inside the `run()` goroutine. The tick interval defaults to the heartbeat period. Timer behaviour per state:

- **Follower/Candidate**: `electionTicks` decrements on each tick. When it reaches zero an election starts. The value is randomised (`electionTick + rand(electionTick)`) to prevent split votes.
- **Leader**: sends heartbeats (AppendEntries with empty entries) to all followers on every heartbeat tick.

### Proposal Pipeline

Client commands flow through `proposalCh`:

1. `Apply(ctx, data)` creates an `ApplyFuture` and sends a `proposalFuture` to `proposalCh`.
2. The `run()` goroutine's `handleProposal()` appends the entry to the log and persists it to the WAL.
3. The leader replicates the entry via `sendAppendEntries()` to all followers.
4. `advanceCommitIndex()` counts `matchIndex` values to find the highest index replicated to a quorum; `commitIndex` is updated accordingly.
5. `applyCommitted()` applies entries from `applyIndex+1` to `commitIndex` to the FSM. Each applied entry resolves the corresponding `ApplyFuture`.

### Commit Advancement

The leader maintains `nextIndex[id]` and `matchIndex[id]` for each peer. After receiving a successful `AppendEntriesResponse`, `matchIndex[id]` is updated and `advanceCommitIndex()` finds the median of all `matchIndex` values (including the leader's own `lastIndex`). A new commit index is accepted only when it corresponds to an entry in the current term (Raft's log-matching safety guarantee).

### Election Protocol

Pre-vote is supported. When `inPreVote` is true the candidate sends `RequestVote` RPCs with `PreVote=true`; it only increments its term and persists `votedFor` once it has gathered a pre-vote quorum. This prevents disruptions from a partitioned node with an inflated term.

---

## Write-Ahead Log (`pkg/storage/wal.go`)

### Segment Files

The WAL is stored as a directory of segment files (`*.wal`). Each segment holds up to 64 MiB of records. When a segment fills, a new one is created with a higher base index. Only the current segment is written; all segments can be read.

Segment files are named by the base log index they start at (e.g., `00000000000000000001.wal`).

### Record Layout

Each WAL record has a fixed 25-byte header followed by a variable-length payload:

```
Offset  Size  Field
0       4     CRC32 checksum (covers bytes [4:end])
4       4     payload length (entryDataLen + 9)
8       1     entry type (0=normal, 1=config, 2=snapshot)
9       8     term (big-endian uint64)
17      8     index (big-endian uint64)
25      N     entry data bytes
```

The constant `recordHeaderSize = 25` defines this layout. CRC32 uses the Castagnoli polynomial.

### In-Memory Index

`WAL` maintains a `logIndex` structure mapping each log index to its byte offset within its segment file. The index is rebuilt on restart by scanning all segment files using `file.Seek(0, io.SeekCurrent)` before each `nextRecord()` call to record the actual offset.

This allows O(1) random reads: `getEntry(idx)` looks up the segment and offset in the index, seeks to that offset, and reads the record.

### Compaction

`Compact(index)` truncates the WAL by deleting all segments whose entries are fully below `index`. It uses internal `firstIndexLocked()` and `deleteRangeLocked()` helpers to avoid deadlocking on the WAL mutex.

---

## Storage Layer (`pkg/storage/`)

### StableStore (bbolt)

`StableStore` wraps a [bbolt](https://github.com/etcd-io/bbolt) database (`stable.db`). It stores durable Raft metadata:

| Key                    | Value                              |
|------------------------|------------------------------------|
| `raft_term`            | Current term (uint64, big-endian)  |
| `raft_voted_for`       | Voted-for server ID (string)       |
| `raft_last_snapshot`   | Last snapshot ID (string)          |
| `raft_last_index`      | Last log index (uint64)            |
| `raft_config`          | Serialised cluster configuration   |

All writes use `bolt.DB.Update()` (serialisable transactions). Reads use `bolt.DB.View()`.

### FileSnapshotStore

`FileSnapshotStore` stores snapshots as files under the data directory. Each snapshot is a directory named by its ID (a combination of index and term). The store retains up to N snapshots (default 2) and automatically prunes older ones.

Snapshot creation:
1. `Create()` returns a `SnapshotSink` wrapping a temporary file.
2. The FSM's `Snapshot()` method writes its serialised state to the sink.
3. `Close()` on the sink renames the temporary file to the final snapshot path and triggers pruning via `pruneLocked()`.

Snapshot restoration:
1. `restoreSnapshotData()` writes incoming snapshot bytes to a new snapshot store entry.
2. `fsm.Restore()` is called with a reader over the snapshot file.

---

## Transport Layer (`pkg/transport/`)

### TCP Transport (`tcp.go`)

The primary transport. It establishes persistent TCP connections to peers and encodes Raft RPCs as newline-delimited JSON messages.

Each message has an envelope:

```json
{"type": "AppendEntries", "payload": {...}}
```

Supported message types: `AppendEntries`, `RequestVote`, `InstallSnapshot`, `TimeoutNow`.

The `TCPTransport` uses a `Handler` interface:

```go
type Handler interface {
    HandleAppendEntries(req *AppendEntriesReq) *AppendEntriesResp
    HandleRequestVote(req *RequestVoteReq) *RequestVoteResp
    HandleInstallSnapshot(req *InstallSnapshotReq) *InstallSnapshotResp
    HandleTimeoutNow(req *TimeoutNowReq) *TimeoutNowResp
}
```

In `cmd/raftd`, a `raftHandlerWrapper` bridges from `transport.Handler` to the `raft.Raft` internal RPC handlers. The wrapper is created before the raft node, then wired in after (`wrapper.raftNode = raftNode`) to break the circular dependency.

Peers are registered with `AddPeer(id, address)`. The transport dials on demand and reconnects on failure.

### gRPC Transport (`grpc.go`)

An alternative transport that uses the protobuf definitions in `proto/`. It implements the same `raft.Transport` interface. It can be used as a drop-in replacement for the TCP transport when tighter schema control or streaming is required.

---

## FSM Interface and KV Store (`pkg/fsm/`)

### FSM Interface

```go
type FSM interface {
    Apply(entry []byte) (result []byte, err error)
    Snapshot() (Snapshot, error)
    Restore(reader io.Reader) error
}
```

`Apply` receives the raw bytes of a committed log entry and returns a result to the waiting client. `Snapshot` serialises the current state. `Restore` replaces the current state from a reader.

### KVStore Implementation

`KVStore` is an in-memory `map[string]string` protected by a `sync.RWMutex`.

Commands are JSON-encoded `kvCommand` structs:

```json
{"op": "set",    "key": "foo", "value": "bar"}
{"op": "get",    "key": "foo"}
{"op": "delete", "key": "foo"}
{"op": "list"}
```

Helper functions (`EncodeSet`, `EncodeGet`, `EncodeDelete`) are provided for clients.

Snapshot serialisation uses `json.Marshal(k.data)`. Restore uses `json.Unmarshal`.

---

## Metrics and Observability (`pkg/metrics/`)

Prometheus metrics are registered via `promauto` (auto-registered with the default registry):

| Metric                                    | Type      | Description                              |
|-------------------------------------------|-----------|------------------------------------------|
| `raft_term`                               | Gauge     | Current Raft term                        |
| `raft_commit_index`                       | Gauge     | Last committed log index                 |
| `raft_applied_index`                      | Gauge     | Last applied log index                   |
| `raft_leader_id`                          | Gauge     | 1 if node has a known leader             |
| `raft_elections_total`                    | Counter   | Total election rounds started            |
| `raft_votes_granted_total`                | Counter   | Total votes granted                      |
| `raft_append_entries_sent_total`          | CounterVec| AppendEntries RPCs sent, labelled by target and success |
| `raft_append_entries_latency_seconds`     | HistogramVec | AppendEntries RPC latency             |
| `raft_request_vote_latency_seconds`       | Histogram | RequestVote RPC latency                  |

Metrics are exposed at `GET /metrics` by the HTTP server using the standard Prometheus handler (`promhttp.Handler()`).

---

## Configuration and Membership Changes

### Server Configuration (`cmd/raftd/`)

The server is configured via a YAML file (default `raftd.yaml`). Key fields:

```yaml
node_id:        node1
listen_addr:    :8080        # TCP transport address
http_addr:      :8081        # HTTP API address
data_dir:       ./data
election_tick:  10
heartbeat_tick: 1
admin_token:    ""           # if non-empty, /admin/* requires Bearer token
cluster:
  - id: node1
    address: localhost:8080
  - id: node2
    address: localhost:8082
  - id: node3
    address: localhost:8084
```

### Raft Configuration (`pkg/raft/types.go`)

```go
type Config struct {
    LocalID           ServerID
    ElectionTick      int           // default 10
    HeartbeatTick     int           // default 1
    MaxSizePerMsg     uint64        // default max int64
    MaxInflight       int           // default 256
    SnapshotInterval  time.Duration // default 120s
    SnapshotThreshold uint64
    TrailingLogs      uint64
    PreVote           bool
    InitialConfiguration Configuration
}
```

Defaults are applied inside `newRaft()` before `Config.Validate()` is called, so zero values are safe.

### Membership Changes (Joint Consensus)

The implementation uses the two-phase joint-consensus protocol from the Raft paper:

1. **Phase 1 — Joint entry**: A `ChangeJoint` configuration entry is appended. While this entry is uncommitted, quorum requires agreement from both the old and new configuration.
2. **Phase 2 — Commit entry**: Once the joint entry is committed, a `ChangeCommitJoint` entry is automatically appended by the leader. When this commits, the new configuration takes effect and the old one is discarded.

`JointConfiguration` holds both `OldConfig` and `NewConfig`. Its `QuorumSize()` returns the maximum quorum required by either configuration.

Supported operations (all via the `Raft` interface):
- `AddLearner(ctx, id, addr)` — add a non-voting learner
- `PromoteLearner(ctx, id)` — promote learner to voter (checks it is caught up within `TrailingLogs`)
- `RemoveServer(ctx, id)` — remove a server via joint consensus
- `Demote(ctx, id)` — demote a voter to learner

---

## Client Library (`pkg/client/`)

`client.Client` provides a Go API for interacting with a cluster over HTTP. It supports leader tracking with an optional lease cache to avoid redundant `/admin/cluster` queries.

Key methods:

```go
c := client.NewClient(
    client.WithAddresses([]string{"localhost:8081", "localhost:8083", "localhost:8085"}),
    client.WithTimeout(5 * time.Second),
)

// Write
result, err := c.Set(ctx, "key", "value")

// Read (routes to current leader)
result, err := c.Get(ctx, "key")

// Cluster info
info, err := c.ClusterInfo(ctx)
```

The client automatically re-discovers the leader when a node returns a 503 or forwards to a different node. Read consistency is configurable (`ReadDefault`, `ReadLinearizable`, `ReadStale`).

---

## Key Data Flows

### Election Flow

```
1. Follower's electionTicks reaches 0
2. If PreVote enabled:
   a. Send PreVote RequestVote (PreVote=true, same term) to all peers
   b. Collect pre-vote quorum; only then proceed
3. Increment term; set votedFor = localID; persist to StableStore
4. Broadcast RequestVote to all peers
5. Collect responses:
   - VoteGranted=true: increment voteCount
   - Higher term in response: revert to Follower
6. If voteCount >= quorum: transition to Leader
7. Leader immediately sends empty AppendEntries (heartbeats) to establish authority
```

### Replication Flow

```
1. Leader receives Apply(ctx, data) via proposalCh
2. handleProposal(): append LogEntry{term, lastIndex+1, Normal, data}; persist to WAL
3. Heartbeat tick or new entry: sendAppendEntries() to all followers
4. Follower receives AppendEntries:
   a. Check prevLogTerm/prevLogIndex (log-matching property)
   b. Append new entries; truncate conflicting entries
   c. Advance commitIndex to min(leaderCommit, lastIndex)
   d. Return AppendEntriesResponse{Success: true, Index: lastIndex}
5. Leader receives success: update matchIndex[follower]
6. advanceCommitIndex(): sort matchIndex values, find median >= quorum
7. New commitIndex must be in current term (safety check)
8. applyCommitted(): apply entries [applyIndex+1 .. commitIndex] to FSM
9. Resolve pending ApplyFutures for applied entries
```

### Snapshot Flow

```
1. Snapshot triggered by:
   - Periodic SnapshotInterval timer
   - Explicit POST /admin/snapshot (authenticated)
   - log size exceeding SnapshotThreshold
2. Leader/follower: fsm.Snapshot() produces a Snapshot with Reader()
3. FileSnapshotStore.Create() returns a SnapshotSink
4. Snapshot data is copied to the sink; sink.Close() finalises the file
5. WAL.Compact(snapshotIndex) deletes segments below the snapshot point
6. StableStore records raft_last_snapshot

InstallSnapshot (leader to lagging follower):
1. Leader detects follower's nextIndex is before the snapshot
2. Send InstallSnapshotRequest with snapshot data in chunks
3. Follower: restoreSnapshotData() writes data to SnapshotStore
4. fsm.Restore(reader) replaces FSM state
5. Follower updates lastIndex/lastTerm to snapshot's index/term
```

# Architecture

This document is a technical deep dive into how `raftd` is built and how the Raft
algorithm is actually implemented in this codebase. It reflects the code, not an
idealized description.

- [Component overview](#component-overview)
- [The Raft core](#the-raft-core)
  - [Ticks, timers, and the run loop](#ticks-timers-and-the-run-loop)
  - [Leader election and pre-vote](#leader-election-and-pre-vote)
  - [Log replication](#log-replication)
  - [Commit advancement](#commit-advancement)
  - [Apply and group commit](#apply-and-group-commit)
  - [Linearizable reads (ReadIndex)](#linearizable-reads-readindex)
  - [Snapshots and compaction](#snapshots-and-compaction)
  - [Joint-consensus membership](#joint-consensus-membership)
- [The KV state machine (FSM)](#the-kv-state-machine-fsm)
- [Watches](#watches)
- [Transport](#transport)
- [The HTTP server](#the-http-server)
- [The Go client](#the-go-client)
- [Data flow: a write](#data-flow-a-write)
- [Data flow: a linearizable read](#data-flow-a-linearizable-read)
- [On-disk formats](#on-disk-formats)

## Component overview

```
cmd/raftd            Server binary: config, HTTP API, wires everything together
cmd/kvctl            CLI client

pkg/raft             Core consensus state machine (raft.go, joint.go, types.go)
pkg/storage          WAL + BoltDB stable store (wal.go), snapshots (snapshot.go)
pkg/fsm              KV state machine (kv.go), transactions (txn.go), watches (watch.go)
pkg/transport        JSON-over-TCP transport (tcp.go), gRPC transport (grpc.go)
pkg/client           Go client library
pkg/metrics          Prometheus metric registration
pkg/tracing          OpenTelemetry (OTLP) providers and spans
pkg/version          Build-time version metadata
proto                gRPC service definitions (RaftService, RaftAdmin)
ui                   React/Vite admin dashboard
charts/raft          Helm chart
```

The `raft` package exposes the `Raft` interface (see `pkg/raft/types.go`). It is
storage-, transport-, and FSM-agnostic: `NewRaft` is handed a `LogStore`, a
`StableStore`, a `SnapshotStore`, an `FSM`, and a `Transport`. `cmd/raftd` provides
the concrete implementations (WAL + BoltDB, file snapshots, the KV FSM, and a
TCP or gRPC transport).

## The Raft core

### Ticks, timers, and the run loop

A single `run()` goroutine owns all state transitions, driven by a `time.Ticker`
firing every **heartbeat interval = `HeartbeatTick × 50ms`**. Each tick:

- Followers and candidates decrement an `electionTicks` counter; when it reaches
  zero they start an election.
- The leader sends a heartbeat round.
- Coalesced signals (proposals, ReadIndex heartbeat triggers, commit-index
  persistence, snapshot triggers) are drained.

The election timeout is randomized per node: a follower waits a tick count drawn
uniformly from `[ElectionTick, 2 × ElectionTick]` before starting an election.
`Config.Validate()` **hard-fails** if `ElectionTick < 3 × HeartbeatTick` and
**warns** below the recommended `10×` ratio, because too little headroom lets a
healthy leader's heartbeats fail to reset follower timers, causing spurious
elections.

### Leader election and pre-vote

When `PreVote` is enabled (`Config.PreVote`), a follower whose timer fires first
runs a **pre-vote round**: it sends `RequestVote` with `PreVote=true` and
`Term = currentTerm + 1` **without** incrementing its own term or recording a
vote. Peers grant a pre-vote only if (a) the proposed term is at least their
current term, (b) they have **not** heard from a leader recently (a node that has
heard a leader within its election window refuses, which is what stops a
partitioned node from forcing needless elections), and (c) the candidate's log is
at least as up-to-date. Only if a quorum of pre-votes is collected does the node
run the real election: it increments its term, votes for itself, **persists term
and vote** to the stable store (with `Sync()`), and sends real `RequestVote`
RPCs.

Vote granting in the real election follows the Raft rules: reject a stale term;
step down on a higher term; grant only if `votedFor` is empty or already this
candidate **and** the candidate's log is up-to-date
(`LastLogTerm > localLastTerm || (equal && LastLogIndex >= localLastIndex)`).
Granting a vote resets the election timer.

Quorum counting is **joint-aware**: during a joint configuration a candidate needs
a majority in **both** the old and new configurations. Learners never vote and
never count toward quorum. A single-voter cluster wins instantly.

On becoming leader the node: clears its heartbeat-ack tracking (so linearizable
reads must re-confirm quorum), initializes `nextIndex[peer] = lastIndex + 1` and
`matchIndex[peer] = 0` for every other server, appends a **no-op entry** (to
establish its term's commit point), and sends an immediate heartbeat.

### Log replication

Each heartbeat round spawns at most **one in-flight replication goroutine per
follower** (`inflightReplication` guards against unbounded goroutine growth).
For a follower, `replicateTo` builds an `AppendEntriesRequest` starting at
`nextIndex[peer]`, batching up to **100 entries** per RPC. It captures the request
term and, on the response, only acts if the node is **still leader** and still in
the **same term** — this rejects stale responses that arrive after a step-down or
re-election.

On the follower side, `handleAppendEntries`:

- Rejects a request whose term is older than the follower's.
- Steps down and resets its election timer on an equal/higher term.
- On a `PrevLogIndex`/`PrevLogTerm` mismatch, **rejects without truncating** and
  returns a **conflict hint**: `ConflictTerm` = the term of its entry at
  `PrevLogIndex`, and `Index` = the first index it holds for that term.
- When entries match up to the append point, it refuses to overwrite already
  **committed** entries, truncates from the first genuine conflict, and appends
  the new suffix in a single durable write.
- Advances its commit index to `min(LeaderCommit, lastIndexInRequest)` so it never
  marks entries beyond what this request actually carried as committed.

The leader uses the conflict hint for **fast backup** (the M7 optimization): if
`ConflictTerm` is set, it rewinds `nextIndex` to just past its own last entry for
that term — skipping an entire divergent term in one round trip instead of
decrementing `nextIndex` one index at a time. `nextIndex` only ever moves backward
on failure.

### Commit advancement

After a successful `AppendEntries`, the leader records the follower's ack
timestamp (used by ReadIndex) and updates `matchIndex`/`nextIndex`, then runs
`advanceCommitIndex`. For each candidate index above the current commit index it:

- Only considers entries **from the current term** (the classic Raft safety rule
  that prevents committing a stale-term entry by counting).
- Counts itself (if a voter) plus every non-learner voter whose `matchIndex`
  reaches that index; commits when the count reaches `QuorumSize()`.
- In a joint configuration, requires a quorum in **both** the old and new configs.

The commit index is persisted to the stable store through a coalescing channel,
written to disk outside the main lock so persistence never blocks replication.

### Apply and group commit

`Apply(ctx, data)` (leader only) enqueues a proposal future and blocks until it is
committed and applied (or `ctx`/shutdown fires). The run loop drains up to **256**
queued proposals and appends them to the WAL in a **single fsync** (group commit),
which is the main write-throughput lever. Proposals are rejected with
`ErrNotLeader` off the leader, `ErrLeadershipLost` during a leadership transfer,
and `ErrNodeBusy` when too many futures are pending.

`applyCommitted` applies entries in the range `(applyIndex, commitIndex]` to the
FSM in order. A panicking FSM apply is **recovered** (the error is recorded and the
entry skipped) so one bad command cannot crash the node. After applying, it
resolves the corresponding futures and signals `WaitApplied` waiters via a
dedicated condition variable (no busy-polling).

If a storage write fails or the FSM panics fatally, the node records a fatal error,
transitions to `Shutdown`, and reports itself unhealthy so it is excluded from
quorum rather than silently corrupting state.

### Linearizable reads (ReadIndex)

Linearizable reads use a **heartbeat-confirmed ReadIndex**, not a wall-clock lease
(so no inter-node clock synchronization is assumed). `ReadIndex(ctx)`:

1. Returns `ErrNotLeader` if the node is not the leader. A single-node cluster
   returns the current commit index immediately.
2. Records `start = time.Now()` on the **leader's own monotonic clock** and
   triggers an immediate heartbeat round.
3. Waits until a **quorum of followers have acked** a heartbeat sent *after*
   `start` (comparing ack timestamps to `start`, all on the leader's clock).
4. Returns the commit index captured in step 1 as the read index.

The HTTP handler then calls `WaitApplied(readIndex)` so the local FSM reflects all
committed entries up to that index before serving the value. No entry is written to
the log for a read, making it far cheaper than an `Apply`.

### Snapshots and compaction

A snapshot ticker fires every `SnapshotInterval` (default **120s**). A snapshot is
taken only when the log has grown enough:
`SnapshotThreshold > 0 && (appliedIndex − lastSnapshotIndex) >= SnapshotThreshold`
(default threshold **8192**). Snapshotting captures the applied index, its term,
the current configuration, and `FSM.Snapshot()` atomically, writes them through the
snapshot store, then **compacts the WAL** up to that index (retaining
`TrailingLogs`, default **10240**, entries so slightly-lagging followers can still
be caught up from the log rather than a full snapshot install).

A follower that has fallen behind the leader's compacted log receives an
`InstallSnapshot` stream. `handleInstallSnapshot` ignores stale snapshots
(`LastIncludedIndex <= commitIndex`), restores the FSM, reconciles its own log
(keeping a matching suffix or discarding all if the boundary term differs), and
sets both commit and applied indexes to the snapshot's last-included index.

### Joint-consensus membership

Membership changes go through **joint consensus (C_old,new)** so quorum is never
lost mid-change, and **at most one change is outstanding at a time**
(`ErrConfigChangeInProgress` otherwise; the guard trips while either a joint config
is active or a pending config entry is uncommitted).

- **AddServer / RemoveServer / ReplaceServer** compute the target configuration and
  append a **joint** configuration entry. When it commits, the node enters joint
  mode (old ∪ new voters both required for quorum); the leader then appends a
  **commit-joint** entry, and when *that* commits the configuration collapses to the
  new one and stale replication trackers are cleaned up. **Demote** is implemented
  as a remove.
- **AddLearner** appends a single-step entry adding a non-voting server. Learners
  receive replication but never vote.
- **PromoteLearner** first checks the learner is caught up (its `matchIndex` is
  within `TrailingLogs` of the leader's last index) and returns `ErrLearnerNotReady`
  otherwise, then appends a promote entry. This is the safe path for scaling: add as
  a learner, let it catch up, then promote.

The server exposes these via `/admin/members`, `/admin/members/{id}`,
`/admin/members/{id}/promote`, and `/admin/members/{id}/demote` (see
[api.md](api.md)); all are leader-only and require the `write` role.

## The KV state machine (FSM)

The FSM (`pkg/fsm/kv.go`) is a revision-versioned key/value store. Commands are
JSON-encoded (`op`, `key`, `value`, optional `txn`, optional `client_id`/`seq_num`)
and produced by `EncodeCommand` / `EncodeCommandWithID` / `EncodeTxn`. Supported
ops include `put`, `get`, `delete`, `range`, and `txn` (plus legacy `set`/`list`).

Each stored value is a `KeyValue`:

| JSON field | Meaning |
|------------|---------|
| `key` | the key |
| `value` | the value |
| `create_revision` | revision at which the key was first created |
| `mod_revision` | revision of the last modification |
| `version` | number of modifications to this key |

A single global, monotonic **revision** counter increments **only on mutations**
(all ops in one transaction share a single increment). A separate `index` counter
increments on **every** applied command and drives the snapshot index.

**Idempotency / dedup**: when a command carries `client_id` + `seq_num`, the FSM
records the result in a per-client dedup table. A replayed command with a
sequence number at or below the last-seen one returns the cached result without
re-applying — this makes client retries safe. The dedup table is included in
snapshots. Its size is bounded with deterministic eviction.

**Transactions** (`pkg/fsm/txn.go`) mirror etcd's mini-transactions:
`TxnRequest{compare, success, failure}`. Each `compare` tests a key's `value`,
`version`, `create_revision`, or `mod_revision` against a target with a result of
`equal` / `not_equal` / `greater` / `less`. If **all** comparisons pass, the
`success` ops run; otherwise the `failure` ops run. Ops are `put` (type `0`) or
`delete` (type `1`). Transactions are **atomic**: every op is pre-validated before
anything is written, so a partial transaction never mutates state, consumes a
revision, or emits events. The response is
`TxnResponse{succeeded, results[], revision}`.

## Watches

The `WatchManager` (`pkg/fsm/watch.go`) fans committed KV changes out to SSE
subscribers on **every** node (watches are served locally — no leader forwarding —
because all nodes apply the same committed log and therefore emit the same event
sequence).

- Events are `Event{type (put/delete), key, kv, prev_kv, revision}`; a batch from a
  transaction shares one revision.
- The FSM keeps a bounded **history ring buffer** (1024 entries) and does
  non-blocking sends on an internal channel; when a consumer can't keep up, events
  are **dropped** and a `DroppedEvents()` counter increments. Late or reconnecting
  subscribers recover missed events via history replay from a revision.
- Each subscription records the revision captured at registration time; live
  dispatch delivers strictly newer revisions while history replay covers everything
  at or before it, which guarantees exactly-once, in-order delivery and closes the
  register/replay race.

Clients pass `?key=` or `?prefix=` and an optional `?revision=` (or `Last-Event-ID`
header) to resume. The HTTP handler streams `id: <revision>\ndata: <json>\n\n`
frames and closes idle connections after `watch_idle_timeout`.

## Transport

Both transports implement the same `raft.Transport` interface (AppendEntries,
RequestVote, InstallSnapshot, TimeoutNow) and are selected by the `transport`
config key.

**TCP (default, `pkg/transport/tcp.go`)** — newline-delimited JSON frames
(`{id, type, payload}`) with a monotonic per-request correlation `id`; the server
echoes it and the client drops the connection on any id/type mismatch. One
persistent, lazily-dialed connection is pooled per peer. Per-message size is
bounded (default 16 MiB). Optional TLS/mTLS pins **TLS 1.3**, requires and verifies
client certs when a CA is set, and refuses group/other-readable key files.

**gRPC (`pkg/transport/grpc.go`)** — uses the generated `RaftService`/`RaftAdmin`
protos (`proto/raft.proto`). Maintains a small connection pool per peer with
round-robin selection, health tracking, and background re-dial of stale/unhealthy
peers. Message sizes default to 64 MiB (512 MiB aggregate for streamed snapshots).
Optional TLS/mTLS pins TLS 1.3; `SetRequireTLS(true)` (config `require_tls`) fails
closed with no plaintext fallback; a per-peer **member allowlist** authorizes each
RPC by the verified peer certificate's CN or DNS SAN.

## The HTTP server

`cmd/raftd/main.go` builds a `http.ServeMux` with middleware layers:

- **CORS** (deny-by-default; allowlist via `cors_origins`).
- **Auth** (`authMiddleware` / `requireRole`) — token → role, fails closed when no
  tokens are configured unless `allow_no_auth` is set.
- **Rate limiting** (`rateLimitMiddleware`) — global and per-IP token buckets on
  write methods only; GET/HEAD are never limited.

Writes and linearizable reads that arrive at a follower are **forwarded to the
leader** over HTTP (the forward hop validates the leader address from static config,
propagates the request context, and uses `https://` when the API is TLS-enabled).
Watches are never forwarded.

## The Go client

`pkg/client/client.go` is an HTTP/JSON client with automatic leader discovery and
retry. It queries `/admin/cluster` to learn the leader, caches it, and retries write
operations across all endpoints with exponential backoff and jitter. Mutating calls
attach a stable `client_id` and a single `seq_num` **reused across all retries**, so
the server-side dedup table makes retries idempotent. It provides `Put`, `GetKV`
(linearizable), `GetKVStale`, `Range`, `DeleteKV`, `Txn`, `Watch`/`WatchPrefix`
(auto-reconnecting SSE), and `GetClusterInfo`.

## Data flow: a write

```
client PUT /v1/kv/foo
      │
      ▼
[any node] ── not leader? ──► forward to leader's http_address ──┐
      │                                                          │
      ▼ (leader)                                                 │
Apply(data) ─► enqueue proposal ─► run loop batches up to 256    │
      │                                                          │
      ▼                                                          │
append to WAL (one fsync for the batch) ─► replicate AppendEntries│
      │                                                          │
      ▼                                                          │
quorum of matchIndex reaches the entry ─► commitIndex advances    │
      │                                                          │
      ▼                                                          │
applyCommitted ─► KV FSM applies put ─► revision bumps            │
      │                                                          │
      ▼                                                          │
future resolves ─► 200 OK with the new KeyValue ◄────────────────┘
```

## Data flow: a linearizable read

```
client GET /v1/kv/foo          (default consistency = linearizable)
      │
      ▼
[any node] ── not leader? ──► forward to leader ──┐
      │                                            │
      ▼ (leader)                                   │
ReadIndex(ctx):                                    │
   readIdx = commitIndex; start = now()            │
   trigger heartbeat round                         │
   wait for quorum of acks newer than `start`      │
      │                                            │
      ▼                                            │
WaitApplied(readIdx)  (FSM caught up)              │
      │                                            │
      ▼                                            │
read from local KV FSM ─► 200 OK ◄─────────────────┘
```

With `?consistency=stale`, the node skips ReadIndex entirely and reads its local FSM
directly (fast, may be slightly behind; the range endpoint marks the response with
`X-Consistency: stale`).

## On-disk formats

Data for a node lives under `data_dir/<node_id>/`:

```
data/<node_id>/
  wal/
    00000000000000000000.wal   segment file (base index in the name, 20 digits)
    meta.db                    BoltDB metadata
  stable.db                    BoltDB: term, votedFor, config, commit index
  snapshots/
    <term>-<index>.snap        snapshot data + trailing checksum footer
    <term>-<index>.meta        JSON sidecar (durable before the .snap is visible)
```

### WAL record layout

Segment files are named by their base index (`%020d.wal`) and rotate at **64 MiB**.
Each record is a 25-byte header followed by the entry data, big-endian:

```
offset  size  field
0       4     CRC32 (IEEE) over bytes [4:] (header tail + data)
4       4     payload length = len(data) + 9
8       1     entry type (Normal / Configuration / Snapshot)
9       8     term  (uint64)
17      8     index (uint64)
25      N     entry data
```

`Append` writes all entries in the batch and then issues **one fsync**; segment
rotation and creation also fsync the directory so the new file's directory entry is
durable. On recovery the WAL scans each segment; a torn tail in the **last** segment
is truncated only if no valid record follows it (otherwise it refuses to open, since
that indicates real mid-segment corruption), while corruption in an earlier segment
is a hard error.

### Snapshot layout

`FileSnapshotStore` writes each snapshot as `<term>-<index>.snap` plus a JSON
sidecar `<term>-<index>.meta`. The `.snap` file ends with an 8-byte footer
(a 4-byte magic + 4-byte CRC32 of the payload). On write, the sidecar (marked
`Checksummed`) is fsynced and renamed into place **first**, then the `.snap` is
renamed in and the directory fsynced — so a visible `.snap` always has durable
metadata. On read, a checksummed snapshot **must** verify its footer or the open
fails. Retention keeps the newest `retainCount` snapshots (raftd configures 2),
pruning the oldest before writing a new one.

### Stable store

Term, `votedFor`, the committed configuration, and the persisted commit index are
stored in a BoltDB file (`stable.db`) under a single `stable` bucket, with keys
`raft_term`, `raft_voted_for`, `raft_config`, `raft_commit_index`, and related
constants (see `pkg/raft/types.go`).

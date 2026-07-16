> A complete, production-ready specification for **Raft Consensus Implementation (Golang)**. Use this as the single source-of-truth while implementing, reviewing, and testing.

## 1. Project Overview

**Goal:** Implement a production-quality Raft consensus system in Go that provides leader election, robust log replication, durable persistence, cluster reconfiguration, and a modern admin UI. The system should be modular (library + server + client), thoroughly tested (unit + integration + fault injection), observable (metrics/tracing/logs), and secure (mTLS + auth).

**Deliverables**

* `raft` Go library (core algorithm, transport-agnostic)
* `raftd` server binary (gRPC/HTTP transport + built-in storage + admin API)
* `rafthub` client library (simple client library for applications)
* Admin web UI (React + Tailwind) with cluster visualizer, metrics, log tail, and node control
* Automated test harness for multi-node scenarios and chaos testing
* CI (unit, integration, stress) and benchmark suite
* Production ops docs: deployment, monitoring, upgrade, backup/restore

## 2. High-Level Architecture

* **Core layer (library)**: `pkg/raft` — algorithm core (state machine, persistent storage interface, RPC interface abstraction).
* **Transport layer**: `pkg/transport` — pluggable transports (gRPC default; optional plain HTTP or binary TCP).
* **Storage layer**: `pkg/storage` — Write-ahead log (WAL), snapshot store, metadata; pluggable backends (bbolt, RocksDB/Badger).
* **FSM (state machine) layer**: implementers provide an `FSM` interface to apply committed log entries.
* **Server (daemon)**: `cmd/raftd` — orchestrates raft instance, exposes Admin API, client API, and UI assets.
* **Client library**: `pkg/client` — provides opaque client sessions, leader discovery, command submission with retries.
* **Admin UI & API**: React SPA talking to `raftd` admin endpoints (metrics, node control, cluster visualizer).
* **Testing harness**: `tools/testharness` — run N processes, simulate partitions, latency, crashes, clock skew.

## 3. Core Features & Requirements

### 3.1 Protocol Features (MUST)

* Leader election (RequestVote RPC): full term handling, election timeout jitter, pre-vote optional.
* Log replication (AppendEntries RPC) with batching.
* Persistence: durable write-ahead log (WAL) and term/index metadata persisted before acknowledging leadership/commit where required by Raft guarantees.
* Safety: preserve Raft invariants (leader must have up-to-date log; committed entries must be durable).
* Heartbeats and leader timeouts.
* Snapshotting and log compaction (periodic and size-triggered): create snapshot, discard older log entries.
* Membership changes (joint consensus): support `AddNode`, `RemoveNode`, `ReplaceNode` transitioning via joint configuration entries.
* Read path: support both linearizable reads (quorum read or lease-based reads) and stale reads for performance (configurable).
* Termed failure handling and leader transfer (graceful step-down).
* Prevent split-brain and ensure single leader per term.

### 3.2 Production & Safety (MUST)

* Durable WAL: fsync/flush policy configurable (safe default = flush on entry).
* Snapshot atomicity: use temp files and atomic rename.
* Recovery path: safe replay from WAL + snapshot, strict verification of indices/terms.
* Node ID & persistent peer metadata.
* TLS for all inter-node and client-server connections (mTLS).
* Authentication & simple RBAC for admin endpoints.
* Graceful shutdown and startup sequences that ensure no data loss.

### 3.3 Observability (MUST)

* Prometheus metrics: term, commitIndex, appliedIndex, leaderId, append RPC latencies, snapshot stats, election counts.
* OpenTelemetry traces for RPCs and snapshotting flows (span per AppendEntries/RequestVote).
* Structured logs (JSON) with node ID and term.
* Health & readiness probes for orchestration.

### 3.4 Testing & Correctness (MUST)

* Unit tests for all deterministic logic (term changes, leader election transitions).
* Integration tests with in-process multi-node clusters.
* Fault-injection tests (partitions, node crashes, delayed RPCs, disk faults).
* Jepsen-style tests in harness (or integrate with Jepsen) to validate under network partitions and restarts.
* Property-based tests for invariants (no two leaders in same term; committed entries persisted).

### 3.5 Performance & Scalability (SHOULD)

* Configurable batching and pipelining for AppendEntries.
* Leader-only reads for low latency with lease safety option.
* Snapshot incremental transfer and throttling.
* Rate limiting of replication to slow followers; fast followers prioritized for catch-up.
* Horizontal scale: optimized for clusters of 3-7 nodes (recommendation: 5 for production).

### 3.6 UX / Admin UI (SHOULD)

* Cluster topology visualization showing leader, followers, and replication lag.
* Log/entry tailing for committed and pending entries.
* Snapshot controls: create, list, restore.
* Node controls: shutdown, restart, show logs.
* Leader history timeline and election event markers.
* Metrics dashboards (charts) embedded in the UI.
* Authentication + role-based access to controls.

## 4. API & Protocol Design

### 4.1 Internal RPCs (transport-agnostic)

Define Protobuf (for gRPC) or equivalent message formats for:

* `RequestVote(term, candidateId, lastLogIndex, lastLogTerm)` → `(term, voteGranted, reason?)`
* `AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit)` → `(term, success, conflictIndex?)`
* `InstallSnapshot(term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, data, done)` → `(term)`
* `JoinCluster` / `LeaveCluster` / `ChangeConfig` commands carried as log entries

### 4.2 Client-facing API

* **gRPC / HTTP JSON** endpoints for:

  * `SubmitCommand` — client appends a command (opaque bytes) to the log and optionally waits for applied result.
  * `Query` — read-only query endpoint supporting linearizable or consistent-stale reads.
  * `Status` — cluster status & metadata.
  * `Admin` — membership changes, snapshot management, debug endpoints.

**Leader forwarding**: Clients can send to any node; non-leaders should either redirect (return leader address) or proxy the request to leader.

### 4.3 Admin API (HTTP/JSON)

* `GET /admin/cluster` — cluster membership & status.
* `POST /admin/nodes/{id}/shutdown` — graceful shutdown.
* `POST /admin/snapshot` — create snapshot.
* `POST /admin/reconfigure` — request a membership change (Add/Remove).
* `GET /admin/logs` — fetch debug logs.

## 5. Storage Details

### 5.1 WAL format

* Durable append-only file(s) with segments.
* Each record: `checksum | length | type | term | index | payload`.
* Rotate segments when size threshold reached; keep recent segments for tailing or follower catch-up.
* Recovery: scan segments, verify checksums, build index into memory (or use small index files).

### 5.2 Snapshots

* Snapshot includes FSM state, lastIncludedIndex, lastIncludedTerm, membership config.
* Store snapshots in object-friendly layout (e.g., `snapshots/<node-id>/<snapshot-id>.tmp` -> atomic rename).
* Transfer via `InstallSnapshot` with chunking.

### 5.3 Storage backend options

* Default: `bbolt` (embedded, simple).
* Optional: `Badger`/RocksDB for higher throughput.
* Provide interfaces so implementers can swap backends.

## 6. State Machine (FSM) Interface

```go
type FSM interface {
    // Apply a committed log entry (opaque slice)
    Apply(entry []byte) (result []byte, err error)

    // Snapshot returns a snapshot object (reader) of current FSM state.
    Snapshot() (Snapshot, error)

    // Restore restores FSM from a snapshot reader.
    Restore(io.Reader) error
}
```

* Snapshots must be consistent with lastIncludedIndex/Term.

## 7. Membership Changes & Reconfiguration

* Use joint consensus per the Raft paper. Implement `ConfigurationEntry` log entries that represent joint configs.
* Special-case: adding a node with empty log -> add as learner first (catch-up) then promote.
* Support "learners" (non-voting replicas) for safe scaling and bootstrapping.

## 8. Read Optimization Options

* **Quorum read** (always safe): require at least majority acknowledgement (extra raft round-trip).
* **Leader lease** (fast reads): leader assumes read safety for `leaseDuration` unless it suspects leadership lost. Make this option configurable and document trade-offs.

## 9. Security

* mTLS for all inter-node and client-server traffic.
* Optional token-based auth for admin APIs.
* Audit logs for membership changes and snapshots.

## 10. Observability

* Export Prometheus metrics (expose `/metrics`).
* Structured JSON logs and `loglevel` control.
* Distributed tracing (OpenTelemetry) with trace context propagated for client & inter-node calls.

## 11. Testing Strategy (high level)

* Unit tests for deterministic algorithm parts.
* Integration tests that run multiple real processes across different ports.
* Chaos test harness to simulate partitions, slow disks, and node crashes.
* Jepsen-style tests: incorporate known failure patterns from Jepsen analyses.
* Performance benchmarks: append throughput, tail latency, snapshot time.

## 12. CI & Release

* CI matrix: unit tests on PR, integration tests on merge, nightly chaos tests and benchmarks.
* Release artifacts: `raftd` binaries for linux/amd64, linux/arm64; docker images.
* Backwards compatibility guarantees for wire protocol (versioning).

## 13. UX/UI Implementation Notes (modern aesthetic)

* Frontend: React + TypeScript + Tailwind CSS + Vite.
* Charts: Recharts or lightweight charting; use Prometheus queries for charts.
* Visuals: topology view, node cards, timeline of elections, replication lag heatmap.
* Admin console served from `raftd` or separate static host with OIDC login.

## 14. Non-functional requirements (example)

* Durability: once client sees success (configurable), entry must survive crash and be applied when majority of nodes restored.
* Target cluster sizes: 3–7 nodes recommended.
* Latency goals: 95th percentile leader-apply < X ms — tune batching, disk settings, network.

## 15. Acceptance Criteria (for each major feature)

* Leader election: deterministic tests show no two leaders in same term for N runs.
* Log replication: entries appended on leader and present on majority even after follower crashes and restarts.
* Snapshotting: create and restore tested with large FSM states (>100MB).
* Membership change: tests for add/remove with joint consensus pass under partitions.
* Chaos tests: no data loss in majority-alive scenarios in Jepsen harness.

---

# Implementation notes & recommended libraries / patterns

* Use `go.etcd.io/raft` and `hashicorp/raft` as design references — both are battle-tested. ([GitHub][2])
* Default storage: `bbolt` for simplicity; `Badger` for higher write throughput if needed.
* RPC: Protobuf + gRPC (supports streaming for `InstallSnapshot` chunks).
* Prometheus + OpenTelemetry for metrics & traces.
* Use `context.Context` everywhere for cancellations & deadline propagation.
* Use a separate goroutine for replication per follower to allow pipelining; keep careful concurrency controls around persistent metadata and state transitions (use channels + mutex where appropriate).
* Consider implementing **pre-vote** to reduce disruption in networks with volatile timing.
* Add a `Learner` role to safely add nodes without counting them in quorum until caught up.

---

# Acceptance & verification checklist (quick)

* Run 3-node cluster, submit 10k commands, crash follower, recover — verify committed entries present on majority.
* Test membership change add/remove under partition: no data loss, no split-brain.
* Chaos test with partitions + leader crash: verify at most one leader per term and committed entries not lost.
* Snapshot restore of a large FSM (>100MB) succeeds and subsequent replication continues.

---

# Quick next steps I recommend

1. Clone and explore `hashicorp/raft` and `etcd/raft` to choose design patterns (WAL layout, snapshot code paths). ([GitHub][2])
2. Start implementing `pkg/raft` with a focus on correctness first (unit tests) before optimizing.
3. Build the test harness early — being able to run 5 nodes locally and inject failures will save huge amounts of time.
4. Iterate the admin UI after core functionality is stable — the UI is mostly a consumer of the admin API.


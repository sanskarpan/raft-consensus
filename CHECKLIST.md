# Raft Consensus Implementation Checklist

## Phase 1: Core Library (pkg/raft)

### Stage 1a — Project Foundation

- [x] **1.1.1** Initialize Go module with `go mod init`
- [x] **1.1.2** Add dependencies: bbolt, grpc, protobuf, prometheus, opentelemetry, zap
- [x] **1.1.3** Create directory structure: pkg/raft, pkg/storage, pkg/transport, pkg/client, pkg/fsm, cmd/raftd, proto, tools/testharness, ui
- [x] **1.1.4** Define protocol buffer messages in proto/raft.proto
- [x] **1.1.5** Generate protobuf Go code
- [x] **1.1.6** Define core interfaces: FSM, LogStore, StableStore, SnapshotStore, Transport
- [x] **1.1.7** Configure structured logging with zap

### Stage 1b — Write-Ahead Log (pkg/storage)

- [x] **1.2.1** Define LogEntry structure with term, index, data, type
- [x] **1.2.2** Implement segment file writer with batched writes
- [x] **1.2.3** Implement segment file reader with index lookup
- [x] **1.2.4** Implement WAL struct with append/get/truncate operations
- [x] **1.2.5** Implement stable store using bbolt for term/votedFor
- [x] **1.2.6** Implement in-memory index for fast log lookup
- [x] **1.2.7** Add fsync policy configuration (per-entry, batched)
- [x] **1.2.8** Implement segment rotation on size threshold

### Stage 1c — Raft State Machine (pkg/raft)

- [x] **1.3.1** Define RaftNode struct with all required fields
- [x] **1.3.2** Implement ServerID and PeerSet types
- [x] **1.3.3** Define RaftState enum (Follower, Candidate, Leader)
- [x] **1.3.4** Implement Config struct with all options
- [x] **1.3.5** Implement election timeout with jitter (randomized 150-300ms)
- [x] **1.3.6** Implement heartbeat timer for leader
- [x] **1.3.7** Implement pre-vote protocol to prevent disruption
- [x] **1.3.8** Implement RequestVote RPC handler
- [x] **1.3.9** Implement RequestVote RPC client
- [x] **1.3.10** Implement AppendEntries RPC handler
- [x] **1.3.11** Implement AppendEntries RPC client with batching
- [x] **1.3.12** Implement leader heartbeat mechanism
- [x] **1.3.13** Implement commit index tracking and advancement
- [x] **1.3.14** Implement log matching (prevLogIndex, prevLogTerm)
- [x] **1.3.15** Implement leader election vote counting
- [x] **1.3.16** Add configurable options: election timeout range, heartbeat interval, batching size

### Stage 1d — Core Testing

- [x] **1.4.1** Write unit tests for leader election transitions
- [x] **1.4.2** Write unit tests for vote granting logic
- [x] **1.4.3** Write unit tests for AppendEntries validation
- [x] **1.4.4** Write unit tests for term progression
- [x] **1.4.5** Test pre-vote protocol behavior
- [x] **1.4.6** Test heartbeat timeout behavior
- [x] **1.4.7** Test election timeout behavior

---

## Phase 2: Persistence & Recovery

### Stage 2a — Snapshotting

- [x] **2.1.1** Define SnapshotMeta structure (version, ID, index, term, configuration)
- [x] **2.1.2** Implement SnapshotSink interface for FSM
- [x] **2.1.3** Implement snapshot creation from FSM
- [x] **2.1.4** Implement file-based snapshot store
- [x] **2.1.5** Implement atomic snapshot writes (temp file + rename)
- [x] **2.1.6** Implement periodic snapshot trigger
- [x] **2.1.7** Implement size-triggered snapshot
- [x] **2.1.10** Implement log compaction after snapshot

### Stage 2b — Recovery

- [x] **2.2.1** Implement startup: load latest snapshot
- [x] **2.2.2** Implement WAL replay after snapshot
- [x] **2.2.3** Implement log consistency verification
- [x] **2.2.4** Handle truncated logs from followers
- [x] **2.2.5** Test crash recovery with committed entries
- [x] **2.2.6** Test snapshot restore with large FSM (>100MB)

---

## Phase 3: Membership & Reconfiguration

### Stage 3a — Joint Consensus

- [x] **3.1.1** Define ConfigurationEntry structure
- [x] **3.1.2** Implement joint consensus (old + new config)
- [x] **3.1.3** Implement AddNode command
- [x] **3.1.4** Implement RemoveNode command
- [x] **3.1.5** Implement ReplaceNode command
- [x] **3.1.6** Ensure quorum in joint config
- [x] **3.1.7** Test single-node membership change
- [x] **3.1.8** Test multi-node membership change under partition

### Stage 3b — Learners

- [x] **3.2.1** Implement Learner node type (non-voting)
- [x] **3.2.2** Implement learner catch-up tracking
- [x] **3.2.3** Implement learner promotion flow
- [x] **3.2.4** Add learner as bootstrap option
- [x] **3.2.5** Test learner catch-up and promotion
- [x] **3.2.6** Test learner with empty log

---

## Phase 4: Transport & Server

### Stage 4a — gRPC Transport

- [x] **4.1.1** Define RaftService gRPC service
- [x] **4.1.2** Implement gRPC server with TLS
- [x] **4.1.3** Implement gRPC client pool
- [x] **4.1.5** Implement streaming for InstallSnapshot
- [x] **4.1.6** Implement connection pooling (optimized)
- [x] **4.1.7** Implement leader forwarding for client requests

### Stage 4b — raftd Server

- [x] **4.2.1** Create cmd/raftd/main.go entry point
- [x] **4.2.2** Implement config loader from YAML
- [x] **4.2.3** Initialize raft node with storage and transport
- [x] **4.2.4** Implement admin HTTP endpoints (cluster, nodes, snapshot, reconfigure)
- [x] **4.2.5** Implement client gRPC service
- [x] **4.2.6** Implement readiness probe endpoint
- [x] **4.2.7** Implement liveness probe endpoint
- [x] **4.2.8** Implement graceful shutdown (drain connections, save state)
- [x] **4.2.9** Test multi-process integration

---

## Phase 5: Client Library & FSM

### Stage 5a — Client Library

- [x] **5.1.1** Define Client interface
- [x] **5.1.2** Implement leader discovery via admin API
- [x] **5.1.3** Implement SubmitCommand with retry logic
- [x] **5.1.4** Implement exponential backoff (50ms→100ms→…→2s, ±10% jitter; watch reconnect caps at 30s)
- [x] **5.1.5** Implement quorum read option
- [x] **5.1.6** Implement lease-based read option
- [x] **5.1.7** Implement stale/read-after-read consistency
- [x] **5.1.8** Test client with cluster failures

### Stage 5b — FSM Implementation

- [x] **5.2.1** Define FSM interface (Apply, Snapshot, Restore)
- [x] **5.2.2** Implement in-memory KV store FSM
- [x] **5.2.3** Implement KV store snapshot
- [x] **5.2.4** Implement KV store restore
- [x] **5.2.5** Test FSM with sample commands

---

## Phase 6: Observability & Telemetry

### Stage 6a — Metrics

- [x] **6.1.1** Integrate Prometheus metrics library
- [x] **6.1.2** Export term gauge
- [x] **6.1.3** Export commitIndex gauge
- [x] **6.1.4** Export appliedIndex gauge
- [x] **6.1.5** Export leaderId gauge
- [x] **6.1.6** Export election counter
- [x] **6.1.7** Export AppendEntries RPC latencies histogram
- [x] **6.1.8** Export RequestVote RPC latencies histogram
- [x] **6.1.9** Export snapshot statistics
- [x] **6.1.10** Expose /metrics endpoint

### Stage 6b — Tracing

- [x] **6.2.1** Integrate OpenTelemetry
- [x] **6.2.2** Add spans for RequestVote RPC
- [x] **6.2.3** Add spans for AppendEntries RPC
- [x] **6.2.4** Add spans for snapshot operations
- [x] **6.2.5** Propagate trace context

### Stage 6c — Logging

- [x] **6.3.1** Use structured JSON logging (zap)
- [x] **6.3.2** Add node ID to all log fields
- [x] **6.3.3** Add term to all log fields
- [x] **6.3.4** Add configurable log levels

---

## Phase 7: Security

### Stage 7a — Transport Security

- [x] **7.1.1** Generate TLS certificates for development (scripts/certs/generate.sh)
- [x] **7.1.2** Implement mTLS between nodes (gRPC transport; TCP transport is plain-text)
- [x] **7.1.3** Implement client certificate verification (gRPC transport with mTLS)
- [ ] **7.1.4** TLS for default TCP transport (currently plain-text; gRPC transport has full mTLS)

### Stage 7b — Access Control

- [x] **7.2.1** Add token-based authentication for admin endpoints
- [x] **7.2.2** Implement basic RBAC for admin actions
- [x] **7.2.3** Add audit logging for membership changes
- [x] **7.2.4** Add audit logging for snapshot operations

---

## Phase 8: Testing & Chaos

### Stage 8a — Test Harness

- [x] **8.1.1** Build harness to start N raftd processes
- [x] **8.1.2** Implement process spawning and management
- [x] **8.1.3** Implement port allocation for test nodes
- [x] **8.1.4** Implement in-process cluster for faster tests

### Stage 8b — Integration Tests

- [x] **8.2.1** Test 3-node cluster with 10k commands
- [x] **8.2.2** Test follower crash and recovery
- [x] **8.2.3** Test committed entries present on majority after recovery
- [x] **8.2.4** Test membership change under partition
- [x] **8.2.5** Test no data loss during membership change

### Stage 8c — Chaos Testing

- [ ] **8.3.1** Implement network partition simulation (process kill/restart only; OS-level partition via iptables/tc netem not yet implemented)
- [x] **8.3.2** Implement leader crash injection (SIGKILL + restart via harness)
- [x] **8.3.3** Test: verify at most one leader per term
- [x] **8.3.4** Test: verify committed entries not lost after leader crash + recovery
- [x] **8.3.5** Test: verify follower restart catches up via log replication

---

## Phase 9: UI & UX

### Stage 9a — Admin UI

- [x] **9.1.1** Initialize React + TypeScript + Vite project
- [x] **9.1.2** Setup Tailwind CSS
- [x] **9.1.3** Implement cluster topology visualization
- [x] **9.1.4** Show leader, followers, learners
- [x] **9.1.5** Show replication lag visualization
- [x] **9.1.6** Implement metrics dashboard with charts
- [x] **9.1.7** Integrate Prometheus queries for charts
- [x] **9.1.8** Implement log viewer with live tail
- [x] **9.1.9** Implement snapshot management UI
- [x] **9.1.10** Implement node control UI (start/stop/restart)
- [x] **9.1.11** Add authentication to UI

### Stage 9b — Documentation

- [x] **9.2.1** Write README with project overview
- [x] **9.2.2** Write architecture.md with diagrams
- [x] **9.2.3** Document API endpoints
- [x] **9.2.4** Write operations runbook
- [x] **9.2.5** Document deployment procedures

---

## Phase 10: Performance & Operations

### Stage 10a — Benchmarks

- [x] **10.1.1** Add AppendEntries throughput benchmark
- [x] **10.1.2** Add apply latency benchmark
- [x] **10.1.3** Add snapshot creation benchmark
- [x] **10.1.4** Profile and optimize hot paths

### Stage 10b — Docker & Kubernetes

- [x] **10.2.1** Build Docker image for raftd
- [x] **10.2.2** Create Dockerfile with multi-stage build
- [x] **10.2.3** Create Helm chart for Kubernetes
- [x] **10.2.4** Add StatefulSet configuration
- [x] **10.2.5** Add service configuration

---

## Phase 11: Release & Maintenance

### Stage 11a — CI/CD

- [x] **11.1.1** Setup GitHub Actions workflow
- [x] **11.1.2** Run unit tests on PR
- [x] **11.1.3** Run integration tests on merge
- [x] **11.1.4** Run nightly chaos tests
- [x] **11.1.5** Build release artifacts

### Stage 11b — Release

- [x] **11.2.1** Establish semantic versioning
- [x] **11.2.2** Create release workflow
- [x] **11.2.3** Add version info to binary
- [x] **11.2.4** Document breaking change policy

---

---

## Phase 12: etcd-lite Distributed KV Store

### Stage 12a — Versioned KV FSM

- [x] **12.1.1** Add `KeyValue` struct with create/mod revision, version counter
- [x] **12.1.2** Add global revision counter (increments on mutations, never reads)
- [x] **12.1.3** Implement `EventPut` / `EventDelete` event types with prev_kv
- [x] **12.1.4** Ring-buffer event history (cap 1024) for late-subscriber replay
- [x] **12.1.5** Buffered `eventCh` (cap 512) for non-blocking FSM→watcher fan-out
- [x] **12.1.6** Backward-compatible "set"/"get"/"delete"/"list" ops (all old tests pass)
- [x] **12.1.7** New "put" op returns full `KeyValue` JSON
- [x] **12.1.8** "range" op — prefix scan returning `[]*KeyValue`
- [x] **12.1.9** "txn" op — atomic compare-and-swap; single revision increment per txn
- [x] **12.1.10** Snapshot backward compat: Restore() tries new format, falls back to old

### Stage 12b — Transactions

- [x] **12.2.1** `TxnRequest` / `Compare` / `TxnOp` / `TxnResponse` types in `pkg/fsm/txn.go`
- [x] **12.2.2** Compare targets: value, version, create_revision, mod_revision
- [x] **12.2.3** Compare operators: equal, not_equal, greater, less
- [x] **12.2.4** Success/failure op lists (put / delete)
- [x] **12.2.5** `EncodeTxn` / `DecodeTxnResult` wire helpers

### Stage 12c — Watch API

- [x] **12.3.1** `WatchManager` in `pkg/fsm/watch.go` — fan-out goroutine from eventCh
- [x] **12.3.2** `Watch(ctx, key, sinceRevision)` — exact key match, history replay
- [x] **12.3.3** `WatchPrefix(ctx, prefix, sinceRevision)` — prefix match
- [x] **12.3.4** Non-blocking dispatch — slow subscribers drop live events (reconnect via Last-Event-ID)
- [x] **12.3.5** Context-driven cancel / cleanup with no goroutine leak
- [x] **12.3.6** Snapshot-revision deduplication (no duplicate live+history delivery)

### Stage 12d — HTTP v1 API

- [x] **12.4.1** `GET/PUT/DELETE /v1/kv/{key}` — CRUD with leader forwarding on writes
- [x] **12.4.2** `GET /v1/kv?prefix=` — stale range query served from local FSM
- [x] **12.4.3** `POST /v1/txn` — CAS transaction with leader forwarding
- [x] **12.4.4** `GET /v1/watch?key=|prefix=` — SSE stream from any node (no leader forwarding)
- [x] **12.4.5** `GET /v1/status` — node_id, state, leader, term, last_index, applied_index, revision
- [x] **12.4.6** `?consistency=stale` query param — bypass Raft, read local FSM directly
- [x] **12.4.7** `Last-Event-ID` SSE header for auto-reconnect with history replay
- [x] **12.4.8** `ensureLeader()` helper — centralised leader-forwarding for all write endpoints

### Stage 12e — Client Library v2

- [x] **12.5.1** `KVPair`, `ClientTxnRequest/Response`, `ClientWatchEvent` types
- [x] **12.5.2** `Put`, `GetKV`, `GetKVStale`, `DeleteKV`, `Range`, `Txn` methods
- [x] **12.5.3** `Watch(ctx, key)` / `WatchPrefix(ctx, prefix)` returning `<-chan ClientWatchEvent`
- [x] **12.5.4** `WithRevision(rev)` watch option for history replay
- [x] **12.5.5** `watchLoop` auto-reconnect with exponential backoff (100ms→…→30s)
- [x] **12.5.6** SSE streaming parser with `bufio.Scanner` and `Last-Event-ID` header

### Stage 12f — kvctl CLI Tool

- [x] **12.6.1** `cmd/kvctl/main.go` — standalone binary using pkg/client
- [x] **12.6.2** `put`, `get`, `delete`, `range`, `txn`, `watch`, `status` subcommands
- [x] **12.6.3** `--endpoints`, `--timeout`, `--stale` flags
- [x] **12.6.4** `watch` auto-reconnects with `--revision={lastSeen}`
- [x] **12.6.5** `txn` reads JSON from file or stdin

### Stage 12g — Tests

- [x] **12.7.1** `pkg/fsm/kv_test.go` — 19 tests: versioned KV, revision, range, events, CAS, snapshot, backward compat
- [x] **12.7.2** `pkg/fsm/watch_test.go` — 8 tests: exact-key, delete, prefix, cancel, history replay, concurrent, txn batch, large prefix
- [x] **12.7.3** `tools/testharness/integration_test.go` — `TestV1API` (CRUD/Range/TxnCAS/LinearizableGet/LeaderForwarding) + `TestV1WatchAPI`

---

## Phase 13: Production Hardening

### Stage 13a — Linearizable Read Optimization

- [x] **13.1.1** `ReadIndex(ctx) (uint64, error)` method on Raft interface
- [x] **13.1.2** Leader lease via `heartbeatAcks map[ServerID]time.Time` (updated on successful AppendEntries)
- [x] **13.1.3** Lease window = `electionTimeout / 2`; single-node fast-path
- [x] **13.1.4** `waitApplied(ctx, idx)` polls AppliedIndex (5ms interval) until FSM catches up
- [x] **13.1.5** `/v1/kv GET` (linearizable) uses ReadIndex + waitApplied + local FSM read (no WAL write)
- [ ] **13.1.6** ReadIndex unit tests (single-node lease, multi-node quorum, step-down clears acks)

### Stage 13b — Rate Limiting & Request Guards

- [x] **13.2.1** Token-bucket rate limiter (`writeLimiter`) — configurable RPS (default 500/s)
- [x] **13.2.2** `rateLimitMiddleware` — 429 Too Many Requests on write endpoints
- [x] **13.2.3** `http.MaxBytesReader` on all write endpoints (default 1 MiB limit)
- [x] **13.2.4** `RateLimitRPS` and `MaxRequestBodyBytes` in YAML config
- [x] **13.2.5** HTTP server timeouts: ReadTimeout=30s, WriteTimeout=60s, IdleTimeout=120s
- [ ] **13.2.6** Per-IP rate limiting (current is global per-server)

### Stage 13c — gRPC Transport as Production Default

- [x] **13.3.1** gRPC transport exists with mTLS, connection pooling (4 conns/peer), keepalive
- [x] **13.3.2** Wire gRPC transport via config (`transport: grpc|tcp`)
- [x] **13.3.3** Add `tls_cert`, `tls_key`, `tls_ca` YAML fields for gRPC mTLS
- [ ] **13.3.4** Integration tests with gRPC transport (gRPC wired; no dedicated grpc integration test yet)

### Stage 13d — Snapshot Integrity

- [x] **13.4.1** WAL CRC32 per record (existing)
- [x] **13.4.2** WAL fsync on segment rotation (existing — `rotateSegment` calls `Sync()`)
- [x] **13.4.3** Snapshot file CRC32 footer on write; verify on open (backward-compat with legacy snapshots)
- [x] **13.4.4** Corrupted snapshot detection: CRC32 mismatch returns error on Open (caller falls back to previous snapshot via List())

### Stage 13e — Transaction UI

- [x] **13.5.1** "Transactions" tab in KVExplorer.tsx
- [x] **13.5.2** Compare-condition builder (key / target / operator / value)
- [x] **13.5.3** Success/failure op lists (put / delete), both with add/remove
- [x] **13.5.4** Submit via `kvTxn()` API; display TxnResponse (succeeded, results, revision)

---

## Acceptance Tests (Quick Verification)

- [x] **AT-1** Run 3-node cluster, submit 10k commands, crash follower, recover — verify committed entries present on majority
- [x] **AT-2** Test membership change add/remove under partition: no data loss, no split-brain
- [x] **AT-3** Chaos test with partitions + leader crash: verify at most one leader per term and committed entries not lost
- [x] **AT-4** Snapshot restore of a large FSM (>100MB) succeeds and subsequent replication continues

---

## Dependencies Summary

| Package | Purpose | Version |
|---------|---------|---------|
| go.etcd.io/bbolt | Embedded KV store | v1.4.x |
| google.golang.org/grpc | RPC framework | v1.79.x |
| google.golang.org/protobuf | Protocol buffers | v1.36.x |
| github.com/prometheus/client_golang | Prometheus metrics | v1.23.x |
| go.uber.org/zap | Structured logging | v1.26.x |
| go.opentelemetry.io/otel | OpenTelemetry tracing | v1.41.x |

---

## File Structure

```
/Users/sanskar/dev/Research/Projects/Raft-Consensus/
├── cmd/
│   └── raftd/
│       └── main.go            # Server binary with HTTP API, auth middleware
├── pkg/
│   ├── raft/
│   │   ├── raft.go            # Core state machine (election, replication, joint consensus, pre-vote, leadership transfer)
│   │   ├── raft_test.go       # 47 tests + 4 benchmarks
│   │   ├── types.go           # Interfaces, types, Config (StartAsLearner, ReplaceServer)
│   │   └── joint.go           # Joint consensus types and helpers
│   ├── storage/
│   │   ├── wal.go             # Write-ahead log (segments, CRC32, in-memory index)
│   │   ├── wal_test.go        # 18 tests
│   │   ├── snapshot.go        # File-based snapshot store
│   │   ├── verify.go          # Log consistency verification
│   │   └── stable.go          # bbolt-backed stable store
│   ├── transport/
│   │   ├── tcp.go             # JSON-over-TCP transport (primary)
│   │   └── grpc.go            # gRPC transport with mTLS support
│   ├── client/
│   │   └── client.go          # Client library (quorum/stale/lease reads)
│   ├── fsm/
│   │   ├── kv.go              # In-memory KV store FSM
│   │   └── kv_test.go         # 10 tests
│   ├── metrics/
│   │   └── raft.go            # Prometheus metrics
│   ├── tracing/
│   │   ├── otel.go            # OpenTelemetry provider setup
│   │   └── spans.go           # Span helpers for RequestVote, AppendEntries, Snapshot
│   └── version/
│       └── version.go         # Build-time version info (ldflags injectable)
├── proto/
│   └── raft.proto             # Protobuf definitions
├── tools/
│   └── testharness/
│       └── harness.go         # Process-based test harness
├── charts/
│   └── raft/
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│           ├── statefulset.yaml
│           ├── service.yaml   # ClusterIP + headless services
│           └── configmap.yaml
├── scripts/
│   └── certs/
│       └── generate.sh        # TLS/mTLS certificate generation
├── docs/
│   ├── architecture.md        # Full architecture documentation
│   ├── runbook.md             # Operations runbook
│   ├── deployment.md          # Bare-metal, Docker, Kubernetes deployment
│   └── versioning.md          # Semantic versioning and breaking change policy
├── .github/
│   └── workflows/
│       ├── ci.yml             # CI: tests + docker build (with nightly schedule)
│       └── release.yml        # Release: multi-platform binaries + GitHub Release
├── .gitignore
├── Dockerfile                 # Multi-stage build with VERSION ARG
├── CHANGELOG.md               # Keep a Changelog format
├── go.mod / go.sum
├── README.md
├── CHECKLIST.md
└── SPEC.md
```

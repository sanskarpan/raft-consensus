# Enhancements & Roadmap

An in-depth, research-backed catalog of possible enhancements, additions, and
improvements for this Raft consensus + KV store. Every item below is an
**enhancement over already-correct, already-audited code** — this is not a bug
list (see `AUDIT.md` / `ISSUES.md` for the closed defect history). It was
produced by a subsystem-by-subsystem audit comparing this codebase against the
Raft literature (Ongaro's dissertation, etcd/raft, HashiCorp raft, Dragonboat,
TiKV) and state-of-the-art KV stores (etcd, Consul, TiKV, FoundationDB).

**Legend** — Impact: **H**igh / **M**edium / **L**ow (operational or user value).
Effort: **S**mall (hours–days) / **M**edium (days–weeks) / **L**arge (weeks+).
Status: ✅ shipped · 🔲 open (tracked as a GitHub issue).

---

## ✅ Progress log

Items resolved since this catalog was written (each shipped as an issue-linked,
CI-green PR merged to `main`):

| Item | PR / Issue |
|---|---|
| Multi-chunk InstallSnapshot reassembly (**bug** — >1 MiB snapshots restored from only the last chunk) | #194 / #193 |
| Vote-grant durability (**bug** — vote reported granted before durable persist) | #196 / #195 |
| Dead nightly chaos/soak job → moved to `ci.yml` cron | #196 |
| Dead metrics wired (`RecordInstallSnapshotLatency`, `raft_snapshot_size_bytes`) | #196 |
| Module path renamed → `github.com/sanskarpan/raft-consensus` (now `go`-gettable) | #197 |
| CheckQuorum — leader steps down on lost quorum | #224 / #198 |
| Disruptive-server vote rejection (§4.2.3, gated on CheckQuorum) | #225 / #199 |
| Go/Process Prometheus collectors | already satisfied by the default registry (#210 closed) |
| `raft_leader_changes_total` + `raft_proposal_commit_latency_seconds` | #226 / #211 |
| Grafana dashboard JSON + PrometheusRule alerts (+ `checkQuorum` Helm wiring) | #226 / #212 |

The remaining items below are tracked as open GitHub issues (labels `enhancement`
+ `area/*`).

---

## ⚠️ Verify-first items — ALL RESOLVED ✅

These surfaced during the audit as correctness-adjacent / dead code and have all
been investigated and fixed (see the progress log above):

| Item | Resolution |
|---|---|
| Multi-chunk InstallSnapshot reassembly | ✅ **Confirmed real bug**; receiver now reassembles Offset-ordered chunks; gRPC server preserves per-chunk offset. Regression tests added. |
| Dead nightly chaos/soak job | ✅ Moved to `ci.yml` (daily cron); now runs. |
| Dead metric: InstallSnapshot latency | ✅ Wired on the send path. |
| Dead UI metric reference (`raft_snapshot_size_bytes`) | ✅ Metric now exported (send + restore paths). |
| Module path vs. import path mismatch | ✅ Module renamed to match the repo; all imports/ldflags/docs updated. |
| Vote-grant persistence variant | ✅ **Confirmed real bug**; grant now uses the error-checked persist and denies the vote on failure. |

---

## Prioritized roadmap (cross-cutting synthesis)

### Phase 0 — Verify & quick hygiene (do first)
Verify-first items above + these near-zero-effort wins:
- Register Go/Process Prometheus collectors (runtime stats currently absent).
- Add `raft_leader_changes_total` (the canonical leader-instability SLI).
- Ship a machine-readable `PrometheusRule` (prose alerts already exist in `docs/operations.md`).
- PodDisruptionBudget + pod anti-affinity/topology-spread in the Helm chart (quorum-critical).
- Add package-level `doc.go` comments (makes the library discoverable on pkg.go.dev).

### Phase 1 — High-leverage features (S–M effort, H impact)
- **Raft:** CheckQuorum leader step-down; disruptive-server vote rejection on the real-vote path; true replication flow-control (`MaxInflight` window) + AppendEntries pipelining.
- **Transport:** message compression (gzip/zstd) on AppendEntries + snapshots; TLS cert rotation without restart; separate `admin` RBAC role for membership/snapshot ops.
- **Client/API:** range pagination (`limit`/cursor); TTL/lease key expiry; atomic counter op; client leader-aware routing for v2 writes; conditional PUT (`If-Match`).
- **Observability:** commit-latency histogram; broaden write-path distributed tracing; Grafana dashboard JSON; ServiceMonitor.
- **Deployment:** PreStop hook + grace period; Secret-based tokens/TLS (not ConfigMap); container image publishing + signing in CI.
- **Testing:** Go native fuzzing for on-disk parsers; golden-file format tests; codecov gate; reconnect the nightly soak.
- **Perf/DX:** default-TCP JSON→binary wire format; `sync.Pool` for hot buffers; `examples/` dir + runnable godoc examples; fix the module path.

### Phase 2 — Medium features
Storage group-commit/batched fsync; snapshot compression + streaming restore; MVCC historical reads + compaction; watch filters/progress-notifications/compaction-gap signal; OTLP metric export + exemplars; scheduled S3/GCS backup + restore tooling; kvctl admin/member commands + output formats; expanded Jepsen nemeses (partition/clock-skew/disk-fault) + Elle transactional checking; deterministic simulation (DES) harness.

### Phase 3 — Large bets
Kubernetes Operator/CRD for lifecycle; multi-Raft/sharding; witness/arbiter replicas; TLA+/Ivy formal spec in CI; gRPC client SDK with streaming watch; pluggable storage backend; SPIFFE/OIDC identity.

---

## 1. Raft consensus core (`pkg/raft/`)

**Already present:** pre-vote, leadership transfer (TimeoutNow), learners + promotion,
joint-consensus membership with dual-quorum, at-most-one-outstanding-config-change,
heartbeat-confirmed ReadIndex (§6.4), snapshot streaming/chunking, conflict-term
fast backup, group-commit proposal batching, continuous replication drain,
pending-future backpressure, async apply + WaitApplied, self-removal step-down,
fatal-halt on storage/FSM panic, log compaction.

| Title | Impact | Effort | Rationale | Where |
|---|---|---|---|---|
| ✅ CheckQuorum (step down on lost quorum) | H | M | **Shipped (#198).** Leader steps down if < quorum voters acked within an election timeout. Opt-in via `Config.CheckQuorum`. | `tickLeader`, `heartbeatAcks` |
| ✅ Disruptive-server vote rejection (real path) | H | S | **Shipped (#199).** §4.2.3 guard gated on CheckQuorum; bypassed by a `LeaderTransfer` flag. | `handleRequestVote` |
| True inflight/flow-control window | M | M | `MaxInflight` only sizes the pending-future cap; actual AppendEntries has no per-follower unacked-entry cap, so a fast leader can flood a slow follower. | `replicateOnce`, `nextIndex`/`matchIndex` |
| AppendEntries pipelining | M | M | Replication is strictly request→response→next; no multiple in-flight batches per follower, capping throughput at batch/RTT. | `replicateTo`/`replicateOnce` |
| Honor `MaxSizePerMsg` in batching | M | S | Batching is by entry count (100), ignoring byte size — large entries make oversized RPCs; tiny entries under-fill. | `replicateOnce` |
| Proposal forwarding to leader | M | M | `DisableProposalForwarding` config exists but no forwarding is implemented; follower `Apply` always returns `ErrNotLeader`. | `Apply`; transport propose RPC |
| Lease-based ReadIndex fast path | M | M | `ReadIndex` always costs a heartbeat round-trip; an opt-in clock-lease read serves with zero round-trips when clocks are trusted. | `ReadIndex` |
| Batched/queued ReadIndex | M | M | Each `ReadIndex` triggers its own heartbeat; batch pending reads behind one confirmation round (etcd readIndexQueue). | `ReadIndex`, `triggerHeartbeat` |
| Concurrent snapshot (don't hold `r.mu`) | M | M | `processSnapshot` calls `fsm.Snapshot()` under `r.mu.RLock()`, stalling all ops for the snapshot duration on a large FSM. | `processSnapshot` |
| Async/parallel FSM apply pipeline | M | L | Apply is strictly serial in `run()`; a slow `FSM.Apply` blocks elections/replication. Decouple via a dedicated apply goroutine. | `run`/`applyCommitted` |
| Priority-based election/transfer | M | M | No node priority; transfer picks purely by `matchIndex`. Bias toward preferred (same-AZ/higher-spec) nodes. | `randomElectionTickCount`, `doLeadershipTransfer` |
| Witness / arbiter replica | M | L | Only voter/learner roles; a vote-only no-data replica gives cheap tie-breaking for 2-DC deployments. | `types.go` role; `advanceCommitIndex` |
| Snapshot-in-flight guard per follower | M | S | `sendSnapshotTo` can be re-entered, streaming overlapping full snapshots to one lagging follower. | `sendSnapshotTo` |
| Single-step config change (skip joint) | M | M | Every add/remove uses full joint consensus; single-server deltas are safe single-step (`ChangeAuto` enum unused). | `AddServer`/`RemoveServer` |
| Learner auto-promotion when caught up | M | M | Promotion is manual; auto-promote once `matchIndex` reaches threshold. | `tickLeader`, `PromoteLearner` |
| Adaptive replication backoff | M | S | A persistently-failing follower is retried every heartbeat with no backoff. | `replicateOnce` error path |
| Per-entry proposal size cap | M | S | `handleProposal` appends arbitrarily large data; one huge proposal can stall followers. | `handleProposal` |
| Per-node seeded election RNG | M | S | `randomElectionTickCount` uses global `math/rand`; identical binaries can pick correlated timeouts → split votes. | `randomElectionTickCount` |
| Expose apply/append batch tunables | L | S | `maxBatch`/`maxProposalBatch` are hardcoded consts; expose as `Config`. | consts; `Config` |
| Follower/learner reads | L | M | `ReadStale` enum unused; a caught-up follower could offload the leader. | new follower-read path |
| Adaptive election timeout under load | L | M | Fixed timeouts cause spurious elections under GC/load spikes. | `tickFollower` |
| Multi-Raft / shard grouping | L | L | Single-group only; a multi-raft manager (shared tick) scales to many groups per node. | new layer |
| Unsafe forced reconfiguration (DR) | L | S | No operator recovery path when quorum is permanently lost. | new recovery API |

---

## 2. Storage & durability (`pkg/storage/`)

**Already present:** per-batch fsync + directory fsync, CRC32 on records & snapshots,
torn-tail vs. mid-file-corruption discrimination, allocation-DoS bounds, segmented
WAL (64 MiB), positional concurrent reads, atomic snapshot create, retention/pruning,
bbolt StableStore, log consistency verifier, WAL fsync metric.

| Title | Impact | Effort | Rationale | Where |
|---|---|---|---|---|
| Group-commit / batched fsync | H | M | `Append` fsyncs under the global write lock, serializing every concurrent proposal into its own fsync. Coalesce waiters → one fsync per batch. | `Append`, `fsyncCurrentLocked` |
| ✅ Receiver-side chunk reassembly | H | M | **Shipped (#194)** — was a real bug; receiver now reassembles Offset-ordered chunks. | `restoreSnapshotData` |
| Snapshot compression (gzip/zstd) | H | M | Snapshots written raw; FSM state is highly compressible — cuts disk + InstallSnapshot transfer. | `fileSnapshotSink.Write`/`Close` |
| WAL segment preallocation (`fallocate`) | H | M | Segments grow record-by-record; preallocate to avoid fragmentation, cut fsync-path metadata updates, fail early on ENOSPC. | `createSegment`/`rotateSegment` |
| crc32c / xxhash checksums | M | S | CRC32/IEEE has no hardware acceleration; Castagnoli (crc32c) uses the CPU CRC instruction (~5–10×). | WAL + snapshot checksum sites |
| Storage metrics beyond fsync | M | S | No metrics for WAL bytes, segment rotations, batch size, snapshot bytes/duration, compaction counts, fsync errors. | `wal.go`, `snapshot.go`, `metrics` |
| Disk-space monitoring + ENOSPC backpressure | M | M | Nothing checks free space; ENOSPC surfaces mid-record. Add statfs gauge + low-space rejection + quota. | `appendEntry`, `Create` |
| bbolt tuning (`NoFreelistSync`, map freelist, batch) | M | S | StableStore/meta open bbolt with only a timeout; tiny frequently-written state benefits from freelist + batch tuning. | `NewWAL`, `NewStableStore` |
| mmap sealed-segment reads | M | M | `ReadAt` is a syscall per read on the replication hot path; mmap sealed segments for zero-syscall reads. | `getEntry`, `openSegment` |
| Hot-entry read cache (LRU) | M | S | Followers repeatedly re-read + CRC the same tail entries; a small decoded-entry LRU cuts disk + CRC work. | `getEntry`, `Iterate` |
| Streaming snapshot restore | M | M | Restore writes full snapshot → close → reopen → `fsm.Restore`; stream directly into Restore (tee for durability). | `restoreSnapshotData` |
| Configurable durability modes (O_DSYNC/async/NoSync) | M | M | Durability is hard-coded per-batch `Sync()`; offer O_DSYNC, eventual-flush, and NoSync (tests). `WALOptions.SegmentSize` is even ignored. | `WALOptions`, `Append` |
| Incremental / differential snapshots | M | L | Always full snapshots; a base+delta scheme reduces write amplification + transfer. | `Create`, `sidecarMeta` |
| Corruption-repair / salvage tooling | M | M | `ErrMidSegmentCorruption` refuses to open with no recovery path; add offline scan/salvage of the valid prefix. | new tool; `rebuildIndex` |
| Backup/restore tooling (hot backup + PITR) | M | M | No atomic WAL+snapshot+stable-store copy; add `Backup(w)`/restore (bbolt `WriteTo` + sealed segments + latest snapshot). | new `Backup`/`Restore` |
| Encryption-at-rest (AES-GCM) | M | M | WAL records + snapshots are plaintext; add optional envelope encryption keyed via `sidecarMeta`/`meta.db`. | encode/decode + snapshot write |
| Compaction scheduling + backpressure | M | M | `Compact`/`DeleteRange` are caller-driven under the write lock, stalling appends; add a background scheduler that yields the lock. | `Compact`, `deleteRangeLocked` |
| O(1) per-segment index bounds | M | S | `segmentIndexBounds`/`segmentEntryCount`/compaction scan the whole index map (O(n)); keep per-segment min/max/count. | index helpers |
| Honor `WALOptions.SegmentSize` | M | S | Accepted but never read; wire it through so segment size is tunable. | `NewWAL`, `appendEntry` |
| Pluggable storage backend | M | L | WAL is one concrete impl; formalize a backend registry (RocksDB/LSM/in-memory) atop `raft.LogStore`. | `wal.go`, `StableStore` |
| Snapshot bandwidth limiting | L | S | Chunks stream at full speed, starving normal AppendEntries; add a token bucket. | `sendSnapshotTo` |
| Direct I/O (O_DIRECT) option | L | M | Opt-in predictable latency for write-heavy WAL, bypassing page cache. | `createSegment` |
| Cold-storage tiering for sealed segments | L | L | Sealed segments/old snapshots are only deleted; add an S3/GCS archive hook. | `deleteRangeLocked`, `pruneLocked` |
| Sealed-segment compression | L | M | Immutable sealed segments stored uncompressed. | `rotateSegment` |
| Integrity background scrubber | L | S | CRCs only checked on open; a periodic scrubber surfaces bit-rot in retained-but-unopened data. | `verifysnapChecksum`, segment CRCs |

---

## 3. Transport, networking & security (`pkg/transport/`, auth in `cmd/raftd`)

**Already present:** mTLS with TLS 1.3 floor, per-peer SNI verification, fail-closed TLS,
peer-identity allowlist, key-permission checks, gRPC message-size limits, connection
pooling (gRPC + TCP), matched keepalives, background reconnect, TCP request-ID
correlation, accept-loop hardening, graceful shutdown, HTTP API auth (fail-closed,
constant-time, RBAC), rate limiting, CORS deny-by-default, request-ID propagation.

| Title | Impact | Effort | Rationale | Where |
|---|---|---|---|---|
| Message compression (gzip/zstd) | H | S | No compressor registered; log batches + 512 MiB snapshots cross the wire uncompressed. | gRPC opts; TCP send/recv |
| Streaming/chunked InstallSnapshot client | H | M | The gRPC client sends the whole snapshot as one `stream.Send` frame (buffered fully in memory); chunk it. | `GrpcTransport.InstallSnapshot` |
| TLS certificate rotation without restart | H | M | Certs load once into immutable `tls.Config`; rotation requires a full restart (drops quorum). Use `GetCertificate` callbacks + reload. | `NewGrpcTransport*`, `SetTLS`, TCP TLS |
| Separate `admin` RBAC role | M | M | Only read/write; a `write` token can add/remove servers. Split membership/snapshot ops into an `admin` scope. | `requireRole`, admin handlers |
| OTel trace propagation over transport | M | M | Neither transport injects/extracts trace context; traces break at every inter-node hop. | gRPC StatsHandler; TCP message struct |
| gRPC health service | M | S | No `grpc_health_v1`; peers/LBs/k8s can't gRPC-probe the Raft port. | `NewGrpcTransport*` |
| Per-peer inbound rate limiting | M | M | No per-peer RPC rate limit; a misbehaving peer can flood AppendEntries. | auth interceptors; TCP serve |
| Circuit breaker / hedging | M | M | Fixed reconnect interval, no half-open probe, no request hedging for tail latency. | `peerConn`, `clientFor` |
| Request signing + replay protection | M | M | Authenticity rests on mTLS + allowlist; no per-message HMAC/nonce. In plaintext/proxy-terminated mode, forged AppendEntries are possible. | RPC envelopes |
| HTTP/2 window / buffer tuning | M | S | Default 64 KiB windows throttle high-BDP snapshot streams; tune `InitialWindowSize`/buffers. | server opts |
| Connection warmup / eager dial | M | S | `grpc.NewClient` is lazy; first replication RPC pays full handshake latency. | `AddPeer` |
| DNS-based peer discovery (A/SRV) | M | M | Peers come only from static config; add a resolver + periodic re-resolve for rolling-IP/k8s. | `initRaft`, `AddPeer` |
| RPC deadline propagation (gRPC) | M | S | Transport imposes its own fixed timeouts; honor the sooner of caller ctx and default (TCP already does). | gRPC call sites |
| gRPC metadata/token auth (defense-in-depth) | M | M | Auth is purely mTLS; add an optional shared-secret/JWT interceptor for proxy-terminated/plaintext modes. | `peerAuthorized` |
| Audit logging of admin/security events | M | S | No tamper-evident audit stream for membership/snapshot/auth-failure events. | admin handlers, transport rejections |
| OIDC/JWT bearer auth for HTTP API | M | L | Static shared tokens only; add OIDC discovery + JWT with claims→role. | `authMiddleware` |
| mTLS on the leader-forward hop | M | S | `forwardToLeader` uses a plain client with no client cert / pinned CA. | `forwardToLeader` |
| Transport-level metrics | M | S | Transport records nothing to Prometheus (per-RPC latency/errors/bytes). | interceptors |
| Network partition detection + metrics | M | M | Failure tracked only as reconnect `failCount`; no quorum-loss/per-peer reachability signal. | `peerConn`, `/ready` |
| SPIFFE/SVID identity | M | L | Identity is raw CN/SAN string matching; SPIFFE gives short-lived rotated identities. | peer authz |
| Configurable pool size / gate insecure ctor | L | S | Pool sizes hardcoded; `NewGrpcTransportInsecure` always available. | consts |
| IPv6 SNI/dial audit + tests | L | S | Bare IPv6 literals may mis-derive ServerName; no `[::1]:port` test coverage. | `serverNameFor`, `dialPeer` |
| Admin `GetSnapshot` chunking | L | S | Sends whole snapshot in a single stream frame despite streaming signature. | `grpcAdminHandler.GetSnapshot` |
| Implement or drop `MessageStream` RPC | L | M | Declared in proto but returns `Unimplemented`; either implement for pipelined messaging or remove. | `proto/raft.proto` |

---

## 4. Client SDK, KV API & FSM (`pkg/client`, `pkg/fsm`, `cmd/kvctl`)

**Already present:** MVCC versioning metadata, etcd-style mini-transactions (Txn),
compare-and-swap, prefix range scan, watch (key+prefix, SSE, resume, exactly-once
ordering, history replay), idempotency/dedup, read-consistency menu (legacy path),
linearizable reads, jittered retry/backoff, leader-aware routing (SubmitCommand),
FSM snapshot/restore, backpressure counters, watch connection limits, key/value size
limits, kvctl put/get/delete/range/txn/watch/status.

| Title | Impact | Effort | Rationale | Where |
|---|---|---|---|---|
| Range pagination (limit/cursor) | H | M | `Range` hard-caps at 10k and *errors* past it — no limit/continuation. >10k keys under a prefix are unlistable. | `KVStore.Range`, `serveRange`, client `Range` |
| `[key, range_end)` intervals | H | M | Range is prefix-only; etcd's core primitive is a half-open key interval (prefix is a special case). | `Range`, `evalCompare` |
| TTL / lease-based key expiry | H | L | No TTL/lease anywhere — table-stakes for leader election / service registration. Needs deterministic apply-time ticks. | `KeyValue`, `Apply`; lease ops |
| MVCC historical reads (read at revision) | H | L | Revision metadata exists but only the latest version is stored; no time-travel reads (etcd's defining feature). | `KVStore.data`, `Get`, `Range` |
| Atomic counter / increment op | M | S | No server-side INCR/ADD; clients must do racy read-modify-write via Txn. | `Apply` switch |
| Client leader-routing for v2 writes | M | S | `Put`/`DeleteKV`/`Txn` loop over addrs in list order, not preferring the leader hint (unlike SubmitCommand) — every write may pay a forward hop. | client `Put`/`DeleteKV`/`Txn` |
| Conditional PUT/DELETE (If-Match) | M | S | CAS only via full `/v1/txn`; a lightweight `If-Match: <mod_revision>` header skips the txn envelope. | `handleV1Put` |
| Batch get / multi-key get | M | S | `GetKV` is one key per round trip; add `BatchGet`. | client `GetKV` |
| Richer Txn ops (range/get/nested) | M | M | Txn supports only put/delete branches; add range-read/get/nested and lexical value compares. | `applyTxnLocked`, `evalCompare` |
| Watch filters (event-type/value) | M | S | Watch matches only key/prefix; add NOPUT/NODELETE filters + `prev_kv` opt-out. | `watchEntry`, `dispatch` |
| Watch progress notifications | M | S | Idle watches only emit a timeout then close; periodic progress notifications let clients checkpoint without replay. | `handleV1Watch` |
| Watch compaction-gap signal | M | M | History ring is 1024; resuming from an evicted revision silently drops the gap. Return an `ErrCompacted`-style signal. | `GetHistory`, `replayHistory` |
| MVCC compaction API | M | M | No `Compact(revision)`; any MVCC store grows unbounded. | `pkg/fsm/kv.go` |
| Binary/bytes values | M | M | `Value string` everywhere; binary payloads need caller base64. Store `[]byte`. | `KeyValue.Value`, `handleV1Put` |
| Key namespaces / multi-tenancy + quotas | M | M | One flat keyspace; no per-namespace quota. | `pkg/fsm/kv.go` |
| KV export/import (backup CLI) | M | M | `/admin/snapshot` only triggers a Raft snapshot; no download/bulk-load of KV contents. | `handleSnapshot`, `kvctl` |
| kvctl output formats (`-w json|table|simple`) | M | S | Always pretty JSON; no raw-value print for scripting. | `kvctl` `prettyJSON` |
| kvctl admin/member commands | M | S | Server exposes `/admin/*` but client/kvctl expose none — operators must curl. | `kvctl`, client |
| kvctl CAS/txn-builder convenience | M | S | CAS requires hand-authored txn JSON; add `cas`/`put --prev-value`/`del --prefix`. | `kvctl` |
| gRPC client API | M | L | Client is HTTP/JSON + SSE only; a gRPC client gives a real bidi watch stream + lower overhead. | client |
| Endpoint discovery in client | M | M | `c.addrs` is static; the client fetches cluster config but never updates routing from it. | `GetClusterInfo` |
| Consistency menu on v2 path | M | S | ReadQuorum/ReadLease/ReadStale apply only to legacy `get`, not `GetKV`/`Range`. | `GetValueWithConsistency` |
| Smarter retry policy | L | S | Retry is hardcoded and retries any non-404 including 4xx; expose `WithRetryPolicy`, stop on 4xx. | `doWithRetry` |
| `prev_kv` on PUT/DELETE responses | L | S | Computed for watch events but not returned in HTTP responses (saves a prior GET). | `handleV1Put`/`Delete` |
| Range count-only / keys-only | L | S | No cheap prefix sizing or key-listing without values. | `Range`, `handleV1KVList` |
| Secondary indexes / query-by-value | L | L | Only key-prefix lookup; no value queries without a full scan. | `pkg/fsm/kv.go` |
| Watch resume by opaque token | L | S | Resume uses raw revision; an opaque cursor carries filter/compaction state. | `handleV1Watch`, `streamSSE` |

---

## 5. Observability & operability (`pkg/metrics`, `pkg/tracing`, endpoints, UI)

**Already present:** rich Prometheus metrics (term/index/leader/elections/per-target AE
latency/replication lag/fsync/apply lag/proposals/rejections/HTTP duration), gated
`/metrics`, OTel tracing scaffolding + KV write-path spans, health/readiness split,
structured logging + request-ID correlation, auth-gated pprof, `/v1/status`, kvctl,
React dashboard, ops docs, Helm probes.

| Title | Impact | Effort | Rationale | Where |
|---|---|---|---|---|
| ✅ Register Go & Process collectors | H | S | **Already satisfied (#210 closed)** — the default registry already exports `go_*`/`process_*`. | `metrics`, `/metrics` path |
| ✅ `raft_leader_changes_total` | H | S | **Shipped (#211).** Incremented on becomeLeader + observed-leader-change. | `metrics`, becomeLeader/leader-learn |
| ✅ Commit latency histogram (propose→commit) | H | M | **Shipped (#211)** as `raft_proposal_commit_latency_seconds`. | Apply path + commit advance |
| Broaden write-path distributed tracing | H | M | The internal Raft path (Apply→WAL→replicate→commit→apply) isn't traced; handler spans aren't linked to replication. | Apply/replicate/apply, WAL |
| ✅ Grafana dashboard JSON | H | M | **Shipped (#212)** — `docs/grafana-dashboard.json`. | `docs/grafana-dashboard.json` |
| ✅ Prometheus alerting rules (PrometheusRule) | H | S | **Shipped (#212)** — opt-in chart `PrometheusRule`. | `charts/raft/templates/prometheusrule.yaml` |
| ✅ Wire the dead InstallSnapshot latency metric | M | S | **Shipped (#196).** | `metrics`, `raft.go` |
| ✅ Export snapshot-size metric | M | S | **Shipped (#196)** — `raft_snapshot_size_bytes`. | `metrics`, `raft.go` |
| Proposal queue depth / in-flight gauge | M | S | Leading indicator before apply-lag grows. | `metrics`, `raft.go` |
| Entries-applied/committed counters | M | S | Only gauges exist; counters give clean throughput graphs across restarts. | `metrics`, `raft.go` |
| Log-size / compaction metrics | M | M | No last/first index, entry count, or last-compacted index — disk-fill root cause invisible. | `metrics`, `wal.go` |
| Exemplars on histograms | M | M | Wire trace exemplars so Grafana jumps p99→trace. | Observe calls, tracing |
| OTLP metric export | M | M | Metrics are Prom-pull only; a push OTLP pipeline unifies the backend. | new OTel bridge |
| ServiceMonitor + scrape annotations | M | S | No auto-discovery by Prometheus Operator. | chart |
| Startup probe | M | S | Slow WAL replay/restore can trip liveness and crash-loop a recovering node. | chart statefulset |
| Formal SLO doc + recording rules | M | S | No SLI/SLO codification or burn-rate alerts. | new `docs/slo.md` |
| Event/audit log + `/admin/audit` | M | M | No queryable audit stream for admin/auth ops. | admin handlers |
| Runtime config introspection endpoint | M | S | No redacted effective-config view for drift debugging. | `buildMux` |
| `raft_build_info` metric | L | S | No version/commit/go_version gauge to correlate regressions with deploys. | `metrics`, `version` |
| Runtime log-level endpoint | M | S | Level fixed at startup; `zap.AtomicLevel` HTTP handler enables debug under incident. | `main.go` |
| Deepen `/health` semantics | L | S | Always 200 even with a wedged disk (never reflects `Healthy()`). | `handleHealth` |
| Continuous profiling hook | L | M | pprof is on-demand only; Pyroscope/Parca push gives continuous flamegraphs. | debug server |
| Quorum-health aggregate signal + UI badge | M | M | No single "quorum OK/AT-RISK/LOST" gauge/badge. | `metrics`, `/v1/status`, UI |
| UI: historical time-series charts | M | L | Dashboard is instantaneous only (no ring buffer); add trend lines + rate graphs. | `useMetrics`, dashboards |
| UI: membership/admin controls + trace links | M | M | UI is read-only for topology; add add/remove/promote/transfer + request-ID deep-links. | `ClusterTopology`, `client.ts` |

---

## 6. Deployment, operations & lifecycle (`Dockerfile`, `charts/`, CI/CD)

**Already present:** graceful shutdown + leader transfer on SIGTERM, health/readiness
split, membership API, snapshots, metrics + tracing, StatefulSet done right for
consensus (Parallel management, publishNotReadyAddresses, non-root + fsGroup),
hardened distroless container, full CI + GoReleaser (SBOM via syft), Dependabot,
runbooks, cert-gen script, docker-compose, version stamping.

| Title | Impact | Effort | Rationale | Where |
|---|---|---|---|---|
| PodDisruptionBudget | H | S | No PDB; a node drain can evict 2+ of 3 voters and destroy quorum. | new `pdb.yaml` |
| Pod anti-affinity + topology spread | H | S | No affinity; k8s may co-locate 2 of 3 voters on one node/AZ. | statefulset |
| PreStop hook + terminationGracePeriod | H | S | Grace period isn't aligned with the internal drain/transfer budget; no preStop for endpoint deregistration. | statefulset |
| Kubernetes Operator / CRD | H | L | Scaling is manual (`/admin/members`); an operator automates join/leave, reconciliation, rolling upgrades. | new `operator/` |
| Membership reconciliation on reschedule | H | M | Nothing reconciles Raft config with running pods after PVC loss / recreation. | sidecar/init container |
| Scheduled backup to S3/GCS | H | M | Backup is manual `cp -r`; add a CronJob/built-in scheduler with retention. | new `cronjob-backup.yaml` |
| Restore automation / DR bootstrap | H | M | Cluster restore is a manual multi-step procedure; add `kvctl restore` + init-container pull. | `kvctl`, chart |
| Container image publishing + signing | M | S | CI builds with `push: false`; goreleaser ships only archives. Add multi-arch push + cosign + provenance. | `.goreleaser.yml`, release.yml |
| ServiceMonitor / PodMonitor | M | S | Chart ships no ServiceMonitor; Prometheus-Operator won't auto-scrape. | new chart template |
| NetworkPolicy | M | S | No default-deny; peer port should only accept sibling-pod traffic. | new `networkpolicy.yaml` |
| Secret-based tokens/TLS | M | S | `admin_token` renders in plaintext into a ConfigMap; move to Secret / external-secrets. | `configmap.yaml`, new `secret.yaml` |
| Shipped systemd unit (bare-metal) | M | S | Only a doc snippet; ship a hardened `.service` + sysusers/tmpfiles via goreleaser nfpms. | new `packaging/systemd/` |
| PV resize support + storageClass wiring | M | S | Fixed volumeClaim size; `persistence.enabled` is ignored. | statefulset, values |
| Guaranteed-QoS production preset | M | S | Burstable QoS risks CPU throttling → spurious elections; provide requests==limits preset. | values, `docs/tuning.md` |
| HPA-unsuitability guardrail + doc | M | S | Nothing warns that autoscaling voters breaks quorum. | `docs/deployment.md` |
| Config-validation init container | M | M | `node_id: ${HOSTNAME}` must match member IDs; validate + fail fast. | statefulset init container |
| Multi-AZ / multi-region guide + values | M | M | No WAN topology guidance (learners across regions, RTT tuning). | new `docs/topology.md` |
| Rolling-upgrade orchestration in chart | M | M | Default updateStrategy; add partition-based leader-last canary + minReadySeconds. | statefulset, `docs/versioning.md` |
| Chart `_helpers.tpl`, labels, SA, schema, NOTES | M | M | Missing baseline chart hygiene for a publishable chart. | chart |
| Config hot-reload (SIGHUP) | L | M | No hot-reload; changing tokens/log-level/limits needs a restart (→ election). | `main.go` |
| Live chaos/DR harness (Chaos Mesh / kvctl) | L | M | Chaos is only in Go unit tests; no operator-facing harness against a live cluster. | new tooling |
| Cluster discovery / bootstrap tooling | L | M | Bootstrap needs identical static lists; add DNS-SRV/token discovery. | new tooling |
| Capacity-planning doc | L | S | Sizing tables exist but no throughput/fsync/WAL-growth formulas. | `docs/tuning.md` |
| Helm test hooks (smoke test) | L | S | No `helm test` to verify a fresh release forms quorum. | new `templates/tests/` |
| Pin base images by digest + SLSA provenance | L | S | Dockerfile uses mutable tags; digest-pinning left as a comment. | `Dockerfile`, release.yml |

---

## 7. Testing, verification & engineering quality

**Already present:** Porcupine linearizability checker, process-level chaos harness,
in-memory partition injection, all four Raft safety properties in example tests,
adversarial/Byzantine-ish tests, storage fault injection, WAL/snapshot verification,
alloc-reporting benchmarks + one alloc-regression guard, CI (Go-version matrix +
`-race` + vet + golangci-lint v2 + staticcheck + govulncheck + docker), GoReleaser +
SBOM, Dependabot, goroutine-leak assertion, seeded fault selection.

| Title | Impact | Effort | Rationale | Where |
|---|---|---|---|---|
| Go native fuzzing for on-disk parsers | H | M | Three hand-rolled parsers over untrusted bytes have zero fuzz coverage (`nextRecord`, `decodeKVCommand`, `verifysnapChecksum`). | storage + fsm parsers |
| Golden-file format tests | H | S | No `testdata/*.golden`; on-disk formats are only round-trip-tested, so silent drift breaks cross-version compat undetected. | storage + fsm |
| Fix the dead nightly soak/chaos job | H | S | See verify-first — the job never runs. | workflows |
| Deterministic simulation (DES) harness | H | L | No virtual clock/scheduler; cluster tests use wall-clock + sleeps (flaky). A seeded DES gives bit-reproducible runs (FDB model). | new sim package |
| TLA+/Ivy formal spec in CI | H | L | No formal spec; `SPEC.md` lists this as an unmet SHOULD. Gold standard for consensus. | new `spec/raft.tla` |
| Expand Jepsen/Porcupine nemeses | H | M | Only docker pause + SIGKILL; add true partitions, clock skew, disk faults, netem latency/loss, asymmetric partitions. | lincheck, chaos, harness |
| Elle transactional consistency checking | M | M | Porcupine runs a per-key register model only, yet `/v1/txn` is multi-key. | lincheck |
| Fault-injection framework (ENOSPC/read errors) | M | M | Ad-hoc seams; no disk-full or injected recovery read errors. | storage tests |
| Property-based testing (rapid/quick) | M | M | Single hand-picked corruption scenarios; generative properties broaden input space. | storage/fsm/raft tests |
| Codecov upload + coverage gate | M | S | Coverage profile is only an artifact — invisible to reviewers, can't gate. | ci.yml |
| benchstat regression tracking | M | M | Benchmarks exist but CI never runs them; no baseline. | bench targets, CI |
| OS matrix (macOS/Windows) | M | S | Always ubuntu; storage fsync/locking/paths are OS-specific. | ci.yml |
| Mutation testing | M | M | Safety-handler test power is unmeasured; mutation testing quantifies fault-catching. | raft tests |
| Reusable invariant checker | M | M | Safety invariants asserted ad-hoc per test; a `CheckInvariants(cluster)` after every step is a continuous monitor. | raft tests |
| Upgrade/downgrade compat tests | M | M | Format version bytes exist but nothing verifies old↔new replay. | storage/fsm |
| Injectable clock to deflake timing tests | M | M | 19 `time.Sleep` in `raft_test.go` alone; an injectable clock removes most flakiness. | raft tests |
| Deadlock detection in CI | M | S | Only `-race`; add a `go-deadlock` variant or goroutine-dump-on-timeout. | ci.yml |
| Seed-logging + replay for chaos | M | S | Soaks seed 42/7 but don't log the seed or accept `-seed`; failures aren't replayable. | adversarial/lincheck/chaos |
| Branch protection + CODEOWNERS | M | S | No CODEOWNERS or committed required-checks artifact. | `.github/` |
| Membership-churn-under-fault tests | M | M | Config change during leader loss (highest-risk scenario) is untested end-to-end. | testharness, joint tests |
| Separate integration/e2e CI job + artifacts | L | S | Unit + heavy integration run together; lincheck isn't in CI at all. | ci.yml |
| Pin staticcheck/govulncheck versions | L | S | `@latest` makes CI non-reproducible. | ci.yml |
| SIGTERM (graceful) fault variety | L | S | Harness only hard-kills; clean-shutdown flush/transfer untested. | harness, chaos |

---

## 8. Developer experience, docs, tooling & performance

**Already present:** binary command codec, proposal batching + group commit, continuous
replication drain, gRPC pool, protobuf RPCs, AppendEntries batching, idempotency dedup,
snapshots + compaction, split locks + channel WaitApplied, pprof + metrics + tracing +
zap, rich `docs/`, Makefile, golangci-lint v2, CI, Helm, docker-compose, cert-gen,
chaos/lincheck/testharness, CHANGELOG/CONTRIBUTING/SECURITY, benchmarks.

### Performance

| Title | Impact | Effort | Rationale | Where |
|---|---|---|---|---|
| Default-TCP JSON→binary wire format | H | M | The default `transport: tcp` JSON-encodes every RPC (reflection+alloc) on the hot path; reuse proto/binary framing. | `tcp.go` |
| Binary-encode FSM `KvResult` in Apply | H | M | Every op in the serial apply loop `json.Marshal`s the result (reflection) — the most-executed alloc site. | `pkg/fsm/kv.go` |
| Streaming FSM snapshot (no full-map JSON) | H | L | `KVStore.Snapshot()` deep-clones the whole keyspace under RLock then `json.Marshal`s it — O(N) memory spike. | `pkg/fsm/kv.go` |
| `sync.Pool` for hot encode/decode buffers | M | S | No pooling anywhere; AE encode, WAL encode, FSM marshal, snapshot chunk buffers all allocate fresh. | raft/fsm/transport |
| Shard the FSM map | M | L | Single RWMutex over one map; reads contend with the serial writer. Sharded maps scale reads on multi-core. | `pkg/fsm/kv.go` |
| Read-path result caching | M | M | Hot-key linearizable reads re-clone each time; a revision-keyed cache cuts allocations. | `pkg/fsm/kv.go` |
| Sorted index for range scans | L | M | `Range` full-scans the map under RLock; a btree/skiplist makes prefix range O(log N + k). | `pkg/fsm/kv.go` |
| GOMAXPROCS/GC tuning knobs | M | S | No container-aware GOMAXPROCS (bad under CPU limits) or `SetMemoryLimit` (OOM on large snapshots). | `cmd/raftd/main.go` |
| Avoid `fmt.Sprintf("%v")` in `list` op | L | S | Legacy `list` formats the whole slice via reflection in the apply loop. | `pkg/fsm/kv.go` |
| Benchmark regression gate + results page | M | M | Benchmarks never run in CI; no published numbers. | ci.yml, README |

### Developer experience & docs

| Title | Impact | Effort | Rationale | Where |
|---|---|---|---|---|
| Fix module/import path mismatch | H | S | The advertised library is currently un-importable (see verify-first). | `go.mod`, README, docs |
| Package-level `doc.go` comments | H | S | No `// Package …` in any `pkg/*`; pkg.go.dev renders these as the landing page. | `pkg/*/` |
| `examples/` directory | H | M | No runnable examples; single-node embed, 3-node launcher, watch consumer, txn demo cut onboarding cost. | new `examples/` |
| Runnable godoc `Example` functions | M | M | Zero `func Example*`; they render on pkg.go.dev and are compile-checked. | `pkg/client`, `pkg/raft` |
| Publish stable client library surface | H | M | `pkg/client` has a clean API but is undocumented/un-versioned as a lib. | `pkg/client` |
| API stability policy + `internal/` boundary | M | M | Nothing marks public vs internal packages for semver. | `docs/versioning.md`, `pkg/` |
| Automate CHANGELOG/release notes | M | S | Hand-maintained; adopt release-please/git-cliff. | CHANGELOG, goreleaser |
| `.editorconfig` + pre-commit hooks | M | S | Neither exists; add gofmt/goimports/lint hooks. | repo root |
| Developer scripts (cluster up/down) + `make dev` | M | S | `scripts/` has only docker+certs; wrap the manual 3-node run. | `scripts/`, Makefile |
| Dev docker-compose (Prometheus/Grafana/Jaeger) | M | M | Prod-style compose only; bundle observability so contributors see metrics/traces. | `scripts/docker/` |
| Wire the React UI into build/dev + `go:embed` | M | M | `ui/` is fully separate; no `make ui`, not embedded/served by `raftd`. | Makefile, Dockerfile, `ui/` |
| OpenAPI spec + hosted godoc (`make docs`) | M | M | HTTP API is prose-only; an OpenAPI spec enables client-gen + Swagger UI. | `docs/api.md`, new `openapi.yaml` |
| Link SPEC/design docs from README + invariants doc | M | S | `SPEC.md`/`AUDIT.md` aren't linked; the `H-*`/`C*` invariant codes deserve a consolidated rationale doc. | README, SPEC.md |
| Out-of-the-box quickstart (`config-dev.yaml`) | M | S | Quickstart fails closed on auth; ship an `allow_no_auth: true` dev config. | README, configs |
| `buf` for proto (lint/breaking/gen + drift check) | L | S | `make proto` shells to raw `protoc`; no staleness check on committed `*.pb.go`. | Makefile, CI, proto |
| Architecture/onboarding map in CONTRIBUTING | L | S | No "where does a write go / who owns what" map for the 3k-line `raft.go`. | CONTRIBUTING, architecture |
| Interactive demo/playground | L | M | No hosted demo; a pre-seeded compose one-liner or asciinema walkthrough. | new `examples/demo/` |

---

*Generated from an 8-track subsystem audit. Counts are approximate; ~200 distinct
items across 8 domains. Nothing here is a known defect — items marked ⚠️ in the
verify-first section should be confirmed before being scheduled as features vs. fixes.*

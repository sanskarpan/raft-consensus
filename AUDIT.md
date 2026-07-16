# Production-Readiness Audit — raft-consensus

Adversarial, read-only audit across six dimensions (correctness/concurrency, observability,
reliability/ops, security, performance, supply-chain/CI). Baseline verified first:
`go build ./...` ✅, `go vet ./...` ✅, `go test ./... -count=1` ✅, `-race` ✅ (no races),
integration harness ✅, chaos ✅. Coverage: raft 62%, storage 72%, fsm 80%, transport 52%,
client 53%, cmd/raftd 36%.

No regressions found against the 52 previously-remediated issues. Findings below are **new**
production-readiness gaps, prioritized Critical → Low. Severity reflects production blast radius.

---

## CRITICAL

- **C-A1 — Leader never sends `InstallSnapshot`; a follower behind the compaction point can never recover.** `pkg/raft/raft.go` `replicateTo` — the outbound `Transport.InstallSnapshot` is never called anywhere in the core (verified: `grep '\.InstallSnapshot(' pkg/raft` is empty). After snapshot+`Compact`, a lagging follower's `nextIndex` drops below `FirstIndex()`; `replicateTo` errors on `log.Get` and spins forever. Silent fault-tolerance loss. *Fix:* send a chunked snapshot when the needed entry is compacted; set `nextIndex = LastIncludedIndex+1` on success. *Test:* partition a follower, drive writes past the snapshot threshold, heal, assert it catches up.
- **C-S1 — Three reachable dependency/toolchain CVEs.** `govulncheck` exit 3: **GO-2026-4762** (gRPC `:path` authz-bypass, grpc→v1.79.3), **GO-2026-4559** (`x/net` HTTP/2 frame panic DoS, x/net→v0.51.0), **GO-2026-5856** (stdlib TLS ECH, toolchain→go1.26.5+). All reachable via the gRPC peer listener / HTTP client. *Fix:* bump deps + toolchain, `go mod tidy`, re-run `govulncheck`.
- **C-O1 — RPC latency, replication lag, and apply latency are unobservable.** `pkg/metrics/raft.go` defines `AppendEntriesLatencyHistogram`, `RequestVoteLatencyHistogram`, `FSMApplyLatencyHistogram`, `RecordReplicationLag`, `RecordAppendEntriesSent` — all with **zero call sites**. WAL fsync latency has no metric at all. An operator cannot diagnose a slow/flapping cluster, lagging follower, or FSM hot loop. *Fix:* wire the histograms at the send/apply sites; add a `raft_wal_fsync_seconds` histogram; add `raft_apply_lag`. *Test:* assert histogram counts increment; `/metrics` shows non-zero buckets.

## HIGH

- **H-R1 — `Shutdown()` doesn't wait for the drain → data-loss window.** `pkg/raft/raft.go:331` closes `stopCh` and returns; `drainFuturesOnShutdown`/final `flushCommitIndex` run async in `run()`. Process can exit before the drain completes. *Fix:* add a `doneCh` closed at end of `run()`; block `Shutdown()` on it with a timeout. *Test:* propose N, Shutdown, assert applied==committed.
- **H-R2 — WAL/disk write errors are log-and-continue → silent zombie member.** `pkg/raft/raft.go` follower append + apply loop log an error and keep running/counting toward quorum on `persistLog`/`Append` failure (e.g. ENOSPC). *Fix:* treat persistent storage-write failure as fatal / mark unready. *Test:* inject ENOSPC, assert node goes unready.
- **H-C1 — Leader self-removal nil-panics the run loop.** `pkg/raft/raft.go:1031` `GetServer(r.localID).Learner` — nil when the leader removed itself; no step-down guard. *Fix:* nil-guard + step down when no longer a voter. *Test:* commit `RemoveServer(leader)`, assert no panic.
- **H-C2 — WAL directory never fsync'd after segment create/rotate → entries can vanish on power loss.** `pkg/storage/wal.go` `createNewSegment`/rotate/delete never call `fsyncDir` (the snapshot path does). *Fix:* `fsyncDir(w.path)` after rotation and compaction. *Test:* rotate, crash-inject, reopen, assert entries survive.
- **H-R3 — FSM apply panic is swallowed and `applyIndex` advanced anyway → silent replica divergence.** `pkg/raft/raft.go:1140` recovers, records error, then unconditionally advances `applyIndex`. *Fix:* halt applying / shut down on FSM panic rather than skip a committed entry.
- **H-O1 — `/ready` reports Ready for a follower with no known leader.** `cmd/raftd/main.go` returns 200 for any Follower/Leader state. A partitioned follower is put in LB rotation and fails every linearizable request. *Fix:* require `Leader() != ""` (and apply-caught-up); keep `/health` as pure liveness.
- **H-R4 — SSE watches are killed every 60s by the HTTP `WriteTimeout`.** `cmd/raftd/main.go:763`. Makes `WatchIdleTimeout > 60s` a no-op, forces reconnect churn. *Fix:* clear the write deadline per-watch via `http.ResponseController`, or a separate mux with `WriteTimeout: 0`.
- **H-R5 — TCP transport serializes all RPCs to a peer behind one mutex over the network round-trip.** `pkg/transport/tcp.go` `sendRequest` holds `peer.mu` across the exchange; single conn, no pool. Heartbeats queue behind slow AppendEntries → heartbeat starvation → spurious elections. *Fix:* per-peer pool or multiplex on the existing `message.ID`.
- **H-P1 — No proposal batching / group commit: one fsync per client write.** `pkg/raft/raft.go` processes `proposalCh` one at a time; `WAL.Append` fsyncs each. Throughput capped at ~1 fsync/write. *Fix:* drain the channel into a batch, one `Append`+fsync per batch. *Bench:* `BenchmarkProposalThroughput` (fsyncs ≪ N).
- **H-P2 — WAL opens a fresh fd on every read (hot path).** `pkg/storage/wal.go` `getEntry` does `os.Open`+seek+close per `Get`; called many times per commit/apply cycle. *Fix:* `ReadAt` on a shared fd (race-free, no per-read open) or a pooled reader. *Bench:* `BenchmarkWALGet`.
- **H-S1 — No per-RPC peer authorization on the Raft transport.** `pkg/transport/grpc.go`/`tcp.go` — any cert chaining to the CA can drive consensus and call `RaftAdmin` (AddServer/RemoveServer/TransferLeadership); no interceptor checks the peer identity is an expected cluster member, and client/peer CAs are typically shared. *Fix:* server interceptor mapping peer cert SAN → member ID; separate client vs peer trust roots.
- **H-CI1 — CI has no lint or vuln gate.** `.github/workflows/ci.yml` runs build/vet/race/test but no `golangci-lint`/`staticcheck`/`govulncheck`, no dependabot. *Fix:* add lint + govulncheck jobs, `.golangci.yml` (excluding `ui/node_modules`), `dependabot.yml`.
- **H-H1 — No `LICENSE`.** Legally unusable as OSS. *Fix:* add a license (e.g. Apache-2.0 or MIT).
- **H-T1 — Joint-consensus apply transition untested; `applyConfigurationEntry` 0% covered.** The C6/C7 remediation tested the propose side but never adds a real new voter / promotes a learner end-to-end. Highest-consequence untested code. *Fix:* integration test that adds a voter and promotes a learner through joint → new config.
- **H-O2 — RPC tracing spans are dead code + no trace-context propagation.** `pkg/tracing/spans.go` `SpanAppendEntries/RequestVote/Snapshot` have zero call sites; `replicateTo` uses `context.Background()`; transport never injects/extracts trace context. Cross-node traces are impossible. *Fix:* wrap RPCs in spans, thread ctx, inject/extract propagation headers.

## MEDIUM

- **M-P1** No replication pipelining — one in-flight AppendEntries per follower (`MaxInflight` defined but unused). Caps per-follower throughput at `batch/RTT`.
- **M-P2** Per-RPC goroutine creation on every heartbeat/proposal (`go replicateTo`); prefer long-lived per-follower replication loops.
- **M-P3** FSM read path contends with `Apply` on the same lock; `Apply` holds the lock across `json.Marshal`+event emit. Move marshal/clone out of the critical section.
- **M-P4** FSM `Apply` uses reflection-based JSON per entry (largest allocator on the apply path); consider a binary codec.
- **M-P5** `encodeRecord` allocates 3 buffers + double-copies data per WAL append; write into one buffer, CRC over `result[4:]`.
- **M-R1** gRPC transport sets no explicit `MaxRecvMsgSize`/`MaxSendMsgSize` — >4 MiB AppendEntries/InstallSnapshot rejected; inconsistent with TCP's 128 MiB.
- **M-R2** TCP accept loop spawns unbounded goroutine-per-conn with no cap and no accept backoff (reconnect storm → FD/goroutine exhaustion, hot-spin on accept error).
- **M-R3** `http.Shutdown` always blocks the full 10s when watches are open (per-request ctx not cancelled). Cancel active watches at drain start with a clean `event: shutdown`.
- **M-R4** No ordered write-drain at shutdown; leader transfer targets the first peer (not the most up-to-date) and is best-effort.
- **M-R5** `pendingFutures` grows unbounded if the apply loop stalls (no overload rejection/backpressure).
- **M-R6** Mid-segment WAL corruption (bit-rot) is indistinguishable from a torn tail and silently truncated — possible committed-data loss. Probe forward past a CRC failure; refuse to start on genuine mid-file corruption.
- **M-R7** gRPC RPC timeouts hardcoded (10s AE, 30s snapshot); make configurable, default AE near the heartbeat interval.
- **M-R8** Config validation gaps: `SnapshotInterval/Threshold`, `TrailingLogs`, `FSyncInterval`, `MaxSizePerMsg`, `MaxInflight` unvalidated/undefaulted in `Config.Validate`.
- **M-S1** Auth token compare is not constant-time (`token == AdminToken`); use `crypto/subtle`.
- **M-S2** TCP 128 MiB single-message default + no gRPC recv cap → pre-auth allocation amplification. Lower the default.
- **M-S3** No file-permission check on TLS private keys (world-readable key silently accepted).
- **M-S4** gRPC `InstallSnapshot` reassembly accumulates chunks with no aggregate size bound.
- **M-C1** Raft send path uses `context.Background()` (replicate/vote/prevote/timeoutnow) — no deadline/cancel on shutdown; a stuck RPC holds the follower's in-flight slot.
- **M-C2** Data race on `peer.conn` between `RemovePeer` (under `t.mu`) and `sendRequest` (under `peer.mu`) during membership churn.
- **M-O1** Missing core metrics: proposal/apply throughput counters, error/rejection-rate counters, watch-connection gauge (`watchCount` not exported), HTTP request latency (`promhttp.InstrumentHandler*` unused), InstallSnapshot RPC count/latency, leader-id as a labeled series.
- **M-O2** No correlation/request-ID linking a client request across forward → apply.
- **M-O3** `/metrics` unauthenticated (leaks topology/term/leader). Bind internal or gate.
- **M-CI1** Release workflow advertises checksums but generates none; no SBOM/signing; raw binaries. Adopt GoReleaser (checksums+SBOM+cosign+archives).
- **M-CI2** golangci-lint scans `ui/node_modules` (spurious findings); add `.golangci.yml` with dir excludes.
- **M-CI3** Pervasive `time.Sleep`-based test synchronization (37 sites) — flaky under `-race` CI. Replace with condition polling.
- **M-L1** staticcheck: 26 findings (20 unused/dead code, 6 deprecated `grpc.Dial`/`WithInsecure`). golangci-lint: 71 (50 errcheck, 19 unused).

## LOW

- **L1** `ReadIndex` retry loop leaks `time.After` timers; reuse a Timer.
- **L2** `WaitApplied` spawns a watcher goroutine per slow read.
- **L3** Client watch reconnect: no jitter, no address failover, no SSE read deadline; `SubmitCommand` legacy path no backoff.
- **L4** `NewWAL` `bolt.Open` has no timeout (locked meta.db hangs startup).
- **L5** `watchPerIP` counters and orphan snapshot `.tmp` files never GC'd.
- **L6** `StableStore`'s `LogStore` methods are silent no-ops; WAL short-write not detected; `segmentEntryCount` over-counts; snapshot sidecar delete error swallowed.
- **L7** Election/heartbeat floor is 3× (conventional ≈10×); GC/network blips can trigger spurious elections.
- **L8** Dockerfile base images pinned by tag, not digest. No Go-version matrix / coverage upload / UI build in CI. Stray `//nolint` prose parsed as a linter name.
- **L9** Repo hygiene missing: `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `Makefile`, issue/PR templates, `dependabot.yml`.

---

## Recommended remediation order
1. **Security CVEs (C-S1)** — dep/toolchain bump; near-zero risk, closes 3 reachable CVEs.
2. **Correctness (C-A1, H-C1, H-C2, H-R2, H-R3)** — InstallSnapshot send, self-removal panic, dir fsync, disk-failure halt, FSM-panic halt.
3. **Reliability (H-R1, H-R4, H-R5)** — shutdown drain join, SSE timeout, TCP per-peer concurrency.
4. **Observability (C-O1, H-O1, H-O2)** — wire metrics/tracing, fix `/ready`.
5. **CI/supply-chain + hygiene (H-CI1, H-H1)** — lint/vuln gates, LICENSE.
6. **Performance (H-P1, H-P2)** — group commit, WAL `ReadAt`.
7. **Medium/Low** — batched cleanup.
8. **Docs** — production README, `docs/` deep-dives, hygiene files.

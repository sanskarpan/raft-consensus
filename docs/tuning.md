# Performance Tuning

How to tune `raftd` for throughput, latency, and stability, and how to benchmark
your changes. Parameter names and defaults come from `pkg/raft/types.go` and
`cmd/raftd/main.go`.

- [The latency budget](#the-latency-budget)
- [Election and heartbeat ticks](#election-and-heartbeat-ticks)
- [Snapshotting and log retention](#snapshotting-and-log-retention)
- [Batching and group commit](#batching-and-group-commit)
- [fsync and durability](#fsync-and-durability)
- [Timeouts and message sizes](#timeouts-and-message-sizes)
- [Rate limits and connection caps](#rate-limits-and-connection-caps)
- [Reads: linearizable vs stale](#reads-linearizable-vs-stale)
- [Benchmarking](#benchmarking)

## The latency budget

A committed write costs, roughly:

```
WAL append + fsync (leader)  +  1 RTT to a quorum of followers (each fsyncs)  +  FSM apply
```

The dominant terms are **disk fsync latency** and **one network round trip**. Fast
NVMe and low-latency links between nodes are the highest-leverage improvements.
Throughput (not latency) is improved by **group commit** — many concurrent proposals
share one fsync.

## Election and heartbeat ticks

One tick is **50 ms**. Two knobs (`election_tick`, `heartbeat_tick`) govern
stability.

- `heartbeat_tick: 1` → heartbeats every 50 ms. Keep it small (1–2) so followers'
  election timers are reset frequently.
- `election_tick` is the election timeout in ticks; the effective timeout is
  randomized in `[election_tick, 2×election_tick]`.
- **Constraint**: `election_tick ≥ 3 × heartbeat_tick` (hard fail otherwise);
  **recommended** `≈ 10×` (a warning is logged below that).

Guidance:

| Environment | heartbeat_tick | election_tick | Effective election timeout |
|-------------|----------------|---------------|----------------------------|
| Low-latency LAN / same host | 1 | 10 | 500 ms – 1 s |
| Cross-AZ / noisier network | 1 | 20 | 1 s – 2 s |
| Cross-region / high jitter | 2 | 20–30 | 2 s – 3 s |

If you see leadership flapping (`rate(raft_elections_total[5m])` elevated) with a
healthy network, **increase `election_tick`** — GC pauses or CPU starvation are
delaying heartbeats past the follower timeout. If failover is too slow, decrease it
(staying within the ratio constraint).

## Snapshotting and log retention

(`raft.Config` fields; the server currently uses their code defaults.)

- `SnapshotInterval` (default **120s**) — how often the snapshot ticker checks.
- `SnapshotThreshold` (default **8192**) — snapshot only when
  `appliedIndex − lastSnapshotIndex ≥ threshold`.
- `TrailingLogs` (default **10240**) — entries kept after compaction.

Trade-offs:
- **Lower `SnapshotThreshold`** → snapshots more often → smaller WAL, faster restart,
  but more snapshot CPU/IO. **Higher** → fewer snapshots, larger WAL, slower recovery.
- **Higher `TrailingLogs`** → a lagging follower can catch up from the log instead of
  a full `InstallSnapshot` (watch `raft_snapshots_restored_total`), at the cost of
  disk. If followers frequently need full installs, raise `TrailingLogs`.

## Batching and group commit

- **Proposal group commit**: the leader drains up to **256** queued proposals and
  appends them to the WAL in a **single fsync**. Under concurrency this amortizes
  fsync cost across many writes — the main throughput lever. To exploit it, issue
  writes concurrently rather than strictly serially.
- **Replication batching**: `AppendEntries` carries up to **100** entries per RPC;
  `MaxSizePerMsg` (default 1 MiB) bounds bytes per message. Larger messages improve
  catch-up throughput for lagging followers but increase per-RPC latency and memory.
- `MaxInflight` (default **256**) bounds concurrent in-flight replication messages.

## fsync and durability

- `FSyncInterval` default is **0** = fsync **every** write (safest, no data loss on
  crash). The WAL issues one fsync per `Append` batch, and segment
  rotation/creation fsync the directory as well.
- Durability here is not a tunable you should relax lightly: weakening fsync trades
  data-loss safety for throughput. Prefer faster disks or more batching over
  disabling fsync.
- Keep `data_dir` on a real block device with a battery-backed / power-loss-protected
  write cache for the best fsync latency.

## Timeouts and message sizes

Server-side HTTP timeouts (in `initHTTP`): read 30 s, write 60 s, idle 120 s.
Per-operation Apply/ReadIndex contexts are bounded at 10 s. Transport limits:

- TCP: per-message cap **16 MiB** (`SetMaxMessageBytes`).
- gRPC: message size **64 MiB**, streamed snapshot aggregate **512 MiB**; per-RPC
  deadlines (AppendEntries/RequestVote/TimeoutNow 10 s, snapshot 30 s).

Raise these only if you store large values (remember: key ≤ 4 KiB, value ≤ 512 KiB
are enforced at the HTTP layer) or run very large snapshots.

## Rate limits and connection caps

- `rate_limit_rps` (500) and `per_ip_rate_limit_rps` (50) throttle **writes**; reads
  are never limited. Raise them for write-heavy workloads behind trusted clients.
- `max_watch_connections` (1024) and `max_watch_connections_per_ip` (32) bound SSE
  fan-out memory. Raise for many watchers; keep per-IP bounded to resist DoS.
- Behind a proxy/LB, set `trusted_proxy_cidrs` so per-IP limits key off the real
  client IP.

## Reads: linearizable vs stale

- **Linearizable** (default) reads go through ReadIndex (a heartbeat quorum
  confirmation, no WAL write) and are served from the leader after `WaitApplied`.
  They are cheap relative to writes but still cost a heartbeat round.
- **Stale** reads (`?consistency=stale`) hit the local FSM with no coordination —
  lowest latency, servable from any node, may lag the committed state. Use them for
  read-heavy paths that tolerate slight staleness (caches, dashboards) and route them
  to followers to offload the leader.

## Benchmarking

Benchmarks live beside the unit tests and use standard Go tooling:

```bash
# Everything, with allocation stats
go test -bench=. -benchmem ./...

# Raft apply throughput
go test -bench='BenchmarkSingleNodeApply|BenchmarkThreeNodeApply' -benchmem ./pkg/raft
go test -bench=BenchmarkAppendEntriesHandler -benchmem ./pkg/raft
go test -bench=BenchmarkSnapshotCreation    -benchmem ./pkg/raft

# WAL encode / read (durability path)
go test -bench='BenchmarkEncodeRecord|BenchmarkWALGet|BenchmarkWALGetParallel' -benchmem ./pkg/storage

# FSM apply and read-under-write contention
go test -bench='BenchmarkFSMApplyPut|BenchmarkFSMApplyTxn|BenchmarkKVGetUnderApply' -benchmem ./pkg/fsm
```

Tips:
- Run several iterations for stable numbers: `-count=5 -benchtime=3s`, and compare
  with [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat).
- `BenchmarkSingleNodeApply` isolates the WAL+FSM path (no network);
  `BenchmarkThreeNodeApply` adds replication — the gap is your replication/fsync
  overhead.
- Profile hot paths via the pprof endpoint: set `debug_addr` (auth-gated) and pull
  `/debug/pprof/profile`.
- Validate real-world throughput end-to-end against a running cluster with concurrent
  `kvctl put` / client `Put` calls, and watch `raft_fsm_apply_latency_seconds` and
  `raft_append_entries_latency_seconds` while doing so.

# Production-Readiness TODO

Issues blocking true production use, ordered by severity. Each item links to the fix status.

---

## CRITICAL — Data Integrity

### C-1 · Snapshot metadata lost on restart ❌ → ✅ (fixing)
**File:** `pkg/storage/snapshot.go` — `readSnapshotMeta()`

**Problem:** `readSnapshotMeta` reconstructs `SnapshotMeta` from the filename alone, setting only
`ID`. The fields `Index`, `Term`, `Version`, and `Configuration` remain zero. These are populated
only in the **in-memory** `s.snapshots` slice via `sink.Close()`. After a process restart,
`loadSnapshots()` rebuilds the list from disk — every entry has `Index=0`, so `sort.Slice` by
`Index desc` produces arbitrary ordering. The Raft node then opens the "latest" snapshot with
`meta.Index = 0` and `meta.Configuration = {}`, which means:
- It replays the entire WAL on top of the snapshot (entries already encoded in the snapshot are
  applied a second time → duplicate FSM state).
- Cluster membership (`Configuration`) is lost → the restarted node has an empty peer list.

**Fix:** Parse `term` and `index` from the filename (`{term}-{index}.snap`). Write the full
`SnapshotMeta` (as JSON) to a sidecar `{id}.meta` file when the sink is closed. Read the sidecar
in `readSnapshotMeta`. Backward-compatible: if no sidecar exists, fall back to filename parsing
with empty configuration.

---

## CRITICAL — Security

### C-2 · /v1/watch SSE endpoint has no authentication ❌ → ✅ (fixing)
**File:** `cmd/raftd/main.go` — `initHTTP()`

**Problem:** The watch route is registered without `authMiddleware`:
```go
mux.HandleFunc("/v1/watch", s.handleV1Watch)  // unauthenticated!
```
Any client that can reach the HTTP port can subscribe to all key-change events in real time,
including confidential values (secrets, config). The watch stream runs indefinitely with no
session timeout.

**Fix:** Wrap with `s.authMiddleware` so a valid token is required. Add a configurable
`watch_idle_timeout` (default 5 min) to close stale SSE connections.

### C-3 · All Raft wire traffic and HTTP API are plaintext ❌ → ✅ (fixing)
**Files:** `cmd/raftd/main.go`, `pkg/transport/tcp.go`

**Problem:**
- HTTP server always uses `ListenAndServe` (plain HTTP). Auth tokens in `Authorization` headers
  are sent in cleartext.
- TCP transport sends all Raft RPCs (AppendEntries, RequestVote, InstallSnapshot — which includes
  snapshot data and therefore all FSM state) as unauthenticated, unencrypted JSON over TCP.
  Any on-path attacker can read or inject Raft messages.

**Fix:**
- HTTP: if `tls_cert` + `tls_key` are configured, use `ListenAndServeTLS`. Add HSTS header.
- TCP: add optional TLS wrapping to the TCP transport (reuse the cert paths already in Config).
  gRPC transport already has mTLS; make it the documented production choice.

---

## CRITICAL — Operational Safety

### C-4 · No graceful leader transfer on shutdown ❌ → ✅ (fixing)
**File:** `cmd/raftd/main.go` — `Server.Shutdown()`

**Problem:** `Shutdown()` calls `s.raftNode.Shutdown()` immediately, even when this node is the
cluster leader. The remaining nodes must wait for an election timeout (~1–2 s) before electing a
new leader. During that window, **all writes are rejected** (ErrNotLeader). Rolling restarts
(deployments, upgrades) cause repeated unavailability windows.

**Fix:** Before shutting down, if `s.raftNode.State() == StateLeader`, identify the most
up-to-date follower (highest `LastIndex` from `/admin/cluster`) and call the `TimeoutNow` RPC on
it. Wait up to 3 s for this node to step down (state transitions to Follower). Then proceed with
the normal shutdown sequence.

---

## IMPORTANT — Correctness

### I-1 · ReadIndex implementation has no unit tests ⚠️ → ✅ (fixing)
**File:** `pkg/raft/raft_test.go`

**Problem:** The `ReadIndex` / leader-lease implementation (added in a previous session) has zero
test coverage. The correctness of the lease window (`electionTimeout/2`) and the heartbeat-ack
tracking are untested edge cases.

**Fix:** Add unit tests:
- Single-node cluster: `ReadIndex` returns `commitIndex` immediately (single-node fast path).
- Multi-node cluster: `ReadIndex` blocks until quorum of heartbeat acks arrive within lease window.
- Follower: `ReadIndex` returns `ErrNotLeader`.
- After step-down: `heartbeatAcks` is cleared; subsequent `ReadIndex` waits again.

### I-2 · Write retries can double-apply in edge cases ⚠️ → ✅ (fixing)
**File:** `pkg/client/client.go`, `pkg/fsm/kv.go`

**Problem:** `doWithRetry` retries writes up to 4 times on failure. If the command committed on
the server but the response was lost in transit, the retry sends a second command — potentially
applying the same logical write twice. For idempotent KV ops (PUT key=val, DELETE) this is safe,
but for future counter/append-style ops it is not.

**Fix:** Embed a per-client `(clientID UUID, seqNum uint64)` pair in every command envelope.
The FSM maintains a dedup table `map[clientID]lastSeqNum` with the last result per client. On
apply, if `seqNum ≤ lastApplied[clientID]`, return the cached result without re-applying. The
client generates a UUID `clientID` at construction and increments `seqNum` per write.

---

## IMPORTANT — Observability

### I-3 · OpenTelemetry tracing exists but is completely unwired ⚠️ → ✅ (fixing)
**Files:** `pkg/tracing/otel.go`, `pkg/tracing/spans.go`, `cmd/raftd/main.go`

**Problem:** `pkg/tracing` has a full OTLP provider implementation with span helpers
(`SpanRequestVote`, `SpanAppendEntries`, `SpanSnapshot`), but `main.go` never imports or
initializes the tracing package. Zero spans are emitted. The CHECKLIST marks this `[x]` but it
does nothing.

**Fix:** In `main.go`:
- Add `otlp_endpoint` optional config field.
- Initialize `tracing.NewOTLPProvider(ctx, endpoint)` if configured, else `tracing.NewNoopProvider()`.
- Add spans to `handleV1KV`, `handleV1Txn`, `handleCommand`, `handleCluster`.
- Call `provider.Shutdown(ctx)` in `Server.Shutdown()`.

---

## MODERATE — Security Hardening

### M-1 · Rate limiting is global, not per-IP ⚠️ → ✅ (fixing)
**File:** `cmd/raftd/main.go` — `rateLimitMiddleware`

**Problem:** The single global `writeLimiter` is shared across all clients. One client sending
500 req/s consumes the entire cluster write budget, starving all others. A single misbehaving
or malicious client can DoS the cluster's write path.

**Fix:** Add a per-IP token bucket map (`sync.Map[string]*writeLimiter`) with a tighter per-IP
limit (default: 50 RPS). Add a background goroutine that sweeps and removes idle entries (>5 min
without traffic) to bound memory usage.

### M-2 · TCP transport has no peer authentication ⚠️
**File:** `pkg/transport/tcp.go`

**Problem:** Any host that can reach the TCP listen port can send valid AppendEntries/RequestVote
messages. There is no peer identity verification (unlike the gRPC transport which has mTLS).

**Status:** Deferred — documented as "use gRPC transport in production". Adding full mTLS to the
TCP transport would require significant protocol changes (TLS handshake before the JSON framing).
The recommended production path is `transport: grpc` with TLS certs.

---

## LOW — Code Quality

### L-1 · Tracing spans in pkg/tracing/spans.go accept a `tracing.Provider` interface
The span helpers require a `Provider` to be passed; they are not integrated with context
propagation. A future improvement would use `otel.GetTracerProvider()` from the context.

### L-2 · `readSnapshotMeta` reads the first line of the snapshot file
After the CRC32 footer was added, the "first line" is now a partial read of FSM JSON data (which
may not contain a newline for small snapshots). The `bufio.ReadString('\n')` is effectively a
no-op or reads partial data. After the C-1 fix (sidecar file), this function no longer reads the
snapshot file body at all.

### L-3 · No connection draining on HTTP shutdown
`http.Server.Shutdown(ctx)` is called with a 10 s timeout, which does drain in-flight HTTP
requests (Go's built-in behaviour). However, SSE watch connections are long-lived and will be
forcibly closed. Clients should handle this via reconnect (which they do via `watchLoop`).
This is acceptable but worth noting.

---

## Status Tracker

| ID  | Issue                                        | Severity | Status      |
|-----|----------------------------------------------|----------|-------------|
| C-1 | Snapshot metadata lost on restart            | Critical | 🔧 In Progress |
| C-2 | /v1/watch has no authentication              | Critical | 🔧 In Progress |
| C-3 | HTTP + TCP transport plaintext               | Critical | 🔧 In Progress |
| C-4 | No graceful leader transfer on shutdown      | Critical | 🔧 In Progress |
| I-1 | ReadIndex has no unit tests                  | Important| 🔧 In Progress |
| I-2 | Write retry idempotency                      | Important| 🔧 In Progress |
| I-3 | OpenTelemetry tracing unwired                | Important| 🔧 In Progress |
| M-1 | Rate limiting is global not per-IP           | Moderate | 🔧 In Progress |
| M-2 | TCP transport no peer auth (use gRPC instead)| Moderate | ⏭ Deferred  |
| L-1 | Tracing context propagation                  | Low      | ⏭ Deferred  |
| L-2 | readSnapshotMeta reads body unnecessarily    | Low      | 🔧 Fixed by C-1 |
| L-3 | SSE connections forcibly closed on shutdown  | Low      | ✅ Acceptable |

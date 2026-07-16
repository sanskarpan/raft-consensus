# Raft-Consensus — Defect Tracker

Findings from the adversarial audit (2026-07-16). Each issue has a stable ID, severity,
category, location, description, reproduction, and fix. Status is tracked in the table
and updated as fixes land with tests.

**Legend:** ☐ open · ◐ in progress · ☑ fixed + tested

## Status summary

| ID  | Sev | Area | Title | Status |
|-----|-----|------|-------|--------|
| C1  | Critical | storage | WAL never fsyncs entries before ack/commit | ☑ |
| C2  | Critical | storage | `StableStore.Sync()` is a no-op | ☑ |
| C3  | Critical | raft | AppendEntries truncation can delete committed entries / valid suffix | ☑ |
| C4  | Critical | raft | commitIndex/applyIndex move backward on stale InstallSnapshot | ☑ |
| C5  | Critical | raft | InstallSnapshot doesn't reconcile log; restart doesn't load snapshot meta | ☑ |
| C6  | Critical | raft | Elections/commits ignore joint-consensus dual-quorum → two leaders | ☑ |
| C7  | Critical | raft | No single-outstanding-config-change enforcement | ☑ |
| C8  | Critical | fsm | Dedup diverges between replicas (random eviction, exact-seq match) | ☑ |
| C9  | Critical | server | Auth fail-open; `authMiddleware` bypassed when no tokens | ☑ |
| C10 | Critical | server | `/command` write endpoint has no auth at all | ☑ |
| C11 | Critical | transport | Both transports trust the network (no default mTLS / plaintext fallback) | ☑ |
| C12 | Critical | transport | TCP transport: no request/response correlation, no size bound | ☑ |
| C13 | Critical | storage | WAL recovery can't tell torn tail from corruption; OOM on bad length | ☑ |
| C14 | Critical | storage | Snapshot write not atomic/durable; compaction unordered | ☑ |
| H1  | High | raft | Mutex held across disk Sync; per-tick goroutine explosion | ☑ |
| H2  | High | raft | replicateTo TOCTOU on matchIndex | ☑ |
| H3  | High | raft | Non-contiguous append / commit-over-gap | ☑ |
| H4  | High | transport | gRPC pool health machinery is dead code; use-after-close; div-by-zero | ☑ |
| H5  | High | client | Client retries non-idempotent writes without keys | ☑ |
| H6  | High | server | Follower forwarding leaks tokens / SSRF via member address | ☑ |
| H7  | High | client/server | Quorum/linearizable reads are fake | ☑ |
| H8  | High | raft | No log fsync on follower ack path (same root as C1) | ☑ |
| H9  | High | transport | gRPC InstallSnapshot truncates multi-chunk snapshots | ☑ |
| H10 | High | fsm | Snapshot `index` not serialized/restored; `list` non-deterministic | ☑ |
| H11 | High | fsm | Watch register/snapshot-revision race → dup + out-of-order | ☑ |
| H12 | High | server | Config validation minimal; debug/pprof unauth when no tokens | ☑ |
| M1  | Medium | raft | Leadership transfer reports success prematurely | ☑ |
| M2  | Medium | raft | Shutdown fails committed futures with ErrNotStarted | ☑ |
| M3  | Medium | raft | triggerSnapshot index/lock inconsistency | ☑ |
| M4  | Medium | raft | ReadIndex pure-clock lease, no drift bound | ☑ |
| M5  | Medium | transport | TCP ignores ctx; single conn per peer; no write deadline | ☑ |
| M6  | Medium | transport | gRPC Close holds mu across wg.Wait (deadlock); double-close panic | ☑ |
| M7  | Medium | transport | AppendEntriesResponse drops ConflictTerm | ☑ |
| M8  | Medium | storage | getEntry segment selection wrong; reopen loses length/segment | ☑ |
| M9  | Medium | storage | DeleteRange removes partially-overlapping segment files | ☑ |
| M10 | Medium | storage | Snapshot prune deletes newest instead of oldest | ☑ |
| M11 | Medium | fsm | Txn atomicity: delete-missing no-ops but Succeeded=true | ☑ |
| M12 | Medium | server | Internal error strings leaked to clients | ☑ |
| M13 | Medium | server | CORS `*` + Authorization + `?token=` query auth | ☑ |
| M14 | Medium | server | No cap on concurrent watch SSE connections | ☑ |
| M15 | Medium | storage | Snapshot verification bypassed on missing magic | ☑ |
| M16 | Medium | storage | Ignored Close/Truncate/Remove errors mask failures | ☑ |
| L1  | Low | raft | Election jitter too small when ElectionTick≈HeartbeatTick | ☑ |
| L2  | Low | raft | pre-vote inPreVote shared-flag flake | ☑ |
| L3  | Low | raft | GetServer returns pointer to range copy | ☑ |
| L4  | Low | fsm | revision int64 overflow unguarded | ☑ |
| L5  | Low | client | maxLeaseDuration never initialized; unlocked Client fields (race) | ☑ |
| L6  | Low | transport | client keepalive 2h vs server 5m floor | ☑ |
| L7  | Low | server | X-Seq-Num/?revision= parse errors silently default to 0 | ☑ |
| L8  | Low | server | Ambiguous JSON-vs-raw PUT body parsing | ☑ |
| L9  | Low | server | Busy-poll linearizable reads | ☑ |
| L10 | Low | ops | Cert keys world-readable; image tag `latest`; raft ports host-published | ☑ |

---

## CRITICAL

### C1 — WAL never fsyncs entries before they are acknowledged/committed
- **Category:** Data
- **Location:** `pkg/storage/wal.go:311` (`appendEntry`), `:272` (`Append`)
- **Description:** `appendEntry` does `file.Write(data)` and returns; the only `Sync()` is on
  segment rotation. `raft.go persistLog → log.Append` acknowledges an entry that lives only in
  the OS page cache. A leader that replicates → reaches quorum → commits → applies can lose the
  entry on power loss. Followers ack in `HandleAppendEntriesRPC` without fsync too. Breaks
  Leader Completeness / State Machine Safety.
- **Reproduction:** Append entry, power-cut before kernel flush, reopen → entry gone.
- **Fix:** Add a durable `Sync()`/batched fsync in `Append`; call it on the follower ack path
  and before a leader counts its own entry as durable.

### C2 — `StableStore.Sync()` is a no-op
- **Category:** Data
- **Location:** `pkg/storage/wal.go:807`
- **Description:** `func (s *StableStore) Sync() error { return nil }`. `persistTermAndVotedFor`
  relies on it as the durability barrier before voting/becoming leader. A lost vote across a
  crash → two votes one term → two leaders.
- **Reproduction:** Vote for A in term T, crash before Bolt fsync, restart, vote for B in term T.
- **Fix:** Implement `Sync()` to force a durable flush of the bolt DB.

### C3 — AppendEntries truncation can delete committed entries and drops a valid suffix
- **Category:** Logic/Data
- **Location:** `pkg/raft/raft.go:1941` (append loop), `:1926` (prevLogTerm mismatch)
- **Description:** `truncateLog(entry.Index-1)` on a term mismatch with no `entry.Index > commitIndex`
  guard; a delayed/duplicate AppendEntries at an index ≤ commitIndex deletes an applied entry.
  Truncating on a bare prevLogTerm mismatch plus one-at-a-time re-append can destroy a longer
  legitimate suffix the request doesn't carry.
- **Reproduction:** Follower committed index 5 term 1; duplicate AE `{Index:5,Term:2}` → truncates 5.
- **Fix:** Never truncate at/below commitIndex; truncate only at the first genuinely conflicting
  incoming entry and append the rest from the request.

### C4 — commitIndex/applyIndex move backward on stale InstallSnapshot
- **Category:** Data
- **Location:** `pkg/raft/raft.go:2078-2079`; Restore path `:1677`
- **Description:** `commitIndex = applyIndex = LastIncludedIndex` unconditionally; a delayed
  snapshot with a smaller index rolls both backward → re-apply / rollback.
- **Reproduction:** commit=100, delayed InstallSnapshot LastIncludedIndex=50 → re-applies 51..100.
- **Fix:** Accept only when `LastIncludedIndex > commitIndex`; never regress commit/apply.

### C5 — InstallSnapshot doesn't reconcile the log; restart doesn't load snapshot meta
- **Category:** Data
- **Location:** `pkg/raft/raft.go:2071-2082`, `Start`/`loadLastIndex` `:1738`
- **Description:** On install, the code sets indices + restores FSM but never deletes conflicting
  log entries, so `log.LastIndex()` disagrees with `r.lastIndex`. On restart after compaction,
  snapshot meta isn't loaded so `lastTerm=0` and the node can't win elections / re-accepts from 1.
- **Fix:** Reset the log so first index = LastIncludedIndex+1 discarding conflicts; on Start load
  snapshot meta into lastIndex/lastTerm/applyIndex/commitIndex before the log tail.

### C6 — Elections and commits ignore joint-consensus dual-quorum
- **Category:** Logic
- **Location:** `pkg/raft/raft.go:590-604`, `:448`, `:657`, `:884`, `:1124`
- **Description:** Vote counting + single-node/ReadIndex fast paths use `configuration.QuorumSize()`
  (old config only) while leader init uses joint `AllServers()`. During a joint transition a
  candidate can win with only old-majority while another wins with new-majority → two leaders.
  Votes counted from non-voters too.
- **Fix:** One `effectiveQuorum` that requires majorities in both configs during joint consensus
  everywhere; reject votes from non-voters.

### C7 — No single-outstanding-config-change enforcement
- **Category:** Logic
- **Location:** `pkg/raft/raft.go:1273-1384`, joint apply `:970-1046`, learner path `:1480`
- **Description:** Membership-change methods don't check for an uncommitted config change; two
  overlapping changes derive from the old config → indeterminate configuration. Leader crash
  mid-joint can strand the cluster in joint mode.
- **Fix:** Reject a membership change while one is uncommitted; re-drive CommitJoint on becoming
  leader with jointConfig set; route promotion through joint protocol.

### C8 — FSM dedup diverges between replicas
- **Category:** Data
- **Location:** `pkg/fsm/kv.go:543` (`evictDedupIfNeeded`), `:155` (lookup), `txn.go:51`
- **Description:** Eviction deletes an arbitrary map entry and `dedupTable` is snapshotted →
  replicas diverge. Exact `SeqNum ==` match re-applies reordered retries. Txns carry no
  ClientID/SeqNum → never deduplicated.
- **Fix:** Deterministic eviction (lowest-seq / insertion order); store highest seq, treat
  `SeqNum <= stored` as cached hit; add idempotency fields to txn encode + route through dedup.

### C9 — Auth is fail-open; authMiddleware is a fake approval point
- **Category:** Security
- **Location:** `cmd/raftd/main.go:787`
- **Description:** Returns early (allow all) when no tokens configured; all shipped configs and
  helm default set no tokens, so membership/KV/snapshot/watch are open by default.
- **Fix:** Fail closed on non-loopback listeners without auth, gated by explicit opt-in.

### C10 — `/command` write endpoint has no auth
- **Category:** Security
- **Location:** `cmd/raftd/main.go:600`; `/metrics` `:603`
- **Description:** `/command` wrapped only in rateLimit; never auth even when tokens set. Any
  caller submits raw FSM commands.
- **Fix:** Wrap `/command` in `requireRole("write", …)`; gate/relocate `/metrics`.

### C11 — Both transports trust the network
- **Category:** Security
- **Location:** `pkg/transport/grpc.go:378-382, 332-338`; `pkg/transport/tcp.go:159-179`
- **Description:** gRPC silently falls back to `WithInsecure()`; `ServerName` unset;
  `ClientAuth` defaults to NoClientCert unless MutualTLS set. TCP dispatches Raft RPCs with no
  identity check when TLS off. Unauthenticated peer can forge AppendEntries / call admin RPCs.
- **Fix:** Require TLS+mTLS by default (fail closed); set ServerName/MinVersion TLS1.3; verify
  peer identity vs claimed ServerID.

### C12 — TCP transport has no request/response correlation or size bound
- **Category:** Race/Data/Security
- **Location:** `pkg/transport/tcp.go:433-493`, `:245`
- **Description:** `sendRequest` assumes the next frame is the response; `resp.Type` unchecked;
  a late/skipped frame mis-correlates → wrong Term/Success into Raft. No max message size →
  OOM DoS from unauthenticated peer.
- **Fix:** Monotonic request ID echoed + verified; `io.LimitReader` max frame size.

### C13 — WAL recovery can't distinguish torn tail from corruption; OOM on bad length
- **Category:** Data/Security
- **Location:** `pkg/storage/wal.go:174-206`, `:686-696`, `:708`
- **Description:** Any CRC mismatch fails `NewWAL` entirely (normal torn tail makes node
  unbootable). `dataLen` from an unvalidated length is used in `make([]byte, dataLen)` before
  CRC → OOM on corrupt header.
- **Fix:** Truncate to last-good offset at a bad/short tail record; bound `dataLen` vs remaining
  file size before allocating.

### C14 — Snapshot write not atomic/durable; compaction unordered
- **Category:** Data
- **Location:** `pkg/storage/snapshot.go:191-205`; `wal.go:220-237`, `:540-566`
- **Description:** No directory fsync after create/rename; `.meta` sidecar is plain WriteFile with
  no fsync → crash yields empty Configuration. Truncation ignores errors, not fsynced; head
  compaction not persisted. No barrier before `wal.Compact` → permanent range loss on crash.
- **Fix:** temp+fsync+rename+dir-fsync for snap and meta; check+fsync truncation; compact only
  after durable snapshot.

## HIGH / MEDIUM / LOW
See the consolidated audit for full detail on H1–H12, M1–M16, L1–L10 (summarized in the table
above). These are addressed after the Criticals, each with a regression test.

---

## Fix log (what has landed)

Each fix below has a regression test that fails against the pre-fix code and passes after.
Full suite (`go test ./...`) is green, including `-race` on every changed package.
`TestMembershipAPI` (red even at baseline) is now green.

| ID | Fix | Test |
|----|-----|------|
| C1 | `WAL.Append` fsyncs the segment before returning; surfaces fsync errors | `pkg/storage/wal_durability_test.go` |
| C2 | `StableStore.Sync` calls `db.Sync()` (was a no-op) | `pkg/storage/wal_durability_test.go` |
| C3 | AppendEntries never truncates at/below commitIndex; truncates only at first real conflict then appends the suffix; no truncate on a bare prevLog mismatch | `pkg/raft/safety_fixes_test.go` |
| C4 | InstallSnapshot ignored when `LastIncludedIndex <= commitIndex` (monotonic) | `pkg/raft/safety_fixes_test.go` |
| C5 | InstallSnapshot reconciles the log (retain-suffix or discard-all); `Start` loads snapshot meta via `loadSnapshotState` | `pkg/raft/safety_fixes_test.go` |
| C6 | Joint-consensus-aware, voter-only vote quorum (`hasVoteQuorum`) for vote + pre-vote | `pkg/raft/safety_fixes_test.go` |
| C7 | At-most-one outstanding config change (`pendingConfigIndex`/`configChangePending`) across all 5 membership ops; reject removing a non-member | `pkg/raft/safety_fixes_test.go` |
| C8 | Deterministic dedup eviction (lowest Order, tie by ID); monotonic seq dedup | `pkg/fsm/kv_dedup_test.go` |
| C9 | Auth fails closed with no tokens unless `allow_no_auth`; dev opt-in grants write role | `cmd/raftd/auth_fixes_test.go` |
| C10 | `/command` now wrapped in `requireRole("write")` | `cmd/raftd/auth_fixes_test.go` |
| C11 | `SetRequireTLS` fail-closed option: no silent plaintext fallback (gRPC). **Deferred:** default-on mTLS + ServerName/MinVersion (config-policy change). | `pkg/transport/grpc_requiretls_test.go` |
| C12 | Per-message size bound on the TCP transport (`SetMaxMessageBytes`, default 128 MiB) — stops unauthenticated OOM. **Deferred:** request-ID correlation (protocol change). | `pkg/transport/tcp_maxmsg_test.go` |
| C13 | WAL recovery truncates a torn tail record instead of failing; bounds record-data allocation by remaining file bytes | `pkg/storage/wal_recovery_test.go` |
| C14 | Snapshot meta written durably+atomically before the `.snap` is visible; directory fsync; snapshot list kept sorted | `pkg/storage/snapshot_test.go` |
| M10 | Snapshot prune deletes the oldest, not the newest (sorted list) | `pkg/storage/snapshot_test.go` |
| — | Bonus: `/admin/members` GET support; DELETE of a non-member now errors; `AddServer` added to the `Raft` interface; `TestMembershipAPI` fixed | `tools/testharness/integration_test.go` |

### High / Medium / Low batch (multi-agent, one agent per package)

Fixed with per-issue regression tests; full suite green including `-race` and the integration
harness. Cross-package items (M7, L10) done by hand afterward.

- **pkg/raft:** H1 (persist commitIndex outside `r.mu` via a coalescing signal + per-follower
  in-flight guard), H2 (drop stale replicateTo responses after a term change), H3 (contiguous
  `persistLog` + commit capped at request entries), M1 (leadership transfer resolves only on real
  step-down/timeout), M2 (`drainFuturesOnShutdown` resolves committed futures as success), M3
  (snapshot index reflects applyIndex), M4 (heartbeat-confirmed ReadIndex, §6.4 — quorum must ack a heartbeat after the read starts; leader-local monotonic clock only, no clock-sync assumption), L1
  (ElectionTick ≥ 3×HeartbeatTick), L2 (explicit pre-vote flag), L3 (`GetServer`/`Voters`/
  `Learners` return real slice elements).
- **pkg/transport:** H4 (pool nil/empty guard, idempotent Close, wired health recording,
  drain-before-close), H9 (InstallSnapshot reassembles chunks), M5 (honor ctx + write deadline),
  M6 (no lock across `wg.Wait`, `sync.Once` shutdown), L6 (30s keepalive).
- **pkg/fsm:** H10 (serialize+restore apply index, deterministic `list`), H11 (no dup/out-of-order
  watch delivery, drain eventCh on Restore), M11 (txn atomicity on op error), L4 (overflow guard).
- **pkg/storage:** M8 (segment ownership in index), M9 (don't delete partially-overlapping
  segments), M15 (checksummed-snapshot flag, reject missing footer), M16 (propagate Close/Truncate
  errors).
- **pkg/client:** H5 (idempotency keys on all writes), H7-client (quorum reads require agreement),
  L5 (init lease duration + mutex on client fields, `-race` clean).
- **cmd/raftd:** H6 (TLS-aware forward scheme, address validation, bounded body+ctx), H7-server
  (linearizable range unless `consistency=stale`), H12 (cluster config validation + gated pprof),
  M12 (no internal error leakage), M13 (CORS deny-by-default, drop `?token=`), M14 (watch
  connection caps), L7 (400 on malformed numeric input), L8 (content-type-aware body + size limits).
- **M7** (`AppendEntriesResponse.ConflictTerm` plumbed end-to-end raft→transport→proto with
  leader term-skip backup): `pkg/raft/conflictterm_test.go`.
- **L10** (ops): cert keys chmod 600 + 4096-bit, helm image tag pinned (no `latest`), raft TCP
  ports no longer host-published in docker-compose.

### Final batch (deferred Critical sub-parts + L9, multi-agent + adversarial verify)

- **C11** (full): TLS path hardened — `MinVersion` TLS 1.3 on server+client configs, default mTLS
  (`RequireAndVerifyClientCert`) whenever a CA is present, verified server (no `InsecureSkipVerify`).
  `ServerName` is derived **per-peer** from the dial address (`serverNameFor`, respecting an
  explicit value, falling back for unspecified listen addrs) — fixing the verifier's finding that
  a hardcoded "localhost" would misverify multi-host peers. `require_tls` config wired to
  `SetRequireTLS` so production can fail closed. Tests: `pkg/transport/tls_hardening_test.go`,
  `servername_test.go`.
- **C12** (full): TCP `message` carries a monotonic request ID; the client rejects any response
  whose ID/type doesn't match the in-flight request (closes the connection) — no mis-correlated
  frames. Size bound retained. Test: `pkg/transport/tls_hardening_test.go`.
- **L9**: `WaitApplied(ctx, idx)` on the `raft.Raft` interface (sync.Cond-based, no missed wakeup,
  no lock-order deadlock, ctx-aware) replaces the server's 5 ms busy-poll. Tests:
  `pkg/raft/wait_applied_test.go`, `cmd/raftd` delegation tests.

Each change was **adversarially re-reviewed** by an independent verifier agent; the C11 ServerName
and RequireTLS-wiring findings were fixed as a result.

**Status: 52/52 issues fixed with tests (14 Critical, 12 High, 16 Medium, 10 Low).** Full suite
`go test ./...` green including chaos + integration harness; `-race` clean on every changed package.

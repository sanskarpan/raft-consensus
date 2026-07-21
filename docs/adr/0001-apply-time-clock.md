# ADR-0001: Deterministic apply-time clock for TTL expiry

**Status:** Accepted
**Date:** 2026-03
**Issue:** #207

---

## Context

The KV store needed TTL/lease-based key expiry. The obvious implementation would call `time.Now()` inside the FSM's `Apply()` method to check whether a key's TTL has elapsed. However, this approach has a critical flaw in a replicated state machine.

### Problem: wall-clock divergence

In a Raft cluster, the FSM is applied independently on each node. If the FSM calls `time.Now()` to decide whether a key is expired, different nodes may observe different wall-clock times and reach different conclusions:

- Node A's clock: 10:00:00.001 → key is NOT expired
- Node B's clock: 10:00:00.003 → key IS expired

This divergence would cause the replicas' FSM states to differ, violating the fundamental Raft safety property that all replicas apply the same log entries to the same deterministic function and reach byte-identical states.

The same problem applies to snapshots: if a snapshot is taken on node A at time T and restored on node B at time T+5, the restored FSM might immediately expire keys that were still live at snapshot time on node A.

### Why `time.Now()` is banned in the FSM

The FSM's `Apply()` function must be a **pure deterministic function** of the log entry contents. Any call to `time.Now()`, `rand.Float64()`, or any other non-deterministic source inside `Apply()` is a correctness bug that will cause split-brain FSM state.

---

## Decision

Use an **apply-time monotonic clock** (`applyTimeMs`) that is:

1. **Leader-stamped** — only the leader reads the wall clock (`time.Now().UnixMilli()`), encodes the timestamp in the log entry as `LeaderTimestampMs`, and sends it to followers via Raft replication.
2. **Applied identically** — when any node applies the log entry, it advances its `applyTimeMs` to `LeaderTimestampMs` (monotonically: never regressing). All nodes apply the same entry, so all advance to the same value.
3. **Persisted in snapshots** — the `applyTimeMs` and per-key `ExpiresAtMs` fields are serialised in the snapshot (format v2). A restored FSM resumes the correct virtual clock state.

### Tick commands

To ensure the virtual clock advances even when no client writes are occurring (which would carry `LeaderTimestampMs`), the leader proposes a `tick` command every `ttl_tick_interval` (default 1 s). The tick command:

1. Advances `applyTimeMs`
2. Sweeps all keys whose `ExpiresAtMs <= applyTimeMs` in sorted key order (deterministic deletion)
3. Emits `EventDelete` watch events for each expired key

### ExpiresAtMs

When a key is written with `ttl_seconds = N`, the FSM computes:

```
ExpiresAtMs = LeaderTimestampMs + TTLSeconds * 1000
```

A key is considered live if `ExpiresAtMs == 0 || ExpiresAtMs > applyTimeMs`.

---

## Consequences

**Good:**

- TTL expiry is byte-deterministic across all replicas — no split-brain FSM state
- Snapshot-consistent — a snapshot can be taken, transferred, and restored without expiry skew
- `ExpiresAtMs` is monotonically increasing and correct even after leader failover (the new leader's next tick advances the clock)
- The DES test harness can inject a controlled `applyTimeMs` sequence via the simulated clock without any real-time dependency

**Neutral:**

- Keys may expire slightly later than their nominal TTL during leader failover (up to one `ttl_tick_interval` delay)
- The leader's wall-clock reading introduces a small dependency on its accuracy, but only for the granularity of expiry (seconds), not for correctness

**Bad:**

- More complex than a naive `time.Now()` check
- Requires all write operations that should affect TTL to carry `LeaderTimestampMs` (backward-compatible binary codec extension)
- The virtual clock never runs on a follower that is not receiving ticks (partitioned follower) — but this is correct: keys on such a follower are invisible to clients anyway

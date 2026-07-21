# ADR-0004: Deterministic simulation (DES) testing harness

**Status:** Accepted
**Date:** 2026-03
**Issue:** #220

---

## Context

Before the DES harness, the test suite had two tiers:

1. **Unit tests** — fast but limited to single-node or stub-based testing; could not exercise multi-node consensus
2. **Integration tests** (`tools/testharness/`) — used real `raftd` processes with real TCP sockets and real timers; required `time.Sleep` to wait for elections

The integration tests had persistent problems:

- **Flakiness** — a slow CI runner (CPU throttling, memory pressure) could cause an election to take longer than the expected sleep, making assertions fail spuriously
- **Slowness** — each test had to sleep through at least one election timeout (typically 150–500 ms). 20 tests × 200 ms = 4 seconds just waiting for elections
- **Non-reproducibility** — if a test failed, it was nearly impossible to reproduce the exact message-ordering that triggered the failure, because it depended on goroutine scheduling and real network timing

### Root cause

The root cause was that `pkg/raft/raft.go` called `time.Now()` and `time.NewTicker()` directly at 11 and 2 sites respectively. These calls made the Raft state machine's behaviour depend on real wall-clock time.

---

## Decision

Make the Raft state machine's clock and ticker **injectable** by introducing two interfaces:

```go
// Clock is the injectable time source used by the Raft state machine.
type Clock interface {
    Now() time.Time
}

// Ticker is the injectable tick source. It mirrors time.Ticker's channel interface.
type Ticker interface {
    C() <-chan time.Time
    Stop()
}
```

The `raft.Config` struct gains two optional fields:

```go
type Config struct {
    // ...
    Clock     Clock          // nil → realClock (time.Now())
    NewTicker func(d time.Duration) Ticker // nil → time.NewTicker
}
```

All 11 `time.Now()` and 2 `time.NewTicker()` calls in `raft.go` are replaced with `r.clock.Now()` and `r.newTicker()`. In production, the zero-value config uses `realClock` and `time.NewTicker` — zero behavior change.

### simClock and simTicker

For tests, a `simClock` is provided:

```go
type simClock struct {
    mu      sync.Mutex
    now     time.Time
    tickers []*simTicker
}

func (c *simClock) Advance(d time.Duration) {
    // Moves clock forward and fires simTickers whose interval has elapsed
}
```

`simTicker` is driven by `simClock.Advance()`. No real-time waiting occurs.

### simNetwork

All network I/O is replaced with `simNetwork`, which delivers RPCs in-process using direct Go function calls. The `simNetwork`:

- Uses a seeded `rand.Rand` to determine message ordering (fully reproducible)
- Supports bidirectional partition injection (`Partition`/`Heal`)
- Supports configurable per-hop latency (implemented by advancing the clock)
- Routes RPCs directly to registered `*raft` nodes (no TCP sockets)

### Quiescence barrier

`Tick()` advances the clock and then calls `runtime.Gosched()` in a loop to let all triggered goroutines run to completion before returning. This prevents races where `Tick()` returns before an AppendEntries triggered by the clock advance has been processed by the follower.

---

## Consequences

**Good:**

- Tests complete in milliseconds (no `time.Sleep`)
- Fully reproducible: a failing test with `seed=42` always fails the same way on any machine
- Can inject arbitrary network partitions and latency with a single function call
- Can advance time by hours to test TTL expiry without waiting
- Enables property-based testing: run the same scenario with 1 000 different seeds to find edge cases

**Neutral:**

- Slightly more complex `raft.go` (injectable fields vs. direct calls)
- Two code paths to test (real clock in integration, sim clock in unit tests)

**Bad:**

- The DES harness does not test real TCP, TLS, or OS scheduling — those are covered by the process-based integration tests in `tools/testharness/`
- `simNetwork` delivers RPCs synchronously; the real TCP transport may deliver them asynchronously. Subtle timing differences between sync and async delivery are not caught by the DES harness alone.

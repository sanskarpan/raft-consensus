# DES harness

The Deterministic Simulation (DES) harness allows you to write cluster tests that are **reproducible**, **fast**, and **free of `time.Sleep`**. It replaces the real clock and network with injectable, manually-advanceable alternatives driven by a seeded random-number generator.

## Motivation

Conventional cluster tests have two problems:

1. **Flakiness** — real-time election timeouts make tests non-deterministic: a slow CI runner can cause a follower to win an election at a different tick than on a fast machine, breaking assertions that depend on ordering.
2. **Slowness** — tests must `time.Sleep` to wait for elections (typically 150–500 ms). With hundreds of scenarios, this adds minutes to the test suite.

The DES harness solves both: the simulated clock only advances when you call `Tick()`, so the cluster progresses at the speed of the test, not the wall clock. A seeded network scheduler makes message ordering fully reproducible.

---

## Key components

### `simClock` — manually-advanceable clock

```go
// pkg/raft/sim_clock.go

type simClock struct {
    mu      sync.Mutex
    now     time.Time
    tickers []*simTicker
}

func (c *simClock) Advance(d time.Duration) {
    // Moves the clock forward and fires all simTickers whose interval has elapsed.
}
```

All 11 `time.Now()` and 2 `time.NewTicker()` calls in `pkg/raft/raft.go` are replaced with injectable `r.clock.Now()` / `r.newTicker()` calls. In production, `realClock` (wrapping `time.Now()`) is used. In simulation tests, `simClock` is injected.

### `simTicker` — clock-driven ticker

`simTicker` implements the same `Ticker` interface as `time.Ticker`. When `simClock.Advance(d)` is called, every `simTicker` whose next-fire time has elapsed fires its channel — with no real-time waiting.

### `simNetwork` — in-process transport with seeded RNG

```go
// pkg/raft/sim_network.go

type simNetwork struct {
    mu      sync.Mutex
    rng     *rand.Rand    // seeded for reproducibility
    nodes   map[ServerID]*raft
    latency time.Duration // optional per-hop latency
    partitions map[[2]ServerID]bool
    clk     *simClock
}
```

The `simNetwork` delivers RPCs in-process (no actual TCP sockets). It:

- Supports **bidirectional partition injection** via `Partition(a, b)` / `Heal(a, b)`
- Optionally injects **per-hop latency** by advancing the clock before delivery
- Uses a **seeded RNG** so the message scheduling order is deterministic across runs

### `simCluster` — 3-node test cluster

```go
type simCluster struct {
    clk   *simClock
    net   *simNetwork
    nodes []*raft
}
```

`simCluster` is the entry point for most simulation tests. It wires three Raft nodes through the `simClock` and `simNetwork`:

```go
sc := newSimCluster(seed)  // seed = 42 for a fixed run, or time.Now().UnixNano() for random
sc.Start(t)
defer sc.Shutdown()

// Advance time and check state
for i := 0; i < 100; i++ {
    sc.Tick(t)
}
sc.RequireLeader(t)
```

### `Tick()` and quiescence barrier

`Tick()` advances the clock by one heartbeat interval and then **quiesces**: it yields the scheduler until all in-flight goroutine work settles before returning. This prevents races where `Tick()` returns before an AppendEntries triggered by the clock advance has been processed by the follower.

Quiescence is detected via `runtime.Gosched()` loops:

```go
func (sc *simCluster) Tick(t *testing.T) {
    sc.clk.Advance(sc.heartbeat)
    // Yield to let goroutines run
    for i := 0; i < 10; i++ {
        runtime.Gosched()
    }
}
```

---

## Writing a simulation test

```go
// pkg/raft/sim_test.go
package raft

import (
    "testing"
)

func TestElectionConverges(t *testing.T) {
    sc := newSimCluster(42) // fixed seed → reproducible
    sc.Start(t)
    defer sc.Shutdown()

    // Drive enough ticks for an election to complete
    for i := 0; i < 50; i++ {
        sc.Tick(t)
    }

    // Assert exactly one leader
    sc.RequireLeader(t)
}

func TestPartitionAndRecover(t *testing.T) {
    sc := newSimCluster(42)
    sc.Start(t)
    defer sc.Shutdown()

    // Elect a leader
    for i := 0; i < 50; i++ {
        sc.Tick(t)
    }
    sc.RequireLeader(t)

    // Partition node s1 from the rest
    sc.net.Partition("s1", "s2")
    sc.net.Partition("s1", "s3")

    // Drive enough ticks for a new leader election
    for i := 0; i < 100; i++ {
        sc.Tick(t)
    }
    // s2 and s3 should have elected a new leader
    sc.RequireLeader(t)

    // Heal the partition
    sc.net.Heal("s1", "s2")
    sc.net.Heal("s1", "s3")

    // s1 catches up
    for i := 0; i < 50; i++ {
        sc.Tick(t)
    }
}
```

---

## Partition and latency injection

```go
// Partition: drop all messages between two nodes (bidirectional)
sc.net.Partition("s1", "s2")

// Heal: restore connectivity
sc.net.Heal("s1", "s2")

// Latency: add artificial per-hop delay (clock-based, not wall-clock)
sc.net = newSimNetwork(seed, clk, 5*time.Millisecond)
```

---

## Process-based integration harness

For full end-to-end tests that run real `raftd` binaries, use the process harness in `tools/testharness/`:

```go
// tools/testharness/integration_test.go
package testharness

func TestMultiProcessCluster(t *testing.T) {
    dir := t.TempDir()
    h := NewHarness(dir, 19200)

    // Start a 3-node cluster
    h.StartNode(t, "node1")
    h.StartNode(t, "node2")
    h.StartNode(t, "node3")
    defer h.StopAll()

    // Wait for a leader
    leader := h.WaitForLeader(t, 30*time.Second)

    // Write a key via HTTP
    h.Put(t, leader, "hello", "world")

    // Read it back and verify
    val := h.Get(t, leader, "hello")
    if val != "world" {
        t.Fatalf("expected world, got %s", val)
    }
}
```

The process harness:

- Compiles `raftd` once (via `go build`) and caches the binary
- Assigns ports deterministically, reusing assigned ports on node restart
- Provides helpers for `Put`, `Get`, `Delete`, `WaitForLeader`, `StopNode`, `RestartNode`
- Supports `WithExtraConfig(yaml)` for feature-specific settings (e.g., `ttl_tick_interval`)

---

## Running the tests

```bash
# Unit + simulation tests (fast, no real time)
go test ./pkg/raft/... -race -count=1

# Process-based integration tests (requires build)
go build -o raftd ./cmd/raftd
go test ./tools/testharness/... -race -count=1 -timeout 120s
```

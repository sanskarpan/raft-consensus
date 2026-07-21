# ADR-0002: Binary TCP wire format with sync.Pool

**Status:** Accepted
**Date:** 2026-03
**Issue:** #222

---

## Context

The original TCP transport serialised all Raft RPCs (AppendEntries, RequestVote, InstallSnapshot, TimeoutNow) as JSON. JSON is human-readable and easy to debug, but it uses reflection-based encoding and produces variable-length output that the decoder must fully parse before use. On the hot AppendEntries path — which fires every heartbeat tick and on every write — this overhead was measurable.

### Benchmark results (before vs after)

| Operation | JSON | Binary | Speedup |
|---|---|---|---|
| Marshal | 1.00x | 4.7x faster | 4.7x |
| Unmarshal | 1.00x | 19x faster | 19x |

The 19x unmarshal speedup comes from:
- No reflection
- No map allocation for unknown JSON fields
- No string-to-int parsing for log indices and terms
- Fixed field order: the decoder reads fields in the same order the encoder wrote them

### Rolling upgrade requirement

We could not simply replace JSON with binary and ship a breaking change, because:
- Existing nodes in a running cluster still speak JSON
- A big-bang cutover would require simultaneous restart of all nodes
- We need a smooth migration path

---

## Decision

### Binary frame format

Replace JSON on the hot path with a **9-byte binary frame header**:

```
[4 bytes magic: RF\x02\x00] [1 byte type tag] [4 bytes payload length big-endian uint32]
```

The type tag selects the message type (AppendEntriesReq, AppendEntriesResp, RequestVoteReq, etc.) so the decoder knows which struct to instantiate without a discriminator field in the payload.

### Per-connection negotiation

Binary framing is negotiated per-connection:

1. The connecting client sends the 4-byte magic probe
2. The server echoes it to confirm
3. Both sides switch to binary framing for the lifetime of the connection
4. If the server does not echo the magic (e.g., it is an old JSON-only node), the client falls back to JSON

This means:
- New nodes interoperate with old JSON-only nodes during a rolling upgrade
- Old nodes never receive binary frames they cannot parse
- The negotiation adds a single round-trip per connection, not per RPC

The negotiation result is cached on the `peer` struct so the check is free on subsequent RPCs.

### sync.Pool encode buffers

Outbound RPC payloads are serialised into `*bytes.Buffer` values drawn from a `sync.Pool`:

```go
var encBufPool = sync.Pool{
    New: func() interface{} { return new(bytes.Buffer) },
}
```

This reuses `bytes.Buffer` allocations across requests on the same goroutine, reducing per-RPC heap allocations and GC pressure on the hot AppendEntries path.

### Configuration

A `BinaryTransport bool` field (default `true`) was added to `TCPTransportConfig`. Operators can set `binary_transport: false` in the config to force JSON framing for debugging or to roll back.

---

## Consequences

**Good:**

- 4.7x marshal / 19x unmarshal speedup on the hot AppendEntries path
- Lower GC pressure from pooled encode buffers
- Smooth rolling upgrade: old and new nodes coexist
- Debug/rollback path: set `binary_transport: false` to fall back to human-readable JSON

**Neutral:**

- Two code paths to maintain (`handleConnBinary` and `handleConnJSON`)
- The binary codec is not self-describing — adding new fields requires a new type tag or explicit versioning

**Bad:**

- The binary wire format cannot be read by `curl` / `nc` directly (but a flag restores this)
- Adds complexity to the transport layer

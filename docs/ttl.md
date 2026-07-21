# TTL & key expiry

Keys can be given a time-to-live (TTL) that causes them to be deleted automatically after a specified number of seconds.

## How it works

TTL expiry uses a **deterministic apply-time clock** (`applyTimeMs`) rather than wall-clock time. This is a critical design choice — see [ADR-0001](adr/0001-apply-time-clock.md) for the rationale.

### Apply-time clock

The `KVStore` maintains a virtual monotonic clock (`applyTimeMs`, an int64 Unix millisecond timestamp). This clock is **never** advanced by `time.Now()` on individual replicas. Instead, only the leader stamps commands with the current wall-clock time (`LeaderTimestampMs`), and all replicas advance `applyTimeMs` to that value when they apply the same log entry. Because all replicas apply the same log entries in the same order, their `applyTimeMs` values are byte-identical.

### Key expiry

When a key is written with a TTL:

1. The leader stamps the write command with `LeaderTimestampMs = time.Now().UnixMilli()` and `TTLSeconds = N`
2. The FSM computes `ExpiresAtMs = LeaderTimestampMs + TTLSeconds*1000` and stores it on the `KeyValue`
3. A key is considered *expired* (invisible) when `ExpiresAtMs > 0 && ExpiresAtMs <= applyTimeMs`

### Tick loop

The leader proposes a `tick` command every `ttl_tick_interval` (default 1 second). When the FSM applies a tick:

1. `applyTimeMs` advances to the leader's timestamp
2. All keys with `ExpiresAtMs <= applyTimeMs` are deleted
3. An `EventDelete` watch event is emitted for each deleted key

This ensures expiry happens eagerly on a predictable schedule rather than lazily on the next read.

---

## Setting a TTL

### HTTP API

Include a `ttl_seconds` query parameter on a PUT request:

```bash
curl -s -X PUT \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d '{"value":"temporary"}' \
  "http://localhost:8012/v1/kv/session/user123?ttl_seconds=300" | jq .
```

Response includes `expires_at_ms`:

```json
{
  "key": "session/user123",
  "value": "temporary",
  "create_revision": 5,
  "mod_revision": 5,
  "version": 1,
  "expires_at_ms": 1750000300000
}
```

### kvctl CLI

```bash
# Key expires in 30 seconds
kvctl --leader=localhost:8012 put --ttl=30 mykey "short-lived"

# Key expires in 1 hour
kvctl --leader=localhost:8012 put --ttl=3600 session/abc "token-data"
```

---

## Reading TTL keys

A key past its TTL is treated as non-existent for all read operations:

- `GET /v1/kv/key` returns 404
- `GET /v1/kv?prefix=...` excludes the expired key from range results
- Linearizable reads via ReadIndex also see the key as gone

The key is not immediately removed from storage when a read detects it is expired — removal only happens during a tick sweep. However, expired keys are never returned to callers.

---

## Expiry events

When a key expires during a tick sweep, the `WatchManager` delivers an `EventDelete` event:

```json
{
  "events": [{
    "type": 1,
    "key": "session/user123",
    "prev_kv": {
      "key": "session/user123",
      "value": "temporary",
      "create_revision": 5,
      "mod_revision": 5,
      "version": 1,
      "expires_at_ms": 1750000300000
    },
    "revision": 48
  }],
  "revision": 48
}
```

Clients watching the key or its prefix receive this event exactly once, on all replicas simultaneously.

---

## Tick interval configuration

```yaml
# How often the leader proposes a tick to advance the FSM clock
# Default: 1s. Set to 0 to disable TTL expiry entirely.
ttl_tick_interval: 1s
```

!!! warning "Leader-only ticks"
    Only the leader proposes tick commands. Followers do not advance `applyTimeMs` on their own. If a tick is lost (e.g., during leader failover), the next tick after the new leader takes over catches up. Keys may expire slightly later than their nominal TTL during failover.

---

## Snapshot compatibility

The apply-time clock (`applyTimeMs`) and per-key expiry times (`ExpiresAtMs`) are persisted in the snapshot (format version 2). When a node restores from a snapshot, it resumes the correct virtual clock state. Old format-v1 snapshots are loaded without `applyTimeMs` (zero), causing a brief grace period until the next tick.

---

## Disabling TTL

Set `ttl_tick_interval: 0` in the config to disable the leader tick loop entirely. Keys written with a TTL are still stored with `ExpiresAtMs`, but they are never swept and are invisible to reads once `applyTimeMs` reaches their expiry time. Setting the interval to zero is appropriate when TTL keys are not used and you want to eliminate the tick proposal overhead.

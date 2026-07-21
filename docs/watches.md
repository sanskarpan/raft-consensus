# Watches & events

The watch API delivers real-time change notifications over [Server-Sent Events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events) (SSE). Any node — leader or follower — can serve watch streams.

## How it works

1. The client opens a long-lived `GET /v1/watch` connection
2. The server registers a `watchEntry` in the `WatchManager`
3. Every committed FSM mutation emits `Event` values into a buffered channel
4. The `WatchManager` fans events out to all matching subscribers
5. The SSE connection delivers each event batch as a `data:` line

The `WatchManager` holds a ring buffer of 1 024 past events for late-subscriber **history replay**. When a client reconnects and sends a `Last-Event-ID` header with the last-seen revision, the server replays events from the buffer and then seamlessly transitions to the live stream — with no duplicate events and no gaps.

---

## Endpoint

```
GET /v1/watch
```

**Query parameters:**

| Parameter | Description |
|---|---|
| `key` | Watch an exact key (mutually exclusive with `prefix`) |
| `prefix` | Watch all keys with this prefix |
| `since_revision` | Replay history since this revision (defaults to current) |

**Request headers:**

| Header | Description |
|---|---|
| `Authorization` | Bearer token (required when auth is configured) |
| `Last-Event-ID` | Revision to resume from on reconnect |

---

## Event format

Each SSE message carries a JSON-encoded `WatchEvent`:

```
id: 42
data: {"events":[{"type":0,"key":"mykey","kv":{"key":"mykey","value":"hello","create_revision":1,"mod_revision":42,"version":5,"expires_at_ms":0},"prev_kv":{"key":"mykey","value":"old","create_revision":1,"mod_revision":38,"version":4,"expires_at_ms":0},"revision":42}],"revision":42}
```

The `id` field is the revision of the last event in the batch. Clients use this as `Last-Event-ID` when reconnecting.

### `Event` object fields

| Field | Type | Description |
|---|---|---|
| `type` | int | `0` = put (create or update), `1` = delete |
| `key` | string | The affected key |
| `kv` | KeyValue | The new key-value (absent for deletes) |
| `prev_kv` | KeyValue | The previous key-value (absent for creates) |
| `revision` | int64 | The global revision at which this event occurred |

---

## Examples

### Watch an exact key

```bash
curl -s -N \
  -H "Authorization: Bearer secret" \
  "http://localhost:8012/v1/watch?key=mykey"
```

In a second terminal, write to the key:

```bash
curl -s -X PUT \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d '{"value":"updated"}' \
  http://localhost:8012/v1/kv/mykey
```

The watch stream receives:

```
id: 5
data: {"events":[{"type":0,"key":"mykey","kv":{...},"prev_kv":{...},"revision":5}],"revision":5}
```

### Watch a prefix

```bash
curl -s -N \
  -H "Authorization: Bearer secret" \
  "http://localhost:8012/v1/watch?prefix=app/config/"
```

Any write to a key under `app/config/` fires an event.

### Resume from a revision

If a watch connection drops, reconnect using the last received revision:

```bash
curl -s -N \
  -H "Authorization: Bearer secret" \
  -H "Last-Event-ID: 42" \
  "http://localhost:8012/v1/watch?key=mykey"
```

The server replays buffered history events with revision > 42, then continues with live events. This is guaranteed to have no gaps and no duplicates as long as the missed events are still in the 1 024-event ring buffer.

### Watch using kvctl

```bash
# Watch an exact key
kvctl --leader=localhost:8012 watch mykey

# Watch all keys under a prefix
kvctl --leader=localhost:8012 watch --prefix app/config/

# Watch from a specific revision
kvctl --leader=localhost:8012 watch --since-revision=42 mykey
```

---

## Watch limits

To prevent resource exhaustion:

| Limit | Default | Config key |
|---|---|---|
| Total concurrent watch connections | 1 024 | `max_watch_connections` |
| Watch connections per client IP | 32 | `max_watch_connections_per_ip` |
| Idle timeout (no events) | 5 minutes | `watch_idle_timeout` |

When a limit is exceeded the server returns `429 Too Many Requests` and the client should reconnect with backoff.

---

## Transaction events

When a transaction commits multiple operations, all resulting events share the **same revision** (one revision increment per transaction). They are delivered as a single event batch:

```json
{
  "events": [
    {"type":0,"key":"lock","kv":{...},"revision":10},
    {"type":0,"key":"data","kv":{...},"revision":10}
  ],
  "revision": 10
}
```

---

## TTL expiry events

When a key expires due to a TTL, the leader's tick command triggers a sweep on all replicas simultaneously. Each expired key emits an `EventDelete` event with `type: 1`. Watchers subscribed to an expiring key or its prefix receive the delete event exactly once.

---

## Dropped events

If the `WatchManager` cannot fan out events fast enough (a slow subscriber), it increments the `watch_dropped_events` counter visible in `/v1/status`. The dropped client will miss live events but can recover by reconnecting with `Last-Event-ID` to trigger history replay.

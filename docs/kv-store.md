# KV Store API reference

The KV store is the finite state machine (FSM) that backs the Raft cluster. It provides an etcd-style versioned key-value interface over HTTP.

## Data model

Each key holds a `KeyValue` object:

```json
{
  "key": "mykey",
  "value": "hello",
  "create_revision": 1,
  "mod_revision": 3,
  "version": 2,
  "expires_at_ms": 0
}
```

| Field | Description |
|---|---|
| `key` | The key string |
| `value` | The value string (arbitrary bytes encoded as a string) |
| `create_revision` | Global cluster revision at which the key was first created |
| `mod_revision` | Global cluster revision of the most recent modification |
| `version` | Number of times this key has been written (monotonically increasing) |
| `expires_at_ms` | Unix-millisecond expiry time; `0` means the key never expires |

The **global cluster revision** is a monotonically increasing int64 counter that increments on every write (put, delete, transaction) committed through Raft. All replicas maintain the same revision because they apply the same log entries in the same order.

---

## Authentication

All KV endpoints require a Bearer token when any `admin_token` or `admin_tokens` is configured:

```bash
curl -H "Authorization: Bearer <token>" http://localhost:8012/v1/kv/mykey
```

In development with `allow_no_auth: true`, the header may be omitted.

---

## HTTP endpoints

### `PUT /v1/kv/{key}` — write a key

Writes a key. Must be sent to the **leader** (or any node; non-leaders forward automatically).

**Request body** (JSON):

```json
{
  "value": "hello world"
}
```

Optional query parameter `ttl_seconds=N` (integer) sets the key's TTL. See [TTL & key expiry](ttl.md).

**Response** (200 OK):

```json
{
  "key": "mykey",
  "value": "hello world",
  "create_revision": 1,
  "mod_revision": 1,
  "version": 1,
  "expires_at_ms": 0
}
```

**Example:**

```bash
curl -s -X PUT \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d '{"value":"hello world"}' \
  http://localhost:8012/v1/kv/mykey | jq .
```

With TTL:

```bash
curl -s -X PUT \
  -H "Authorization: Bearer secret" \
  -H "Content-Type: application/json" \
  -d '{"value":"temp"}' \
  "http://localhost:8012/v1/kv/session?ttl_seconds=30" | jq .
```

---

### `GET /v1/kv/{key}` — linearizable read

Returns the value for a key. By default this is a **linearizable** read: the node issues a ReadIndex RPC to the leader and waits until its FSM has caught up to at least the leader's commit index before serving the response. This guarantees the response reflects all writes that were committed before the read was issued.

**Response** (200 OK):

```json
{
  "key": "mykey",
  "value": "hello world",
  "create_revision": 1,
  "mod_revision": 1,
  "version": 1,
  "expires_at_ms": 0
}
```

**Response** (404 Not Found) when the key does not exist:

```json
{"error": "key not found"}
```

**Stale read:** add `?stale=true` to skip the ReadIndex round-trip and read directly from the local FSM. The value may be slightly behind the leader.

**Example:**

```bash
# Linearizable read
curl -s -H "Authorization: Bearer secret" \
  http://localhost:8012/v1/kv/mykey | jq .

# Stale read (faster, may be slightly behind)
curl -s -H "Authorization: Bearer secret" \
  "http://localhost:8012/v1/kv/mykey?stale=true" | jq .
```

---

### `DELETE /v1/kv/{key}` — delete a key

Deletes a key. Must be sent to the leader (or any node; non-leaders forward automatically).

**Response** (200 OK):

```json
{"status": "deleted"}
```

**Response** (404) when the key does not exist.

**Example:**

```bash
curl -s -X DELETE \
  -H "Authorization: Bearer secret" \
  http://localhost:8012/v1/kv/mykey | jq .
```

---

### `GET /v1/kv?prefix={prefix}` — range scan

Returns all keys whose name has the given prefix, sorted lexicographically. Results are capped at 10 000 keys per request (returns an error if the prefix would match more).

**Response** (200 OK):

```json
[
  {"key":"app/config/db","value":"mydb","create_revision":2,"mod_revision":2,"version":1,"expires_at_ms":0},
  {"key":"app/config/host","value":"db.example.com","create_revision":3,"mod_revision":3,"version":1,"expires_at_ms":0}
]
```

**Example:**

```bash
curl -s -H "Authorization: Bearer secret" \
  "http://localhost:8012/v1/kv?prefix=app/config/" | jq .
```

---

### `GET /v1/status` — cluster and FSM status

Returns the current node's Raft state, cluster membership, FSM revision, and observability counters.

**Response** (200 OK):

```json
{
  "node_id": "node1",
  "state": "leader",
  "leader": "node1",
  "term": 2,
  "commit_index": 42,
  "applied_index": 42,
  "revision": 15,
  "fsm_dropped_events": 0,
  "watch_dropped_events": 0
}
```

**Example:**

```bash
curl -s -H "Authorization: Bearer secret" \
  http://localhost:8012/v1/status | jq .
```

---

## Leader forwarding

Non-leader nodes automatically forward write requests (PUT, DELETE, POST) to the leader's HTTP address. The `X-Raft-Leader-Address` response header tells the client the leader's address so it can route future writes there directly, avoiding the extra hop.

```
HTTP/1.1 200 OK
X-Raft-Leader-Address: localhost:8012
X-Request-ID: a3f9c2b1e4d7...
```

Watch requests (`GET /v1/watch`) are **not** forwarded — any node can serve watches.

---

## kvctl CLI

The `kvctl` binary provides a command-line interface for all KV operations:

```bash
# Put a key
kvctl --leader=localhost:8012 put mykey hello

# Get a key (linearizable)
kvctl --leader=localhost:8012 get mykey

# Get a key (stale)
kvctl --leader=localhost:8012 get --stale mykey

# Delete a key
kvctl --leader=localhost:8012 delete mykey

# Range scan
kvctl --leader=localhost:8012 range app/config/

# Cluster status
kvctl --leader=localhost:8012 status
```

See `kvctl --help` for the full list of subcommands and flags.

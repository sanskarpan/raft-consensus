# API Reference

This document covers the `raftd` HTTP API and the `kvctl` CLI. Endpoint behavior is
defined in `cmd/raftd/main.go`; the CLI in `cmd/kvctl/main.go`; the Go client in
`pkg/client/client.go`.

- [Conventions](#conventions)
- [Authentication and roles](#authentication-and-roles)
- [Endpoint summary](#endpoint-summary)
- [Health and status](#health-and-status)
- [Key/value API (v1)](#keyvalue-api-v1)
- [Transactions](#transactions)
- [Watches (SSE)](#watches-sse)
- [Admin API](#admin-api)
- [Membership API](#membership-api)
- [Legacy command API](#legacy-command-api)
- [Common status codes](#common-status-codes)
- [kvctl command reference](#kvctl-command-reference)
- [Go client library](#go-client-library)

## Conventions

- Base URL is `http://<http_addr>` (or `https://` when `https_cert`/`https_key` are
  set). All examples below use `localhost:8002` (node1's HTTP port in the bundled
  configs).
- Request/response bodies are JSON unless noted (watches are SSE; PUT accepts a raw
  body).
- **Leader forwarding**: writes and linearizable reads sent to a follower are
  transparently forwarded to the leader over HTTP. Watches are served by any node.
- **Idempotency headers** (optional, on writes): `X-Client-ID` and `X-Seq-Num`
  (unsigned integer). When both are present, the FSM deduplicates retries.
- Internal error detail is logged server-side and never returned; clients get a
  generic `{"error": "..."}` body.

## Authentication and roles

Send the token as an HTTP header:

```
Authorization: Bearer <token>
```

Roles come from config (`admin_token` → `write`; `admin_tokens` map → `read` or
`write`). `write` implies `read`.

| Behavior | Condition |
|----------|-----------|
| Request allowed with `write` role | `allow_no_auth: true` and no tokens configured (dev). |
| All requests rejected (401) | No tokens configured and `allow_no_auth` unset. |
| 401 Unauthorized | Missing/unknown token. |
| 403 Forbidden | Valid `read` token used on a `write` endpoint. |

`/health` and `/ready` are always unauthenticated.

## Endpoint summary

| Method | Path | Auth / role | Notes |
|--------|------|-------------|-------|
| GET | `/health` | none | Liveness. |
| GET | `/ready` | none | Readiness (leader or follower). |
| GET | `/metrics` | none | Prometheus metrics. |
| GET/PUT/POST/DELETE | `/v1/kv/{key}` | read (GET) / write | Single-key ops. |
| GET | `/v1/kv?prefix=` | read | Range scan. |
| POST | `/v1/txn` | write | Compare-and-swap transaction. |
| GET | `/v1/watch` | read | SSE change stream (any node). |
| GET | `/v1/status` | read | Extended status + revision. |
| POST/PUT | `/command` | write | Legacy raw FSM command apply. |
| GET | `/admin/cluster` | read | Raft configuration + role/term. |
| POST | `/admin/snapshot` | write | Trigger a snapshot. |
| GET/POST | `/admin/members` | read (GET) / write (POST) | List / add voting member. |
| DELETE/POST | `/admin/members/{id}[/promote|/demote]` | write | Remove / promote / demote. |

> The membership routes are registered under `requireRole("write", …)`, so a valid
> `write` token is required even for the `GET /admin/members` listing;
> `GET /admin/cluster` only requires authentication (any valid token).

## Health and status

### `GET /health`
Always returns `200 OK` with body `ok`. Use for liveness probes.

### `GET /ready`
Returns `200 OK` (`ready`) when the node is a leader or follower;
`503 Service Unavailable` (`not ready`) when it is a candidate/learner/shutting
down. Use for readiness probes.

```bash
curl -i localhost:8002/ready
```

### `GET /v1/status`
Extended cluster status. Requires a valid token.

```bash
curl localhost:8002/v1/status -H "Authorization: Bearer $T"
```

```json
{
  "node_id": "node1",
  "state": "Leader",
  "leader": "node1",
  "term": 4,
  "last_index": 128,
  "applied_index": 128,
  "revision": 57,
  "cluster": { "Servers": [ { "ID": "node1", "Address": "localhost:8001", "Learner": false }, ... ] },
  "fsm_dropped_events": 0,
  "watch_dropped_events": 0
}
```

## Key/value API (v1)

### `GET /v1/kv/{key}`
Read a single key. **Linearizable by default** (ReadIndex quorum confirmation +
FSM catch-up). Pass `?consistency=stale` for a fast local read that may be slightly
behind.

- **200** — JSON `KeyValue`.
- **404** — `{"error":"key not found"}`.

```bash
curl localhost:8002/v1/kv/hello -H "Authorization: Bearer $T"
curl "localhost:8002/v1/kv/hello?consistency=stale" -H "Authorization: Bearer $T"
```

```json
{ "key": "hello", "value": "world", "create_revision": 12, "mod_revision": 30, "version": 3 }
```

### `PUT /v1/kv/{key}` (or `POST`)
Set a key. Body is the **raw value** unless `Content-Type: application/json`, in
which case a `{"value":"..."}` envelope is expected. Requires the `write` role.
Optional `X-Client-ID` / `X-Seq-Num` for idempotent retries.

- **200** — the resulting `KeyValue`.
- **400** — bad body, invalid JSON, or key/value too large (key > 4 KiB, value > 512 KiB).

```bash
# raw
curl -X PUT localhost:8002/v1/kv/hello -H "Authorization: Bearer $T" -d 'world'
# JSON envelope
curl -X PUT localhost:8002/v1/kv/hello -H "Authorization: Bearer $T" \
     -H 'Content-Type: application/json' -d '{"value":"world"}'
```

### `POST /v1/kv/{key}?op=incr`
Atomically add a signed `delta` to an integer-valued key and return the updated
`KeyValue` (`value` is the new count). A missing key starts at `0`. Requires the
`write` role. Optional `X-Client-ID` / `X-Seq-Num` for idempotent retries.
Responses include `X-Raft-Leader-Address` so clients can route writes to the leader.

- **200** — the updated `KeyValue`.
- **400** — non-integer delta, non-integer existing value, or int64 overflow.

```bash
curl -X POST 'localhost:8002/v1/kv/visits?op=incr' -H "Authorization: Bearer $T" \
     -H 'Content-Type: application/json' -d '{"delta":1}'
```

### `DELETE /v1/kv/{key}`
Delete a key. Requires `write`. Optional idempotency headers.

- **200** — `{"status":"deleted"}`.
- **404** — key did not exist (`{"error": "..."}`).

```bash
curl -X DELETE localhost:8002/v1/kv/hello -H "Authorization: Bearer $T"
```

### `GET /v1/kv?prefix={p}`
Range scan by prefix. **Linearizable by default**; forwarded to the leader and
gated on ReadIndex. Pass `?consistency=stale` for a local read (response carries
`X-Consistency: stale`). Returns a JSON array (`[]` when empty, never `null`).

**Pagination** — add `&limit=N` (and `&start_after=<cursor>`) to bound the
response to `N` keys strictly after the cursor, in key order. The response then
carries `X-Has-More: true|false` and `X-Next-Cursor: <last-key>`; pass that
cursor as the next `start_after` to walk large key sets without the single-shot
10k-key cap.

```bash
curl "localhost:8002/v1/kv?prefix=user/" -H "Authorization: Bearer $T"
# paginated
curl -i "localhost:8002/v1/kv?prefix=user/&limit=500" -H "Authorization: Bearer $T"
```

```json
[ { "key": "user/1", "value": "alice", "create_revision": 5, "mod_revision": 5, "version": 1 } ]
```

## Transactions

### `POST /v1/txn`
Atomic compare-and-swap transaction. Requires `write`. Forwarded to the leader.

Request body:

```json
{
  "compare": [
    { "key": "hello", "target": "value", "result": "equal", "value": "world", "rev": 0 }
  ],
  "success": [ { "type": 0, "key": "hello", "value": "there" } ],
  "failure": [ { "type": 1, "key": "hello" } ]
}
```

- `compare[].target`: `value` | `version` | `create_revision` | `mod_revision`.
- `compare[].result`: `equal` | `not_equal` | `greater` | `less`.
- op `type`: `0` = put, `1` = delete.
- If **all** comparisons pass, `success` ops run; otherwise `failure` ops run.
  The transaction is atomic — nothing is written unless all ops validate.

Response:

```json
{
  "succeeded": true,
  "results": [ { "kv": { "key": "hello", "value": "there", "create_revision": 12, "mod_revision": 31, "version": 4 } } ],
  "revision": 31
}
```

## Watches (SSE)

### `GET /v1/watch`
Server-Sent-Events stream of KV changes. Requires a valid token. **Served by any
node** (not forwarded). Query parameters:

| Param | Effect |
|-------|--------|
| `key` | Watch an exact key (mutually exclusive with `prefix`; one is required). |
| `prefix` | Watch all keys with this prefix. |
| `revision` | Replay history from this revision (also accepted via `Last-Event-ID` header). |

Each event is emitted as an SSE frame `id: <revision>\ndata: <json>\n\n`. Idle
connections are closed after `watch_idle_timeout` (an `event: timeout` frame is
sent first) so clients reconnect and resume from `Last-Event-ID`.

- **400** — neither `key` nor `prefix`, or an invalid `revision`.
- **503** — watch-connection limit reached (`Retry-After: 5`).

```bash
curl -N "localhost:8002/v1/watch?prefix=user/" -H "Authorization: Bearer $T"
```

```
id: 31
data: {"events":[{"type":0,"key":"user/1","kv":{"key":"user/1","value":"alice","create_revision":5,"mod_revision":31,"version":2},"prev_kv":{...},"revision":31}],"revision":31}
```

Event `type`: `0` = put, `1` = delete. `kv` is null on delete; `prev_kv` is null on
create.

## Admin API

### `GET /admin/cluster`
Raft configuration and this node's view of the cluster. Requires a valid token.

```bash
curl localhost:8002/admin/cluster -H "Authorization: Bearer $T"
```

```json
{
  "node_id": "node1",
  "state": "Leader",
  "leader": "node1",
  "term": 4,
  "commit_idx": 128,
  "config": { "Servers": [ { "ID": "node1", "Address": "localhost:8001", "Learner": false }, ... ] }
}
```

### `POST /admin/snapshot`
Trigger a snapshot on this node. Requires `write`.

- **200** — `{"status":"ok"}`.
- **500** — snapshot failed.

```bash
curl -X POST localhost:8002/admin/snapshot -H "Authorization: Bearer $T"
```

## Membership API

All membership mutations require the `write` role and must be sent to the **leader**
(a follower returns `503 {"error":"not leader"}`). Raft allows only one outstanding
configuration change at a time.

### `GET /admin/members`
List current members.

```json
{ "members": [ { "ID": "node1", "Address": "localhost:8001", "Learner": false }, ... ] }
```

### `POST /admin/members`
Add a **voting** member. Body: `{"id": "...", "address": "..."}`.

- **200** — `{"status":"ok","id":"node4"}`.
- **400** — missing `id`/`address`.
- **409** — already a member, or a config change is already in progress.
- **503** — not the leader.

```bash
curl -X POST localhost:8002/admin/members -H "Authorization: Bearer $T" \
     -d '{"id":"node4","address":"localhost:8007"}'
```

> For a brand-new node that must catch up first, prefer adding it as a learner and
> promoting once caught up (Raft permits only one config change at a time). The
> HTTP layer does not expose a dedicated add-learner route — use the Raft
> `AddLearner`/`PromoteLearner` API when embedding the package, or the promote/demote
> routes below for an existing learner.

### `DELETE /admin/members/{id}`
Remove a server. **200** `{"status":"removed","id":"..."}`.

```bash
curl -X DELETE localhost:8002/admin/members/node4 -H "Authorization: Bearer $T"
```

### `POST /admin/members/{id}/promote`
Promote a learner to a voter. Fails if the learner is not sufficiently caught up.
**200** `{"status":"promoted","id":"..."}`.

### `POST /admin/members/{id}/demote`
Demote a voter to a learner (implemented as a remove of the voting role).
**200** `{"status":"demoted","id":"..."}`.

## Legacy command API

### `POST /command` (or `PUT`)
Applies a raw, pre-encoded FSM command (`fsm.EncodeCommand` wire format). Requires
`write`; rate-limited; forwarded to the leader. Prefer the `/v1/*` endpoints. Used
by the Go client's legacy `SubmitCommand`/`GetValue`/`DeleteValue` methods.

- **200** — `{"result":"<encoded result>"}`.
- **503** — no leader.

## Common status codes

| Code | Meaning |
|------|---------|
| 200 | Success. |
| 204 | CORS preflight (`OPTIONS`). |
| 400 | Bad request (body, size, invalid param). |
| 401 | Authentication required / unknown token. |
| 403 | Write role required. |
| 404 | Key not found. |
| 405 | Method not allowed. |
| 409 | Conflict (already a member / config change in progress). |
| 421 | Misdirected request (not leader — from `writeError`). |
| 429 | Rate limit exceeded (`Retry-After: 1`). |
| 500 | Internal error (detail logged, not returned). |
| 503 | No leader / not ready / not leader / too many watches. |

## kvctl command reference

```
kvctl [flags] <command> [args...]
```

**Global flags**

| Flag | Default | Effect |
|------|---------|--------|
| `--endpoints` | `localhost:8101` | Comma-separated node HTTP addresses. |
| `--timeout` | `10s` | Per-request timeout. |
| `--stale` | `false` | Use stale consistency for `get`/`range`. |
| `--prefix` | `false` | With `watch`, watch by prefix instead of exact key. |
| `--revision` | `0` | Start `watch`/history from this revision. |

**Commands**

| Command | Usage | Description |
|---------|-------|-------------|
| `put` | `put <key> <value>` | Set a key. |
| `get` | `get <key>` | Read a key (linearizable; `--stale` for local read). |
| `delete` | `delete <key>` | Delete a key. |
| `range` | `range [prefix]` | List keys, optionally by prefix. |
| `txn` | `txn [file\|-]` | Execute a transaction from a JSON file or stdin. |
| `watch` | `watch <key>` | Stream change events (`--prefix`, `--revision`). Ctrl-C to stop. |
| `status` | `status` | Print cluster status and revision. |

```bash
export EP=localhost:8002,localhost:8004,localhost:8006
kvctl --endpoints $EP put user/1 alice
kvctl --endpoints $EP get user/1
kvctl --endpoints $EP get user/1 --stale
kvctl --endpoints $EP range user/
kvctl --endpoints $EP watch user/ --prefix --revision 10
echo '{"compare":[],"success":[{"type":0,"key":"k","value":"v"}],"failure":[]}' | kvctl --endpoints $EP txn -
kvctl --endpoints $EP status
```

## Go client library

`import "github.com/sanskarpan/raft-consensus/pkg/client"`

```go
c := client.NewClient(
    client.WithAddresses([]string{"localhost:8002", "localhost:8004", "localhost:8006"}),
    client.WithTimeout(10 * time.Second),
)

kv, err := c.Put("hello", "world")     // PUT /v1/kv/hello
kv, err = c.GetKV("hello")             // linearizable read
kv, err = c.GetKVStale("hello")        // stale local read
kvs, err := c.Range("user/")           // GET /v1/kv?prefix=user/
err = c.DeleteKV("hello")              // DELETE /v1/kv/hello
resp, err := c.Txn(&client.ClientTxnRequest{ /* compare/success/failure */ })
info, err := c.GetClusterInfo()        // /admin/cluster

// Watch (auto-reconnecting SSE)
ch, err := c.Watch(ctx, "hello", client.WithRevision(10))
for ev := range ch { /* ev.Events, ev.Revision, ev.Err */ }
chp, err := c.WatchPrefix(ctx, "user/")
```

The client discovers the leader from `/admin/cluster`, caches it, and retries write
operations across all endpoints with exponential backoff. Mutating calls send
`X-Client-ID` + a stable `X-Seq-Num` so retries are deduplicated server-side.
Options: `WithAddresses`, `WithTimeout`, `WithMaxLeaseDuration`, and the per-watch
`WithRevision`.

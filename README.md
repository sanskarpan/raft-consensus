# Raft Consensus

> A linearizable, strongly-consistent distributed key/value store built on a from-scratch Raft implementation in Go — WAL storage, gRPC/TCP transport, mTLS, snapshots, joint-consensus membership, watches, and a React dashboard.

[![CI](https://github.com/sanskarpan/raft-consensus/actions/workflows/ci.yml/badge.svg)](https://github.com/sanskarpan/raft-consensus/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)](https://go.dev/dl/)
[![Go Report Card](https://goreportcard.com/badge/github.com/sanskarpan/raft-consensus)](https://goreportcard.com/report/github.com/sanskarpan/raft-consensus)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)

`raftd` is a replicated key/value store that keeps a cluster of nodes in agreement using the [Raft consensus algorithm](https://raft.github.io/raft.pdf). Writes are committed to a majority before being acknowledged; reads can be **linearizable** (confirmed by a heartbeat quorum via ReadIndex) or **stale** (served locally for speed). It ships with an HTTP API, a Go client, a `kvctl` CLI, Prometheus metrics, OpenTelemetry tracing, and a Helm chart.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Quickstart](#quickstart)
  - [Build from source](#build-from-source)
  - [Docker Compose](#docker-compose)
  - [Kubernetes / Helm](#kubernetes--helm)
- [Configuration reference](#configuration-reference)
- [Client usage](#client-usage)
  - [kvctl](#kvctl)
  - [HTTP API (curl)](#http-api-curl)
- [Operational notes](#operational-notes)
- [Benchmarks](#benchmarks)
- [Security](#security)
- [Documentation](#documentation)
- [License](#license)

---

## Features

- **Leader election with pre-vote** — randomized election timeouts and an optional pre-vote round that prevents a partitioned node from disrupting a stable leader.
- **Log replication** — `AppendEntries` with group-commit batching and a conflict-term fast-backup optimization for quick log reconciliation.
- **Durable WAL** — segment-based write-ahead log (64 MiB segments) with per-record CRC32 and fsync-on-append; BoltDB stable store for term/vote/config.
- **Snapshots & compaction** — periodic and size-triggered snapshots with atomic, checksummed on-disk format and `InstallSnapshot` streaming to lagging followers.
- **Joint-consensus membership** — safe cluster reconfiguration (add/remove voters) via C_old,new joint consensus; at most one change outstanding at a time.
- **Learners** — non-voting replicas that catch up before promotion, so scaling never risks quorum.
- **Linearizable reads (ReadIndex)** — heartbeat-confirmed quorum reads that never write to the log, plus opt-in **stale** local reads.
- **Transactions** — compare-and-swap `Txn` (mini-transactions with compare/success/failure branches), applied atomically.
- **Watches** — Server-Sent-Events (SSE) streams for key or prefix changes, with revision-based history replay and automatic client reconnect.
- **Transports** — JSON-over-TCP (default) or gRPC, both with optional TLS/mTLS and a fail-closed `require_tls` mode.
- **Security** — token auth with read/write roles, mTLS between nodes, per-IP and global rate limiting, CORS deny-by-default, request-size limits.
- **Observability** — Prometheus metrics at `/metrics`, OpenTelemetry (OTLP) traces, structured Zap logging, optional pprof.
- **Admin UI** — a React/Vite dashboard (`ui/`) for cluster topology, KV exploration, metrics, and node control.

## Architecture

```
                            ┌──────────────────────────────────────────┐
      kvctl / curl / UI ───►│  HTTP API (:http_addr)                    │
      Go client            │  /v1/kv /v1/txn /v1/watch /v1/status       │
                            │  /command /admin/* /health /ready /metrics │
                            └───────────────┬────────────────────────────┘
                                            │ (forwards writes/lin-reads to leader)
                                            ▼
   ┌───────────────┐   Raft RPCs   ┌───────────────┐   Raft RPCs   ┌───────────────┐
   │    node1      │◄─────────────►│    node2      │◄─────────────►│    node3      │
   │  (leader)     │  TCP or gRPC  │  (follower)   │   TLS/mTLS    │  (follower)   │
   ├───────────────┤               ├───────────────┤               ├───────────────┤
   │ Raft core     │  AppendEntries│ Raft core     │  RequestVote  │ Raft core     │
   │ WAL + Stable  │  Snapshot     │ WAL + Stable  │  TimeoutNow   │ WAL + Stable  │
   │ KV FSM        │               │ KV FSM        │               │ KV FSM        │
   │ WatchManager  │               │ WatchManager  │               │ WatchManager  │
   └───────────────┘               └───────────────┘               └───────────────┘
```

A write flows: client → any node → forwarded to leader → `Apply` appends to the WAL and replicates → committed once a quorum acks → applied to the KV FSM → response returned. A linearizable read confirms the leader still holds quorum (ReadIndex heartbeat round) and waits for the FSM to catch up before serving from local state. See [docs/architecture.md](docs/architecture.md) for the full deep dive.

## Quickstart

### Build from source

Requires Go 1.25+ (the module pins toolchain `go1.26.5`).

```bash
git clone https://github.com/sanskarpan/raft-consensus.git
cd raft-consensus
go build -o raftd ./cmd/raftd
go build -o kvctl ./cmd/kvctl
```

Run a 3-node cluster locally using the bundled per-node configs (raft ports 8011/8013/8015, HTTP ports 8012/8014/8016):

```bash
./raftd -config config-node1.yaml &
./raftd -config config-node2.yaml &
./raftd -config config-node3.yaml &

./kvctl --endpoints localhost:8012,localhost:8014,localhost:8016 put hello world
./kvctl --endpoints localhost:8012,localhost:8014,localhost:8016 get hello
```

> These sample configs set `allow_no_auth` implicitly off; when no `admin_token`/`admin_tokens` are configured the auth middleware **fails closed** and rejects requests. For local experimentation add `allow_no_auth: true` to each config, or configure a token (see [Configuration](#configuration-reference)).

### Docker Compose

Builds the image from the root `Dockerfile` and starts a 3-node cluster on an internal bridge network; only the HTTP API ports are published to the host (8002/8004/8006):

```bash
docker compose -f scripts/docker/docker-compose.yml up --build
```

```bash
curl -s localhost:8002/v1/status | jq .
```

### Kubernetes / Helm

The chart in `charts/raft` deploys a StatefulSet (with a per-pod `data` PVC), a ClusterIP Service, a headless Service, and a rendered config ConfigMap.

```bash
helm install my-raft ./charts/raft \
  --set replicaCount=3 \
  --set image.repository=raftd \
  --set image.tag=0.1.0 \
  --set config.adminToken=$(openssl rand -hex 32)
```

Default chart ports are `service.raftPort: 8080` and `service.httpPort: 8081`. See [docs/deployment.md](docs/deployment.md) for cert generation, sizing, and upgrades.

## Configuration reference

`raftd` is configured entirely via a YAML file passed with `-config` (default `raftd.yaml`). Key options and their code defaults:

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `node_id` | string | *(required)* | Unique ID of this node; must appear in `cluster`. |
| `listen_addr` | string | `:8080` | Address for inter-node Raft RPCs. |
| `http_addr` | string | `:8081` | Address for the HTTP API. |
| `data_dir` | string | `./data` | Base directory; data lives under `data_dir/<node_id>/`. |
| `cluster` | list | *(required)* | Initial members: `{id, address, http_address}`. |
| `election_tick` | int | `10` | Election timeout in ticks (must be ≥ 3× `heartbeat_tick`). |
| `heartbeat_tick` | int | `1` | Heartbeat interval in ticks (1 tick = 50 ms). |
| `transport` | string | `tcp` | Raft transport: `tcp` or `grpc`. |
| `admin_token` | string | `""` | Legacy single token; grants the `write` role. |
| `admin_tokens` | map | `{}` | `token → role` map (`read` or `write`). |
| `allow_no_auth` | bool | `false` | Explicitly run without auth (dev only); otherwise auth fails closed. |
| `auto_tls` | bool | `false` | Generates self-signed cert+key in `data_dir` on first startup for automatic encrypted inter-node traffic. |
| `insecure_transport` | bool | `false` | Suppresses the cleartext warning (dev only); production must use `tls_cert`/`tls_key`/`tls_ca` or `auto_tls`. |
| `tls_cert` / `tls_key` / `tls_ca` | string | `""` | Peer transport TLS/mTLS cert, key, CA. |
| `require_tls` | bool | `false` | Fail closed: peers may only be dialed over TLS. |
| `https_cert` / `https_key` | string | `""` | Enable TLS on the HTTP API (both required together). |
| `rate_limit_rps` | int | `500` | Global write requests/sec (token bucket). |
| `per_ip_rate_limit_rps` | int | `50` | Per-client-IP write requests/sec. |
| `max_request_body_bytes` | int64 | `1048576` (1 MiB) | Max HTTP write body size. |
| `max_watch_connections` | int | `1024` | Global cap on concurrent `/v1/watch` streams. |
| `max_watch_connections_per_ip` | int | `32` | Per-IP cap on concurrent watch streams. |
| `watch_idle_timeout` | duration | `5m` | Idle SSE connection is closed after this. |
| `cors_origins` | string | `""` (deny) | Comma-separated origin allowlist, or `*`. |
| `trusted_proxy_cidrs` | list | `[]` | CIDRs whose `X-Forwarded-For`/`X-Real-IP` is trusted. |
| `otlp_endpoint` | string | `""` | OTLP/gRPC endpoint for traces (e.g. `localhost:4317`). |
| `debug_addr` | string | `""` | pprof server address (auth-gated; loopback-only when auth is off). |

The full reference — including internal Raft tunables (`snapshot_threshold`, `trailing_logs`, `max_size_per_msg`, etc.) and env — is in [docs/configuration.md](docs/configuration.md).

## Client usage

### kvctl

```bash
# Global flags: --endpoints (default localhost:8101), --timeout, --stale, --prefix, --revision
export EP=localhost:8002,localhost:8004,localhost:8006

kvctl --endpoints $EP put user/1 alice          # set a key
kvctl --endpoints $EP get user/1                 # linearizable read
kvctl --endpoints $EP get user/1 --stale         # fast local read
kvctl --endpoints $EP range user/                # list by prefix
kvctl --endpoints $EP delete user/1              # delete
kvctl --endpoints $EP status                     # cluster status + revision
kvctl --endpoints $EP watch user/1               # SSE stream (Ctrl-C to stop)
kvctl --endpoints $EP watch user/ --prefix       # prefix watch
kvctl --endpoints $EP txn ./txn.json             # transaction from JSON file or stdin
```

### HTTP API (curl)

All examples assume a token `$T` (send it as `Authorization: Bearer $T`). Omit the header only when `allow_no_auth: true`.

```bash
# Put (raw value)
curl -X PUT localhost:8002/v1/kv/hello -H "Authorization: Bearer $T" -d 'world'

# Put (JSON envelope)
curl -X PUT localhost:8002/v1/kv/hello -H "Authorization: Bearer $T" \
     -H 'Content-Type: application/json' -d '{"value":"world"}'

# Get (linearizable) / Get (stale)
curl localhost:8002/v1/kv/hello -H "Authorization: Bearer $T"
curl "localhost:8002/v1/kv/hello?consistency=stale" -H "Authorization: Bearer $T"

# Range by prefix
curl "localhost:8002/v1/kv?prefix=user/" -H "Authorization: Bearer $T"

# Transaction (compare-and-swap)
curl -X POST localhost:8002/v1/txn -H "Authorization: Bearer $T" -d '{
  "compare":[{"key":"hello","target":"value","result":"equal","value":"world"}],
  "success":[{"type":0,"key":"hello","value":"there"}],
  "failure":[]
}'

# Watch (SSE)
curl -N "localhost:8002/v1/watch?key=hello" -H "Authorization: Bearer $T"

# Membership: add a voting member (leader only, write role)
curl -X POST localhost:8002/admin/members -H "Authorization: Bearer $T" \
     -d '{"id":"node4","address":"localhost:8007"}'
```

See [docs/api.md](docs/api.md) for the complete endpoint reference (methods, roles, status codes) and the Go client library.

## Operational notes

- **TLS / mTLS** — Generate dev certs with `scripts/certs/generate.sh` (outputs to `scripts/certs/generated/`). Set `tls_cert`/`tls_key`/`tls_ca` for peer TLS and `require_tls: true` in production; set `https_cert`/`https_key` to serve the HTTP API over TLS. Peer TLS pins TLS 1.3 and enforces client-cert verification (mTLS) when a CA is configured.
- **Auth tokens** — Configure `admin_tokens` with per-token roles (`read`/`write`). `/command`, `/v1/kv` writes, `/v1/txn`, `/admin/snapshot`, and `/admin/members*` require the `write` role. Health/readiness are always open.
- **Metrics / observability** — Scrape `/metrics` (Prometheus). Key series: `raft_term`, `raft_commit_index`, `raft_applied_index`, `raft_leader_id`, `raft_elections_total`, `raft_replication_lag`, `raft_fsm_apply_latency_seconds`. See [docs/operations.md](docs/operations.md) for example queries and alerts.
- **Backups via snapshots** — Trigger a snapshot with `POST /admin/snapshot`; snapshots are written atomically under `data_dir/<node_id>/snapshots/` and can be copied for offline backup. See [docs/operations.md](docs/operations.md#backup--restore).
- **Graceful shutdown** — On `SIGINT`/`SIGTERM` the leader transfers leadership (via `TimeoutNow`) before stopping, so followers avoid a full election timeout.

## Benchmarks

Micro-benchmarks live alongside the unit tests. Run them with the standard Go tooling:

```bash
# Everything
go test -bench=. -benchmem ./...

# Raft apply throughput (single-node and 3-node)
go test -bench='BenchmarkSingleNodeApply|BenchmarkThreeNodeApply' -benchmem ./pkg/raft

# WAL record encode / read
go test -bench='BenchmarkEncodeRecord|BenchmarkWALGet' -benchmem ./pkg/storage

# FSM apply (put / txn) and get-under-write contention
go test -bench='BenchmarkFSMApplyPut|BenchmarkFSMApplyTxn|BenchmarkKVGetUnderApply' -benchmem ./pkg/fsm
```

Available benchmarks: `BenchmarkSingleNodeApply`, `BenchmarkThreeNodeApply`, `BenchmarkAppendEntriesHandler`, `BenchmarkSnapshotCreation` (`pkg/raft`); `BenchmarkEncodeRecord`, `BenchmarkWALGet`, `BenchmarkWALGetParallel` (`pkg/storage`); `BenchmarkFSMApplyPut`, `BenchmarkFSMApplyTxn`, `BenchmarkKVGetUnderApply` (`pkg/fsm`). See [docs/tuning.md](docs/tuning.md) for interpreting results and tuning throughput vs. durability.

## Security

- Two trust boundaries: **client ↔ node** (token auth + roles, optional HTTPS) and **node ↔ node** (Raft RPCs, optional TLS/mTLS, `require_tls` fail-closed, gRPC per-peer member allowlist).
- Auth **fails closed**: with no tokens configured and `allow_no_auth` unset, every request is rejected.
- Defense-in-depth: global + per-IP write rate limiting, request-body size caps, key/value size limits (4 KiB key / 512 KiB value), CORS deny-by-default, watch-connection caps, internal error detail never leaked to clients, and an auth-gated (loopback-only when auth is off) pprof endpoint.

Full threat model and hardening checklist: [docs/security.md](docs/security.md).

## Documentation

| Doc | Contents |
|-----|----------|
| [docs/architecture.md](docs/architecture.md) | Components, the Raft algorithm as implemented, write/read data flows, on-disk formats. |
| [docs/deployment.md](docs/deployment.md) | Binary, Docker/Compose, Kubernetes/Helm, sizing, TLS certs, upgrade/rollback. |
| [docs/configuration.md](docs/configuration.md) | Every config key, flag, and env: type, default, effect. |
| [docs/api.md](docs/api.md) | Full HTTP API + kvctl command reference. |
| [docs/operations.md](docs/operations.md) | Runbook: health, metrics, alerts, failure remediation, backup/restore, scaling. |
| [docs/tuning.md](docs/tuning.md) | Performance tuning and benchmarking. |
| [docs/security.md](docs/security.md) | Security model, threat model, hardening checklist. |
| [docs/versioning.md](docs/versioning.md) | Versioning and compatibility policy. |
| [docs/changelog.md](docs/changelog.md) | Release history and version changelog. |
| [docs/disaster-recovery.md](docs/disaster-recovery.md) | Disaster recovery procedures and data restoration. |
| [docs/index.md](docs/index.md) | Documentation index and overview. |
| [docs/kv-store.md](docs/kv-store.md) | Key/value store semantics, versioning, and lease/TTL design. |
| [docs/observability.md](docs/observability.md) | Detailed metrics reference, dashboard templates, and alerting rules. |
| [docs/pki-guide.md](docs/pki-guide.md) | PKI setup guide for mTLS certificate management. |
| [docs/quickstart.md](docs/quickstart.md) | Expanded quickstart guide with Docker Compose and Kubernetes. |
| [docs/runbook.md](docs/runbook.md) | Incident response runbook for common failure scenarios. |
| [docs/testing.md](docs/testing.md) | Test architecture: unit, integration, chaos, and linearizability verification. |
| [docs/transactions.md](docs/transactions.md) | Transaction protocol: compare-and-swap branches and atomicity guarantees. |
| [docs/transport.md](docs/transport.md) | Transport layer: TCP binary, gRPC, TLS/mTLS configuration. |
| [docs/ttl.md](docs/ttl.md) | TTL/lease design: key expiry, tick loop, and sweep semantics. |
| [docs/watches.md](docs/watches.md) | Watch/SSE streaming: key and prefix watches, revision history, reconnect. |

## License

Licensed under the [Apache License 2.0](./LICENSE).

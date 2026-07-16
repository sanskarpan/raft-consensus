# Configuration Reference

`raftd` is configured with a single YAML file, plus a couple of command-line flags
on the binary itself. This document lists every configuration key, its type,
default, and effect. Values are parsed by `loadConfig` in `cmd/raftd/main.go`;
Raft-core tunables are defined in `pkg/raft/types.go`.

- [Command-line flags](#command-line-flags)
- [Environment](#environment)
- [YAML: server configuration](#yaml-server-configuration)
  - [Cluster membership](#cluster-membership)
  - [Timing / Raft ticks](#timing--raft-ticks)
  - [Authentication and roles](#authentication-and-roles)
  - [Transport and TLS](#transport-and-tls)
  - [HTTP API TLS](#http-api-tls)
  - [Rate limiting and limits](#rate-limiting-and-limits)
  - [Watches](#watches)
  - [CORS and proxies](#cors-and-proxies)
  - [Observability](#observability)
- [Raft-core tunables](#raft-core-tunables)
- [Example configurations](#example-configurations)

## Command-line flags

`raftd` accepts only two flags; everything else comes from the config file.

| Flag | Default | Effect |
|------|---------|--------|
| `-config` | `raftd.yaml` | Path to the YAML config file. |
| `-version` | `false` | Print version (`Version (commit=…, built=…)`) and exit. |

```bash
raftd -config /etc/raftd/config.yaml
raftd -version
```

`kvctl` flags are documented in [api.md](api.md#kvctl-command-reference).

## Environment

`raftd` itself reads no environment variables for configuration — all runtime
options come from the YAML file. Build-time values (`Version`, `GitCommit`,
`BuildDate` in `pkg/version`) are injected via `-ldflags`.

The Helm chart's ConfigMap templates `node_id: "${HOSTNAME}"` — this relies on the
StatefulSet pod hostname and is expanded by the shell/entrypoint environment in
that deployment path, not by `raftd`.

## YAML: server configuration

Top-level keys (defaults shown are what `loadConfig` applies when the key is unset
or zero):

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `node_id` | string | *(required)* | This node's unique ID. Must appear in `cluster`; startup fails otherwise. |
| `listen_addr` | string | `:8080` | Address for inter-node Raft RPCs (the transport listener). |
| `http_addr` | string | `:8081` | Address for the HTTP API server. |
| `data_dir` | string | `./data` | Base data directory. Node state lives under `data_dir/<node_id>/`. |
| `debug_addr` | string | `""` | If set, serves pprof at this address (auth-gated; see below). |

### Cluster membership

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `cluster` | list | *(required, non-empty)* | Initial cluster members. |
| `cluster[].id` | string | *(required, unique)* | Member ID. |
| `cluster[].address` | string | *(required)* | Member's Raft (peer) `host:port`. |
| `cluster[].http_address` | string | `""` | Member's HTTP `host:port`, used for leader forwarding. Falls back to `address` if empty. |

Startup validation (`validateCluster`) requires: `cluster` non-empty, no empty or
duplicate IDs, and the local `node_id` present in the list.

### Timing / Raft ticks

One tick is **50 ms**. Election and heartbeat are expressed in ticks.

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `election_tick` | int | `10` | Election timeout in ticks. Actual timeout is randomized in `[election_tick, 2×election_tick]`. Must be ≥ `3 × heartbeat_tick` or the node refuses to start. |
| `heartbeat_tick` | int | `1` | Heartbeat interval in ticks (default 50 ms). |

The recommended ratio is `election_tick ≈ 10 × heartbeat_tick`; below that the node
logs a warning (`LastValidateWarning`). See [tuning.md](tuning.md).

### Authentication and roles

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `admin_token` | string | `""` | Legacy single bearer token. Grants the `write` role. |
| `admin_tokens` | map[string]string | `{}` | Map of `token → role`. Roles: `read` or `write` (`write` implies `read`). |
| `allow_no_auth` | bool | `false` | When true **and** no tokens are configured, requests are allowed with the `write` role (dev only). When false and no tokens are configured, auth **fails closed** (every request → 401). |

Send the token as `Authorization: Bearer <token>`. The query-parameter fallback was
removed to avoid leaking credentials into logs.

### Transport and TLS

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `transport` | string | `tcp` | Raft peer transport: `tcp` (JSON-over-TCP) or `grpc`. |
| `tls_cert` | string | `""` | Path to the node's TLS certificate (peer transport). |
| `tls_key` | string | `""` | Path to the node's TLS private key. |
| `tls_ca` | string | `""` | Path to the CA certificate. When set, enables mTLS (client-cert verification). |
| `require_tls` | bool | `false` | Fail closed: peers may only be dialed over TLS, never plaintext. Set in production. |

For gRPC, mTLS is enabled when all three of `tls_cert`/`tls_key`/`tls_ca` are set;
otherwise an insecure transport is used (dev). For TCP, any of the three being set
enables TLS. Both transports pin TLS 1.3 and reject group/other-readable key files.

### HTTP API TLS

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `https_cert` | string | `""` | Enable HTTPS on the HTTP API. Must be set together with `https_key`. |
| `https_key` | string | `""` | HTTPS private key. |

Setting only one of the two is a startup error. When HTTPS is enabled, leader
forwarding also uses `https://` to avoid downgrading the forwarded `Authorization`
header.

### Rate limiting and limits

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `rate_limit_rps` | int | `500` | Global write-request rate (token bucket). Reads (GET/HEAD) are never limited. |
| `per_ip_rate_limit_rps` | int | `50` | Per-client-IP write-request rate. |
| `max_request_body_bytes` | int64 | `1048576` (1 MiB) | Max HTTP request body for write endpoints. |

Key/value size limits are enforced in code (not configurable): **key ≤ 4 KiB**,
**value ≤ 512 KiB**.

### Watches

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `max_watch_connections` | int | `1024` | Global cap on concurrent `/v1/watch` SSE streams. |
| `max_watch_connections_per_ip` | int | `32` | Per-IP cap on concurrent watch streams. |
| `watch_idle_timeout` | duration | `5m` | Idle SSE connection (no events delivered) is closed after this. Accepts Go duration strings (e.g. `10m`, `1h`). |

### CORS and proxies

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `cors_origins` | string | `""` (deny) | Comma-separated allowlist of browser origins, or the literal `*` to allow all. Empty = no CORS headers = browser blocks cross-origin. |
| `trusted_proxy_cidrs` | list[string] | `[]` | CIDRs (e.g. `10.0.0.0/8`) whose `X-Forwarded-For`/`X-Real-IP` headers are trusted for extracting the real client IP (rate limiting, watch caps). Invalid CIDRs fail startup. |

### Observability

| Key | Type | Default | Effect |
|-----|------|---------|--------|
| `otlp_endpoint` | string | `""` | OTLP/gRPC endpoint for OpenTelemetry traces (e.g. `localhost:4317`). Empty → tracing is a no-op. |
| `debug_addr` | string | `""` | pprof server address. **Always auth-gated.** If no tokens are configured, `debug_addr` must bind to loopback (`127.0.0.1`/`::1`) or startup fails. |

Prometheus metrics are always exposed at `/metrics` on `http_addr` (no config
required). See [operations.md](operations.md#metrics) for the metric catalog.

## Raft-core tunables

These fields exist on `raft.Config` (`pkg/raft/types.go`) and have sane defaults
applied by `Config.Validate()`. The `raftd` server currently sets only
`LocalID`, `ElectionTick`, `HeartbeatTick`, and `InitialConfiguration` from YAML;
the rest fall back to their code defaults. They are documented here because they
govern durability/throughput behavior and are relevant when embedding the `raft`
package directly.

| Field | Default | Effect |
|-------|---------|--------|
| `ElectionTick` | `10` | Election timeout in ticks (from `election_tick`). |
| `HeartbeatTick` | `1` | Heartbeat interval in ticks (from `heartbeat_tick`). |
| `SnapshotInterval` | `120s` | How often the snapshot ticker checks whether to snapshot. |
| `SnapshotThreshold` | `8192` | Minimum `appliedIndex − lastSnapshotIndex` before a snapshot is taken. |
| `TrailingLogs` | `10240` | Log entries retained after compaction (for catching up slightly-lagging followers and gating learner promotion). |
| `MaxSizePerMsg` | `1048576` (1 MiB) | Max bytes per replication message. |
| `MaxInflight` | `256` | Max in-flight replication messages. |
| `FSyncInterval` | `0` | `0` = fsync every write (safest). |
| `PreVote` | `false` | Enable the pre-vote round before real elections. |
| `DisableProposalForwarding` | `false` | If true, followers do not forward proposals. |
| `StartAsLearner` | `false` | Start in learner mode (never initiates elections). |
| `LearnerMaxOldLogIndex` | `0` | Learner catch-up bound. |

`Validate()` hard-fails if `ElectionTick < 3 × HeartbeatTick`, `ElectionTick`/
`HeartbeatTick` are non-positive, `SnapshotInterval`/`FSyncInterval` are negative,
or `LocalID` is empty.

## Example configurations

### Local single-node dev

```yaml
node_id: node1
listen_addr: :8001
http_addr: :8002
data_dir: ./data
election_tick: 10
heartbeat_tick: 1
allow_no_auth: true          # dev only — never in production
cluster:
  - id: node1
    address: localhost:8001
    http_address: localhost:8002
```

### Local 3-node cluster (matches the bundled configs)

`config-node1.yaml` (node2/node3 are symmetric with their own `node_id`/ports):

```yaml
node_id: node1
listen_addr: :8001
http_addr: :8002
data_dir: ./data
election_tick: 50
heartbeat_tick: 1
cluster:
  - id: node1
    address: localhost:8001
    http_address: localhost:8002
  - id: node2
    address: localhost:8003
    http_address: localhost:8004
  - id: node3
    address: localhost:8005
    http_address: localhost:8006
```

### Production 3-node with mTLS and auth

```yaml
node_id: node1
listen_addr: 10.0.1.11:8080
http_addr: 10.0.1.11:8081
data_dir: /var/lib/raftd
election_tick: 10
heartbeat_tick: 1

transport: grpc
tls_cert: /etc/raftd/certs/server.crt
tls_key:  /etc/raftd/certs/server.key
tls_ca:   /etc/raftd/certs/ca.crt
require_tls: true

https_cert: /etc/raftd/certs/server.crt
https_key:  /etc/raftd/certs/server.key

admin_tokens:
  "REPLACE_WITH_A_LONG_RANDOM_WRITE_TOKEN": write
  "REPLACE_WITH_A_LONG_RANDOM_READ_TOKEN":  read

rate_limit_rps: 1000
per_ip_rate_limit_rps: 100
cors_origins: "https://dashboard.example.com"
trusted_proxy_cidrs:
  - 10.0.0.0/8
otlp_endpoint: otel-collector:4317

cluster:
  - id: node1
    address: 10.0.1.11:8080
    http_address: 10.0.1.11:8081
  - id: node2
    address: 10.0.1.12:8080
    http_address: 10.0.1.12:8081
  - id: node3
    address: 10.0.1.13:8080
    http_address: 10.0.1.13:8081
```

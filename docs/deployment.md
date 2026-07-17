# Deployment Guide

This guide covers building and deploying a `raftd` cluster as a binary, with
Docker Compose, and on Kubernetes via the bundled Helm chart. It also covers TLS
certificate generation, sizing, and upgrade/rollback.

- [Prerequisites](#prerequisites)
- [Cluster fundamentals](#cluster-fundamentals)
- [Binary deployment](#binary-deployment)
- [Docker and Docker Compose](#docker-and-docker-compose)
- [Kubernetes / Helm](#kubernetes--helm)
- [TLS certificate generation](#tls-certificate-generation)
- [Sizing](#sizing)
- [Upgrades and rollback](#upgrades-and-rollback)

## Prerequisites

- **Go 1.25+** to build from source (the module pins toolchain `go1.26.5`).
- **Docker** (with Buildx) for container builds / Compose.
- **Kubernetes + Helm 3** for the chart.
- **openssl** for the cert generation script.

## Cluster fundamentals

- Run an **odd number** of voting nodes (3 or 5) so a majority is well-defined.
  A 3-node cluster tolerates 1 failure; a 5-node cluster tolerates 2.
- Every node needs the **same `cluster` list** with matching IDs and addresses.
- Each node uses two addresses: `listen_addr` (Raft peer RPCs) and `http_addr`
  (client API). `cluster[].http_address` must be reachable by peers so the leader
  can be reached for forwarding.
- Each node stores state under `data_dir/<node_id>/`; this must be durable
  (a real disk / PVC, not tmpfs).
- Configure authentication (`admin_token`/`admin_tokens`) before exposing the API;
  otherwise auth fails closed and every request is rejected.

## Binary deployment

Build:

```bash
go build -o raftd ./cmd/raftd
go build -o kvctl ./cmd/kvctl
# with version metadata:
go build -trimpath \
  -ldflags="-s -w -X github.com/sanskarpan/raft-consensus/pkg/version.Version=v1.0.0" \
  -o raftd ./cmd/raftd
```

Write a config per node (see [configuration.md](configuration.md#example-configurations))
and start each:

```bash
./raftd -config /etc/raftd/node1.yaml
./raftd -config /etc/raftd/node2.yaml
./raftd -config /etc/raftd/node3.yaml
```

### systemd unit (example)

```ini
[Unit]
Description=raftd
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/raftd -config /etc/raftd/config.yaml
Restart=on-failure
RestartSec=2
User=raftd
Group=raftd
# graceful leadership transfer happens on SIGTERM
KillSignal=SIGTERM
TimeoutStopSec=30
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

On `SIGTERM`, a leader transfers leadership before exiting, so give it enough
`TimeoutStopSec` (the transfer waits up to ~3s, then HTTP/raft shutdown up to ~10s).

## Docker and Docker Compose

### Image

The root `Dockerfile` is a two-stage build producing a distroless, non-root static
binary:

```bash
docker build -t raftd:local .
# with a version:
docker build --build-arg VERSION=v1.0.0 -t raftd:v1.0.0 .
```

The runtime image `ENTRYPOINT` is `/raftd` with default args
`-config /etc/raftd/config.yaml`; mount your config there.

### Compose (3-node local cluster)

```bash
docker compose -f scripts/docker/docker-compose.yml up --build
```

This starts `node1`/`node2`/`node3` on an internal bridge network. The Raft ports
(8001/8003/8005) are **network-internal only**; only the HTTP API ports are
published to the host:

| Node | HTTP API (host) | Config |
|------|-----------------|--------|
| node1 | `localhost:8002` | `scripts/docker/node1.yaml` (`data_dir: /data`) |
| node2 | `localhost:8004` | `scripts/docker/node2.yaml` |
| node3 | `localhost:8006` | `scripts/docker/node3.yaml` |

Each node uses a named volume (`nodeN-data`) mounted at `/data`. Verify:

```bash
curl -s localhost:8002/v1/status | jq .
```

> The compose configs do not set an admin token and do not set `allow_no_auth`, so
> auth fails closed. For a working local cluster, add `allow_no_auth: true` (dev) or
> `admin_token: <token>` to each `scripts/docker/nodeN.yaml`.

## Kubernetes / Helm

The chart in `charts/raft` deploys:

- a **StatefulSet** (`<release>-raft`) with a per-pod PVC (`data`, mounted at
  `/data`), liveness probe on `/health`, readiness probe on `/ready`;
- a **ClusterIP Service** (`<release>-raft`) exposing the raft and http ports;
- a **headless Service** (`<release>-raft-headless`) for stable pod DNS;
- a **ConfigMap** rendering `config.yaml` with the cluster membership derived from
  `replicaCount` (member IDs `<release>-raft-0..N`, peer addresses via the headless
  service).

### Install

```bash
helm install my-raft ./charts/raft \
  --set replicaCount=3 \
  --set image.repository=<registry>/raftd \
  --set image.tag=1.0.0 \
  --set config.adminToken=$(openssl rand -hex 32)
```

### Configurable values (defaults)

| Value | Default |
|-------|---------|
| `replicaCount` | `3` |
| `image.repository` | `raftd` |
| `image.tag` | `0.1.0` |
| `image.pullPolicy` | `IfNotPresent` |
| `service.type` | `ClusterIP` |
| `service.raftPort` | `8080` |
| `service.httpPort` | `8081` |
| `resources.requests` | `100m` CPU / `128Mi` |
| `resources.limits` | `500m` CPU / `512Mi` |
| `persistence.size` | `10Gi` |
| `persistence.storageClass` | `""` (cluster default) |
| `config.electionTick` | `10` |
| `config.heartbeatTick` | `1` |
| `config.adminToken` | `""` |

Notes and caveats when running the chart:

- The chart mounts the ConfigMap and pushes an `admin_token` when
  `config.adminToken` is set. **Set it** — otherwise auth fails closed.
- `config.yaml` uses `node_id: "${HOSTNAME}"` while the generated cluster member IDs
  are `<release>-raft-<ordinal>`. For the node ID to match the membership, the pod
  hostname must equal the StatefulSet pod name (the default) — verify pod names
  match member IDs before relying on this in production, and adjust the ConfigMap if
  your environment differs.
- `persistence.enabled` exists in `values.yaml` but the StatefulSet always creates
  the `volumeClaimTemplate`; storage is not conditional.
- The chart does not currently wire TLS/mTLS or advanced config keys; for those,
  supply your own ConfigMap or extend the chart.

### Scaling

Do **not** simply change `replicaCount` and upgrade to grow the voting set — the
initial `cluster` list is fixed at bootstrap and new voters must be added through
the membership API so quorum is preserved. See
[operations.md](operations.md#scaling-membership).

## TLS certificate generation

`scripts/certs/generate.sh` produces a self-signed CA and server/client
certificates for development into `scripts/certs/generated/` (created `0700`; keys
written `0600`):

```bash
./scripts/certs/generate.sh
```

Outputs:

| File | Purpose |
|------|---------|
| `ca.crt` | CA certificate (`tls_ca`). |
| `server.crt` / `server.key` | Node cert/key (`tls_cert` / `tls_key`, and `https_cert`/`https_key`). |
| `client.crt` / `client.key` | Client cert/key for mTLS. |

The server certificate carries SANs `DNS:localhost, DNS:*.raft.local, IP:127.0.0.1`.
For production, issue certificates from your own CA/PKI with SANs matching each
node's real hostnames/IPs, and reference them via the TLS config keys. Enable
`require_tls: true` so plaintext peer connections are refused.

## Sizing

Starting points — adjust to your workload and measure (see [tuning.md](tuning.md)):

| Profile | CPU | Memory | Disk |
|---------|-----|--------|------|
| Dev / small | 100m–500m | 128–512 Mi | 10 Gi SSD |
| Production (moderate) | 1–2 cores | 1–2 Gi | 50–100 Gi NVMe SSD |
| High write throughput | 2–4 cores | 2–4 Gi | Fast NVMe (fsync latency dominates) |

- Write latency is bounded by **fsync latency** on the WAL plus one network round
  trip to a quorum. Fast, low-latency disks matter most.
- Memory scales with the working set held in the KV FSM (in-memory map) plus
  in-flight replication and watch buffers.
- The KV FSM keeps all keys/values in memory; size RAM for the full dataset plus
  headroom for snapshots.

## Upgrades and rollback

The Raft wire protocol, WAL, and snapshot formats are stable across compatible
releases (see [versioning.md](versioning.md)). Perform a **rolling upgrade**, one
node at a time:

1. Snapshot for a clean backup point: `curl -X POST .../admin/snapshot`.
2. Upgrade a **follower** first. Drain it with `SIGTERM` (graceful shutdown), swap
   the binary/image, restart, and wait for `/ready` = 200 and its replication lag to
   return to ~0 (`raft_replication_lag` ≈ 0).
3. Repeat for each remaining follower.
4. Upgrade the **leader last**. `SIGTERM` triggers a graceful leadership transfer;
   confirm a new leader before proceeding.

**Rollback**: reverse the process. Because the on-disk formats are backward
compatible within a major version, a rolled-back binary reads the existing WAL and
snapshots. Never mix incompatible major versions in one cluster. Keep a snapshot
from before the upgrade as a restore point (see
[operations.md](operations.md#backup--restore)).

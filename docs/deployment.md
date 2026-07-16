# Deployment Guide

This guide covers building, configuring, and deploying the Raft consensus cluster in various environments.

## Prerequisites

- **Go 1.24+** — required to build from source
- **Linux or macOS** — the server binary targets both platforms
- **OpenSSL** — for TLS certificate generation (optional but recommended for production)
- **Docker** — for containerized deployment
- **kubectl + Helm 3** — for Kubernetes deployment

---

## 1. Building from Source

### Binary build

```bash
git clone https://github.com/your-org/raft-consensus.git
cd raft-consensus

# Build with version info
VERSION=v1.0.0
COMMIT=$(git rev-parse --short HEAD)
DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)

CGO_ENABLED=0 go build \
  -trimpath \
  -ldflags="-s -w \
    -X github.com/raft-consensus/pkg/version.Version=${VERSION} \
    -X github.com/raft-consensus/pkg/version.GitCommit=${COMMIT} \
    -X github.com/raft-consensus/pkg/version.BuildDate=${DATE}" \
  -o raftd ./cmd/raftd
```

The resulting `raftd` binary is statically linked with no external dependencies.

### Docker build

The project ships a multi-stage Dockerfile that produces a minimal distroless image:

```bash
docker build --build-arg VERSION=v1.0.0 -t raftd:v1.0.0 .
```

The build stage uses `golang:1.24-alpine` and the runtime stage uses
`gcr.io/distroless/static-debian12:nonroot`, keeping the final image under 10 MB.

Cross-compile for Linux/amd64 from macOS:

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath \
  -o raftd-linux-amd64 ./cmd/raftd
```

---

## 2. Single-Node Deployment (Development)

Single-node mode is useful for local development and integration testing. The
cluster list in the config must still include the node itself.

```yaml
# config.yaml
node_id: node1
listen_addr: :8080
http_addr: :8081
data_dir: ./data
election_tick: 10
heartbeat_tick: 1
cluster:
  - id: node1
    address: localhost:8080
```

Start the server:

```bash
./raftd -config config.yaml
```

Verify the node is healthy:

```bash
curl http://localhost:8081/health
curl http://localhost:8081/ready
```

Write and read a value:

```bash
curl -X PUT http://localhost:8081/kv/hello -d '{"value":"world"}'
curl http://localhost:8081/kv/hello
```

---

## 3. Three-Node Cluster on Bare Metal

### Configuration

Create three config files — one per node. Example for a cluster on hosts
`192.168.1.10`, `192.168.1.11`, `192.168.1.12`:

**node1 (`/etc/raftd/config.yaml` on host 192.168.1.10):**

```yaml
node_id: node1
listen_addr: :8080
http_addr: :8081
data_dir: /var/lib/raftd
election_tick: 10
heartbeat_tick: 1
admin_token: "change-me-in-production"
cluster:
  - id: node1
    address: 192.168.1.10:8080
  - id: node2
    address: 192.168.1.11:8080
  - id: node3
    address: 192.168.1.12:8080
```

Repeat for node2 and node3, changing only `node_id` and `listen_addr` as appropriate.

### Systemd service

Place the binary at `/usr/local/bin/raftd` and create the following unit file
on each host at `/etc/systemd/system/raftd.service`:

```ini
[Unit]
Description=Raft Consensus Node
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=raftd
Group=raftd
ExecStart=/usr/local/bin/raftd -config /etc/raftd/config.yaml
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

# Restrict permissions
CapabilityBoundingSet=
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/var/lib/raftd

[Install]
WantedBy=multi-user.target
```

Enable and start on each node:

```bash
# Create the raftd user and data directory
sudo useradd -r -s /bin/false raftd
sudo mkdir -p /var/lib/raftd
sudo chown raftd:raftd /var/lib/raftd

# Install and enable the service
sudo systemctl daemon-reload
sudo systemctl enable raftd
sudo systemctl start raftd

# Check status
sudo systemctl status raftd
sudo journalctl -u raftd -f
```

Start the nodes in any order. The cluster will elect a leader automatically once
a quorum (2 of 3) is reachable.

---

## 4. Docker Compose Deployment

For local multi-node testing with Docker Compose, create `docker-compose.yml`:

```yaml
version: "3.9"

networks:
  raft:
    driver: bridge

volumes:
  data-node1:
  data-node2:
  data-node3:

services:
  node1:
    image: raftd:latest
    build:
      context: .
    command: ["-config", "/etc/raftd/config.yaml"]
    ports:
      - "8081:8081"
    networks:
      - raft
    volumes:
      - data-node1:/data
      - ./config-node1.yaml:/etc/raftd/config.yaml:ro
    environment:
      - NODE_ID=node1
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8081/health"]
      interval: 5s
      timeout: 3s
      retries: 10

  node2:
    image: raftd:latest
    command: ["-config", "/etc/raftd/config.yaml"]
    ports:
      - "8082:8081"
    networks:
      - raft
    volumes:
      - data-node2:/data
      - ./config-node2.yaml:/etc/raftd/config.yaml:ro
    environment:
      - NODE_ID=node2
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8081/health"]
      interval: 5s
      timeout: 3s
      retries: 10

  node3:
    image: raftd:latest
    command: ["-config", "/etc/raftd/config.yaml"]
    ports:
      - "8083:8081"
    networks:
      - raft
    volumes:
      - data-node3:/data
      - ./config-node3.yaml:/etc/raftd/config.yaml:ro
    environment:
      - NODE_ID=node3
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8081/health"]
      interval: 5s
      timeout: 3s
      retries: 10
```

The `config-nodeN.yaml` files are the per-node configs already present at the
project root. Start the cluster:

```bash
# Build the image first
docker build -t raftd:latest .

# Start all three nodes
docker-compose up -d

# Tail logs
docker-compose logs -f

# Stop and clean up volumes
docker-compose down -v
```

---

## 5. Kubernetes Deployment with Helm

The project ships a Helm chart in `charts/raft/`. It creates:

- A **StatefulSet** with a configurable replica count (default 3)
- A **headless Service** for stable DNS-based peer discovery
- A **ClusterIP Service** for client access
- A **ConfigMap** with the per-node `config.yaml` templated from values

### Prerequisites

```bash
# Ensure kubectl is connected to a cluster
kubectl cluster-info

# Install Helm 3
helm version
```

### Install the chart

```bash
# Dry run to verify template rendering
helm install raft-cluster ./charts/raft --dry-run --debug

# Install with default values (3-node cluster)
helm install raft-cluster ./charts/raft

# Override replica count or image tag
helm install raft-cluster ./charts/raft \
  --set replicaCount=5 \
  --set image.tag=v1.0.0 \
  --set config.adminToken=mysecrettoken
```

### Watch the rollout

```bash
kubectl rollout status statefulset/raft-cluster-raft
kubectl get pods -l app=raft-cluster-raft -w
```

### Access the cluster

```bash
# Port-forward the HTTP API of the leader
kubectl port-forward svc/raft-cluster-raft 8081:8081

# Check health
curl http://localhost:8081/health
```

### Uninstall

```bash
helm uninstall raft-cluster

# PVCs are not deleted automatically; remove them explicitly if needed
kubectl delete pvc -l app=raft-cluster-raft
```

---

## 6. Configuration Reference

All fields are in `config.yaml` (YAML format).

| Field            | Type     | Default    | Description                                                      |
|------------------|----------|------------|------------------------------------------------------------------|
| `node_id`        | string   | (required) | Unique identifier for this node within the cluster               |
| `listen_addr`    | string   | `:8080`    | TCP address for Raft peer-to-peer communication                  |
| `http_addr`      | string   | `:8081`    | TCP address for the admin HTTP API                               |
| `data_dir`       | string   | `./data`   | Directory for WAL segments, stable store, and snapshot files     |
| `election_tick`  | int      | `10`       | Number of heartbeat intervals before triggering a new election   |
| `heartbeat_tick` | int      | `1`        | Interval (in ticks) at which the leader sends heartbeats         |
| `admin_token`    | string   | `""`       | Bearer token required for admin API endpoints; empty = disabled  |
| `cluster`        | list     | (required) | List of all cluster members (id + address); must include self    |
| `cluster[].id`   | string   | (required) | Node ID matching the `node_id` of the corresponding node         |
| `cluster[].address` | string | (required) | Host:port for Raft peer communication                           |
| `tls.cert_file`  | string   | `""`       | Path to TLS certificate file (enables TLS when set)              |
| `tls.key_file`   | string   | `""`       | Path to TLS private key file                                     |
| `tls.ca_file`    | string   | `""`       | Path to CA certificate for mTLS client verification              |
| `otlp_endpoint`  | string   | `""`       | OTLP gRPC endpoint for distributed traces (e.g. `localhost:4317`)|

---

## 7. TLS / mTLS Setup

TLS certificates are generated using the script at `scripts/certs/generate.sh`.
The script creates a CA, server certificate, and client certificate for mTLS.

```bash
# Generate all certificates
bash scripts/certs/generate.sh

# Output is written to scripts/certs/generated/:
#   ca.crt     - CA certificate (distribute to all nodes and clients)
#   server.key - Server private key (keep secret)
#   server.crt - Server certificate
#   client.key - Client private key
#   client.crt - Client certificate (for mTLS)
```

Add TLS configuration to each node's config:

```yaml
tls:
  cert_file: /etc/raftd/certs/server.crt
  key_file:  /etc/raftd/certs/server.key
  ca_file:   /etc/raftd/certs/ca.crt    # enables mTLS client verification
```

For Kubernetes, store certs in a Secret and mount them:

```bash
kubectl create secret generic raftd-tls \
  --from-file=ca.crt=scripts/certs/generated/ca.crt \
  --from-file=server.crt=scripts/certs/generated/server.crt \
  --from-file=server.key=scripts/certs/generated/server.key
```

Add a `volumeMount` for the secret in your Helm `values.yaml` override or patch the StatefulSet.

---

## 8. Monitoring Setup

### Prometheus scrape configuration

The HTTP API exposes Prometheus metrics at `/metrics` on the `http_addr` port.
Add the following to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'raftd'
    static_configs:
      - targets:
          - 'node1:8081'
          - 'node2:8081'
          - 'node3:8081'
    metrics_path: /metrics
    scrape_interval: 15s
```

For Kubernetes, use a `ServiceMonitor` (requires Prometheus Operator):

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: raftd
spec:
  selector:
    matchLabels:
      app: raft-cluster-raft
  endpoints:
    - port: http
      path: /metrics
      interval: 15s
```

### Key metrics

| Metric                          | Type    | Description                                  |
|---------------------------------|---------|----------------------------------------------|
| `raft_leader_changes_total`     | counter | Number of leader elections completed         |
| `raft_commit_index`             | gauge   | Current commit index                         |
| `raft_applied_index`            | gauge   | Last FSM-applied log index                   |
| `raft_log_entries_total`        | counter | Total log entries appended                   |
| `raft_snapshot_total`           | counter | Number of snapshots taken                    |
| `raft_rpc_duration_seconds`     | hist.   | Latency of AppendEntries / RequestVote RPCs  |

### Grafana dashboard

Import the dashboard by creating a panel with the following example queries:

```
# Leader changes rate
rate(raft_leader_changes_total[5m])

# Commit lag (commit index - applied index) per node
raft_commit_index - raft_applied_index

# RPC p99 latency
histogram_quantile(0.99, rate(raft_rpc_duration_seconds_bucket[5m]))
```

---

## 9. Upgrade Procedures (Rolling Upgrade)

The rolling upgrade procedure ensures no downtime for a 3-node (or larger) cluster.

1. **Build and publish the new binary / image** for the target version.

2. **Upgrade follower nodes first.** For a 3-node cluster, upgrade nodes 2 and 3
   (whichever are followers) before the leader:

   ```bash
   # Bare metal: deploy new binary, restart service
   sudo cp raftd-new /usr/local/bin/raftd
   sudo systemctl restart raftd

   # Kubernetes StatefulSet: update image, Kubernetes will restart pods one at a time
   kubectl set image statefulset/raft-cluster-raft raftd=raftd:v1.1.0
   kubectl rollout status statefulset/raft-cluster-raft
   ```

3. **Trigger a leadership transfer** before upgrading the current leader node
   to minimize client disruption:

   ```bash
   curl -X POST http://leader:8081/admin/transfer-leader \
     -H "Authorization: Bearer $ADMIN_TOKEN"
   ```

4. **Upgrade the (former) leader node** using the same steps as step 2.

5. **Verify cluster health** after all nodes are upgraded:

   ```bash
   curl http://node1:8081/health
   curl http://node1:8081/v1/cluster/status
   ```

For Kubernetes StatefulSets the `RollingUpdate` strategy is used by default,
which restarts pods in reverse ordinal order (highest pod number first), which
naturally upgrades followers before the pod-0 leader in most cases.

---

## 10. Backup and Snapshot Procedures

### Automatic snapshots

The Raft node takes snapshots automatically when the log grows beyond a
configurable threshold (default: 8192 entries since the last snapshot). Snapshot
files are stored under `<data_dir>/snapshots/`.

### Manual snapshot trigger

Trigger a snapshot via the admin API:

```bash
curl -X POST http://node1:8081/admin/snapshot \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

### Backup

To back up a node's data directory:

```bash
# Stop the node, copy, restart (safest method)
sudo systemctl stop raftd
sudo tar -czf raftd-backup-$(date +%Y%m%d).tar.gz /var/lib/raftd
sudo systemctl start raftd
```

For online backup, copy only the snapshot directory (WAL segments are not
sufficient alone because they may reference snapshot state):

```bash
# Copy latest snapshot (while node is running)
SNAPSHOT_DIR=/var/lib/raftd/snapshots
LATEST=$(ls -1t "$SNAPSHOT_DIR" | head -1)
cp -r "$SNAPSHOT_DIR/$LATEST" /backup/raft-snapshot-$(date +%Y%m%d)/
```

### Restore from snapshot

1. Stop the node.
2. Clear the data directory: `rm -rf /var/lib/raftd/*`
3. Place the snapshot directory into `<data_dir>/snapshots/`.
4. Start the node. The Raft state machine will load the snapshot on startup.

For a full cluster restore from backup:
1. Restore the snapshot on all nodes (or a majority).
2. Start all nodes simultaneously.
3. The cluster will replay any WAL entries after the snapshot index.

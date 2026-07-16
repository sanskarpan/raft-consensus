# Operations Runbook

This runbook covers day-to-day operations of a `raftd` cluster.

> **See also:** [operations.md](operations.md) is the canonical operations reference
> — it documents the full metric catalog (with exact `/metrics` names), example
> Prometheus queries, suggested alerts, failure remediation, backup/restore, and
> membership scaling. This file retains cluster-bring-up procedures.

---

## Starting a 3-Node Cluster

### Prerequisites

- Go 1.24+ or a Docker-compatible runtime
- Three machines or ports reachable by each node
- A data directory per node (default `./data/<node_id>`)

### Configuration Files

Create one config file per node. Example for a 3-node cluster on a single host:

**config-node1.yaml**
```yaml
node_id:        node1
listen_addr:    :8080
http_addr:      :8081
data_dir:       ./data
election_tick:  10
heartbeat_tick: 1
admin_token:    "change-me-in-production"
cluster:
  - id: node1
    address: localhost:8080
  - id: node2
    address: localhost:8082
  - id: node3
    address: localhost:8084
```

**config-node2.yaml** — same cluster block, change `node_id`, `listen_addr`, `http_addr`.

**config-node3.yaml** — same cluster block, change `node_id`, `listen_addr`, `http_addr`.

### Starting the Nodes

```bash
# Terminal 1
./raftd -config config-node1.yaml

# Terminal 2
./raftd -config config-node2.yaml

# Terminal 3
./raftd -config config-node3.yaml
```

Or with Docker (after `docker build -t raftd .`):

```bash
docker run -d --name node1 \
  -v $(pwd)/config-node1.yaml:/etc/raftd/config.yaml \
  -v $(pwd)/data/node1:/data/node1 \
  -p 8080:8080 -p 8081:8081 \
  raftd

docker run -d --name node2 \
  -v $(pwd)/config-node2.yaml:/etc/raftd/config.yaml \
  -v $(pwd)/data/node2:/data/node2 \
  -p 8082:8082 -p 8083:8083 \
  raftd

docker run -d --name node3 \
  -v $(pwd)/config-node3.yaml:/etc/raftd/config.yaml \
  -v $(pwd)/data/node3:/data/node3 \
  -p 8084:8084 -p 8085:8085 \
  raftd
```

Allow a few seconds for an election. One node will become leader.

---

## Health Checks and Monitoring

### Liveness Check

Returns `200 ok` if the process is running:

```bash
curl http://localhost:8081/health
```

### Readiness Check

Returns `200 ready` if the node is a leader or follower (i.e., part of a quorum). Returns `503 not ready` if it is a candidate or in an unknown state:

```bash
curl http://localhost:8081/ready
```

Use this as a Kubernetes readiness probe.

### Cluster State

Returns full cluster information (requires admin token if configured):

```bash
curl -H "Authorization: Bearer change-me-in-production" \
     http://localhost:8081/admin/cluster
```

Example response:

```json
{
  "node_id": "node1",
  "state": "Leader",
  "leader": "node1",
  "term": 3,
  "commit_idx": 47,
  "config": {
    "Servers": [
      {"ID": "node1", "Address": "localhost:8080", "Learner": false},
      {"ID": "node2", "Address": "localhost:8082", "Learner": false},
      {"ID": "node3", "Address": "localhost:8084", "Learner": false}
    ]
  }
}
```

Fields of interest:

| Field        | Healthy value                           |
|--------------|-----------------------------------------|
| `state`      | `Leader` or `Follower`                  |
| `leader`     | Non-empty string                        |
| `term`       | Stable or slowly increasing             |
| `commit_idx` | Advancing (not stuck)                   |

### Prometheus Metrics

```bash
curl http://localhost:8081/metrics
```

Key metrics:

| Metric                              | Alert condition                          |
|-------------------------------------|------------------------------------------|
| `raft_term`                         | Rapidly increasing = election storm      |
| `raft_elections_total`              | Counter accelerating = instability       |
| `raft_commit_index`                 | Not advancing on write workload = issue  |
| `raft_applied_index`                | Lagging behind commit_index = FSM issue  |
| `raft_append_entries_latency_seconds` | p99 > 100ms = network or disk issue    |

Recommended Grafana dashboard panels:
- `raft_term` over time — flat is healthy
- `rate(raft_elections_total[5m])` — should be near zero in steady state
- `raft_commit_index - raft_applied_index` — should be 0 or near 0

---

## Triggering Snapshots

Snapshots compact the WAL and reduce recovery time. Trigger manually with:

```bash
curl -X POST \
     -H "Authorization: Bearer change-me-in-production" \
     http://localhost:8081/admin/snapshot
```

Response on success: `{"status":"ok"}`

Snapshots also trigger automatically based on:
- `SnapshotInterval` (default 120 seconds)
- `SnapshotThreshold` log entries accumulated since last snapshot

The `FileSnapshotStore` retains 2 snapshots by default. Older ones are pruned automatically.

---

## Adding Nodes

### Add a Learner (Non-voting)

A learner receives log replication but does not vote. Add it before promoting to voter so it can catch up without affecting quorum:

```bash
# Start the new node with its own config pointing to the existing cluster
./raftd -config config-node4.yaml

# Add it as a learner via the client library or directly via future CLI
# (The cluster membership API is exposed via the Raft interface — use the Go
# client or implement a management endpoint using pkg/client.)
```

Using the Go client:

```go
c := client.NewClient(client.WithAddresses([]string{"localhost:8081"}))
err := c.AddLearner(ctx, "node4", "localhost:8086")
```

### Promote Learner to Voter

Once the learner's `matchIndex` is within `TrailingLogs` (default 1000) of the leader, promote it:

```go
err := c.PromoteLearner(ctx, "node4")
```

The implementation uses joint consensus: a two-phase log entry ensures both the old and new configurations must agree before the new configuration is committed.

---

## Removing Nodes

```go
err := c.RemoveServer(ctx, "node3")
```

This initiates joint consensus. The cluster size effectively increases temporarily during the joint phase. Ensure quorum is maintained in both old and new configurations before removing.

Shutdown the removed node's process after the removal is committed (verify via `/admin/cluster` that the node no longer appears in `config.Servers`).

---

## Troubleshooting

### No Leader Elected

**Symptoms**: All nodes report `state: Candidate` or `state: Follower` with empty `leader`. `/ready` returns 503 on all nodes.

**Causes and fixes**:

1. **Less than a quorum of nodes are running.** For a 3-node cluster, at least 2 must be reachable. Start the missing nodes.

2. **Network partition.** Verify TCP connectivity on `listen_addr` ports between all nodes:
   ```bash
   nc -zv node2-host 8082
   ```

3. **Persistent election loop.** Check for rapidly increasing `raft_term` and `raft_elections_total`. This can happen if `ElectionTick` is too low relative to network latency. Increase `election_tick` in config (e.g., from 10 to 20).

4. **Stale `raft_voted_for` in StableStore.** Rarely, a corrupted bbolt file can prevent voting. Back up and remove `stable.db` to force a fresh start (you will lose voted-for state, which is safe if the WAL is intact).

### Split-Brain Detection

Raft prevents true split-brain by design — only one leader can be elected per term. However, if you suspect a problem:

1. Query `/admin/cluster` on all nodes and compare `leader` and `term` fields.
2. If two nodes report different leaders with the same term, this indicates a serious bug. Collect logs from both nodes immediately.
3. Nodes with a higher term are authoritative. The lower-term "leader" will step down as soon as it receives any RPC from the higher-term node.

Monitor `raft_term` across nodes; all nodes should converge to the same value within a heartbeat interval.

### Disk Full

**Symptoms**: WAL writes fail; log shows `failed to append log entries`. The node may step down from leader.

**Immediate actions**:

1. Free disk space (remove old logs, other data, or expand the volume).
2. Trigger a snapshot on the current leader to compact the WAL:
   ```bash
   curl -X POST -H "Authorization: Bearer <token>" http://leader:8081/admin/snapshot
   ```
3. If the node is a follower and its disk is full, it may be unable to receive the snapshot. Free space first, then restart the process.

**Prevention**: Monitor disk usage. Set alerting when the data directory exceeds 70% capacity. Schedule periodic snapshots to prevent unbounded WAL growth.

### FSM Application Lag

**Symptom**: `raft_applied_index` is consistently behind `raft_commit_index`.

**Cause**: The FSM's `Apply()` method is slow or blocking. The KV store's `Apply()` holds a mutex — avoid long-running operations there in production FSMs.

**Fix**: Profile the FSM. Move expensive work (e.g., I/O) out of `Apply()` into a background goroutine, writing only metadata in `Apply()`.

### WAL Corruption

**Symptom**: Node fails to start with `corrupt log` error.

**Recovery**:

1. Identify the last valid snapshot from `FileSnapshotStore` (check the data directory for snapshot subdirectories).
2. Truncate the WAL by removing segment files that are beyond the snapshot's index. The node will restore from the snapshot on next start.
3. The node will then receive any missing entries from the leader via normal replication or a new `InstallSnapshot`.

---

## Backup and Restore

### Backup

A consistent backup consists of:

1. **Snapshot file**: The most recent snapshot in `<data_dir>/<node_id>/` (the directory with the highest index in its name).
2. **StableStore**: The `stable.db` bbolt file (copy while the process is stopped, or use bbolt's online backup API if embedding).
3. **WAL segments** (optional, for point-in-time recovery beyond the snapshot).

For a live backup, trigger a snapshot first, then copy the snapshot directory:

```bash
curl -X POST -H "Authorization: Bearer <token>" http://node:8081/admin/snapshot
cp -r ./data/node1/<snapshot-id>/ /backup/node1-snapshot-$(date +%Y%m%d)/
```

### Restore

To restore a node from a backup:

1. Stop the node process.
2. Clear the node's data directory: `rm -rf ./data/<node_id>/`.
3. Copy the backed-up snapshot into the correct location:
   ```bash
   mkdir -p ./data/node1/
   cp -r /backup/node1-snapshot-YYYYMMDD/ ./data/node1/<snapshot-id>/
   ```
4. Copy the backed-up `stable.db` to `./data/node1/stable.db`.
5. Start the node. It will restore from the snapshot and then replicate any missing entries from the cluster leader.

To restore an entire cluster from a common snapshot (disaster recovery):

1. Stop all nodes.
2. Copy the snapshot to each node's data directory.
3. Delete WAL files from all nodes (`rm ./data/<node_id>/wal/*.wal`).
4. Start all nodes simultaneously. They will elect a leader and replay from the snapshot.

---

## Configuration Reference

| YAML Key          | Default   | Description                                              |
|-------------------|-----------|----------------------------------------------------------|
| `node_id`         | (required)| Unique identifier for this node                          |
| `listen_addr`     | `:8080`   | TCP address for Raft RPC transport                       |
| `http_addr`       | `:8081`   | HTTP address for API and metrics                         |
| `data_dir`        | `./data`  | Root directory for WAL, stable store, and snapshots      |
| `election_tick`   | `10`      | Ticks before starting election (must be > heartbeat_tick)|
| `heartbeat_tick`  | `1`       | Ticks between leader heartbeats                          |
| `admin_token`     | `""`      | Bearer token for `/admin/*` endpoints; empty = no auth   |
| `cluster`         | `[]`      | List of `{id, address}` for all cluster members          |

Raft-level settings (not in YAML, set programmatically via `raft.Config`):

| Field                | Default   | Description                                            |
|----------------------|-----------|--------------------------------------------------------|
| `MaxSizePerMsg`      | max int64 | Maximum bytes per AppendEntries message                |
| `MaxInflight`        | 256       | Maximum in-flight AppendEntries messages per follower  |
| `SnapshotInterval`   | 120s      | Automatic snapshot interval                            |
| `SnapshotThreshold`  | 0         | Log entries before automatic snapshot (0 = disabled)   |
| `TrailingLogs`       | 1000      | Log entries a learner may lag before promotion blocked |
| `PreVote`            | false     | Enable pre-vote phase to reduce term disruption        |

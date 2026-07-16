# Operations Runbook

Day-to-day operation of a `raftd` cluster: health checks, metrics and alerting,
failure remediation, backup/restore, and membership scaling. Metric names below are
exactly those registered in `pkg/metrics/raft.go` and exposed at `/metrics`.

- [Health and readiness](#health-and-readiness)
- [Metrics](#metrics)
- [Example Prometheus queries](#example-prometheus-queries)
- [Suggested alerts](#suggested-alerts)
- [Common failures and remediation](#common-failures-and-remediation)
- [Backup and restore](#backup--restore)
- [Scaling membership](#scaling-membership)

## Health and readiness

| Endpoint | Meaning | Use |
|----------|---------|-----|
| `GET /health` | Always `200 ok` if the process serves HTTP. | Liveness probe. |
| `GET /ready` | `200 ready` when leader or follower; `503 not ready` when candidate/learner/shutdown. | Readiness probe / load-balancer gating. |
| `GET /v1/status` | State, leader, term, last/applied index, revision, dropped-event counters. | Quick human check. |
| `GET /admin/cluster` | Raft configuration + role/term/commit index. | Membership inspection. |

```bash
curl -s localhost:8002/v1/status -H "Authorization: Bearer $T" | jq .
```

Key fields to watch in `/v1/status`: `state` (should be exactly one `Leader` across
the cluster), `applied_index` close to `last_index`, and
`fsm_dropped_events`/`watch_dropped_events` staying at 0.

## Metrics

All metrics are Prometheus, exposed unauthenticated at `/metrics` on `http_addr`.
No namespace/subsystem prefix — names are exactly as listed.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `raft_term` | Gauge | — | Current Raft term. |
| `raft_commit_index` | Gauge | — | Last committed log index. |
| `raft_applied_index` | Gauge | — | Last index applied to the FSM. |
| `raft_leader_id` | Gauge | — | `1` if this node currently has a leader, else `0`. |
| `raft_elections_total` | Counter | — | Elections started. |
| `raft_votes_granted_total` | Counter | — | Votes granted. |
| `raft_append_entries_sent_total` | Counter | `target`, `success` | AppendEntries RPCs sent (per peer, `success` = `true`/`false`). |
| `raft_append_entries_latency_seconds` | Histogram | `target` | AppendEntries RPC latency per peer. |
| `raft_request_vote_latency_seconds` | Histogram | — | RequestVote RPC latency. |
| `raft_snapshots_total` | Counter | — | Snapshots taken. |
| `raft_snapshots_restored_total` | Counter | — | Snapshots restored (InstallSnapshot). |
| `raft_fsm_apply_latency_seconds` | Histogram | — | FSM apply latency. |
| `raft_replication_lag` | Gauge | `target` | Per-follower replication lag (leader's last index minus follower's matched index). |

> `raft_leader_id` is a boolean-style presence gauge (1 = a leader is known), not the
> numeric ID of the leader.

## Example Prometheus queries

```promql
# Is there exactly one leader? (a node is leader when its commit index is advancing
# and it emits append-entries; the cleanest cluster-wide check is state via status,
# but leadership churn shows up as elections)
rate(raft_elections_total[5m])                     # elevated = leadership instability

# Replication lag per follower (should be near 0)
max by (target) (raft_replication_lag)

# AppendEntries failure ratio per follower
sum by (target) (rate(raft_append_entries_sent_total{success="false"}[5m]))
  / sum by (target) (rate(raft_append_entries_sent_total[5m]))

# p99 FSM apply latency
histogram_quantile(0.99, sum by (le) (rate(raft_fsm_apply_latency_seconds_bucket[5m])))

# p99 AppendEntries latency per follower (network / disk health)
histogram_quantile(0.99, sum by (le, target) (rate(raft_append_entries_latency_seconds_bucket[5m])))

# Commit-to-apply gap (FSM falling behind the log)
raft_commit_index - raft_applied_index

# Snapshot activity
increase(raft_snapshots_total[1h])
increase(raft_snapshots_restored_total[1h])       # spikes = followers falling behind and needing full installs
```

## Suggested alerts

| Alert | Expression (sketch) | Severity | Meaning |
|-------|---------------------|----------|---------|
| No leader | `min(raft_leader_id) == 0 for 30s` | critical | No node reports a leader — cluster cannot serve writes. |
| Leadership flapping | `rate(raft_elections_total[5m]) > 0.2` | warning | Frequent elections — likely tick misconfig, network, or GC pauses. |
| Follower lagging | `max by (target)(raft_replication_lag) > 1000 for 2m` | warning | A follower is falling behind; risk of full snapshot install. |
| Apply backlog | `(raft_commit_index - raft_applied_index) > 1000 for 1m` | warning | FSM cannot keep up with commits. |
| High apply latency | `histogram_quantile(0.99, ...fsm_apply...) > 0.5 for 5m` | warning | Slow FSM / disk. |
| AppendEntries failures | failure ratio `> 0.1 for 2m` | warning | Network or peer-health problems. |
| Frequent snapshot restores | `increase(raft_snapshots_restored_total[10m]) > 0` | info | Followers repeatedly need full installs (log compaction too aggressive or follower too slow). |

Tune thresholds to your cluster; `raft_leader_id` is per-node, so aggregate with
`min`/`max` across the job.

## Common failures and remediation

### Leader loss / no leader elected

**Symptoms**: writes return `503 no leader elected`; `raft_leader_id` = 0;
`rate(raft_elections_total)` elevated.

**Remediate**:
1. Check node reachability — a lost quorum (majority of voters down or partitioned)
   makes elections impossible. With 3 nodes, 2 must be up and able to talk.
2. Inspect logs for repeated pre-vote/vote cycles. Frequent elections with a healthy
   network usually mean `election_tick` is too small relative to `heartbeat_tick`
   (must be ≥ 3×; recommended 10×) or GC/CPU starvation is delaying heartbeats.
   See [tuning.md](tuning.md).
3. Confirm clocks/CPU aren't pathologically slow; the tick loop is 50 ms/tick.
4. Once a quorum of nodes is back and stable, a leader is elected automatically.

### Lagging follower

**Symptoms**: `raft_replication_lag{target="nodeX"}` high; occasional
`raft_snapshots_restored_total` increments.

**Remediate**:
1. Check the follower's disk and CPU — WAL fsync latency and FSM apply are the usual
   bottlenecks (`raft_fsm_apply_latency_seconds`, `raft_append_entries_latency_seconds{target}`).
2. If the follower fell so far behind that the leader compacted past its log, the
   leader sends a full `InstallSnapshot` automatically. That is expected recovery,
   not an error, but frequent restores suggest `trailing_logs` is too small or the
   follower is chronically slow.
3. Verify network throughput/latency between the follower and leader.
4. If a follower is corrupt or unrecoverable, remove it from membership, wipe its
   `data_dir/<node_id>/`, and re-add it (as a learner, then promote). See
   [scaling](#scaling-membership).

### Disk full

**Symptoms**: appends/snapshots fail; the node records a **fatal error**,
transitions to `Shutdown`, and reports unhealthy (so it is excluded from quorum
rather than corrupting state).

**Remediate**:
1. Free space or expand the volume for `data_dir`.
2. WAL growth between snapshots is bounded by `snapshot_threshold` + `trailing_logs`;
   if the log grows unexpectedly, confirm snapshotting is happening
   (`raft_snapshots_total` increasing) and that the leader is healthy.
3. Old snapshots are pruned automatically (retain count 2); stray `.tmp` files from
   an interrupted write are safe to remove.
4. Restart the node once space is available; it recovers from WAL + latest snapshot.

### Certificate / TLS issues

**Symptoms**: peers cannot connect; logs show TLS handshake or client-cert
verification failures; with `require_tls: true`, plaintext dials are refused.

**Remediate**:
1. Verify each node's `tls_cert`/`tls_key`/`tls_ca` paths exist and the key is not
   group/world-readable (the loader **rejects** permissive key files).
2. Ensure all nodes trust the same CA (`tls_ca`) and that server cert SANs match the
   hostnames/IPs peers dial (`cluster[].address`). TLS is pinned to **TLS 1.3**.
3. For gRPC mTLS, confirm the member allowlist (peer cert CN/DNS SAN) matches the
   configured members.
4. Regenerate/rotate certs (see [deployment.md](deployment.md#tls-certificate-generation))
   and roll nodes one at a time.

### Auth failures (401/403)

- **401 everywhere**: no tokens configured and `allow_no_auth` not set → auth fails
  closed. Configure `admin_token`/`admin_tokens` (or set `allow_no_auth: true` for
  dev only).
- **403 on writes**: a `read` token was used on a write endpoint. Use a `write`
  token.

### Rate limiting (429)

`429` with `Retry-After: 1` means the global or per-IP write bucket is empty. Raise
`rate_limit_rps` / `per_ip_rate_limit_rps`, or back off. Reads are never limited.

## Backup and restore

### Backup

1. Trigger a snapshot on a healthy node (ideally the leader):
   ```bash
   curl -X POST localhost:8002/admin/snapshot -H "Authorization: Bearer $T"
   ```
2. Copy the latest snapshot pair from `data_dir/<node_id>/snapshots/`:
   `<term>-<index>.snap` and its `<term>-<index>.meta` sidecar. Snapshots are written
   atomically (meta durable before the `.snap` becomes visible) and are
   checksum-verified on read, so a copied pair is a consistent, self-verifying
   backup.
3. Store copies off-host on a schedule.

### Restore

A node recovers automatically on restart from its latest snapshot plus the WAL. To
seed a replacement or rebuild a corrupt node:

1. Stop the node.
2. Clear its `data_dir/<node_id>/` and place the backed-up snapshot pair under
   `data_dir/<node_id>/snapshots/`.
3. Start the node; it restores from the snapshot and catches up the tail from the
   leader via replication (or a fresh `InstallSnapshot`).

For a full cluster rebuild, restore one node from a snapshot, bring it up as the
seed, then add the others as learners and promote them once caught up.

## Scaling membership

Raft permits **one** outstanding configuration change at a time. The safe pattern to
add a node is: add as a learner → let it catch up → promote.

1. Start the new node with a config whose `cluster` includes all existing members
   **and** itself, pointing `data_dir` at empty storage.
2. Add it. The HTTP membership route `POST /admin/members` adds a **voter** directly;
   for a node that must catch up first, prefer the learner path via the Raft API
   (`AddLearner`) and then promote:
   ```bash
   # add as a voting member directly (small clusters / already-provisioned nodes)
   curl -X POST localhost:8002/admin/members -H "Authorization: Bearer $T" \
        -d '{"id":"node4","address":"10.0.1.14:8080"}'
   # or, once an existing learner has caught up:
   curl -X POST localhost:8002/admin/members/node4/promote -H "Authorization: Bearer $T"
   ```
3. Confirm `raft_replication_lag{target="node4"}` ≈ 0 and it appears in
   `GET /admin/members`.

To remove a node:

```bash
curl -X DELETE localhost:8002/admin/members/node4 -H "Authorization: Bearer $T"
```

Always keep the voting set an odd number and change membership **one node at a
time**, waiting for each change to commit (the API returns `409` if a change is
still in progress). All membership calls must go to the **leader** (a follower
returns `503 not leader`).

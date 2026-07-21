# Observability

raft-consensus exposes Prometheus metrics, OpenTelemetry distributed tracing, structured logging, and per-request correlation IDs.

## Prometheus metrics

Metrics are exposed at `GET /metrics` in Prometheus text format. All metric names are prefixed with `raft_`.

When any `admin_token` / `admin_tokens` is configured, `/metrics` requires at least the `read` role (set `metrics_auth: true` to force this even in no-auth mode).

### Raft state

| Metric | Type | Description |
|---|---|---|
| `raft_term` | Gauge | Current Raft term |
| `raft_commit_index` | Gauge | Last committed log index |
| `raft_applied_index` | Gauge | Last applied log index (FSM has applied up to here) |
| `raft_leader_id` | Gauge | 1 if this node knows a leader, 0 otherwise |
| `raft_apply_lag` | Gauge | `commit_index - applied_index` — entries committed but not yet applied |
| `raft_leader_changes_total` | Counter | Number of leader changes observed by this node |

### Elections and votes

| Metric | Type | Description |
|---|---|---|
| `raft_elections_total` | Counter | Total elections initiated by this node |
| `raft_votes_granted_total` | Counter | Total votes this node has granted to candidates |

### Replication

| Metric | Type | Description |
|---|---|---|
| `raft_append_entries_sent_total` | CounterVec | AppendEntries RPCs sent, labelled by `target` and `success` |
| `raft_append_entries_latency_seconds` | HistogramVec | AppendEntries round-trip latency, labelled by `target` |
| `raft_request_vote_latency_seconds` | Histogram | RequestVote round-trip latency |
| `raft_replication_lag` | GaugeVec | Per-follower replication lag (entries behind leader), labelled by `target` |

### Snapshots

| Metric | Type | Description |
|---|---|---|
| `raft_snapshots_total` | Counter | Snapshots taken |
| `raft_snapshots_restored_total` | Counter | Snapshots restored (via InstallSnapshot or admin/restore) |
| `raft_install_snapshot_latency_seconds` | Histogram | Outbound InstallSnapshot RPC latency |
| `raft_snapshot_size_bytes` | Gauge | Byte size of the most recent snapshot transferred |

### Proposals

| Metric | Type | Description |
|---|---|---|
| `raft_proposals_total` | CounterVec | Client proposals by `outcome` (`ok` or `error`) |
| `raft_proposal_commit_latency_seconds` | Histogram | Latency from proposal to commit+apply on the leader |
| `raft_rejections_total` | CounterVec | Rejections by `kind` (`append_entries`, `vote`, `not_leader`, `forward`) |

### Storage

| Metric | Type | Description |
|---|---|---|
| `raft_wal_fsync_seconds` | Histogram | WAL fsync latency (classic Raft write-latency source) |
| `raft_fsm_apply_latency_seconds` | Histogram | FSM apply latency |

### HTTP API

| Metric | Type | Description |
|---|---|---|
| `raft_http_request_duration_seconds` | HistogramVec | HTTP API latency by `handler`, `method`, and response `code` |
| `raft_watch_connections` | Gauge | Currently open `/v1/watch` SSE connections |

---

## Sample Prometheus scrape config

```yaml
# prometheus.yml
scrape_configs:
  - job_name: raft-consensus
    static_configs:
      - targets:
          - localhost:8012  # node1 HTTP
          - localhost:8014  # node2 HTTP
          - localhost:8016  # node3 HTTP
    # If auth is configured:
    authorization:
      credentials: your-read-token
```

---

## Recommended alerts

```yaml
groups:
  - name: raft
    rules:
      - alert: RaftNoLeader
        expr: sum(raft_leader_id) == 0
        for: 10s
        annotations:
          summary: "No Raft leader elected"

      - alert: RaftHighApplyLag
        expr: raft_apply_lag > 1000
        for: 30s
        annotations:
          summary: "FSM is {{ $value }} entries behind commit index"

      - alert: RaftHighProposalLatency
        expr: histogram_quantile(0.99, rate(raft_proposal_commit_latency_seconds_bucket[5m])) > 0.5
        for: 5m
        annotations:
          summary: "p99 proposal commit latency > 500ms"

      - alert: RaftFrequentLeaderChanges
        expr: rate(raft_leader_changes_total[5m]) > 0.1
        for: 1m
        annotations:
          summary: "Leader instability: {{ $value | humanize }} changes/s"

      - alert: RaftWatchDrops
        expr: rate(raft_watch_connections[5m]) > 0
        for: 1m
        annotations:
          summary: "Watch events being dropped — slow SSE subscribers"
```

---

## OpenTelemetry tracing

When `otlp_endpoint` is configured, `raftd` exports distributed traces via OTLP/gRPC.

```yaml
otlp_endpoint: "localhost:4317"   # OTLP/gRPC endpoint (e.g., OpenTelemetry Collector)
```

Trace spans are created for:

- KV write proposals (`kv.Command`)
- Each HTTP handler via the `instrument()` wrapper

The `node_id` is set as the trace service name so spans from different nodes are distinguishable in Jaeger/Grafana Tempo.

Example with the OpenTelemetry Collector + Jaeger:

```bash
# Start Jaeger (all-in-one for dev)
docker run -d --name jaeger \
  -p 4317:4317 \
  -p 16686:16686 \
  jaegertracing/all-in-one:latest

# Configure raftd
echo "otlp_endpoint: localhost:4317" >> config-node1.yaml
./raftd -config config-node1.yaml

# View traces at http://localhost:16686
```

---

## Structured logging

`raftd` uses [zap](https://github.com/uber-go/zap) for structured JSON logging. Key log fields:

| Field | Description |
|---|---|
| `node_id` | Node identifier |
| `term` | Current Raft term |
| `state` | Node state (`follower`/`candidate`/`leader`) |
| `leader` | Current leader ID |
| `request_id` | Per-request correlation ID (`X-Request-ID`) |

---

## Request correlation IDs

Every HTTP request carries an `X-Request-ID` header. If the client does not supply one, `raftd` generates a random 128-bit hex ID. The ID is echoed in the response and propagated on the leader-forward hop so a single client request can be correlated across multiple nodes' logs.

```bash
curl -v -H "X-Request-ID: my-trace-123" http://localhost:8012/v1/kv/mykey 2>&1 | grep X-Request-ID
# > X-Request-ID: my-trace-123
# < X-Request-ID: my-trace-123
```

---

## Grafana dashboard

A pre-built Grafana dashboard JSON is included at `docs/grafana-dashboard.json`. Import it into your Grafana instance (Dashboards > Import > Upload JSON file) and set the Prometheus data source to your scrape endpoint.

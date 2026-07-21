# raft-consensus

A production-grade, fully-featured [Raft](https://raft.github.io/) consensus implementation in Go, with a distributed key-value store, transactional API, server-sent event watches, TTL expiry, mTLS, and a deterministic simulation testing harness.

---

## Features

- **Raft consensus** — leader election, log replication, joint-consensus membership changes, flow-control pipelining (256-entry window), ReadIndex linearizable reads
- **Distributed KV store** — etcd-style versioned keys with `create_revision`, `mod_revision`, and `version` counters
- **Transactions** — compare-and-swap transactions with `compare`, `success`, and `failure` branches, atomic multi-key updates
- **Server-sent event watches** — real-time key and prefix watches on any node, `Last-Event-ID` resume, 1 024-event ring-buffer history
- **TTL key expiry** — deterministic apply-time clock, leader tick loop, sweep on every replica
- **gRPC and TCP transport** — binary 9-byte framing (4.7x faster than JSON), gzip compression, per-connection negotiation, JSON fallback for rolling upgrades
- **mTLS and SPIFFE** — five deployment patterns from zero-config auto-TLS to SPIFFE/SPIRE workload identity
- **Backup and disaster recovery** — snapshot download/upload, MinIO/S3 backend, Helm CronJob, `kvctl backup/restore`
- **Observability** — 20+ Prometheus metrics, OpenTelemetry OTLP tracing, structured zap logging, `X-Request-ID` correlation
- **RBAC** — three roles (`read`, `write`, `admin`), constant-time token comparison, per-IP rate limiting
- **Deterministic simulation harness** — injectable clock and network, seeded RNG, partition/latency injection, quiescence barrier

---

## Quick start (5 lines)

```bash
git clone https://github.com/sanskarpan/raft-consensus && cd raft-consensus
go build -o raftd ./cmd/raftd && go build -o kvctl ./cmd/kvctl
raftd -config config-node1.yaml &
raftd -config config-node2.yaml &
raftd -config config-node3.yaml &
kvctl --leader=localhost:8012 put mykey hello
kvctl --leader=localhost:8012 get mykey
```

See the [Quick-start guide](quickstart.md) for full step-by-step instructions including verification.

---

## Start here

| I want to... | Go to |
|---|---|
| Run a 3-node cluster in 5 minutes | [Quick-start](quickstart.md) |
| Understand how the system works | [Architecture overview](architecture.md) |
| Configure a node | [Configuration reference](configuration.md) |
| Use the HTTP KV API | [KV Store API reference](kv-store.md) |
| Set up mTLS or SPIFFE | [PKI & TLS](pki-guide.md) |
| Monitor with Prometheus | [Observability](observability.md) |
| Back up and restore | [Backup & disaster recovery](disaster-recovery.md) |
| Write deterministic cluster tests | [DES harness](testing.md) |
| Understand key design decisions | [ADR index](adr/index.md) |

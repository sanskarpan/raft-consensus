# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Release now builds, pushes (GHCR, multi-arch), and cosign-signs the container image with provenance + SBOM
- Golden-file tests for the WAL record + command byte formats; Codecov upload + nightly benchmark/benchstat run
- Go native fuzz targets for the WAL, snapshot, and command parsers (seed corpus in CI, nightly fuzz run)
- Package doc comments for all `pkg/*`, an `examples/kvclient` program, and godoc examples (pkg.go.dev discoverability)
- RBAC `admin` role (admin>write>read); membership/snapshot ops now require `admin` (legacy `admin_token` = admin)
- Range pagination: `?limit=&start_after=` with `X-Next-Cursor`/`X-Has-More` headers, `client.RangePage`, and `kvctl range --limit`
- Atomic counter: `incr` FSM op, `POST /v1/kv/{key}?op=incr`, `client.Increment`, and `kvctl incr`
- Client v2 writes now prefer the leader (`X-Raft-Leader-Address` hint + last-known-leader routing)
- CheckQuorum: leaders step down on lost quorum contact (opt-in `check_quorum`)
- Disruptive-server vote rejection on the real-vote path (Ongaro §4.2.3), gated on CheckQuorum
- Metrics: `raft_leader_changes_total`, `raft_proposal_commit_latency_seconds`, `raft_snapshot_size_bytes`
- Grafana dashboard (`docs/grafana-dashboard.json`) and opt-in Helm `PrometheusRule` alerts
- Nightly chaos/soak job now runs on the CI cron
- Pre-vote protocol to prevent term inflation during network partitions
- Joint consensus for safe two-phase membership changes
- Learner replication and promotion safety (ErrLearnerNotReady)
- Leadership transfer via TimeoutNow protocol
- ReplaceServer for atomic leader replacement
- Token-based authentication for admin endpoints
- RBAC (read/write roles) for admin API
- Audit logging for membership changes and snapshot operations
- Multi-stage Dockerfile for minimal container images
- GitHub Actions CI/CD with race detector and coverage
- TLS certificate generation script
- OpenTelemetry tracing integration (OTLP export)
- Kubernetes Helm chart with StatefulSet and headless service
- Architecture documentation and operations runbook

### Fixed
- InstallSnapshot: snapshots larger than 1 MiB restored from only the final chunk (data corruption)
- Election safety: a vote was reported granted before it was durably persisted
- Module path aligned with the repository (`github.com/sanskarpan/raft-consensus`) so the library is `go get`-able
- WAL header buffer overflow (20→25 bytes)
- WAL deadlock in Compact() via locked helpers
- WAL index tracking recording actual file offsets
- Snapshot store deadlock in pruneLocked()
- raft.go Apply() now correctly waits for commit
- Election timer fires on countdown, not every tick
- Leader no longer truncates own log on follower rejection
- FSM application via applyCommitted()
- commitIndex advanced via quorum of matchIndex

## [0.1.0] - 2026-03-03

### Added
- Initial implementation of Raft consensus algorithm
- Write-ahead log with segment files and CRC32 checksums
- File-based snapshot store with atomic writes
- In-memory KV store FSM
- TCP JSON transport
- gRPC transport
- Prometheus metrics
- Admin HTTP API
- Test harness for process-based testing

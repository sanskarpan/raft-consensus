# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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

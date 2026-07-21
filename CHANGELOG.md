# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] — #264 Production PKI rollout strategy

### Added
- `pkg/transport`: `EnsureIntermediateCAAndCert()` — separate CA and node certs
  for production PKI (ECDSA P-384 root CA, P-256 node cert, 1-year validity,
  idempotent with 30-day renewal threshold, signed chain).  Returns
  `IntermediateCAPaths{CACertFile, NodeCertFile, NodeKeyFile}`.
- `pkg/transport/autocert_test.go`: 3 new tests —
  `TestEnsureIntermediateCAAndCert_creates` (file creation, IsCA flag, chain
  verification), `TestEnsureIntermediateCAAndCert_idempotent` (CA reuse, no
  unnecessary node-cert regeneration), `TestEnsureIntermediateCAAndCert_nodeRotation`
  (expired node cert triggers regeneration; CA serial unchanged).
- `scripts/pki/vault-pki-setup.sh` — HashiCorp Vault PKI engine bootstrap:
  root CA generation (or external import), intermediate CA, `raft-node` role
  (1-year TTL, EC P-256, mTLS), `raft-pki` Vault policy, test issuance.
- `scripts/pki/certmanager-install.sh` — cert-manager installation check and
  Issuer setup: verifies/installs cert-manager, creates `SelfSigned`
  ClusterIssuer, bootstraps cluster CA Certificate, creates CA-backed `Issuer`
  `raft-cluster-issuer`, tests certificate issuance.
- `scripts/pki/rotate-node-cert.sh` — zero-downtime node cert rotation: cert
  validation (expiry, key-pair match, CA chain), atomic file replacement,
  SIGHUP to trigger cert reloader, post-rotation health check, rollback guidance.
- `deploy/helm/raft-cluster/values.yaml`: `tls` stanza with 5 modes
  (`disabled`, `auto`, `cert-manager`, `manual`, `spiffe`); cert-manager
  `issuerRef`/`duration`/`renewBefore`/`extraDNSNames`/`extraIPAddresses`;
  manual `certSecretName`; SPIFFE `workloadAPISocket`.
- `deploy/helm/raft-cluster/templates/cert-manager-issuer.yaml` — cert-manager
  CRDs: `SelfSigned` ClusterIssuer (bootstrap), CA `Certificate` + Secret,
  CA-backed `Issuer`, and one `Certificate` per node replica (per-node DNS SANs,
  IP SANs, 1-year TTL, ECDSA P-256).
- `deploy/helm/raft-cluster/templates/tls-secret.yaml` — named templates
  `raft-cluster.tls-volumes`, `raft-cluster.tls-volume-mounts`, and
  `raft-cluster.tls-env` used by StatefulSet/Deployment pods; renders correct
  volume/env depending on `tls.mode`.
- `docs/pki-guide.md` — comprehensive PKI deployment guide covering all 5
  patterns (auto TLS, intermediate CA, cert-manager, Vault PKI, SPIFFE/SPIRE)
  with prerequisites, step-by-step commands, config snippets, rotation
  procedures, and a decision tree.

## [Unreleased] — #222 TCP binary wire format + sync.Pool

### Added
- `pkg/transport/binary.go`: varint/uvarint binary codec for all 8 TCP RPC
  types (AppendEntriesReq/Resp, RequestVoteReq/Resp, InstallSnapshotReq/Resp,
  TimeoutNowReq/Resp). No reflection; explicit field-by-field encode/decode.
- `pkg/transport/tcp.go`: `encBufPool` (`sync.Pool`) reuses `*bytes.Buffer`
  allocations on the hot send path, reducing per-RPC heap allocations.
- Binary frame protocol: 9-byte header (4-byte magic + 1-byte type tag +
  4-byte payload length big-endian). `WriteBinaryFrame`/`ReadBinaryFrame`.
- Per-connection negotiation: client sends `binaryMagic` probe; server echoes
  to confirm. Result cached on `peer` struct. JSON fallback is automatic.
- `TCPTransportConfig` struct + `NewTCPTransportWithConfig` constructor.
  `BinaryTransport bool` field (default `true` in `NewTCPTransport`).
- `cmd/raftd/main.go`: `binary_transport` YAML config key.
- Benchmarks: 4.4x faster marshal, 19x faster unmarshal vs JSON.
- E2E tests: `TestBinaryTransportCluster`, `TestBinaryTransportFallbackJSON`.

### Changed
- `NewTCPTransport`/`NewTCPTransportTLS` now delegate to
  `NewTCPTransportWithConfig`; behavior unchanged (binary on by default).
- `handleConn` refactored into `handleConnJSON`/`handleConnBinary` +
  dispatch helpers; JSON path uses pooled encode buffers.

## [Unreleased]

### Added
- S3/GCS backup and disaster recovery (#216): `GET /admin/snapshot/download` streams the latest Raft snapshot as `application/octet-stream` with `X-Snapshot-Index`/`X-Snapshot-Term` headers; `POST /admin/snapshot/upload` forces a snapshot then uploads to a pluggable `backup.Uploader` (NoOp by default, real S3/GCS via drop-in swap); `PUT /admin/restore` applies a binary snapshot to the live FSM with no cluster restart; `kvctl backup [file]` downloads a snapshot to a local file; `kvctl restore <file>` uploads a local snapshot via `--leader=<addr>`; Helm CronJob in `deploy/helm/raft-cluster/` for scheduled nightly backups; DR runbook in `docs/disaster-recovery.md` covering manual backup/restore, Helm scheduling, integrity verification, and RTO/RPO targets.
- Deterministic Simulation (DES) harness (#220): injectable `Clock`/`Ticker` interfaces with `realClock`/`realTicker` production pass-throughs; `simClock` manually-advanceable wall-clock + `simTicker` driven by `simClock.Advance`; `simNetwork` in-process deterministic transport with seeded RNG, bidirectional partition injection (`Partition`/`Heal`), and configurable per-hop latency; `nodeTransport` per-node Transport wrapper; 5 subtasks, 15 new tests covering election, partition recovery, and scenario reproducibility. All 11 `time.Now()` and 2 `time.NewTicker()` sites in `pkg/raft/raft.go` replaced with injectable `r.clock.Now()` / `r.newTicker()` — zero behavior change in production.
- Replication flow-control window + AppendEntries pipelining (#200): per-follower `inflightWindow` ring-buffer (cap = `MaxInflight`, default 256) tracks in-flight AppendEntries batches and blocks new sends when full; Probe/Replicate state machine starts each follower in `stateProbe` (one batch in-flight at a time for safe log-position discovery) and advances to `stateReplicate` (full pipelining up to `MaxInflight`) on first successful ack, reverting to probe on rejection or snapshot; `MaxSizePerMsg` byte cap now honoured in the entry-batching loop (first entry always included to prevent starvation on oversized entries); 6-test ring-buffer unit suite + 3-test E2E soak covering MaxSizeCap, rapid-write data-loss, and leader-failover during pipelining
- TTL/lease-based key expiry (#207): deterministic apply-time monotonic clock (`applyTimeMs`) advanced only by leader-stamped committed commands; backward-compatible binary codec extension (`LeaderTimestampMs`+`TTLSeconds`); committed `tick` op for eager sweep with sorted deterministic deletion; snapshot v2 format preserves `applyTimeMs`+`ExpiresAtMs` across restart; leader tick loop proposes ticks at configurable `ttl_tick_interval` (default 1s); HTTP `ttl_seconds` JSON field; `client.PutWithTTL`; `kvctl put --ttl N`; 19-test unit suite + 4-test E2E suite covering replica consistency, watch delete events, and leader failover
- Streaming FSM snapshot: binary encode/decode via io.Pipe, avoiding the full-map JSON clone (backward compatible)
- TLS certificate rotation without restart: SIGHUP reloads gRPC certs via GetCertificate-backed reloader
- WAL group commit: concurrent Append fsyncs coalesce into one, preserving per-batch durability
- Write-path distributed tracing: a client write now emits a raft.commit_apply span spanning propose->commit->apply
- Opt-in gzip snapshot compression at rest (snapshot_compression)
- Opt-in gzip compression for inter-node gRPC RPCs (grpc_compression)
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

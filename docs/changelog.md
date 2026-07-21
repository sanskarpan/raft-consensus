# Changelog

This page summarises the major recent milestones. See `CHANGELOG.md` in the repository root for the full history in Keep a Changelog format.

---

## Recent milestones

### PKI rollout strategy (PR #264)

Added a production-ready PKI layer with five deployment patterns:

- `EnsureIntermediateCAAndCert()` — separate ECDSA P-384 CA and P-256 node certs with 30-day renewal threshold
- HashiCorp Vault PKI engine bootstrap script (`scripts/pki/vault-pki-setup.sh`)
- cert-manager installation and Issuer setup (`scripts/pki/certmanager-install.sh`)
- Zero-downtime cert rotation script (`scripts/pki/rotate-node-cert.sh`)
- Helm `tls` stanza covering `disabled`, `auto`, `cert-manager`, `manual`, and `spiffe` modes
- Comprehensive [PKI deployment guide](pki-guide.md)

### Deterministic simulation harness (PR #255)

A fully injectable clock and network for reproducible cluster tests without `time.Sleep`:

- `simClock` / `simTicker` — manually-advanceable deterministic clock
- `simNetwork` — in-process transport with seeded RNG, bidirectional partition injection, and configurable latency
- All 11 `time.Now()` / 2 `time.NewTicker()` sites in `raft.go` replaced with injectable interfaces
- 15 simulation tests covering election, partition recovery, and scenario reproducibility

See the [DES harness docs](testing.md) for usage examples.

### Replication flow-control + AppendEntries pipelining (PR #254)

Removed the single-outstanding-RPC bottleneck from log replication:

- Per-follower `inflightWindow` ring-buffer (cap 256) tracks in-flight batches
- Probe/Replicate state machine: starts in single-batch Probe, advances to full Replicate on first ack
- `MaxSizePerMsg` byte cap on entry batching with starvation protection
- 4.7x marshal speedup from binary framing (PR #222)

### TTL / lease-based key expiry (PR #253)

Deterministic, snapshot-consistent key expiry:

- Apply-time monotonic clock (`applyTimeMs`) stamped only by the leader
- Binary codec extension (`LeaderTimestampMs`, `TTLSeconds`) backward-compatible with old entries
- Leader tick loop proposes ticks at `ttl_tick_interval` (default 1 s)
- `ExpiresAtMs` persisted in snapshot format v2
- Watch `EventDelete` events for expired keys

See [TTL & key expiry](ttl.md) for details.

### Streaming FSM snapshot (PR #243)

Binary snapshot encoding via `io.Pipe`, streaming record-by-record without materialising the full serialised payload in memory. Backward-compatible with JSON snapshots from earlier versions.

### S3/MinIO backup and disaster recovery (PR #216)

- `GET /admin/snapshot/download` — stream latest snapshot as `application/octet-stream`
- `POST /admin/snapshot/upload` — force snapshot and push to MinIO/S3
- `PUT /admin/restore` — apply a snapshot file to the live FSM without cluster restart
- `kvctl backup` / `kvctl restore` subcommands
- Helm CronJob for scheduled nightly backups
- [Disaster recovery runbook](disaster-recovery.md)

---

For the complete entry-by-entry history including all bug fixes, security hardening, and minor improvements, see [`CHANGELOG.md`](https://github.com/sanskarpan/raft-consensus/blob/main/CHANGELOG.md) in the repository.

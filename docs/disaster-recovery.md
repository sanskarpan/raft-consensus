# Disaster Recovery Runbook

## Overview

This document describes procedures for backing up and restoring the Raft KV cluster.

## Manual Backup

Trigger a snapshot and download it to a local file:

```bash
# Trigger snapshot and download
kvctl --leader=<leader-addr> backup [output-file]
# Default output: backup-<unix-timestamp>.snap
```

Or directly via HTTP:

```bash
curl -sf -H "Authorization: Bearer <token>" \
  http://<leader-addr>/admin/snapshot/download \
  -o backup.snap
```

## Restore Procedure

To restore from a backup file to a running cluster:

```bash
kvctl --leader=<leader-addr> restore backup.snap
```

Or directly via HTTP:

```bash
curl -sf -X PUT \
  -H "Authorization: Bearer <token>" \
  --data-binary @backup.snap \
  http://<leader-addr>/admin/restore
```

**Important**: The restore call applies the snapshot to the FSM. All nodes will catch up via the Raft log or InstallSnapshot RPC. No cluster restart is required.

## Scheduled Backup via Helm

Enable the CronJob in your Helm values:

```yaml
backup:
  enabled: true
  schedule: "0 2 * * *"   # daily at 2 AM UTC
adminToken: "your-admin-token"
```

Then upgrade the chart:

```bash
helm upgrade raft-cluster deploy/helm/raft-cluster --values values.yaml
```

## Verifying Backup Integrity

A backup file is a binary FSM snapshot. To verify it is non-empty and readable:

```bash
# Check file size (should be > 0)
ls -lh backup.snap

# Attempt a dry-run restore (uses a test cluster)
kvctl --leader=<test-cluster-addr> restore backup.snap
```

## RTO / RPO Targets

| Metric | Target |
|--------|--------|
| RPO (max data loss) | Last snapshot + uncommitted entries |
| RTO (time to recover) | < 5 minutes for small clusters |

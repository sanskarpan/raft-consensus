# Quick-start: 5-minute cluster setup

This guide walks you from zero to a running 3-node Raft cluster with a working KV operation in under five minutes.

## Prerequisites

- Go 1.24 or later
- `git`

## Step 1 — Clone and build

```bash
git clone https://github.com/sanskarpan/raft-consensus
cd raft-consensus
go build -o raftd ./cmd/raftd
go build -o kvctl ./cmd/kvctl
```

Both binaries land in the repo root. `raftd` is the server daemon; `kvctl` is the command-line client.

## Step 2 — Start the 3-node cluster

The repo ships with ready-to-use configs for a 3-node cluster on `localhost`. Each config assigns a distinct Raft port (`:8011`, `:8013`, `:8015`) and HTTP port (`:8012`, `:8014`, `:8016`).

Open three terminal tabs (or use `&` to background them):

```bash
# Terminal 1
./raftd -config config-node1.yaml

# Terminal 2
./raftd -config config-node2.yaml

# Terminal 3
./raftd -config config-node3.yaml
```

You should see output like:

```
{"level":"info","msg":"server started","node_id":"node1"}
{"level":"info","msg":"became leader","term":1}
```

One of the three nodes will win the election and become leader (usually within 1–2 seconds).

## Step 3 — Write and read a key

```bash
# Write a key (routes to leader automatically)
./kvctl --leader=localhost:8012 put greeting "hello world"

# Read it back (linearizable by default)
./kvctl --leader=localhost:8012 get greeting
```

Expected output:

```
put ok  revision=1
hello world  (create_revision=1 mod_revision=1 version=1)
```

## Step 4 — Verify consensus

Check the cluster state on all three nodes to confirm they are all followers or the leader:

```bash
curl -s http://localhost:8012/admin/cluster | jq .
curl -s http://localhost:8014/admin/cluster | jq .
curl -s http://localhost:8016/admin/cluster | jq .
```

Each should report the same `leader` field and the same `commit_idx`.

## Step 5 — Try a range scan and watch

```bash
# Write several keys
./kvctl --leader=localhost:8012 put app/config/host "db.example.com"
./kvctl --leader=localhost:8012 put app/config/port "5432"
./kvctl --leader=localhost:8012 put app/config/db   "mydb"

# Prefix range scan
./kvctl --leader=localhost:8012 range app/config/

# Watch all keys under app/config/ for changes (press Ctrl-C to stop)
./kvctl --leader=localhost:8012 watch --prefix app/config/ &

# Trigger an update to see the watch fire
./kvctl --leader=localhost:8012 put app/config/db "newdb"
```

## Next steps

- [Architecture overview](architecture.md) — understand the Raft state machine and apply pipeline
- [Configuration reference](configuration.md) — all YAML config keys
- [KV Store API reference](kv-store.md) — full HTTP API documentation
- [PKI & TLS](pki-guide.md) — enable mTLS for production

!!! note "Authentication"
    The example configs use `insecure_transport: true` and `allow_no_auth: true` for local development. For production, configure `admin_token` / `admin_tokens` and mTLS. See the [PKI guide](pki-guide.md).

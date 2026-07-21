# TCP & gRPC transport

raft-consensus supports two inter-node transports: a binary TCP transport (default) and a gRPC transport. Both support optional TLS and mTLS using the same configuration keys.

## TCP transport (default)

The TCP transport (`pkg/transport/tcp.go`) is the default. It uses a **9-byte binary frame header** for all Raft RPCs on the hot path:

```
[4 bytes magic] [1 byte type tag] [4 bytes payload length (big-endian uint32)]
```

Magic value: `RF\x02\x00` (bytes `0x52 0x46 0x02 0x00`).

The binary framing eliminates reflection-based JSON marshalling on the hot AppendEntries path. Benchmarks show a **4.7x speedup in marshal time** and a **19x speedup in unmarshal time** compared to the previous JSON encoding.

### Binary negotiation

Binary framing is negotiated per-connection. The connecting client sends the 4-byte magic probe; the server echoes it to confirm. If the echo does not match, both sides fall back to JSON framing automatically. This enables rolling upgrades: new binary-capable nodes interoperate with old JSON-only nodes during a migration.

### sync.Pool encode buffers

Outbound RPC payloads are serialised into `*bytes.Buffer` values drawn from a `sync.Pool`. This reuses allocations on the hot send path and avoids GC pressure from short-lived encode buffers.

### Configuration

```yaml
transport: tcp          # default; can also be "grpc"
binary_transport: true  # enable binary framing (default: true)
                        # set false to force JSON (debugging/rollback)
```

---

## gRPC transport

The gRPC transport (`pkg/transport/grpc.go`) uses protocol buffers and standard gRPC semantics.

**Advantages:**
- Native HTTP/2 multiplexing
- Optional gzip compression for snapshot traffic
- Standards-based credentials and interoperability

**Disadvantages:**
- Higher per-RPC overhead than the binary TCP transport for small messages
- Requires proto compilation (handled in `proto/`)

### Configuration

```yaml
transport: grpc
grpc_compression: true   # enable gzip on AppendEntries/InstallSnapshot (default: false)
```

---

## TLS modes

Both transports share the same TLS configuration keys. Choose one of the following modes:

### Mode 1: No TLS (development only)

```yaml
insecure_transport: true  # suppresses the cleartext warning
```

!!! warning
    Cleartext inter-node traffic leaks Raft log entries (which contain all KV data). Never use this in production.

### Mode 2: Auto TLS (self-signed, zero config)

```yaml
auto_tls: true
```

On first startup, `raftd` generates an ECDSA P-256 self-signed certificate in `{data_dir}/{node_id}/`. The same cert acts as both the server cert and the CA. Nodes need to trust each other's CA out-of-band (e.g., by copying certs) in a multi-node cluster. Ideal for development and CI.

Certificates are reloaded on `SIGHUP` without restarting.

### Mode 3: Manual mTLS

Provide CA-issued certificates for each node:

```yaml
tls_cert: /etc/raft/node1.crt   # path to PEM-encoded server certificate
tls_key:  /etc/raft/node1.key   # path to PEM-encoded private key
tls_ca:   /etc/raft/ca.crt      # path to PEM-encoded CA certificate
require_tls: true                # fail closed: never dial peers without TLS
```

When `require_tls: true` is set, the transport refuses to connect to any peer without TLS — there is no plaintext fallback.

When TLS is configured, the transport also enforces **peer authorization**: only nodes whose certificate Common Name (CN) matches one of the configured cluster member IDs may participate in consensus. This prevents a cert that merely chains to the CA from injecting configuration changes.

### Mode 4: cert-manager (Kubernetes)

See [PKI & TLS — Pattern 3](pki-guide.md) for cert-manager integration. The Helm chart mounts cert-manager-issued certificates and configures `tls_cert`/`tls_key`/`tls_ca` from a Kubernetes Secret.

### Mode 5: SPIFFE/SPIRE

See [PKI & TLS — Pattern 5](pki-guide.md) for SPIFFE workload identity integration.

---

## HTTPS on the HTTP API

The HTTP API server can also be served over TLS. This is separate from the inter-node transport TLS:

```yaml
https_cert: /etc/raft/api.crt   # HTTP API server certificate
https_key:  /etc/raft/api.key   # HTTP API server private key
```

Both `https_cert` and `https_key` must be set together (setting only one is an error that prevents startup). When HTTPS is enabled, leader forwarding uses `https://` to avoid downgrading the Authorization header.

---

## TLS certificate rotation

Certificates can be rotated without restarting `raftd`. Send `SIGHUP`:

```bash
kill -SIGHUP $(pidof raftd)
```

The gRPC transport reloads `tls_cert` and `tls_key` atomically. The TCP transport generates a new auto-TLS certificate (if `auto_tls` is set). In-flight RPCs complete on the old certificate; new connections use the new certificate.

---

## Connection timeouts

The TCP transport uses a 10-second dial timeout per connection. The HTTP API server enforces:

| Timeout | Default |
|---|---|
| Read | 30 s |
| Write | 60 s |
| Idle | 120 s |
| Watch idle | 5 minutes (configurable via `watch_idle_timeout`) |
| TCP connection idle read | 60 s |

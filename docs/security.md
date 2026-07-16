# Security Model

This document describes the trust boundaries, threat model, and hardening controls
for `raftd`. Controls described here are implemented in `cmd/raftd/main.go`,
`pkg/transport/*`, and the storage layer.

- [Trust boundaries](#trust-boundaries)
- [Authentication and authorization](#authentication-and-authorization)
- [Transport security (node ↔ node)](#transport-security-node--node)
- [HTTP API security (client ↔ node)](#http-api-security-client--node)
- [Threat model](#threat-model)
- [Hardening checklist](#hardening-checklist)
- [Known residual risks](#known-residual-risks)

## Trust boundaries

```
        ┌─────────── client ↔ node boundary ───────────┐
        │  token auth + roles, optional HTTPS,          │
kvctl / │  rate limits, body/size caps, CORS deny       │
curl /  ├───────────────────────────────────────────────┤
 UI ───►│  node1 ───── node ↔ node boundary ───── node2 │
        │            Raft RPCs (TCP/gRPC),               │
        │            optional TLS/mTLS, require_tls,     │
        │            gRPC member allowlist               │
        └───────────────────────────────────────────────┘
```

Two boundaries:

1. **Client ↔ node** — external clients (CLI, HTTP, browser dashboard) talking to a
   node's HTTP API.
2. **Node ↔ node** — Raft peers exchanging AppendEntries/RequestVote/InstallSnapshot/
   TimeoutNow.

Each has its own authentication and encryption controls; they are configured
independently.

## Authentication and authorization

**Fail-closed by default.** If no `admin_token` and no `admin_tokens` are configured
and `allow_no_auth` is not set, **every** authenticated endpoint returns `401`. This
prevents the classic "forgot to set a token → wide open" failure.

- `allow_no_auth: true` (dev only) grants unauthenticated callers the `write` role.
- `admin_token` is a legacy single token that always maps to the `write` role.
- `admin_tokens` is a `token → role` map with roles `read` and `write` (write
  implies read).

Tokens are sent as `Authorization: Bearer <token>`. The previous `?token=`
query-param fallback was removed because it leaks credentials into access logs,
proxies, and browser history.

**Role enforcement**: `/command`, `/v1/kv` writes, `/v1/txn`, `/admin/snapshot`, and
all `/admin/members*` routes require the `write` role (`403` for a `read` token).
Health and readiness are intentionally unauthenticated.

## Transport security (node ↔ node)

Peer traffic can be encrypted and mutually authenticated:

- Set `tls_cert` / `tls_key` / `tls_ca`. Both transports pin **TLS 1.3**. When a CA
  is configured, client-certificate verification (**mTLS**) is required, so peers
  authenticate each other, not just encrypt.
- `require_tls: true` makes the gRPC transport **fail closed** — a peer can only be
  dialed over TLS, never plaintext, preventing accidental cleartext inter-node
  traffic.
- The gRPC transport can enforce a **member allowlist**: each RPC is authorized by
  the verified peer certificate's CN or DNS SAN, so a valid cert for an unrelated
  identity cannot join the ring.
- Private key files that are group- or world-readable are **rejected** at load time.

## HTTP API security (client ↔ node)

Defense-in-depth controls on the client-facing HTTP server:

- **HTTPS** for the API via `https_cert` / `https_key` (both required together).
  When enabled, leader forwarding also uses `https://` so the forwarded
  `Authorization` header is never downgraded to plaintext.
- **CORS deny-by-default** — no `Access-Control-Allow-Origin` unless the request
  origin is in `cors_origins` (or it is the literal `*`).
- **Rate limiting** — global (`rate_limit_rps`, 500) and per-IP
  (`per_ip_rate_limit_rps`, 50) token buckets on write methods; `429` with
  `Retry-After` when exhausted.
- **Request-size limits** — `max_request_body_bytes` (1 MiB) plus hard key/value
  caps (key ≤ 4 KiB, value ≤ 512 KiB).
- **Watch connection caps** — global (1024) and per-IP (32) to bound SSE fan-out
  memory against a DoS.
- **Trusted-proxy handling** — `X-Forwarded-For`/`X-Real-IP` are honored only from
  `trusted_proxy_cidrs`, so a client cannot spoof its IP to evade per-IP limits.
- **Generic error responses** — internal error detail (dial errors, host:port,
  wrapped Go errors) is logged server-side but never returned to the client.
- **pprof gating** — `debug_addr` is always behind auth; when no tokens are
  configured it must bind to loopback or the process refuses to start.
- **Forward-target validation** — the leader address used for forwarding is taken
  from static config and validated as a `host:port`, mitigating SSRF via a mutable
  membership address.

## Threat model

### Attacker on the client ↔ node boundary (can reach the HTTP API)

**Can, without a valid token:** hit `/health`, `/ready`, and `/metrics`
(unauthenticated). Metrics reveal cluster topology, term, and index counters.

**Cannot, without a valid token:** read or write KV data, run transactions, open
watches, read `/admin/cluster` or `/v1/status`, or change membership — all return
`401` (fail-closed). A cross-origin browser attacker is additionally blocked by
CORS. Write floods are bounded by global + per-IP rate limits and body/size caps.

**With a `read` token:** can read KV data and status but **cannot** write, run
mutating transactions, snapshot, or change membership (`403`).

**With a `write` token:** full data-plane and membership control — treat `write`
tokens as highly privileged.

### Attacker on the node ↔ node boundary (can reach Raft ports)

**Without TLS/mTLS:** peer traffic is plaintext and unauthenticated — an on-path
attacker can read/modify replication traffic or impersonate a peer. **This is why
mTLS + `require_tls` are essential in any untrusted network.**

**With mTLS + `require_tls` + member allowlist:** the attacker cannot join the ring
or read traffic without a CA-signed cert whose identity is on the allowlist; TLS 1.3
protects confidentiality and integrity; plaintext dials are refused.

### Attacker with local host / disk access

Can read `data_dir` (WAL, snapshots, BoltDB) and any key/cert files. Data at rest is
**not encrypted** by `raftd` — rely on OS-level disk encryption and filesystem
permissions. Private keys are created `0600` by the cert script and rejected if
group/world-readable.

## Hardening checklist

- [ ] Configure `admin_tokens` with distinct `read`/`write` tokens; never rely on
      `allow_no_auth` outside development.
- [ ] Generate long, random tokens (e.g. `openssl rand -hex 32`); rotate periodically.
- [ ] Enable HTTPS on the API (`https_cert`/`https_key`) for any non-loopback exposure.
- [ ] Enable peer TLS **and** mTLS (`tls_cert`/`tls_key`/`tls_ca`) and set
      `require_tls: true`; issue certs from your own CA with correct SANs.
- [ ] Restrict the gRPC transport to known members (member allowlist) where applicable.
- [ ] Do **not** publish Raft peer ports to untrusted networks (the Compose file keeps
      them network-internal — mirror that in your infra).
- [ ] Set `cors_origins` to an explicit allowlist (never `*` in production).
- [ ] Set `trusted_proxy_cidrs` only for your actual proxies/LBs.
- [ ] Tune `rate_limit_rps` / `per_ip_rate_limit_rps` and watch caps for your load.
- [ ] Keep `debug_addr` unset in production, or bound to loopback and behind auth.
- [ ] Ensure `data_dir` and key files have restrictive permissions; enable disk
      encryption at rest.
- [ ] Run the binary as a non-root user (the Docker image already runs `nonroot`).
- [ ] Keep dependencies patched (CI runs `govulncheck`); rebuild on advisories.

## Known residual risks

- **No encryption at rest.** WAL, snapshots, and the stable store are stored in the
  clear; use OS/disk encryption.
- **Tokens are static bearer secrets.** There is no built-in rotation, expiry, or
  revocation list — rotate by updating config and restarting nodes. Anyone with a
  `write` token has full control including membership changes.
- **`/metrics`, `/health`, `/ready` are unauthenticated**, exposing topology and
  liveness. Restrict network access if this metadata is sensitive.
- **The Helm chart does not wire TLS/mTLS** by default and pushes a plaintext
  `admin_token` via a ConfigMap (not a Secret). For production K8s, supply your own
  Secret-backed config and TLS material.
- **Trust in `cluster` addresses.** Leader forwarding trusts the statically
  configured member addresses; keep the config file integrity-protected.

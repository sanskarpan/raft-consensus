# PKI Deployment Guide for raft-consensus

This guide covers all five TLS/PKI deployment patterns supported by
raft-consensus, from zero-config development to enterprise-grade certificate
automation with HashiCorp Vault and SPIFFE/SPIRE.

---

## Overview

Inter-node and client-to-node communication in raft-consensus is encrypted
with mutual TLS (mTLS).  Each node presents a certificate to its peers; peers
verify that certificate against a known CA.  The five deployment patterns below
differ in *who generates the certificate*, *how long it lives*, and *how it
gets rotated*.

| Pattern | Who issues the cert | Rotation | Best for |
|---------|---------------------|----------|----------|
| 1. Auto TLS (self-signed) | raftd itself | Manual restart | Local dev, CI |
| 2. Intermediate CA | `EnsureIntermediateCAAndCert()` | SIGHUP | Dev/staging, single-host |
| 3. cert-manager | cert-manager operator | Automatic (SIGHUP) | Kubernetes |
| 4. HashiCorp Vault PKI | Vault PKI engine | Script + SIGHUP | Enterprise |
| 5. SPIFFE/SPIRE | SPIRE agent (SVID) | Automatic | Cloud-native, zero secrets |

---

## Pattern 1: Development — Auto TLS (self-signed)

### When to use

- Local development and `go test` integration tests.
- CI pipelines where you need encryption but not production-grade PKI.
- Quickstart: zero files to manage, zero config to write.

### How it works

On startup, `EnsureAutoTLSCerts(dataDir, nodeID)` (in `pkg/transport/autocert.go`)
generates a single ECDSA P-256 certificate that acts as *both* the server cert
and the CA.  Every node generates its own self-signed CA+cert pair.  Nodes
trust each other by distributing each other's CA cert out-of-band (or
disabling peer verification in dev).

### Prerequisites

None — no external tools required.

### Setup

No configuration needed.  raftd uses auto TLS by default when `tls_cert` is
absent from the config file:

```yaml
# config-node1.yaml — minimal dev config (TLS auto-enabled)
node_id: node1
http_addr: "127.0.0.1:8101"
raft_addr: "127.0.0.1:9101"
data_dir: ./data/node1
# No tls_cert / tls_key / tls_ca — auto TLS kicks in automatically
```

Start the node:

```bash
./raftd --config config-node1.yaml
```

raftd logs: `auto-tls: generated self-signed cert at data/node1/auto-tls.crt`.

### Operational considerations

- **Expiry**: self-signed certs are valid for 10 years.  No rotation needed
  in dev.
- **Trust**: each node trusts only its own CA.  For multi-node dev clusters,
  either disable peer verification or copy each node's `auto-tls.crt` to the
  other nodes' `tls_ca` paths.
- **Not for production**: no CRL, no revocation, no centralized trust anchor.

---

## Pattern 2: Dev/Staging — Intermediate CA

### When to use

- Multi-node dev/staging clusters where you want a shared trust anchor.
- Teams that want production-like PKI without a separate CA server.
- Testing cert rotation before deploying to production.

### How it works

`EnsureIntermediateCAAndCert(dataDir, nodeID)` (in `pkg/transport/autocert.go`)
generates:

1. A **root CA** (`ca.crt` / `ca.key`) — ECDSA P-384, 10-year validity.
   This is the shared trust anchor distributed to all nodes.
2. A **per-node cert** (`<nodeID>.crt` / `<nodeID>.key`) — ECDSA P-256,
   1-year validity, signed by the root CA.

The function is idempotent:
- If `ca.crt` has more than 30 days remaining, it is reused.
- If the node cert is missing or within 30 days of expiry, it is regenerated
  (signed by the existing CA).
- Node cert rotation does **not** require CA cert changes.

```
dataDir/
  ca.crt       ← shared trust anchor; distribute to ALL nodes
  ca.key       ← kept on the node that generated it; never shared
  node1.crt    ← node-specific; SAN: node1, localhost, 127.0.0.1, ::1
  node1.key
```

### Prerequisites

Go 1.21+ (or the compiled `raftd` binary).  No external PKI tools needed.

### Setup

```bash
# Generate CA + node cert for node1 (done automatically by raftd on startup,
# or manually via the Go API or a small Go tool):
go run - <<'EOF'
package main
import (
    "fmt"
    "github.com/sanskarpan/raft-consensus/pkg/transport"
)
func main() {
    paths, err := transport.EnsureIntermediateCAAndCert("./data/node1", "node1")
    if err != nil { panic(err) }
    fmt.Printf("CA:   %s\nCert: %s\nKey:  %s\n",
        paths.CACertFile, paths.NodeCertFile, paths.NodeKeyFile)
}
EOF

# Copy ca.crt to all other nodes' data directories.
cp ./data/node1/ca.crt ./data/node2/ca.crt
cp ./data/node1/ca.crt ./data/node3/ca.crt

# Generate node2 and node3 certs.  They will reuse the existing CA on the
# same host; for different hosts, copy ca.crt and ca.key first.
go run . --gen-cert node2 --data-dir ./data/node2
go run . --gen-cert node3 --data-dir ./data/node3
```

Config snippet for each node:

```yaml
# config-node1.yaml
node_id: node1
http_addr: "127.0.0.1:8101"
raft_addr: "127.0.0.1:9101"
data_dir: ./data/node1
tls_cert: ./data/node1/node1.crt
tls_key:  ./data/node1/node1.key
tls_ca:   ./data/node1/ca.crt
```

### Certificate rotation

To rotate node1's cert without downtime:

```bash
# 1. Delete the old node cert (will be regenerated on next call):
rm ./data/node1/node1.crt ./data/node1/node1.key

# 2. Regenerate (signed by existing CA):
go run . --gen-cert node1 --data-dir ./data/node1

# 3. Send SIGHUP to raftd to reload the cert (no restart needed):
kill -HUP "$(pgrep -f 'raftd.*node1')"

# Or use the rotation script:
./scripts/pki/rotate-node-cert.sh node1 ./data/node1/node1.crt ./data/node1/node1.key
```

### Operational considerations

- CA key lives on disk next to the CA cert.  Protect `ca.key` with `chmod 600`
  and consider storing it on an encrypted volume.
- The shared CA approach means compromising `ca.key` lets an attacker generate
  valid node certs.  For higher-security environments, use Vault (Pattern 4)
  where the CA key never leaves Vault.
- 30-day renewal threshold: on startup, if the node cert has fewer than 30 days
  remaining, it is automatically regenerated.

---

## Pattern 3: Kubernetes — cert-manager

### When to use

- Production Kubernetes deployments.
- You want automatic certificate issuance and renewal with zero human
  intervention.
- cert-manager is already part of your cluster.

### How it works

cert-manager watches `Certificate` CRDs and:

1. Requests a certificate from the configured `Issuer` or `ClusterIssuer`.
2. Stores the cert/key in a Kubernetes `Secret`.
3. Automatically renews the certificate `renewBefore` the expiry date.
4. raftd's cert reloader (`pkg/transport/certreload.go`) picks up the new cert
   via SIGHUP (triggered by a lifecycle hook or cert-manager's restart annotation).

### Prerequisites

- cert-manager installed in your cluster (`v1.12+` recommended).
- Run the setup script or install manually:

```bash
# Auto-install cert-manager and create the Issuer + bootstrap CA:
NAMESPACE=raft INSTALL_IF_MISSING=true ./scripts/pki/certmanager-install.sh

# Or install manually:
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.5/cert-manager.yaml
```

### Setup

Enable cert-manager in your Helm values:

```yaml
# values-production.yaml
replicaCount: 3

tls:
  mode: "cert-manager"
  certManager:
    enabled: true
    issuerRef:
      name: "raft-cluster-issuer"   # created by certmanager-install.sh
      kind: "Issuer"
      group: "cert-manager.io"
    duration: "8760h"               # 1 year
    renewBefore: "720h"             # renew 30 days before expiry
```

Deploy the chart:

```bash
helm upgrade --install raft-cluster ./deploy/helm/raft-cluster \
  --namespace raft --create-namespace \
  -f values-production.yaml
```

The chart creates:
- `ClusterIssuer` `raft-cluster-selfsigned-bootstrap` (bootstrap root)
- `Certificate` `raft-cluster-ca` → `Secret` `raft-cluster-ca-secret`
- `Issuer` `raft-cluster-issuer` backed by the CA Secret
- `Certificate` `raft-cluster-node{1,2,3}-cert` → `Secret` `raft-cluster-node{1,2,3}-tls`

### Automatic rotation

cert-manager renews certificates automatically.  For raftd to pick up the new
cert without a restart, add a pod annotation that restarts raftd on Secret
change, **or** use the cert-manager `cert-manager.io/inject-restart` annotation
on the Deployment/StatefulSet, **or** rely on SIGHUP:

```yaml
# In the raftd container lifecycle (Helm chart StatefulSet):
lifecycle:
  postStart:
    exec:
      command:
        - /bin/sh
        - -c
        - "while inotifywait -e close_write /tls/$(NODE_ID)/$(NODE_ID).crt; do kill -HUP 1; done &"
```

### Operational considerations

- Use a `ClusterIssuer` if you want to issue certs across multiple namespaces.
- For production, replace the self-signed bootstrap issuer with a Vault or ACME
  issuer (see Pattern 4 / docs below).
- Check cert status: `kubectl get certificate -n raft`
- Inspect renewal events: `kubectl describe certificate raft-cluster-node1-cert -n raft`

---

## Pattern 4: Enterprise — HashiCorp Vault PKI

### When to use

- Enterprise environments with existing Vault deployments.
- Compliance requirements (SOC 2, PCI-DSS) that mandate hardware-backed CA keys
  or full audit trails of certificate issuance.
- Multi-cluster deployments where a single Vault cluster serves as the PKI
  root for all environments.

### How it works

Vault's PKI secrets engine acts as the CA.  The CA private key never leaves
Vault (it lives in Vault's encrypted storage or an HSM backend).  raftd nodes
authenticate to Vault (via AppRole, Kubernetes auth, or AWS IAM auth) and
request a certificate from the `raft-node` role.  Certificates are short-lived
(1 year by default) and renewed on a schedule.

### Prerequisites

- HashiCorp Vault `1.12+` running and accessible from raft nodes.
- `vault` CLI installed on the operator workstation.
- Vault policies allowing raft nodes to issue certificates.

### Setup

Run the Vault PKI setup script:

```bash
export VAULT_ADDR=https://vault.example.com:8200
export VAULT_TOKEN="$(vault login -method=ldap username=ops -format=json | jq -r .auth.client_token)"

./scripts/pki/vault-pki-setup.sh
```

The script:
1. Enables the PKI secrets engine at `pki/` and `pki_int/`.
2. Generates a root CA (P-384, 10 years) — key stays in Vault.
3. Generates an intermediate CA, signs it with the root, installs it.
4. Creates the `raft-node` role (1-year TTL, P-256, mTLS SANs).
5. Writes the `raft-pki` policy granting nodes the right to issue their own certs.
6. Test-issues a certificate to verify the setup.

Issue a node certificate:

```bash
# On each raft node, obtain its certificate from Vault:
vault write -format=json pki_int/issue/raft-node \
  common_name="node1.cluster.local" \
  alt_names="node1,localhost"       \
  ip_sans="127.0.0.1,::1"          \
  > /tmp/vault-cert.json

# Extract cert, key, CA chain:
jq -r '.data.certificate'  /tmp/vault-cert.json > /etc/raft/node1.crt
jq -r '.data.private_key'  /tmp/vault-cert.json > /etc/raft/node1.key
jq -r '.data.ca_chain[]'   /tmp/vault-cert.json > /etc/raft/ca-chain.pem
rm /tmp/vault-cert.json
```

Config snippet:

```yaml
# config-node1.yaml
node_id: node1
http_addr: "0.0.0.0:8101"
raft_addr: "0.0.0.0:9101"
data_dir: /var/lib/raft/node1
tls_cert: /etc/raft/node1.crt
tls_key:  /etc/raft/node1.key
tls_ca:   /etc/raft/ca-chain.pem
```

### Certificate rotation

Certificates from Vault are 1-year leases.  Automate renewal with a cron job
or use `vault-agent` with the `cert` template:

```hcl
# vault-agent config for automatic cert renewal
template {
  source      = "/etc/vault/templates/node-cert.tpl"
  destination = "/etc/raft/node1.crt"
  command     = "kill -HUP $(pgrep raftd)"
}
```

Or use the manual rotation script:

```bash
# Issue a new cert from Vault and rotate it:
vault write -format=json pki_int/issue/raft-node \
  common_name="node1.cluster.local" \
  alt_names="node1,localhost" ip_sans="127.0.0.1,::1" \
  | tee >(jq -r .data.certificate > /tmp/new.crt) \
        >(jq -r .data.private_key  > /tmp/new.key) > /dev/null

./scripts/pki/rotate-node-cert.sh node1 /tmp/new.crt /tmp/new.key
shred -u /tmp/new.crt /tmp/new.key
```

### Operational considerations

- **Audit logging**: every certificate issuance is logged in Vault's audit log.
  Enable `vault audit enable file file_path=/var/log/vault-audit.log`.
- **Revocation**: use `vault write pki_int/revoke serial_number=<serial>` to
  revoke a compromised cert.  Configure `tls_ca` to check the CRL endpoint:
  `vault read pki_int/crl` (or use OCSP if your Go TLS stack supports it).
- **HSM backend**: for FIPS 140-2 Level 3, configure Vault with an HSM seal
  (`seal "pkcs11" { ... }`) so the root CA key is hardware-protected.
- **Vault HA**: run Vault in HA mode (Raft integrated storage or Consul backend)
  so the PKI engine is always available.  A Vault outage prevents new cert
  issuance but does NOT affect already-running raft nodes (they use their cached
  certs until expiry).

---

## Pattern 5: Cloud-native — SPIFFE/SPIRE

### When to use

- Cloud-native deployments where workload identity (not static secrets) is the
  goal.
- Multi-cloud or multi-cluster environments where you want a unified identity
  fabric.
- Zero-static-secrets: no cert files on disk, no rotation scripts.

### How it works

SPIRE (the SPIFFE Runtime Environment) assigns each workload a **SVID**
(SPIFFE Verifiable Identity Document), which is an X.509 certificate issued
by the SPIRE server.  SVIDs are delivered automatically to workloads via the
SPIFFE Workload API Unix socket.  They rotate automatically (typically every
hour).

raftd is configured to read its TLS certificate from the Workload API socket
rather than from files.  The SPIFFE ID embedded in the cert is:

```
spiffe://raft.cluster.local/node/<nodeID>
```

Peers verify the SPIFFE ID in the peer's SVID, not just the CA signature.

### Prerequisites

- SPIRE server and SPIRE agent DaemonSet installed in your cluster.
- `spire-agent` running on each raft node host (provides the Workload API socket).
- raftd built with SPIFFE Workload API support (see `pkg/transport/spiffe.go`
  when available, or use the `tls.mode: spiffe` Helm config to mount the socket).

### Setup

1. Install SPIRE:

```bash
# Install SPIRE via Helm:
helm repo add spire https://spiffe.github.io/helm-charts
helm install spire spire/spire --namespace spire --create-namespace \
  --set spire-server.trustDomain=raft.cluster.local
```

2. Register raft node workloads:

```bash
# Register each raft node's SPIFFE ID:
for i in 1 2 3; do
  kubectl exec -n spire spire-server-0 -- \
    /opt/spire/bin/spire-server entry create \
      -spiffeID "spiffe://raft.cluster.local/node/node${i}" \
      -parentID  "spiffe://raft.cluster.local/agent/k8s-psa/$(kubectl get node -l kubernetes.io/hostname=node${i} -o jsonpath='{.items[0].metadata.name}')" \
      -selector  "k8s:pod-label:raft-consensus/node-id:node${i}"
done
```

3. Configure the Helm chart:

```yaml
# values-spiffe.yaml
tls:
  mode: "spiffe"
  spiffe:
    workloadAPISocket: "/run/spiffe/workload.sock"
```

The `tls-secret.yaml` template mounts the SPIRE agent socket at the configured
path.  raftd reads its SVID from the socket and presents it as its TLS cert.

4. Config snippet for raftd:

```yaml
# config-node1.yaml (SPIFFE mode)
node_id: node1
http_addr: "0.0.0.0:8101"
raft_addr: "0.0.0.0:9101"
data_dir: /var/lib/raft/node1
tls_mode: spiffe
spiffe_socket: /run/spiffe/workload.sock
# No tls_cert / tls_key / tls_ca — identity comes from SPIRE
```

### Certificate rotation

SVIDs rotate automatically (default: every hour).  raftd's cert reloader
watches the Workload API for new SVIDs and rotates them in-process with no
downtime.

### Operational considerations

- **SPIRE server HA**: run SPIRE server with integrated Raft storage
  (`DataStore "sql"` or SPIRE-native HA) to avoid a single point of failure.
- **Trust domain**: the trust domain (`raft.cluster.local`) must match across
  all nodes.  Cross-trust-domain federation is possible via SPIFFE federation.
- **Debugging SVIDs**: use `spire-agent api fetch x509` on the node to inspect
  the current SVID and validate it before raftd starts.
- **SVID TTL vs Raft session length**: default SVID TTL is 1h.  Rotation is
  in-process and transparent, but if the SPIRE agent is unreachable for longer
  than the SVID TTL, raftd will be unable to rotate and will log errors.  Set
  SVID TTL to at least 4h to provide a buffer against SPIRE agent downtime.

---

## Choosing the Right Pattern

```
Are you running Kubernetes?
  Yes → Do you have cert-manager installed?
          Yes → Pattern 3 (cert-manager) ✓
          No  → Do you have Vault?
                  Yes → Pattern 4 (Vault PKI) ✓
                  No  → Install cert-manager → Pattern 3
        Do you want SPIFFE workload identity?
          Yes → Pattern 5 (SPIFFE/SPIRE) ✓

Are you running on bare metal / VMs?
  Is this production?
    Yes → Do you have Vault?
            Yes → Pattern 4 (Vault PKI) ✓
            No  → Pattern 2 (Intermediate CA) — add Vault later
    No  → Pattern 1 (auto TLS) or Pattern 2
```

---

## Certificate Expiry Monitoring

Monitor certificate expiry with Prometheus.  raftd exports:

```
raft_tls_cert_expiry_seconds{node="node1"} 1789000000
```

Alert when a cert has fewer than 30 days remaining:

```yaml
# Prometheus alert rule (included in the Helm chart's PrometheusRule):
- alert: RaftCertExpiryWarning
  expr: raft_tls_cert_expiry_seconds - time() < 30 * 86400
  for: 1h
  labels:
    severity: warning
  annotations:
    summary: "Raft node {{ $labels.node }} certificate expires in < 30 days"
    runbook: "https://github.com/sanskarpan/raft-consensus/blob/main/docs/pki-guide.md"
```

---

## Quick Reference

### Rotate a node cert (any pattern)

```bash
# 1. Obtain new cert (from CA, Vault, or cert-manager)
# 2. Place at data/<nodeID>/<nodeID>.crt and data/<nodeID>/<nodeID>.key
# 3. SIGHUP raftd — cert reloader picks it up in < 1 second

./scripts/pki/rotate-node-cert.sh node1 /path/to/new.crt /path/to/new.key
```

### Verify a node's TLS certificate

```bash
openssl s_client -connect <node-addr>:<raft-port> -servername <nodeID> </dev/null 2>&1 \
  | openssl x509 -noout -subject -issuer -dates
```

### Check cert expiry

```bash
openssl x509 -in ./data/node1/node1.crt -noout -enddate
# Or check all nodes:
for n in node1 node2 node3; do
  echo -n "${n}: "
  openssl x509 -in ./data/${n}/${n}.crt -noout -enddate 2>/dev/null || echo "NOT FOUND"
done
```

### Inspect SANs

```bash
openssl x509 -in ./data/node1/node1.crt -noout -text \
  | grep -A3 "Subject Alternative Name"
```

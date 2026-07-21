# ADR-0003: SPIFFE/SPIRE workload identity

**Status:** Accepted
**Date:** 2026-03
**Issue:** #264

---

## Context

Previous TLS deployment patterns (auto-TLS, manual certs, cert-manager) all share a fundamental property: they use **static certificates**. Even when cert-manager automates renewal, the certificate file exists on disk and must be kept secure. An attacker with read access to the filesystem can extract the private key and impersonate the node indefinitely (until the cert expires and is rotated).

In cloud-native environments (Kubernetes, service mesh), the dominant identity model has shifted toward **workload identity**: short-lived credentials issued dynamically to a running workload, with no static secret on disk. The SPIFFE (Secure Production Identity Framework for Everyone) standard defines a portable API for this.

### Problems with static certificates in production

1. **Secret sprawl** — private keys must be distributed to every node via Secrets, CI/CD pipelines, or configuration management tools. Each distribution point is a potential leak.
2. **Rotation lag** — rotating a static cert requires a coordinated deployment. Missing a node leaves a stale cert in place.
3. **No workload attestation** — a static cert proves the holder knows the private key, but not that the holder is running the expected workload binary with the expected configuration.

---

## Decision

Add **SPIFFE/SPIRE** as the fifth PKI deployment pattern (Pattern 5 in the [PKI guide](../pki-guide.md)).

### How it works

1. A SPIRE Server is deployed in the cluster (or uses a trust domain from a cloud provider's SPIRE integration)
2. SPIRE Agents run as DaemonSets on every Kubernetes node (or as a local process on bare metal)
3. At runtime, `raftd` uses the **SPIFFE Workload API** (a Unix domain socket) to obtain a short-lived X.509 SVID (SPIFFE Verifiable Identity Document) for itself
4. The SVID is a standard X.509 certificate whose Subject Alternative Name (SAN) contains a SPIFFE URI: `spiffe://trust-domain/ns/raft/sa/node1`
5. `raftd` configures its TLS layer to use this SVID as both its server cert and its client cert for mTLS
6. The SPIRE Agent rotates the SVID automatically before it expires (typically every hour)

### Key security properties

- **Zero static secrets** — no private key ever touches disk on the node. The SPIFFE Workload API delivers the key material via a memory-mapped socket.
- **Automatic rotation** — the SPIRE Agent rotates SVIDs without any raftd restart or SIGHUP.
- **Workload attestation** — SPIRE verifies the identity of the workload before issuing the SVID, using platform-specific attestors (Kubernetes pod identity, TPM, AWS instance identity, etc.).
- **Short-lived certs** — default TTL is 1 hour. Even if a key is exfiltrated, it expires quickly.

### Configuration

```yaml
tls_cert: ""        # not used; cert comes from SPIFFE Workload API
tls_key:  ""        # not used
tls_ca:   ""        # not used; trust bundle from SPIFFE
# Instead, configure in the Helm chart values:
#   tls:
#     mode: spiffe
#     workloadAPISocket: unix:///run/spire/sockets/agent.sock
```

### Helm integration

The Helm chart mounts the SPIRE Agent socket as a `hostPath` volume and sets the `SPIFFE_ENDPOINT_SOCKET` environment variable. `raftd` uses the SPIFFE Go SDK to watch for SVID rotations and reload the TLS config transparently.

---

## Consequences

**Good:**

- No static secrets on disk — eliminates the most common exfiltration vector
- Automatic cert rotation without any operator intervention
- Strong workload attestation: only the expected workload binary can obtain the SVID
- Trust federation across multiple clusters / environments via SPIFFE Federation

**Neutral:**

- Requires SPIRE infrastructure (SPIRE Server + SPIRE Agent). This is standard in Kubernetes but adds operational complexity on bare metal.
- The SPIFFE Workload API socket path must be available on all raftd nodes. In Kubernetes this is handled by the DaemonSet; on bare metal it requires the SPIRE Agent to be installed.

**Bad:**

- Adds a runtime dependency: if the SPIRE Agent is unavailable, `raftd` cannot obtain fresh SVIDs. Existing connections continue (they still have a valid cert until it expires), but new connections may fail.
- More complex debugging: certificate issues now involve the SPIRE Server, SPIRE Agent, attestation policy, and the trust bundle.

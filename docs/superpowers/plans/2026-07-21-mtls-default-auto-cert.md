# mTLS Default + Auto-Cert Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add mTLS-by-default hardening to the Raft cluster transport: auto-cert generation for dev, gRPC SAN validation, and a loud cleartext warning unless `insecure_transport: true` is set.

**Architecture:** A new `pkg/transport/autocert.go` generates self-signed ECDSA P-256 certs on first startup and stores them in DataDir. `cmd/raftd/main.go` gets two new config fields (`auto_tls`, `insecure_transport`). The gRPC transport gains a `verifySANForNodeID` hook in `pkg/transport/grpc.go`. The warning path in `initRaft()` covers all transport types.

**Tech Stack:** Go 1.24, crypto/ecdsa, crypto/x509, crypto/tls, go.uber.org/zap (all already in go.mod)

---

## File Map

| Action | File | What changes |
|---|---|---|
| Create | `pkg/transport/autocert.go` | `EnsureAutoTLSCerts`, `AutoCertPaths` |
| Create | `pkg/transport/autocert_test.go` | 4 unit tests |
| Modify | `pkg/transport/grpc.go` | `verifySANForNodeID` helper + wire into `AddPeer` |
| Create | `pkg/transport/grpc_san_test.go` | 3 SAN verification unit tests |
| Modify | `cmd/raftd/main.go` | `InsecureTransport`, `AutoTLS` in `Config`; warn + auto-cert wiring in `initRaft()` |
| Modify | `scripts/certs/generate.sh` | Per-node certs with proper SANs (node1/2/3) |
| Create | `scripts/gen-dev-certs.sh` | New per-node ECDSA cert generator (standalone) |
| Modify | `config-node1.yaml` | Add TLS/auto_tls/insecure_transport comments |
| Modify | `config-node2.yaml` | Same |
| Modify | `config-node3.yaml` | Same |

---

## Task 1: Write failing tests for `EnsureAutoTLSCerts`

**Files:**
- Create: `pkg/transport/autocert_test.go`

- [ ] **Step 1: Write the four failing tests**

```go
package transport_test

import (
    "crypto/x509"
    "encoding/pem"
    "os"
    "path/filepath"
    "testing"

    "github.com/sanskarpan/raft-consensus/pkg/transport"
)

func TestEnsureAutoTLSCertsCreatesFiles(t *testing.T) {
    dir := t.TempDir()
    paths, err := transport.EnsureAutoTLSCerts(dir, "node1")
    if err != nil {
        t.Fatalf("EnsureAutoTLSCerts: %v", err)
    }
    if _, err := os.Stat(paths.CertFile); err != nil {
        t.Errorf("cert file missing: %v", err)
    }
    if _, err := os.Stat(paths.KeyFile); err != nil {
        t.Errorf("key file missing: %v", err)
    }
    if paths.CAFile != paths.CertFile {
        t.Errorf("CAFile should equal CertFile for self-signed; got %q vs %q", paths.CAFile, paths.CertFile)
    }
}

func TestEnsureAutoTLSCertsIdempotent(t *testing.T) {
    dir := t.TempDir()
    p1, err := transport.EnsureAutoTLSCerts(dir, "node1")
    if err != nil {
        t.Fatalf("first call: %v", err)
    }
    certData1, _ := os.ReadFile(p1.CertFile)

    p2, err := transport.EnsureAutoTLSCerts(dir, "node1")
    if err != nil {
        t.Fatalf("second call: %v", err)
    }
    certData2, _ := os.ReadFile(p2.CertFile)

    if string(certData1) != string(certData2) {
        t.Error("second call overwrote the certificate; want idempotent")
    }
}

func TestAutoTLSCertHasSANForNodeID(t *testing.T) {
    dir := t.TempDir()
    nodeID := "my-node-42"
    paths, err := transport.EnsureAutoTLSCerts(dir, nodeID)
    if err != nil {
        t.Fatalf("EnsureAutoTLSCerts: %v", err)
    }
    certPEM, err := os.ReadFile(paths.CertFile)
    if err != nil {
        t.Fatalf("read cert: %v", err)
    }
    block, _ := pem.Decode(certPEM)
    if block == nil {
        t.Fatal("no PEM block in cert file")
    }
    cert, err := x509.ParseCertificate(block.Bytes)
    if err != nil {
        t.Fatalf("parse cert: %v", err)
    }
    found := false
    for _, dns := range cert.DNSNames {
        if dns == nodeID {
            found = true
            break
        }
    }
    if !found {
        t.Errorf("cert DNSNames %v does not contain nodeID %q", cert.DNSNames, nodeID)
    }
}

func TestAutoTLSCertIsCA(t *testing.T) {
    dir := t.TempDir()
    paths, err := transport.EnsureAutoTLSCerts(dir, "node1")
    if err != nil {
        t.Fatalf("EnsureAutoTLSCerts: %v", err)
    }
    certPEM, _ := os.ReadFile(paths.CertFile)
    block, _ := pem.Decode(certPEM)
    cert, _ := x509.ParseCertificate(block.Bytes)
    if !cert.IsCA {
        t.Error("expected IsCA=true on self-signed cert")
    }
    // Verify the cert self-verifies (CA signs itself).
    pool := x509.NewCertPool()
    pool.AddCert(cert)
    _, err = cert.Verify(x509.VerifyOptions{Roots: pool})
    if err != nil {
        t.Errorf("self-signed cert does not verify against its own CA pool: %v", err)
    }
}
```

- [ ] **Step 2: Run tests to confirm FAIL**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go test ./pkg/transport/ -run "TestEnsureAutoTLS|TestAutoTLSCert" -v -count=1 2>&1 | head -30
```

Expected: FAIL — `undefined: transport.EnsureAutoTLSCerts`

---

## Task 2: Implement `EnsureAutoTLSCerts` in `pkg/transport/autocert.go`

**Files:**
- Create: `pkg/transport/autocert.go`

- [ ] **Step 1: Create the implementation file**

```go
package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// AutoCertPaths holds the paths where auto-generated certs are stored.
type AutoCertPaths struct {
	CertFile string
	KeyFile  string
	CAFile   string // same as CertFile for self-signed
}

// EnsureAutoTLSCerts checks if TLS certs exist at the standard paths under
// dataDir; if not, generates a self-signed ECDSA P-256 cert valid for 10 years.
// The cert includes the following SANs:
//   - IP 127.0.0.1
//   - IP ::1
//   - DNS localhost
//   - DNS nodeID
//   - DNS nodeID.cluster.local
//
// The generated cert is a CA cert, enabling it to serve as both the server/
// client cert and the trust anchor (self-signed CA). This is the autocert
// model: zero manual setup, encrypted transport between nodes.
//
// Returns the paths to the (possibly newly-generated) cert, key, and CA files.
// CAFile always equals CertFile for self-signed certs.
func EnsureAutoTLSCerts(dataDir, nodeID string) (*AutoCertPaths, error) {
	certFile := filepath.Join(dataDir, "auto-tls.crt")
	keyFile := filepath.Join(dataDir, "auto-tls.key")

	// If both exist already, return them without regenerating.
	if _, err := os.Stat(certFile); err == nil {
		if _, err := os.Stat(keyFile); err == nil {
			return &AutoCertPaths{CertFile: certFile, KeyFile: keyFile, CAFile: certFile}, nil
		}
	}

	// Generate ECDSA P-256 private key.
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("auto-tls: generate key: %w", err)
	}

	// Build cert template with 10-year validity.
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("auto-tls: generate serial: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"raft-cluster"},
			CommonName:   nodeID,
		},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage: x509.KeyUsageKeyEncipherment |
			x509.KeyUsageDigitalSignature |
			x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:              []string{"localhost", nodeID, nodeID + ".cluster.local"},
	}

	// Self-sign: parent == template, signer == priv.
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, fmt.Errorf("auto-tls: create certificate: %w", err)
	}

	// Write certificate (0600 — owner-readable only).
	cf, err := os.OpenFile(certFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("auto-tls: write cert: %w", err)
	}
	if err := pem.Encode(cf, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		cf.Close()
		return nil, fmt.Errorf("auto-tls: encode cert: %w", err)
	}
	if err := cf.Close(); err != nil {
		return nil, fmt.Errorf("auto-tls: close cert file: %w", err)
	}

	// Write private key (0600).
	kf, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("auto-tls: write key: %w", err)
	}
	privDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		kf.Close()
		return nil, fmt.Errorf("auto-tls: marshal key: %w", err)
	}
	if err := pem.Encode(kf, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER}); err != nil {
		kf.Close()
		return nil, fmt.Errorf("auto-tls: encode key: %w", err)
	}
	if err := kf.Close(); err != nil {
		return nil, fmt.Errorf("auto-tls: close key file: %w", err)
	}

	return &AutoCertPaths{CertFile: certFile, KeyFile: keyFile, CAFile: certFile}, nil
}
```

- [ ] **Step 2: Run tests to confirm PASS**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go test ./pkg/transport/ -run "TestEnsureAutoTLS|TestAutoTLSCert" -race -v -count=1
```

Expected: All 4 tests PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
git add pkg/transport/autocert.go pkg/transport/autocert_test.go
git commit -m "$(cat <<'EOF'
feat(transport): add EnsureAutoTLSCerts for zero-config encrypted dev transport

Generates a self-signed ECDSA P-256 cert with node ID as DNS SAN, stored in
DataDir. Idempotent: skips regen if cert+key already exist.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Write failing tests for gRPC SAN verification

**Files:**
- Create: `pkg/transport/grpc_san_test.go`

- [ ] **Step 1: Write the three failing tests**

Note: These tests use the cert-generation helpers (`newCA`, `issueCert`) already defined in `grpc_tls_test.go` in the same `transport_test` package.

```go
package transport_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/transport"
)

// makeSANTestCert creates a minimal DER-encoded cert with the given DNS SANs and CN.
func makeSANTestCert(t *testing.T, cn string, dnsNames []string) []byte {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 64))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     dnsNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func TestVerifySANForNodeIDMatches(t *testing.T) {
	nodeID := "node1"
	der := makeSANTestCert(t, "some-cn", []string{"node1", "localhost"})
	fn := transport.VerifySANForNodeID(nodeID)
	if err := fn([][]byte{der}, nil); err != nil {
		t.Errorf("expected match, got error: %v", err)
	}
}

func TestVerifySANForNodeIDRejectsWrong(t *testing.T) {
	der := makeSANTestCert(t, "node2", []string{"node2", "localhost"})
	fn := transport.VerifySANForNodeID("node1")
	if err := fn([][]byte{der}, nil); err == nil {
		t.Error("expected error for wrong node ID, got nil")
	}
}

func TestVerifySANForNodeIDFallbackToCN(t *testing.T) {
	// No DNS SANs, CN matches.
	der := makeSANTestCert(t, "node1", nil)
	fn := transport.VerifySANForNodeID("node1")
	if err := fn([][]byte{der}, nil); err != nil {
		t.Errorf("expected CN fallback to match, got error: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to confirm FAIL**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go test ./pkg/transport/ -run "TestVerifySAN" -v -count=1 2>&1 | head -20
```

Expected: FAIL — `undefined: transport.VerifySANForNodeID`

---

## Task 4: Implement `verifySANForNodeID` in `pkg/transport/grpc.go` and wire into `AddPeer`

**Files:**
- Modify: `pkg/transport/grpc.go`
- Modify: `pkg/transport/export_test.go` (add export shim for tests)

- [ ] **Step 1: Add `verifySANForNodeID` function to `grpc.go`**

Add this function near the end of `grpc.go`, before `convertEntries`:

```go
// verifySANForNodeID returns a tls.Config.VerifyPeerCertificate function that
// checks the peer cert contains expectedNodeID in its DNS SANs or CN.
// This prevents a node with a valid cluster CA cert but wrong identity from
// impersonating another node when mTLS is active.
func verifySANForNodeID(expectedNodeID string) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("transport: no certificate provided by peer")
		}
		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("transport: parse peer cert: %w", err)
		}
		// Check DNS SANs first.
		for _, san := range cert.DNSNames {
			if san == expectedNodeID {
				return nil
			}
		}
		// Fall back to CN.
		if cert.Subject.CommonName == expectedNodeID {
			return nil
		}
		return fmt.Errorf("transport: peer cert does not contain expected node ID %q (SANs: %v, CN: %q)",
			expectedNodeID, cert.DNSNames, cert.Subject.CommonName)
	}
}
```

- [ ] **Step 2: Export the function via `export_test.go` so tests can access it**

Read the existing `export_test.go` first to see its pattern:

```bash
cat /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a/pkg/transport/export_test.go
```

Then add the following line to `export_test.go`:

```go
// VerifySANForNodeID exposes the unexported verifySANForNodeID for unit tests.
var VerifySANForNodeID = verifySANForNodeID
```

- [ ] **Step 3: Wire SAN verification into `AddPeer` in `grpc.go`**

Locate the `AddPeer` function in `grpc.go`. Inside the block where `outboundTLS != nil`, after cloning `outboundTLS` and setting `ServerName`, add the `VerifyPeerCertificate` hook:

Find this existing code in `AddPeer` (around line 769):
```go
		peerTLS := outboundTLS.Clone()
		peerTLS.ServerName = serverNameFor(outboundTLS.ServerName, string(addr))
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(peerTLS)))
```

Replace with:
```go
		peerTLS := outboundTLS.Clone()
		peerTLS.ServerName = serverNameFor(outboundTLS.ServerName, string(addr))
		// Wire SAN verification so a cert that merely chains to the cluster CA
		// but has the wrong node identity is rejected (M-TLS4).
		if string(id) != "" {
			peerTLS.VerifyPeerCertificate = verifySANForNodeID(string(id))
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(peerTLS)))
```

- [ ] **Step 4: Run SAN tests to confirm PASS**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go test ./pkg/transport/ -run "TestVerifySAN" -race -v -count=1
```

Expected: All 3 tests PASS.

- [ ] **Step 5: Run all transport tests to confirm nothing broke**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go test ./pkg/transport/... -race -count=1 -timeout=120s 2>&1 | tail -20
```

Expected: ok, no failures.

- [ ] **Step 6: Commit**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
git add pkg/transport/grpc.go pkg/transport/export_test.go pkg/transport/grpc_san_test.go
git commit -m "$(cat <<'EOF'
feat(transport/grpc): add SAN validation for peer node identity (M-TLS4)

Adds verifySANForNodeID which checks the peer cert DNSNames or CN matches
the expected cluster member ID. Wires it into AddPeer so a cert that
chains to the cluster CA but has the wrong identity is rejected at dial time.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Add `InsecureTransport` and `AutoTLS` to Config, wire warning + auto-cert in `initRaft()`

**Files:**
- Modify: `cmd/raftd/main.go`

- [ ] **Step 1: Add two new Config fields**

Locate the `Config` struct in `cmd/raftd/main.go`. After the `RequireTLS bool` field (around line 89), add:

```go
	// InsecureTransport explicitly disables TLS for inter-node communication.
	// Set to true only in development environments. Production deployments should
	// configure tls_cert/tls_key/tls_ca for mTLS. Suppresses the cleartext
	// warning when set. (M-TLS1)
	InsecureTransport bool `yaml:"insecure_transport"`

	// AutoTLS, when true, generates a self-signed certificate+key pair in DataDir
	// on first startup if no TLS certificate is configured. The generated cert is
	// used for both serving and peer verification (self-signed CA). This enables
	// encrypted inter-node traffic with zero manual cert setup. (M-TLS2)
	AutoTLS bool `yaml:"auto_tls"`
```

- [ ] **Step 2: Add cleartext warning + auto-cert logic to `initRaft()`**

In `cmd/raftd/main.go`, find the `initRaft()` function. The function starts at approximately line 624 and begins:

```go
func (s *Server) initRaft() error {
	dataDir := s.config.DataDir
	nodeDir := fmt.Sprintf("%s/%s", dataDir, s.config.NodeID)
```

Add the warning + auto-cert block immediately after `if err := os.MkdirAll(nodeDir, 0755); err != nil {` block (and before the `wal, err :=` lines). Insert:

```go
	// M-TLS1: warn loudly when inter-node traffic will be cleartext.
	// This is not a hard failure so existing deployments without TLS continue to
	// work, but operators who haven't set insecure_transport explicitly are
	// notified that they should address it.
	tlsExplicit := s.config.TLSCert != "" || s.config.TLSKey != "" || s.config.TLSCA != ""
	if !tlsExplicit && !s.config.InsecureTransport && !s.config.AutoTLS {
		s.logger.Warn("inter-node traffic is NOT encrypted — set auto_tls: true for development " +
			"or configure tls_cert/tls_key/tls_ca for production mTLS; " +
			"to silence this warning set insecure_transport: true")
	}

	// M-TLS2: auto-generate a self-signed ECDSA cert if auto_tls is set and no
	// manual cert is configured.
	if s.config.AutoTLS && !tlsExplicit {
		paths, err := transport.EnsureAutoTLSCerts(nodeDir, s.config.NodeID)
		if err != nil {
			return fmt.Errorf("auto TLS cert generation: %w", err)
		}
		s.logger.Info("auto-TLS: using self-signed certificate",
			zap.String("cert", paths.CertFile),
			zap.String("key", paths.KeyFile),
		)
		s.config.TLSCert = paths.CertFile
		s.config.TLSKey = paths.KeyFile
		s.config.TLSCA = paths.CAFile
	}
```

- [ ] **Step 3: Build to verify compilation**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go build ./cmd/raftd/... 2>&1
```

Expected: compiles with no errors.

- [ ] **Step 4: Run vet**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go vet ./cmd/raftd/... ./pkg/transport/...
```

Expected: no output (clean).

- [ ] **Step 5: Run server tests**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go test ./cmd/raftd/... -race -count=1 -timeout=60s 2>&1 | tail -20
```

Expected: ok, no failures.

- [ ] **Step 6: Commit**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
git add cmd/raftd/main.go
git commit -m "$(cat <<'EOF'
feat(raftd): add InsecureTransport flag and AutoTLS config with cleartext warning

When no TLS is configured and neither insecure_transport nor auto_tls is set,
a Warn-level advisory is logged on startup. auto_tls: true generates a
self-signed ECDSA P-256 cert in DataDir and wires it as the transport cert/key/CA.
insecure_transport: true silences the warning without enabling TLS.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Update `scripts/certs/generate.sh` with per-node certs + SANs

**Files:**
- Modify: `scripts/certs/generate.sh`
- Create: `scripts/gen-dev-certs.sh`

- [ ] **Step 1: Update `scripts/certs/generate.sh` to add per-node certs**

The existing script generates a single server cert. Add per-node cert generation for node1, node2, node3 after the existing server cert block (keep the existing server cert for backward compat):

```bash
# Generate per-node certs with node-ID SANs (for mTLS SAN validation)
for NODE in node1 node2 node3; do
    openssl genrsa -out "$CERTS_DIR/${NODE}.key" 4096
    chmod 600 "$CERTS_DIR/${NODE}.key"

    cat > "$CERTS_DIR/${NODE}.ext" <<EXTEOF
subjectAltName=DNS:${NODE},DNS:${NODE}.cluster.local,DNS:localhost,IP:127.0.0.1
EXTEOF

    openssl req -new -key "$CERTS_DIR/${NODE}.key" \
        -out "$CERTS_DIR/${NODE}.csr" \
        -subj "/C=US/O=Raft/CN=${NODE}"

    openssl x509 -req -days 3650 \
        -in "$CERTS_DIR/${NODE}.csr" \
        -CA "$CERTS_DIR/ca.crt" \
        -CAkey "$CERTS_DIR/ca.key" \
        -CAcreateserial \
        -extfile "$CERTS_DIR/${NODE}.ext" \
        -out "$CERTS_DIR/${NODE}.crt"

    echo "  ${NODE}.crt / ${NODE}.key - per-node cert for mTLS SAN validation"
done
```

Add these lines to the echo block at the end:
```bash
echo ""
echo "Per-node certs for mTLS:"
echo "  Config example (node1):"
echo "    tls_cert: $CERTS_DIR/node1.crt"
echo "    tls_key:  $CERTS_DIR/node1.key"
echo "    tls_ca:   $CERTS_DIR/ca.crt"
echo "    require_tls: true"
```

- [ ] **Step 2: Create `scripts/gen-dev-certs.sh` (standalone ECDSA generator)**

```bash
#!/usr/bin/env bash
# Generates self-signed mTLS certificates for local development.
# Uses ECDSA P-256 (smaller and faster than RSA-4096).
# Usage: ./scripts/gen-dev-certs.sh [output-dir]
set -euo pipefail
DIR="${1:-./certs}"
mkdir -p "$DIR"
chmod 700 "$DIR"
umask 077

echo "Generating CA..."
openssl req -new -x509 -days 3650 -nodes \
  -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -out "$DIR/ca.crt" -keyout "$DIR/ca.key" \
  -subj "/CN=raft-ca/O=raft-cluster" \
  -addext "basicConstraints=critical,CA:true" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"
chmod 600 "$DIR/ca.key"

for NODE in node1 node2 node3; do
  echo "Generating cert for $NODE..."
  openssl req -new -nodes \
    -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
    -out "$DIR/$NODE.csr" -keyout "$DIR/$NODE.key" \
    -subj "/CN=$NODE/O=raft-cluster"
  chmod 600 "$DIR/$NODE.key"

  openssl x509 -req -days 3650 \
    -CA "$DIR/ca.crt" -CAkey "$DIR/ca.key" \
    -CAcreateserial \
    -in "$DIR/$NODE.csr" \
    -out "$DIR/$NODE.crt" \
    -extfile <(printf "subjectAltName=DNS:%s,DNS:%s.cluster.local,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth" "$NODE" "$NODE")
done

echo ""
echo "Generated certs in $DIR/"
echo ""
echo "Config example (node1):"
echo "  tls_cert: $DIR/node1.crt"
echo "  tls_key:  $DIR/node1.key"
echo "  tls_ca:   $DIR/ca.crt"
echo "  require_tls: true"
echo ""
echo "Or for zero-config encrypted dev:"
echo "  auto_tls: true"
```

Make it executable:
```bash
chmod +x /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a/scripts/gen-dev-certs.sh
```

- [ ] **Step 3: Commit**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
git add scripts/certs/generate.sh scripts/gen-dev-certs.sh
git commit -m "$(cat <<'EOF'
feat(scripts): add per-node mTLS cert generation with SAN for node IDs

scripts/certs/generate.sh now creates node1/node2/node3 certs signed by
the cluster CA, each with DNS SAN matching the node ID for SAN validation.
New scripts/gen-dev-certs.sh is a standalone ECDSA P-256 cert generator.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Update sample config files with TLS/insecure_transport comments

**Files:**
- Modify: `config-node1.yaml`
- Modify: `config-node2.yaml`
- Modify: `config-node3.yaml`

- [ ] **Step 1: Update `config-node1.yaml`**

Append to the file (after the existing cluster block):

```yaml

# Transport security (M-TLS1 / M-TLS2)
# Choose ONE of the following options:
#
# Option A: Zero-config encrypted dev (self-signed cert generated in data_dir)
# auto_tls: true
#
# Option B: Production mTLS with CA-issued per-node certs
#   Generate certs first: ./scripts/gen-dev-certs.sh certs/
# tls_cert: certs/node1.crt
# tls_key: certs/node1.key
# tls_ca: certs/ca.crt
# require_tls: true
#
# Option C: Explicitly opt out of TLS (development only, not recommended)
insecure_transport: true  # remove in production; suppresses the cleartext warning
```

- [ ] **Step 2: Update `config-node2.yaml`**

Append to the file (same block but node2 cert paths):

```yaml

# Transport security (M-TLS1 / M-TLS2)
# Choose ONE of the following options:
#
# Option A: Zero-config encrypted dev (self-signed cert generated in data_dir)
# auto_tls: true
#
# Option B: Production mTLS with CA-issued per-node certs
#   Generate certs first: ./scripts/gen-dev-certs.sh certs/
# tls_cert: certs/node2.crt
# tls_key: certs/node2.key
# tls_ca: certs/ca.crt
# require_tls: true
#
# Option C: Explicitly opt out of TLS (development only, not recommended)
insecure_transport: true  # remove in production; suppresses the cleartext warning
```

- [ ] **Step 3: Update `config-node3.yaml`**

Append to the file:

```yaml

# Transport security (M-TLS1 / M-TLS2)
# Choose ONE of the following options:
#
# Option A: Zero-config encrypted dev (self-signed cert generated in data_dir)
# auto_tls: true
#
# Option B: Production mTLS with CA-issued per-node certs
#   Generate certs first: ./scripts/gen-dev-certs.sh certs/
# tls_cert: certs/node3.crt
# tls_key: certs/node3.key
# tls_ca: certs/ca.crt
# require_tls: true
#
# Option C: Explicitly opt out of TLS (development only, not recommended)
insecure_transport: true  # remove in production; suppresses the cleartext warning
```

- [ ] **Step 4: Commit**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
git add config-node1.yaml config-node2.yaml config-node3.yaml
git commit -m "$(cat <<'EOF'
docs(config): add mTLS transport security comments to sample node configs

Each config file now documents three security options: auto_tls for zero-config
dev, manual tls_cert/key/ca for production mTLS, or insecure_transport: true
to explicitly opt out and suppress the cleartext warning.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Final verification — build, vet, and full test suite

- [ ] **Step 1: Build everything**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go build ./... 2>&1
```

Expected: no output (clean build).

- [ ] **Step 2: Vet everything**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go vet ./... 2>&1
```

Expected: no output.

- [ ] **Step 3: Run the targeted new tests with -race**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go test -race ./pkg/transport/ -run "TestAutoTLS|TestEnsureAutoTLS|TestVerifySAN" -v -count=1
```

Expected: 7 tests PASS (4 autocert + 3 SAN).

- [ ] **Step 4: Run all transport tests with -race**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go test -race ./pkg/transport/... -count=1 -timeout=120s 2>&1 | tail -5
```

Expected: `ok  github.com/sanskarpan/raft-consensus/pkg/transport`

- [ ] **Step 5: Run the full test suite**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
go test -race ./... -count=1 -timeout=300s 2>&1 | grep -E "^(ok|FAIL|---)" | head -40
```

Expected: all packages `ok`, no `FAIL`.

---

## Task 9: Create the PR

- [ ] **Step 1: Set git config and create the branch**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
git config user.email "sanskarpandey2004@gmail.com"
git config user.name "sanskarpan"
git checkout -b feat/mtls-default-auto-cert 2>/dev/null || git checkout feat/mtls-default-auto-cert
```

- [ ] **Step 2: Push the branch**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
git push -u origin feat/mtls-default-auto-cert
```

- [ ] **Step 3: Create the PR**

```bash
cd /Users/sanskar/dev/Research/Projects/Raft-Consensus/.claude/worktrees/agent-a0f7a933ed3689c2a
gh pr create \
  --title "feat(security): auto-TLS cert generation, insecure_transport flag, gRPC SAN validation" \
  --body "$(cat <<'EOF'
## Summary

- **auto_tls: true** generates a self-signed ECDSA P-256 cert (node ID as DNS SAN) on first start, stored in DataDir. Zero-config encrypted transport for dev/test.
- **insecure_transport: true** explicitly opts out of TLS and silences the cleartext advisory log.
- **Cleartext warning**: when neither TLS nor `insecure_transport` is configured, a `Warn`-level log message fires at startup (not a hard failure — backward-compatible).
- **gRPC SAN validation** (`verifySANForNodeID`): wired into `AddPeer` so a peer cert that merely chains to the cluster CA but has the wrong node ID is rejected at dial time.
- **scripts/gen-dev-certs.sh**: new standalone ECDSA P-256 cert generator with per-node SANs (node1/2/3).
- **config-node*.yaml**: documented all three security modes (auto_tls, manual mTLS, insecure_transport).

## Test plan

- [ ] `go test -race ./pkg/transport/ -run "TestAutoTLS|TestEnsureAutoTLS|TestVerifySAN" -v` — 7 new tests pass
- [ ] `go test -race ./pkg/transport/... -count=1` — full transport suite passes
- [ ] `go test -race ./... -count=1` — full project suite passes
- [ ] `go build ./...` — clean build
- [ ] `go vet ./...` — clean

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

### Spec Coverage Check

| Requirement | Task |
|---|---|
| `InsecureTransport bool` field in Config | Task 5 Step 1 |
| `AutoTLS bool` field in Config | Task 5 Step 1 |
| `EnsureAutoTLSCerts` with ECDSA P-256, 10yr, IP+DNS SANs | Tasks 1–2 |
| Self-signed CA cert (IsCA=true) | Task 2 |
| Idempotent cert generation (skips if files exist) | Tasks 1–2 |
| Cleartext warning when no TLS and no explicit opt-out | Task 5 Step 2 |
| Warning NOT a hard failure | Task 5 Step 2 (uses `s.logger.Warn`) |
| Auto-cert stored in DataDir | Task 5 Step 2 (uses `nodeDir`) |
| `verifySANForNodeID` for gRPC | Tasks 3–4 |
| SAN check falls back to CN | Task 3 test + Task 4 implementation |
| Wire SAN check into gRPC `AddPeer` | Task 4 Step 3 |
| Update `scripts/certs/generate.sh` with per-node SANs | Task 6 Step 1 |
| New `scripts/gen-dev-certs.sh` ECDSA generator | Task 6 Step 2 |
| Update sample config files | Task 7 |
| All existing tests pass | Task 8 |

### Placeholder Scan

No TBD, TODO, or placeholder patterns found. Every step includes actual code or exact commands.

### Type Consistency

- `AutoCertPaths.CertFile`, `AutoCertPaths.KeyFile`, `AutoCertPaths.CAFile` — used consistently in Task 2 (definition) and Task 5 (consumption).
- `transport.EnsureAutoTLSCerts(dataDir, nodeID string) (*AutoCertPaths, error)` — signature matches both the test (Task 1) and the wiring (Task 5).
- `transport.VerifySANForNodeID` (exported via `export_test.go`) exposes `verifySANForNodeID` — consistent between Task 3 tests and Task 4 implementation.
- `s.config.TLSCert`, `s.config.TLSKey`, `s.config.TLSCA` — existing fields, populated by auto-cert path in Task 5; then picked up by the existing TCP/gRPC transport setup logic unchanged.

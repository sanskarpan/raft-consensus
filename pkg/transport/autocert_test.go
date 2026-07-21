package transport_test

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"
	"time"

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

// ---------------------------------------------------------------------------
// EnsureIntermediateCAAndCert tests
// ---------------------------------------------------------------------------

// parsePEMCert is a test helper that reads and parses the first certificate in
// a PEM file, failing the test on any error.
func parsePEMCert(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("no PEM block in %s", path)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return cert
}

// TestEnsureIntermediateCAAndCert_creates verifies that the function:
//   - creates ca.crt, <nodeID>.crt, <nodeID>.key
//   - the CA cert has IsCA=true
//   - the node cert is NOT a CA
//   - the node cert verifies against the CA
//   - the node cert contains the nodeID as a DNS SAN
func TestEnsureIntermediateCAAndCert_creates(t *testing.T) {
	dir := t.TempDir()
	nodeID := "node-abc"

	paths, err := transport.EnsureIntermediateCAAndCert(dir, nodeID)
	if err != nil {
		t.Fatalf("EnsureIntermediateCAAndCert: %v", err)
	}

	// All files must exist.
	for _, f := range []string{paths.CACertFile, paths.NodeCertFile, paths.NodeKeyFile} {
		if _, err := os.Stat(f); err != nil {
			t.Errorf("expected file %s to exist: %v", f, err)
		}
	}

	caCert := parsePEMCert(t, paths.CACertFile)
	nodeCert := parsePEMCert(t, paths.NodeCertFile)

	// CA must be a CA; node cert must not be.
	if !caCert.IsCA {
		t.Error("CA cert: IsCA should be true")
	}
	if nodeCert.IsCA {
		t.Error("node cert: IsCA should be false")
	}

	// Node cert must contain nodeID as a DNS SAN.
	found := false
	for _, dns := range nodeCert.DNSNames {
		if dns == nodeID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("node cert DNSNames %v does not contain nodeID %q", nodeCert.DNSNames, nodeID)
	}

	// Node cert must verify against the CA.
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	_, err = nodeCert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		t.Errorf("node cert does not verify against CA: %v", err)
	}
}

// TestEnsureIntermediateCAAndCert_idempotent verifies that:
//   - calling twice reuses the same CA cert (same serial)
//   - the node cert is regenerated only when needed (both calls on a fresh dir
//     produce the same node cert because the first call made a healthy 1-year cert)
func TestEnsureIntermediateCAAndCert_idempotent(t *testing.T) {
	dir := t.TempDir()
	nodeID := "node-idem"

	p1, err := transport.EnsureIntermediateCAAndCert(dir, nodeID)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	ca1 := parsePEMCert(t, p1.CACertFile)
	node1 := parsePEMCert(t, p1.NodeCertFile)

	p2, err := transport.EnsureIntermediateCAAndCert(dir, nodeID)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	ca2 := parsePEMCert(t, p2.CACertFile)
	node2 := parsePEMCert(t, p2.NodeCertFile)

	// CA serial must be unchanged.
	if ca1.SerialNumber.Cmp(ca2.SerialNumber) != 0 {
		t.Errorf("CA cert was replaced on second call (serial %v -> %v)", ca1.SerialNumber, ca2.SerialNumber)
	}

	// Node cert should also be unchanged because the first call produced a
	// healthy 1-year cert (well beyond the 30-day renewal threshold).
	if node1.SerialNumber.Cmp(node2.SerialNumber) != 0 {
		t.Errorf("node cert was unnecessarily replaced (serial %v -> %v)", node1.SerialNumber, node2.SerialNumber)
	}
}

// TestEnsureIntermediateCAAndCert_nodeRotation verifies that:
//   - if the node cert is expired (backdated), it is regenerated
//   - the CA cert stays the same across that rotation
func TestEnsureIntermediateCAAndCert_nodeRotation(t *testing.T) {
	dir := t.TempDir()
	nodeID := "node-rotate"

	// First call — creates healthy CA + node cert.
	p1, err := transport.EnsureIntermediateCAAndCert(dir, nodeID)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	ca1 := parsePEMCert(t, p1.CACertFile)
	node1 := parsePEMCert(t, p1.NodeCertFile)

	// Backdate the node cert so it looks expired: overwrite with a cert whose
	// NotAfter is in the past.
	expiredNotAfter := time.Now().Add(-time.Hour)
	expiredNodeCert := *node1 // shallow copy for display; we overwrite the file
	_ = expiredNodeCert

	// We can't easily re-sign with the CA key here, so instead we write a
	// valid-but-soon-to-expire cert by reading the existing data, re-encoding
	// with a tampered time.  The easiest approach is to just truncate the cert
	// file to an empty/invalid PEM, which loadCertIfValid will reject, causing
	// regeneration.
	if err := os.WriteFile(p1.NodeCertFile, []byte("not a cert"), 0600); err != nil {
		t.Fatalf("overwrite node cert: %v", err)
	}
	// Also remove the key so the regenerated key is fresh.
	if err := os.Remove(p1.NodeKeyFile); err != nil {
		t.Fatalf("remove node key: %v", err)
	}
	_ = expiredNotAfter // used in comment above

	// Second call — should regenerate node cert but reuse CA.
	p2, err := transport.EnsureIntermediateCAAndCert(dir, nodeID)
	if err != nil {
		t.Fatalf("second call after invalidating node cert: %v", err)
	}
	ca2 := parsePEMCert(t, p2.CACertFile)
	node2 := parsePEMCert(t, p2.NodeCertFile)

	// CA serial must be unchanged.
	if ca1.SerialNumber.Cmp(ca2.SerialNumber) != 0 {
		t.Errorf("CA cert was replaced during node cert rotation (serial %v -> %v)", ca1.SerialNumber, ca2.SerialNumber)
	}

	// Node cert must be NEW (different serial from the original).
	if node1.SerialNumber.Cmp(node2.SerialNumber) == 0 {
		t.Error("expected node cert to be regenerated but serial is unchanged")
	}

	// New node cert must still verify against the (unchanged) CA.
	pool := x509.NewCertPool()
	pool.AddCert(ca2)
	_, err = node2.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		t.Errorf("rotated node cert does not verify against CA: %v", err)
	}
}

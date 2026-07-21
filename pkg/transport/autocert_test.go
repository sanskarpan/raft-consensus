package transport_test

import (
	"crypto/x509"
	"encoding/pem"
	"os"
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

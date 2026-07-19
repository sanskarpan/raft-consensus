package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeKeypair writes a fresh self-signed ECDSA keypair with the given CN/serial
// to certPath/keyPath.
func writeKeypair(t *testing.T, certPath, keyPath, cn string, serial int64) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
}

func servedCN(t *testing.T, r *certReloader) string {
	t.Helper()
	c, err := r.getCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return leaf.Subject.CommonName
}

// TestCertReloaderRotates verifies reload() picks up a rotated keypair on disk
// so the served certificate changes without restarting (#204).
func TestCertReloaderRotates(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	writeKeypair(t, certPath, keyPath, "cert-a", 1)
	r, err := newCertReloader(certPath, keyPath)
	if err != nil {
		t.Fatalf("newCertReloader: %v", err)
	}
	if cn := servedCN(t, r); cn != "cert-a" {
		t.Fatalf("initial served CN = %q, want cert-a", cn)
	}

	// Rotate the files on disk, then reload.
	writeKeypair(t, certPath, keyPath, "cert-b", 2)
	if cn := servedCN(t, r); cn != "cert-a" {
		t.Fatalf("served CN changed before reload: %q (should still be cert-a)", cn)
	}
	if err := r.reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if cn := servedCN(t, r); cn != "cert-b" {
		t.Fatalf("served CN after reload = %q, want cert-b", cn)
	}
}

// TestStaticReloaderSetPathsEnablesReload verifies a static reloader
// (NewGrpcTransport path) reloads from disk once SetCertPaths is called.
func TestStaticReloaderSetPathsEnablesReload(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "c.pem")
	keyPath := filepath.Join(dir, "k.pem")
	writeKeypair(t, certPath, keyPath, "static-a", 1)

	// Simulate NewGrpcTransport: load a cert, wrap statically.
	initial, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	r := newStaticCertReloader(initial)
	if cn := servedCN(t, r); cn != "static-a" {
		t.Fatalf("static CN = %q, want static-a", cn)
	}

	// reload() is a no-op until paths are set.
	writeKeypair(t, certPath, keyPath, "static-b", 2)
	r.reload()
	if cn := servedCN(t, r); cn != "static-a" {
		t.Fatalf("static reloader changed without paths: %q", cn)
	}

	r.setPaths(certPath, keyPath)
	if err := r.reload(); err != nil {
		t.Fatal(err)
	}
	if cn := servedCN(t, r); cn != "static-b" {
		t.Fatalf("after setPaths+reload CN = %q, want static-b", cn)
	}
}

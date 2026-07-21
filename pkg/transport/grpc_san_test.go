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

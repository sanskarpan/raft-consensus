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

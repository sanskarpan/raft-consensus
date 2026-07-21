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

// IntermediateCAPaths holds the file paths for the CA cert and a node's
// individual cert/key pair produced by EnsureIntermediateCAAndCert.
//
// In this model the CA cert is the shared trust anchor distributed to all
// cluster nodes (as the root-of-trust), while each node only holds its own
// NodeCertFile + NodeKeyFile.  Rotating a node cert (e.g., on expiry or
// compromise) only requires replacing that node's pair — the CA cert and every
// other node's trust configuration remain untouched.
type IntermediateCAPaths struct {
	// CACertFile is the PEM-encoded root CA certificate (ECDSA P-384, 10-year
	// validity).  All nodes add this to their TLS trust pool.
	CACertFile string

	// NodeCertFile is the PEM-encoded node certificate signed by the CA
	// (ECDSA P-256, 1-year validity).
	NodeCertFile string

	// NodeKeyFile is the PEM-encoded EC private key for NodeCertFile.
	NodeKeyFile string
}

// renewThreshold is how far before expiry a certificate is considered
// "due for renewal" by EnsureIntermediateCAAndCert.
const renewThreshold = 30 * 24 * time.Hour

// EnsureIntermediateCAAndCert implements the intermediate CA pattern for
// production cluster TLS.  Calling it multiple times on the same dataDir is
// safe (idempotent):
//
//   - CA cert (ca.crt / ca.key in dataDir): created once using ECDSA P-384
//     with a 10-year validity.  If ca.crt already exists and has more than
//     30 days remaining, it is reused as-is; the CA key is never replaced
//     while the cert is still healthy.
//   - Node cert (<nodeID>.crt / <nodeID>.key): created or regenerated
//     whenever the file is missing or the cert has fewer than 30 days
//     remaining.  The node cert is always signed by the current CA key.
//
// Node cert SANs:
//   - DNS: nodeID, localhost
//   - IP:  127.0.0.1, ::1
func EnsureIntermediateCAAndCert(dataDir, nodeID string) (*IntermediateCAPaths, error) {
	caCertFile := filepath.Join(dataDir, "ca.crt")
	caKeyFile := filepath.Join(dataDir, "ca.key")
	nodeCertFile := filepath.Join(dataDir, nodeID+".crt")
	nodeKeyFile := filepath.Join(dataDir, nodeID+".key")

	// --- Step 1: ensure CA ---
	var caCert *x509.Certificate
	var caKey *ecdsa.PrivateKey

	if existingCA, err := loadCertIfValid(caCertFile, renewThreshold); err == nil {
		// CA exists and is healthy — load the key too.
		k, err := loadECKey(caKeyFile)
		if err != nil {
			return nil, fmt.Errorf("intermediate-ca: load CA key: %w", err)
		}
		caCert = existingCA
		caKey = k
	} else {
		// Generate new root CA (P-384, 10-year).
		k, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("intermediate-ca: generate CA key: %w", err)
		}
		serial, err := randomSerial()
		if err != nil {
			return nil, fmt.Errorf("intermediate-ca: CA serial: %w", err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: serial,
			Subject: pkix.Name{
				Organization: []string{"raft-cluster"},
				CommonName:   "raft-cluster Root CA",
			},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
			BasicConstraintsValid: true,
			IsCA:                  true,
		}
		certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
		if err != nil {
			return nil, fmt.Errorf("intermediate-ca: create CA cert: %w", err)
		}
		if err := writePEMFile(caCertFile, "CERTIFICATE", certDER); err != nil {
			return nil, fmt.Errorf("intermediate-ca: write ca.crt: %w", err)
		}
		if err := writeECKeyFile(caKeyFile, k); err != nil {
			return nil, fmt.Errorf("intermediate-ca: write ca.key: %w", err)
		}
		parsed, err := x509.ParseCertificate(certDER)
		if err != nil {
			return nil, fmt.Errorf("intermediate-ca: parse new CA cert: %w", err)
		}
		caCert = parsed
		caKey = k
	}

	// --- Step 2: ensure node cert ---
	if _, err := loadCertIfValid(nodeCertFile, renewThreshold); err != nil {
		// Node cert missing or expiring — regenerate it signed by our CA.
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("intermediate-ca: generate node key: %w", err)
		}
		serial, err := randomSerial()
		if err != nil {
			return nil, fmt.Errorf("intermediate-ca: node serial: %w", err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: serial,
			Subject: pkix.Name{
				Organization: []string{"raft-cluster"},
				CommonName:   nodeID,
			},
			NotBefore: time.Now().Add(-time.Hour),
			NotAfter:  time.Now().Add(365 * 24 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature |
				x509.KeyUsageKeyEncipherment,
			ExtKeyUsage: []x509.ExtKeyUsage{
				x509.ExtKeyUsageServerAuth,
				x509.ExtKeyUsageClientAuth,
			},
			BasicConstraintsValid: true,
			IsCA:                  false,
			IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
			DNSNames:              []string{nodeID, "localhost"},
		}
		certDER, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &k.PublicKey, caKey)
		if err != nil {
			return nil, fmt.Errorf("intermediate-ca: sign node cert: %w", err)
		}
		if err := writePEMFile(nodeCertFile, "CERTIFICATE", certDER); err != nil {
			return nil, fmt.Errorf("intermediate-ca: write node cert: %w", err)
		}
		if err := writeECKeyFile(nodeKeyFile, k); err != nil {
			return nil, fmt.Errorf("intermediate-ca: write node key: %w", err)
		}
	}

	return &IntermediateCAPaths{
		CACertFile:   caCertFile,
		NodeCertFile: nodeCertFile,
		NodeKeyFile:  nodeKeyFile,
	}, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// loadCertIfValid reads a PEM-encoded certificate from path and returns it if
// it exists and its NotAfter is at least minRemaining in the future.
// Returns an error if the file is missing, unparseable, or near expiry.
func loadCertIfValid(path string, minRemaining time.Duration) (*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block in %s", path)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if time.Until(cert.NotAfter) < minRemaining {
		return nil, fmt.Errorf("cert %s expires at %v (< %v remaining)", path, cert.NotAfter, minRemaining)
	}
	return cert, nil
}

// loadECKey reads a PEM-encoded EC private key from path.
func loadECKey(path string) (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block in %s", path)
	}
	return x509.ParseECPrivateKey(block.Bytes)
}

// writePEMFile writes DER-encoded bytes as a PEM file with mode 0600.
func writePEMFile(path, blockType string, der []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if encErr := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); encErr != nil {
		f.Close()
		return encErr
	}
	return f.Close()
}

// writeECKeyFile marshals an ECDSA key to DER and writes it as a PEM file.
func writeECKeyFile(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	return writePEMFile(path, "EC PRIVATE KEY", der)
}

// randomSerial generates a cryptographically random 128-bit certificate serial.
func randomSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}

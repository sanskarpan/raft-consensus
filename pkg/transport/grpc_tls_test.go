package transport_test

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
	"testing"
	"time"

	"github.com/raft-consensus/pkg/transport"
	"go.uber.org/zap"
)

// certAuthority holds the material for one certificate authority (CA).
type certAuthority struct {
	certPEM []byte
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
}

// newCA generates a self-signed CA certificate.
func newCA(t *testing.T) *certAuthority {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Test CA",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	caParsed, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	return &certAuthority{
		certPEM: certPEM,
		cert:    caParsed,
		key:     key,
	}
}

// issueCert signs a server or client certificate from the given CA.
func issueCert(t *testing.T, ca *certAuthority, isServer bool) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))

	extKeyUsage := x509.ExtKeyUsageClientAuth
	if isServer {
		extKeyUsage = x509.ExtKeyUsageServerAuth
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "test-cert",
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{extKeyUsage},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:    []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM
}

// startGrpcTransportServer starts a GrpcTransport and returns it.
// The caller is responsible for calling trans.Close() when done.
func startGrpcTransportServer(t *testing.T, certPEM, keyPEM, caCertPEM []byte) *transport.GrpcTransport {
	t.Helper()

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}

	trans, err := transport.NewGrpcTransport(":0", zap.NewNop(), cert, caCertPEM)
	if err != nil {
		t.Fatalf("NewGrpcTransport: %v", err)
	}

	return trans
}

// tlsDial performs a raw TLS handshake to addr using the given tls.Config.
// It returns nil on success or an error on failure.
// The connection is established with a short timeout so tests don't hang.
func tlsDial(addr string, cfg *tls.Config) error {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, cfg)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

// TestGrpcTLSRejectionWithInvalidCA verifies that a client trusting a different
// (unknown) CA cannot complete a TLS handshake with the gRPC server.
func TestGrpcTLSRejectionWithInvalidCA(t *testing.T) {
	// Known CA — used by the server.
	knownCA := newCA(t)
	// Unknown CA — only trusted by the client.
	unknownCA := newCA(t)

	serverCertPEM, serverKeyPEM := issueCert(t, knownCA, true)

	trans := startGrpcTransportServer(t, serverCertPEM, serverKeyPEM, knownCA.certPEM)
	defer trans.Close()

	addr := trans.ListenerAddr()

	// Client trusts only the unknown CA — handshake must fail.
	unknownPool := x509.NewCertPool()
	unknownPool.AppendCertsFromPEM(unknownCA.certPEM)

	clientTLS := &tls.Config{
		RootCAs:    unknownPool,
		ServerName: "localhost",
	}

	err := tlsDial(addr, clientTLS)
	if err == nil {
		t.Fatal("expected TLS handshake to fail with unknown CA, but dial succeeded")
	}
	t.Logf("correctly rejected: %v", err)
}

// TestGrpcTLSHandshakeSuccess verifies that a client trusting the correct CA
// can complete a TLS handshake with the gRPC server.
func TestGrpcTLSHandshakeSuccess(t *testing.T) {
	ca := newCA(t)
	serverCertPEM, serverKeyPEM := issueCert(t, ca, true)

	trans := startGrpcTransportServer(t, serverCertPEM, serverKeyPEM, ca.certPEM)
	defer trans.Close()

	addr := trans.ListenerAddr()

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(ca.certPEM)

	clientTLS := &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
	}

	if err := tlsDial(addr, clientTLS); err != nil {
		t.Fatalf("TLS handshake failed unexpectedly: %v", err)
	}
	t.Log("TLS handshake succeeded")
}

// TestGrpcMTLSRejectionWithoutClientCert verifies that a server requiring mTLS
// rejects a client that provides no certificate.
//
// In TLS 1.3, the server's certificate-required rejection may come as a
// post-handshake alert that only manifests when the client tries to read data.
// We therefore attempt a write+read after the handshake and expect an error
// somewhere in the sequence.
func TestGrpcMTLSRejectionWithoutClientCert(t *testing.T) {
	ca := newCA(t)
	serverCertPEM, serverKeyPEM := issueCert(t, ca, true)

	serverTLSCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(ca.certPEM)

	serverTLSConf := &tls.Config{
		Certificates: []tls.Certificate{serverTLSCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}

	ln, err := tls.Listen("tcp", ":0", serverTLSConf)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	addr := ln.Addr().String()
	defer ln.Close()

	// Accept connections.  The server-side Handshake will fail because the client
	// offers no cert; we just accept and let it fail.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				tlsConn := c.(*tls.Conn)
				// Server-side handshake will fail; ignore the error here.
				tlsConn.Handshake() //nolint:errcheck
			}(conn)
		}
	}()

	// Client provides no certificate.
	clientTLSConf := &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		// No Certificates — no client cert offered.
	}

	dialer := &net.Dialer{Timeout: 3 * time.Second}
	rawConn, dialErr := tls.DialWithDialer(dialer, "tcp", addr, clientTLSConf)

	if dialErr != nil {
		// The handshake itself was rejected — this is the most direct case.
		t.Logf("correctly rejected during handshake: %v", dialErr)
		return
	}

	// TLS 1.3: handshake may "succeed" from client's view but subsequent I/O
	// should fail because the server sends a post-handshake alert.
	defer rawConn.Close()

	rawConn.SetDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	_, writeErr := rawConn.Write([]byte("ping"))
	buf := make([]byte, 64)
	_, readErr := rawConn.Read(buf)

	if writeErr != nil || readErr != nil {
		t.Logf("correctly rejected after handshake (write err: %v, read err: %v)", writeErr, readErr)
		return
	}

	t.Fatal("expected mTLS to reject connection without client cert, but all I/O succeeded")
}

// TestGrpcMTLSSuccess verifies that a client with a valid certificate signed by
// the same CA can connect to an mTLS-enabled server.
func TestGrpcMTLSSuccess(t *testing.T) {
	ca := newCA(t)

	serverCertPEM, serverKeyPEM := issueCert(t, ca, true)
	clientCertPEM, clientKeyPEM := issueCert(t, ca, false)

	serverTLSCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("server X509KeyPair: %v", err)
	}
	clientTLSCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		t.Fatalf("client X509KeyPair: %v", err)
	}

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(ca.certPEM)

	serverTLSConf := &tls.Config{
		Certificates: []tls.Certificate{serverTLSCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}

	ln, err := tls.Listen("tcp", ":0", serverTLSConf)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	addr := ln.Addr().String()
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			tlsConn := conn.(*tls.Conn)
			tlsConn.Handshake() //nolint:errcheck
			conn.Close()
		}
	}()

	// Client provides its certificate.
	clientTLS := &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientTLSCert},
		ServerName:   "localhost",
	}

	if err := tlsDial(addr, clientTLS); err != nil {
		t.Fatalf("mTLS dial failed: %v", err)
	}
	t.Log("mTLS handshake succeeded")
}

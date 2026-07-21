package transport

import (
	"crypto/tls"
	"crypto/x509"
)

// VerifySANForNodeID exposes the unexported verifySANForNodeID for unit tests.
var VerifySANForNodeID = verifySANForNodeID

// Ensure the type alias compiles (the function returns a func matching tls.Config.VerifyPeerCertificate).
var _ func([][]byte, [][]*x509.Certificate) error = VerifySANForNodeID("")

// Test-only accessors for otherwise-unexported TLS configuration, used by the
// C11 hardening regression tests. Compiled only under `go test`.

// ServerTLSConfig returns the server-side TLS config (gRPC).
func (t *GrpcTransport) ServerTLSConfig() *tls.Config {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tlsConfig
}

// ClientTLSConfig returns the client-side dial TLS config (gRPC).
func (t *GrpcTransport) ClientTLSConfig() *tls.Config {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.clientTLSConfig
}

// ClientTLSConfigForTest returns the client-side dial TLS config (TCP).
func (t *tcpTransport) ClientTLSConfigForTest() *tls.Config {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tlsClient
}

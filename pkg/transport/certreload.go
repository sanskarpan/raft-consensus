package transport

import (
	"crypto/tls"
	"errors"
	"sync/atomic"
)

// certReloader holds a TLS keypair and allows it to be atomically reloaded from
// disk without restarting the process or rebuilding the tls.Config. The
// GetCertificate / GetClientCertificate callbacks read the current cert
// atomically, so a rotation takes effect on the next handshake while in-flight
// connections keep their existing cert (#204).
//
// It supports two sources: a static cert (from NewGrpcTransport, which receives
// an already-loaded keypair) and file paths (from SetTLS, or set later via
// setPaths). reload() re-reads from the file paths when configured.
type certReloader struct {
	certPath string
	keyPath  string
	cert     atomic.Pointer[tls.Certificate]
}

// newCertReloader loads the initial keypair from files. Empty paths yield a nil
// reloader (no cert configured), which callers must handle.
func newCertReloader(certPath, keyPath string) (*certReloader, error) {
	if certPath == "" || keyPath == "" {
		return nil, nil
	}
	r := &certReloader{certPath: certPath, keyPath: keyPath}
	if err := r.reload(); err != nil {
		return nil, err
	}
	return r, nil
}

// newStaticCertReloader wraps an already-loaded keypair. reload() is a no-op
// until setPaths configures file paths.
func newStaticCertReloader(cert tls.Certificate) *certReloader {
	r := &certReloader{}
	r.cert.Store(&cert)
	return r
}

// setPaths enables file-based reloading for a reloader created from a static
// cert, so a subsequent reload() (via ReloadTLS) picks up rotated files.
func (r *certReloader) setPaths(certPath, keyPath string) {
	r.certPath = certPath
	r.keyPath = keyPath
}

// reload re-reads the keypair from disk (if paths are configured) and atomically
// swaps it in. With no paths it is a no-op (static cert).
func (r *certReloader) reload() error {
	if r.certPath == "" || r.keyPath == "" {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(r.certPath, r.keyPath)
	if err != nil {
		return err
	}
	r.cert.Store(&cert)
	return nil
}

func (r *certReloader) getCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	c := r.cert.Load()
	if c == nil {
		return nil, errors.New("transport: no certificate loaded")
	}
	return c, nil
}

func (r *certReloader) getClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	c := r.cert.Load()
	if c == nil {
		return &tls.Certificate{}, nil
	}
	return c, nil
}

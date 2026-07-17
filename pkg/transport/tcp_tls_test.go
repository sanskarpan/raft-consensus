package transport_test

// TCP-transport TLS tests
//
// These tests exercise NewTCPTransportTLS, LoadTLSConfig, and the
// mTLS handshake between two TCP transport instances.  The cert-generation
// helpers (newCA, issueCert, tlsDial) are defined in grpc_tls_test.go.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"github.com/sanskarpan/raft-consensus/pkg/transport"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// LoadTLSConfig unit tests
// ---------------------------------------------------------------------------

func TestLoadTLSConfigNil(t *testing.T) {
	cfg, err := transport.LoadTLSConfig(nil)
	if err != nil {
		t.Fatalf("LoadTLSConfig(nil) returned error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("LoadTLSConfig(nil) = %v, want nil", cfg)
	}
}

func TestLoadTLSConfigEmpty(t *testing.T) {
	cfg, err := transport.LoadTLSConfig(&transport.TCPTLSConfig{})
	if err != nil {
		t.Fatalf("LoadTLSConfig({}) returned error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("LoadTLSConfig({}) = %v, want nil", cfg)
	}
}

func TestLoadTLSConfigMissingKey(t *testing.T) {
	_, err := transport.LoadTLSConfig(&transport.TCPTLSConfig{CertFile: "foo.crt"})
	if err == nil {
		t.Fatal("expected error when KeyFile is missing")
	}
}

func TestLoadTLSConfigFromFiles(t *testing.T) {
	ca := newCA(t)
	certPEM, keyPEM := issueCert(t, ca, true)

	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	caFile := filepath.Join(dir, "ca.crt")

	os.WriteFile(certFile, certPEM, 0600)  //nolint:errcheck
	os.WriteFile(keyFile, keyPEM, 0600)    //nolint:errcheck
	os.WriteFile(caFile, ca.certPEM, 0600) //nolint:errcheck

	cfg, err := transport.LoadTLSConfig(&transport.TCPTLSConfig{
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	})
	if err != nil {
		t.Fatalf("LoadTLSConfig: %v", err)
	}
	if cfg == nil {
		t.Fatal("LoadTLSConfig returned nil tls.Config")
	}
	if len(cfg.Certificates) == 0 {
		t.Fatal("tls.Config has no certificates")
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth = %v, want RequireAndVerifyClientCert", cfg.ClientAuth)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %v, want TLS 1.3", cfg.MinVersion)
	}
}

// ---------------------------------------------------------------------------
// Integration: plain TCP transport still works (no regression)
// ---------------------------------------------------------------------------

// noopHandler satisfies MessageHandler with empty responses.
type noopHandler struct{}

func (n *noopHandler) HandleAppendEntries(req *transport.AppendEntriesReq) *transport.AppendEntriesResp {
	return &transport.AppendEntriesResp{}
}
func (n *noopHandler) HandleRequestVote(req *transport.RequestVoteReq) *transport.RequestVoteResp {
	return &transport.RequestVoteResp{}
}
func (n *noopHandler) HandleInstallSnapshot(req *transport.InstallSnapshotReq) *transport.InstallSnapshotResp {
	return &transport.InstallSnapshotResp{}
}
func (n *noopHandler) HandleTimeoutNow(req *transport.TimeoutNowReq) *transport.TimeoutNowResp {
	return &transport.TimeoutNowResp{}
}

func TestTCPTransportPlainStillWorks(t *testing.T) {
	handler := &noopHandler{}
	srv, err := transport.NewTCPTransport(":0", handler, 3*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer srv.Close()

	addr := srv.ListenerAddr()

	cli, err := transport.NewTCPTransport(":0", handler, 3*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport (client): %v", err)
	}
	defer cli.Close()

	cli.SetLocalID(raft.ServerID("client"))
	cli.AddPeer(raft.ServerID("server"), raft.ServerAddress(addr)) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = cli.RequestVote(ctx, raft.ServerID("server"), &raft.RequestVoteRequest{Term: 1})
	if err != nil {
		t.Fatalf("RequestVote over plain TCP: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Integration: TLS transport — server rejects unknown CA
// ---------------------------------------------------------------------------

func TestTCPTLSRejectsUnknownCA(t *testing.T) {
	knownCA := newCA(t)
	unknownCA := newCA(t)

	serverCertPEM, serverKeyPEM := issueCert(t, knownCA, true)

	dir := t.TempDir()
	writePEM(t, dir, "server.crt", serverCertPEM)
	writePEM(t, dir, "server.key", serverKeyPEM)
	writePEM(t, dir, "ca.crt", knownCA.certPEM)

	serverTLS, err := transport.LoadTLSConfig(&transport.TCPTLSConfig{
		CertFile: filepath.Join(dir, "server.crt"),
		KeyFile:  filepath.Join(dir, "server.key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	})
	if err != nil {
		t.Fatalf("LoadTLSConfig (server): %v", err)
	}

	srv, err := transport.NewTCPTransportTLS(":0", &noopHandler{}, 3*time.Second, zap.NewNop(), serverTLS)
	if err != nil {
		t.Fatalf("NewTCPTransportTLS: %v", err)
	}
	defer srv.Close()

	addr := srv.ListenerAddr()

	// Client trusts only the unknown CA — handshake must fail.
	unknownPool := x509.NewCertPool()
	unknownPool.AppendCertsFromPEM(unknownCA.certPEM)

	err = tlsDial(addr, &tls.Config{
		RootCAs:    unknownPool,
		ServerName: "localhost",
	})
	if err == nil {
		t.Fatal("expected TLS to fail with unknown CA but it succeeded")
	}
	t.Logf("correctly rejected: %v", err)
}

// ---------------------------------------------------------------------------
// Integration: mTLS — two transport instances exchange an RPC
// ---------------------------------------------------------------------------

func TestTCPMTLSRoundTrip(t *testing.T) {
	ca := newCA(t)
	serverCertPEM, serverKeyPEM := issueCert(t, ca, true)
	clientCertPEM, clientKeyPEM := issueCert(t, ca, false)

	dir := t.TempDir()
	writePEM(t, dir, "server.crt", serverCertPEM)
	writePEM(t, dir, "server.key", serverKeyPEM)
	writePEM(t, dir, "client.crt", clientCertPEM)
	writePEM(t, dir, "client.key", clientKeyPEM)
	writePEM(t, dir, "ca.crt", ca.certPEM)

	loadTLS := func(cert, key string) *tls.Config {
		t.Helper()
		cfg, err := transport.LoadTLSConfig(&transport.TCPTLSConfig{
			CertFile: filepath.Join(dir, cert),
			KeyFile:  filepath.Join(dir, key),
			CAFile:   filepath.Join(dir, "ca.crt"),
		})
		if err != nil {
			t.Fatalf("LoadTLSConfig: %v", err)
		}
		return cfg
	}

	serverTLS := loadTLS("server.crt", "server.key")

	var voted bool
	srvHandler := &callbackHandler{
		onRequestVote: func(req *transport.RequestVoteReq) *transport.RequestVoteResp {
			voted = true
			return &transport.RequestVoteResp{VoteGranted: true}
		},
	}

	srv, err := transport.NewTCPTransportTLS(":0", srvHandler, 3*time.Second, zap.NewNop(), serverTLS)
	if err != nil {
		t.Fatalf("NewTCPTransportTLS (server): %v", err)
	}
	defer srv.Close()

	clientTLS := loadTLS("client.crt", "client.key")
	// Client-side TLS needs ServerName to match what the server cert lists.
	clientTLS.ServerName = "localhost"

	cli, err := transport.NewTCPTransportTLS(":0", &noopHandler{}, 3*time.Second, zap.NewNop(), clientTLS)
	if err != nil {
		t.Fatalf("NewTCPTransportTLS (client): %v", err)
	}
	defer cli.Close()

	cli.SetLocalID(raft.ServerID("client"))
	cli.AddPeer(raft.ServerID("server"), raft.ServerAddress(srv.ListenerAddr())) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.RequestVote(ctx, raft.ServerID("server"), &raft.RequestVoteRequest{Term: 1, CandidateID: "client"})
	if err != nil {
		t.Fatalf("RequestVote over mTLS: %v", err)
	}
	if !resp.VoteGranted {
		t.Error("expected vote granted")
	}
	if !voted {
		t.Error("server handler never called")
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func writePEM(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// callbackHandler lets individual tests wire specific handler logic.
type callbackHandler struct {
	noopHandler
	onRequestVote func(*transport.RequestVoteReq) *transport.RequestVoteResp
}

func (c *callbackHandler) HandleRequestVote(req *transport.RequestVoteReq) *transport.RequestVoteResp {
	if c.onRequestVote != nil {
		return c.onRequestVote(req)
	}
	return &transport.RequestVoteResp{}
}

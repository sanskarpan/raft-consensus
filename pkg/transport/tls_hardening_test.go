package transport_test

// C11/C12 regression tests.
//
// C11: the TLS-configured paths must pin TLS 1.3, default to mutual TLS when a
// CA pool is present, and verify the server on the client dial config.
// C12: the TCP transport must correlate responses to requests by a monotonic
// request ID and response type, dropping the connection on a mismatch.

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"github.com/sanskarpan/raft-consensus/pkg/transport"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// C11 — gRPC TLS hardening
// ---------------------------------------------------------------------------

// TestGrpcTLSPinsTLS13AndMTLS verifies that NewGrpcTransport pins TLS 1.3 on
// the server config and defaults to RequireAndVerifyClientCert when a CA is
// supplied. Against the pre-fix code (no MinVersion, NoClientCert default) both
// assertions fail.
func TestGrpcTLSPinsTLS13AndMTLS(t *testing.T) {
	ca := newCA(t)
	serverCertPEM, serverKeyPEM := issueCert(t, ca, true)

	trans := startGrpcTransportServer(t, serverCertPEM, serverKeyPEM, ca.certPEM)
	defer trans.Close()

	cfg := trans.ServerTLSConfig()
	if cfg == nil {
		t.Fatal("ServerTLSConfig is nil")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("server MinVersion = %v, want TLS 1.3", cfg.MinVersion)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("server ClientAuth = %v, want RequireAndVerifyClientCert", cfg.ClientAuth)
	}
	if cfg.ClientCAs == nil {
		t.Error("server ClientCAs is nil; mTLS cannot verify client certs")
	}

	cli := trans.ClientTLSConfig()
	if cli == nil {
		t.Fatal("ClientTLSConfig is nil")
	}
	if cli.MinVersion != tls.VersionTLS13 {
		t.Errorf("client MinVersion = %v, want TLS 1.3", cli.MinVersion)
	}
	if cli.InsecureSkipVerify {
		t.Error("client InsecureSkipVerify is true; server would not be verified")
	}
	if cli.RootCAs == nil {
		t.Error("client RootCAs is nil; server cannot be verified")
	}
	// C11: ServerName is intentionally NOT hardcoded on the base client config;
	// it is derived per-peer from the dial address at AddPeer time so each peer
	// is verified against its own cert identity. End-to-end verification is
	// exercised by the mTLS round-trip tests.
	if cli.ServerName != "" {
		t.Errorf("client base ServerName = %q, want empty (derived per-peer)", cli.ServerName)
	}
}

// ---------------------------------------------------------------------------
// C11 — TCP TLS hardening (client dial config from clone)
// ---------------------------------------------------------------------------

// TestTCPTLSClientDialConfigHardened verifies that the client-side dial config
// derived inside NewTCPTransportTLS pins TLS 1.3, keeps verification on, and
// carries a ServerName.
func TestTCPTLSClientDialConfigHardened(t *testing.T) {
	ca := newCA(t)
	serverCertPEM, serverKeyPEM := issueCert(t, ca, true)

	dir := t.TempDir()
	writePEM(t, dir, "server.crt", serverCertPEM)
	writePEM(t, dir, "server.key", serverKeyPEM)
	writePEM(t, dir, "ca.crt", ca.certPEM)

	serverTLS, err := transport.LoadTLSConfig(&transport.TCPTLSConfig{
		CertFile: dir + "/server.crt",
		KeyFile:  dir + "/server.key",
		CAFile:   dir + "/ca.crt",
	})
	if err != nil {
		t.Fatalf("LoadTLSConfig: %v", err)
	}

	tr, err := transport.NewTCPTransportTLS(":0", &noopHandler{}, 3*time.Second, zap.NewNop(), serverTLS)
	if err != nil {
		t.Fatalf("NewTCPTransportTLS: %v", err)
	}
	defer tr.Close()

	cli := tr.ClientTLSConfigForTest()
	if cli == nil {
		t.Fatal("client dial config is nil")
	}
	if cli.MinVersion != tls.VersionTLS13 {
		t.Errorf("client MinVersion = %v, want TLS 1.3", cli.MinVersion)
	}
	if cli.InsecureSkipVerify {
		t.Error("client InsecureSkipVerify is true")
	}
	if cli.RootCAs == nil {
		t.Error("client RootCAs is nil")
	}
	// C11: ServerName is derived per-peer at dial time, not hardcoded here.
	if cli.ServerName != "" {
		t.Errorf("client base ServerName = %q, want empty (derived per-peer)", cli.ServerName)
	}
	if cli.ClientAuth != tls.NoClientCert {
		t.Errorf("client ClientAuth = %v, want NoClientCert", cli.ClientAuth)
	}
}

// ---------------------------------------------------------------------------
// C12 — TCP request/response correlation
// ---------------------------------------------------------------------------

// TestTCPResponseCorrelationRejectsMismatch stands up a fake TCP server that
// deliberately replies with the WRONG correlation ID and type. The client must
// detect the mismatch and return an error rather than a bogus payload.
func TestTCPResponseCorrelationRejectsMismatch(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		r := bufio.NewReader(conn)
		dec := json.NewDecoder(r)
		var req struct {
			ID      uint64          `json:"id"`
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := dec.Decode(&req); err != nil {
			return
		}
		// Reply with a mismatched ID (req.ID+999) and a wrong type. A correct
		// response payload is included; the client must still reject it.
		body, _ := json.Marshal(transport.RequestVoteResp{Term: 42, VoteGranted: true})
		resp := struct {
			ID      uint64          `json:"id"`
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}{
			ID:      req.ID + 999,
			Type:    "SomethingElse",
			Payload: body,
		}
		enc := json.NewEncoder(conn)
		enc.Encode(resp) //nolint:errcheck
	}()

	cli, err := transport.NewTCPTransport(":0", &noopHandler{}, 3*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer cli.Close()

	cli.SetLocalID(raft.ServerID("client"))
	cli.AddPeer(raft.ServerID("server"), raft.ServerAddress(ln.Addr().String())) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = cli.RequestVote(ctx, raft.ServerID("server"), &raft.RequestVoteRequest{Term: 1})
	if err == nil {
		t.Fatal("expected correlation-mismatch error, got nil (mismatched payload accepted)")
	}
	t.Logf("correctly rejected mismatched response: %v", err)

	<-done
}

// TestTCPResponseCorrelationHappyPath confirms a well-behaved server (real
// transport) still round-trips after the ID/type checks were added.
func TestTCPResponseCorrelationHappyPath(t *testing.T) {
	srvHandler := &callbackHandler{
		onRequestVote: func(req *transport.RequestVoteReq) *transport.RequestVoteResp {
			return &transport.RequestVoteResp{Term: req.Term, VoteGranted: true}
		},
	}
	srv, err := transport.NewTCPTransport(":0", srvHandler, 3*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport (server): %v", err)
	}
	defer srv.Close()

	cli, err := transport.NewTCPTransport(":0", &noopHandler{}, 3*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport (client): %v", err)
	}
	defer cli.Close()

	cli.SetLocalID(raft.ServerID("client"))
	cli.AddPeer(raft.ServerID("server"), raft.ServerAddress(srv.ListenerAddr())) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Two sequential requests on the same pooled connection must each correlate.
	for i := 0; i < 2; i++ {
		resp, err := cli.RequestVote(ctx, raft.ServerID("server"), &raft.RequestVoteRequest{Term: uint64(i + 1)})
		if err != nil {
			t.Fatalf("RequestVote #%d: %v", i, err)
		}
		if !resp.VoteGranted {
			t.Errorf("RequestVote #%d: vote not granted", i)
		}
	}
}

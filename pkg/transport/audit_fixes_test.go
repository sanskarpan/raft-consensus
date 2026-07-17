package transport_test

// Regression tests for the pkg/transport production-readiness audit fixes:
//
//	H-S1 — per-RPC peer authorization (gRPC interceptor + TCP CN check)
//	H-R5 — concurrent RPCs to one peer no longer serialize (TCP)
//	M-R1 — explicit gRPC max message sizes
//	M-R7 — configurable gRPC RPC timeouts
//	M-S2 — lowered TCP default max message size (still configurable)
//	M-S3 — TLS key files with group/other-readable perms are rejected
//	M-S4 — bounded InstallSnapshot reassembly on the gRPC server
//	M-C2 — data race on peer.conn between RemovePeer and sendRequest (TCP)
//	M-L1 — grpc.NewClient / insecure.NewCredentials (behavior preserved)

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"github.com/sanskarpan/raft-consensus/pkg/transport"
	proto "github.com/sanskarpan/raft-consensus/proto"
	"go.uber.org/zap"
)

// grpcHandler is a configurable transport.RaftHandler implemented against the
// proto types the gRPC server dispatches. Unset callbacks return empty responses.
type grpcHandler struct {
	onAppendEntries   func(*proto.AppendEntriesRequest) *proto.AppendEntriesResponse
	onInstallSnapshot func(*proto.InstallSnapshotRequest) *proto.InstallSnapshotResponse
}

func (h *grpcHandler) HandleRequestVote(req *proto.RequestVoteRequest) *proto.RequestVoteResponse {
	return &proto.RequestVoteResponse{Term: req.Term, VoteGranted: true}
}
func (h *grpcHandler) HandleAppendEntries(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
	if h.onAppendEntries != nil {
		return h.onAppendEntries(req)
	}
	return &proto.AppendEntriesResponse{Term: req.Term, Success: true}
}
func (h *grpcHandler) HandleInstallSnapshot(req *proto.InstallSnapshotRequest) *proto.InstallSnapshotResponse {
	if h.onInstallSnapshot != nil {
		return h.onInstallSnapshot(req)
	}
	return &proto.InstallSnapshotResponse{Term: req.Term}
}
func (h *grpcHandler) HandleTimeoutNow(req *proto.TimeoutNowRequest) *proto.TimeoutNowResponse {
	return &proto.TimeoutNowResponse{}
}

// issueCertCN signs a leaf cert with an explicit CommonName + matching SANs.
func issueCertCN(t *testing.T, ca *certAuthority, cn string, isServer bool) (certPEM, keyPEM []byte) {
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
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{extKeyUsage},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost", cn},
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

// ---------------------------------------------------------------------------
// H-S1 — gRPC per-RPC peer authorization
// ---------------------------------------------------------------------------

// TestGrpcRejectsNonMemberIdentity: a client with a valid cert (chains to the
// CA) whose CN is NOT in the allowed-member set is rejected by the server
// interceptor, while an allowed member succeeds.
func TestGrpcRejectsNonMemberIdentity(t *testing.T) {
	ca := newCA(t)

	serverCertPEM, serverKeyPEM := issueCertCN(t, ca, "node-1", true)
	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("server X509KeyPair: %v", err)
	}

	srv, err := transport.NewGrpcTransport(":0", zap.NewNop(), serverCert, ca.certPEM)
	if err != nil {
		t.Fatalf("NewGrpcTransport: %v", err)
	}
	defer srv.Close()
	srv.SetRaftHandler(&grpcHandler{})
	srv.SetAllowedMembers([]string{"node-1", "node-2"})

	addr := srv.ListenerAddr()

	dial := func(cn string) error {
		clientCertPEM, clientKeyPEM := issueCertCN(t, ca, cn, false)
		clientCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
		if err != nil {
			t.Fatalf("client X509KeyPair: %v", err)
		}
		cli, err := transport.NewGrpcTransport(":0", zap.NewNop(), clientCert, ca.certPEM)
		if err != nil {
			t.Fatalf("client transport: %v", err)
		}
		defer cli.Close()
		if err := cli.AddPeer(raft.ServerID("srv"), raft.ServerAddress(addr)); err != nil {
			t.Fatalf("AddPeer: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		_, err = cli.AppendEntries(ctx, raft.ServerID("srv"), &raft.AppendEntriesRequest{Term: 1})
		return err
	}

	if err := dial("node-2"); err != nil {
		t.Fatalf("allowed member node-2 rejected: %v", err)
	}
	if err := dial("intruder"); err == nil {
		t.Fatal("expected non-member identity to be rejected, got nil error")
	} else {
		t.Logf("correctly rejected non-member: %v", err)
	}
}

// ---------------------------------------------------------------------------
// H-S1 — TCP per-RPC peer authorization (verified CN check)
// ---------------------------------------------------------------------------

func TestTCPRejectsNonMemberIdentity(t *testing.T) {
	ca := newCA(t)
	serverCertPEM, serverKeyPEM := issueCertCN(t, ca, "node-1", true)

	dir := t.TempDir()
	writePEM(t, dir, "server.crt", serverCertPEM)
	writePEM(t, dir, "server.key", serverKeyPEM)
	writePEM(t, dir, "ca.crt", ca.certPEM)

	serverTLS, err := transport.LoadTLSConfig(&transport.TCPTLSConfig{
		CertFile: filepath.Join(dir, "server.crt"),
		KeyFile:  filepath.Join(dir, "server.key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	})
	if err != nil {
		t.Fatalf("LoadTLSConfig (server): %v", err)
	}

	srvHandler := &callbackHandler{
		onRequestVote: func(req *transport.RequestVoteReq) *transport.RequestVoteResp {
			return &transport.RequestVoteResp{VoteGranted: true}
		},
	}
	srv, err := transport.NewTCPTransportTLS(":0", srvHandler, 3*time.Second, zap.NewNop(), serverTLS)
	if err != nil {
		t.Fatalf("NewTCPTransportTLS: %v", err)
	}
	defer srv.Close()
	srv.SetAllowedMembers([]string{"node-1", "member"})

	addr := srv.ListenerAddr()

	client := func(cn string) error {
		clientCertPEM, clientKeyPEM := issueCertCN(t, ca, cn, false)
		writePEM(t, dir, cn+".crt", clientCertPEM)
		writePEM(t, dir, cn+".key", clientKeyPEM)
		clientTLS, err := transport.LoadTLSConfig(&transport.TCPTLSConfig{
			CertFile: filepath.Join(dir, cn+".crt"),
			KeyFile:  filepath.Join(dir, cn+".key"),
			CAFile:   filepath.Join(dir, "ca.crt"),
		})
		if err != nil {
			t.Fatalf("LoadTLSConfig (%s): %v", cn, err)
		}
		clientTLS.ServerName = "localhost"
		cli, err := transport.NewTCPTransportTLS(":0", &noopHandler{}, 3*time.Second, zap.NewNop(), clientTLS)
		if err != nil {
			t.Fatalf("NewTCPTransportTLS (%s): %v", cn, err)
		}
		defer cli.Close()
		cli.SetLocalID(raft.ServerID(cn))
		cli.AddPeer(raft.ServerID("server"), raft.ServerAddress(addr)) //nolint:errcheck
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err = cli.RequestVote(ctx, raft.ServerID("server"), &raft.RequestVoteRequest{Term: 1})
		return err
	}

	if err := client("member"); err != nil {
		t.Fatalf("allowed member rejected: %v", err)
	}
	if err := client("intruder"); err == nil {
		t.Fatal("expected non-member identity to be rejected over TCP, got nil")
	} else {
		t.Logf("correctly rejected non-member over TCP: %v", err)
	}
}

// ---------------------------------------------------------------------------
// H-R5 / M-C2 — concurrent RPCs to one peer + concurrent membership churn
// ---------------------------------------------------------------------------

// TestTCPConcurrentRPCsToOnePeer drives concurrent RequestVote calls to one
// peer and verifies (a) each response correlates to its request and (b) the
// server handles more than one at a time (no serialization). Run under -race.
func TestTCPConcurrentRPCsToOnePeer(t *testing.T) {
	var concurrent, maxConcurrent int32

	srvHandler := &callbackHandler{
		onRequestVote: func(req *transport.RequestVoteReq) *transport.RequestVoteResp {
			c := atomic.AddInt32(&concurrent, 1)
			for {
				m := atomic.LoadInt32(&maxConcurrent)
				if c <= m || atomic.CompareAndSwapInt32(&maxConcurrent, m, c) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&concurrent, -1)
			return &transport.RequestVoteResp{Term: req.Term, VoteGranted: true}
		},
	}
	srv, err := transport.NewTCPTransport(":0", srvHandler, 5*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport (server): %v", err)
	}
	defer srv.Close()

	cli, err := transport.NewTCPTransport(":0", &noopHandler{}, 5*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport (client): %v", err)
	}
	defer cli.Close()
	cli.SetLocalID(raft.ServerID("client"))
	cli.AddPeer(raft.ServerID("server"), raft.ServerAddress(srv.ListenerAddr())) //nolint:errcheck

	const n = 6
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(term uint64) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := cli.RequestVote(ctx, raft.ServerID("server"), &raft.RequestVoteRequest{Term: term})
			if err != nil {
				errs <- err
				return
			}
			if resp.Term != term {
				errs <- errors.New("response term mismatch (correlation error)")
			}
		}(uint64(i + 1))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent RequestVote failed: %v", err)
	}

	if got := atomic.LoadInt32(&maxConcurrent); got < 2 {
		t.Fatalf("expected concurrent handling (>1), max observed = %d; RPCs serialized", got)
	}
	t.Logf("max concurrent server-side handlers = %d", atomic.LoadInt32(&maxConcurrent))
}

// TestTCPConcurrentSendAndMembershipChurn exercises the M-C2 lock discipline:
// concurrent AppendEntries and RemovePeer/AddPeer on the same peer must not race
// on peer connection state. Meaningful under -race.
func TestTCPConcurrentSendAndMembershipChurn(t *testing.T) {
	srv, err := transport.NewTCPTransport(":0", &callbackHandler{}, 2*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport (server): %v", err)
	}
	defer srv.Close()

	cli, err := transport.NewTCPTransport(":0", &noopHandler{}, 2*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport (client): %v", err)
	}
	defer cli.Close()
	cli.SetLocalID(raft.ServerID("client"))
	addr := raft.ServerAddress(srv.ListenerAddr())
	cli.AddPeer(raft.ServerID("server"), addr) //nolint:errcheck

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				cli.AppendEntries(ctx, raft.ServerID("server"), &raft.AppendEntriesRequest{Term: 1}) //nolint:errcheck
				cancel()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			cli.RemovePeer(raft.ServerID("server"))
			cli.AddPeer(raft.ServerID("server"), addr) //nolint:errcheck
			time.Sleep(time.Millisecond)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// ---------------------------------------------------------------------------
// M-R1 / M-R7 — configurable gRPC message size and timeouts
// ---------------------------------------------------------------------------

// TestGrpcLargeAppendEntriesRoundTrips: a >4 MiB AppendEntries (rejected by the
// stock 4 MiB default) round-trips once explicit message sizes are set (M-R1).
func TestGrpcLargeAppendEntriesRoundTrips(t *testing.T) {
	var gotBytes int
	handler := &grpcHandler{
		onAppendEntries: func(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
			for _, e := range req.Entries {
				gotBytes += len(e.Data)
			}
			return &proto.AppendEntriesResponse{Term: req.Term, Success: true}
		},
	}

	srv, err := transport.NewGrpcTransportInsecure(":0", zap.NewNop())
	if err != nil {
		t.Fatalf("NewGrpcTransportInsecure (server): %v", err)
	}
	defer srv.Close()
	srv.SetRaftHandler(handler)

	cli, err := transport.NewGrpcTransportInsecure(":0", zap.NewNop())
	if err != nil {
		t.Fatalf("NewGrpcTransportInsecure (client): %v", err)
	}
	defer cli.Close()
	if err := cli.AddPeer(raft.ServerID("server"), raft.ServerAddress(srv.ListenerAddr())); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	payload := make([]byte, 6<<20) // 6 MiB
	req := &raft.AppendEntriesRequest{
		Term:    1,
		Entries: []*raft.LogEntry{{Term: 1, Index: 1, Data: payload}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cli.AppendEntries(ctx, raft.ServerID("server"), req)
	if err != nil {
		t.Fatalf("large AppendEntries failed (M-R1 not applied?): %v", err)
	}
	if !resp.Success {
		t.Fatal("expected success")
	}
	if gotBytes != len(payload) {
		t.Fatalf("server received %d bytes, want %d", gotBytes, len(payload))
	}
}

// TestGrpcConfigurableTimeout: a tiny AppendEntries timeout against an
// unreachable peer fails quickly rather than waiting the 10s default (M-R7).
func TestGrpcConfigurableTimeout(t *testing.T) {
	cli, err := transport.NewGrpcTransportInsecure(":0", zap.NewNop())
	if err != nil {
		t.Fatalf("NewGrpcTransportInsecure: %v", err)
	}
	defer cli.Close()
	cli.SetRPCTimeouts(150*time.Millisecond, time.Second)

	if err := cli.AddPeer(raft.ServerID("dead"), raft.ServerAddress("10.255.255.1:65000")); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	start := time.Now()
	_, err = cli.AppendEntries(context.Background(), raft.ServerID("dead"), &raft.AppendEntriesRequest{Term: 1})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a timeout error dialing a dead peer")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("RPC took %v; SetRPCTimeouts(150ms) not honored", elapsed)
	}
	t.Logf("timed out in %v: %v", elapsed, err)
}

// ---------------------------------------------------------------------------
// M-S2 — TCP default max message size lowered but configurable
// ---------------------------------------------------------------------------

func TestTCPDefaultMaxMessageBytesLowered(t *testing.T) {
	srv, err := transport.NewTCPTransport(":0", &noopHandler{}, 3*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer srv.Close()

	// Default cap must reject a 32 MiB frame (old default was 128 MiB).
	if !tcpMessageRefused(t, srv.ListenerAddr(), 32<<20) {
		t.Fatal("expected 32 MiB message refused under lowered default (M-S2)")
	}

	// Raising the cap makes the same size acceptable again (configurable).
	srv.SetMaxMessageBytes(64 << 20)
	if tcpMessageRefused(t, srv.ListenerAddr(), 32<<20) {
		t.Fatal("expected 32 MiB message accepted after SetMaxMessageBytes(64MiB)")
	}
}

// tcpMessageRefused writes a valid-JSON message of roughly size bytes and
// reports whether the server closed the connection without a response.
func tcpMessageRefused(t *testing.T, addr string, size int) bool {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	blob := make([]byte, size)
	for i := range blob {
		blob[i] = 'a'
	}
	conn.SetWriteDeadline(time.Now().Add(20 * time.Second)) //nolint:errcheck
	head := []byte(`{"id":1,"type":"AppendEntries","payload":{"leader_id":"`)
	tail := []byte(`"}}` + "\n")
	conn.Write(head) //nolint:errcheck
	conn.Write(blob) //nolint:errcheck
	_, werr := conn.Write(tail)

	conn.SetReadDeadline(time.Now().Add(20 * time.Second)) //nolint:errcheck
	buf := make([]byte, 256)
	_, rerr := conn.Read(buf)
	return rerr != nil || werr != nil
}

// ---------------------------------------------------------------------------
// M-S3 — TLS key permission check
// ---------------------------------------------------------------------------

func TestLoadTLSConfigRejectsWorldReadableKey(t *testing.T) {
	ca := newCA(t)
	certPEM, keyPEM := issueCert(t, ca, true)

	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	caFile := filepath.Join(dir, "ca.crt")
	writePEM(t, dir, "server.crt", certPEM)
	writePEM(t, dir, "ca.crt", ca.certPEM)
	// Deliberately world/group-readable key (0644).
	if err := os.WriteFile(keyFile, keyPEM, 0644); err != nil {
		t.Fatalf("write key: %v", err)
	}

	_, err := transport.LoadTLSConfig(&transport.TCPTLSConfig{
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	})
	if err == nil {
		t.Fatal("expected LoadTLSConfig to reject a 0644 key file (M-S3)")
	}
	t.Logf("TCP correctly rejected insecure key: %v", err)

	// gRPC SetTLS must reject it too.
	tr, err := transport.NewGrpcTransportInsecure(":0", zap.NewNop())
	if err != nil {
		t.Fatalf("NewGrpcTransportInsecure: %v", err)
	}
	defer tr.Close()
	if gerr := tr.SetTLS(&transport.GRPCTLSConfig{
		CACert:     caFile,
		ServerCert: certFile,
		ServerKey:  keyFile,
	}); gerr == nil {
		t.Fatal("expected SetTLS to reject a 0644 key file (M-S3)")
	} else {
		t.Logf("gRPC correctly rejected insecure key: %v", gerr)
	}

	// Tightening perms to 0600 must make LoadTLSConfig succeed again.
	if err := os.Chmod(keyFile, 0600); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if _, err := transport.LoadTLSConfig(&transport.TCPTLSConfig{
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	}); err != nil {
		t.Fatalf("LoadTLSConfig with 0600 key should succeed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// M-S4 — bounded InstallSnapshot reassembly (gRPC)
// ---------------------------------------------------------------------------

func TestGrpcInstallSnapshotAggregateBound(t *testing.T) {
	installed := false
	handler := &grpcHandler{
		onInstallSnapshot: func(req *proto.InstallSnapshotRequest) *proto.InstallSnapshotResponse {
			installed = true
			return &proto.InstallSnapshotResponse{Term: req.Term}
		},
	}

	srv, err := transport.NewGrpcTransportInsecure(":0", zap.NewNop())
	if err != nil {
		t.Fatalf("NewGrpcTransportInsecure (server): %v", err)
	}
	defer srv.Close()
	srv.SetRaftHandler(handler)
	srv.SetMaxSnapshotBytes(1 << 20) // 1 MiB cap

	cli, err := transport.NewGrpcTransportInsecure(":0", zap.NewNop())
	if err != nil {
		t.Fatalf("NewGrpcTransportInsecure (client): %v", err)
	}
	defer cli.Close()
	if err := cli.AddPeer(raft.ServerID("server"), raft.ServerAddress(srv.ListenerAddr())); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	req := &raft.InstallSnapshotRequest{
		Term: 1,
		Data: make([]byte, 2<<20), // 2 MiB, over the 1 MiB cap
		Done: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.InstallSnapshot(ctx, raft.ServerID("server"), req); err == nil {
		t.Fatal("expected oversized snapshot to be rejected (M-S4)")
	} else {
		t.Logf("correctly rejected oversized snapshot: %v", err)
	}
	if installed {
		t.Fatal("handler must not be invoked for an over-cap snapshot")
	}
}

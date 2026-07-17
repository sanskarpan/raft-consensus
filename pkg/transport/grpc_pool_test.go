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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// selfSignedCert generates a minimal self-signed TLS certificate for tests
// that need to call NewGrpcTransport.
func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	return cert
}

// ---------------------------------------------------------------------------
// connPool unit tests
// ---------------------------------------------------------------------------

// buildFakePool creates a connPool whose entries are *grpc.ClientConn values
// pointed at a blackhole address with insecure credentials so they are non-nil
// but never actually connected. We only need the pointer identity for the
// round-robin tests; no actual RPC is performed.
func buildFakePool(t *testing.T, n int) *connPool {
	t.Helper()
	conns := make([]*grpc.ClientConn, n)
	for i := 0; i < n; i++ {
		// M-L1: grpc.NewClient (lazy) replaces the deprecated grpc.Dial; the
		// connection attempt happens lazily so this always succeeds immediately.
		c, err := grpc.NewClient("localhost:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		conns[i] = c
	}
	return newConnPool(conns)
}

func TestConnPool_Len(t *testing.T) {
	for _, n := range []int{1, 2, 4, 8} {
		p := buildFakePool(t, n)
		defer p.Close()
		if got := p.Len(); got != n {
			t.Errorf("Len() = %d, want %d", got, n)
		}
	}
}

func TestConnPool_Get_RoundRobin(t *testing.T) {
	const size = 4
	p := buildFakePool(t, size)
	defer p.Close()

	// Track how many times each connection index is returned.
	// Because the counter starts at 0 and the first Add returns 1, the first
	// call returns conns[1 % 4] = conns[1], cycling through 1,2,3,0,1,...
	// What matters is that every slot is hit exactly once per full cycle.
	counts := make([]int, size)
	for i := 0; i < size*3; i++ {
		c := p.Get()
		// Find which index this pointer corresponds to.
		found := false
		for idx, orig := range p.conns {
			if orig == c {
				counts[idx]++
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Get() returned an unknown connection pointer")
		}
	}
	// Each slot should be visited exactly 3 times over 3 full cycles.
	for idx, cnt := range counts {
		if cnt != 3 {
			t.Errorf("conn[%d] used %d times, want 3", idx, cnt)
		}
	}
}

func TestConnPool_Get_Concurrent(t *testing.T) {
	const (
		size       = 4
		goroutines = 16
		callsEach  = 100
	)
	p := buildFakePool(t, size)
	defer p.Close()

	var wg sync.WaitGroup
	var unknown int64 // incremented if an unknown pointer is returned
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < callsEach; i++ {
				c := p.Get()
				found := false
				for _, orig := range p.conns {
					if orig == c {
						found = true
						break
					}
				}
				if !found {
					atomic.AddInt64(&unknown, 1)
				}
			}
		}()
	}
	wg.Wait()
	if unknown != 0 {
		t.Errorf("Get() returned %d unknown pointers under concurrent load", unknown)
	}
}

func TestConnPool_Close(t *testing.T) {
	// Just verify Close() does not panic; the underlying grpc.ClientConn.Close
	// is idempotent.
	p := buildFakePool(t, 4)
	p.Close() // must not panic
}

// ---------------------------------------------------------------------------
// GrpcTransport.PoolSize
// ---------------------------------------------------------------------------

func TestGrpcTransport_PoolSize(t *testing.T) {
	// We cannot call NewGrpcTransport without real TLS material, so we
	// construct a minimal GrpcTransport by hand and call PoolSize().
	tr := &GrpcTransport{}
	if got := tr.PoolSize(); got != defaultConnPoolSize {
		t.Errorf("PoolSize() = %d, want %d", got, defaultConnPoolSize)
	}
}

// ---------------------------------------------------------------------------
// defaultConnPoolSize constant
// ---------------------------------------------------------------------------

func TestDefaultConnPoolSize(t *testing.T) {
	if defaultConnPoolSize != 4 {
		t.Errorf("defaultConnPoolSize = %d, want 4", defaultConnPoolSize)
	}
}

// ---------------------------------------------------------------------------
// peerConn wiring
// ---------------------------------------------------------------------------

func TestPeerConn_UsesPool(t *testing.T) {
	p := buildFakePool(t, defaultConnPoolSize)
	defer p.Close()

	pc := newPeerConn("127.0.0.1:9999", 3, p)
	if pc.pool == nil {
		t.Fatal("peerConn.pool is nil after newPeerConn")
	}
	if pc.pool.Len() != defaultConnPoolSize {
		t.Errorf("pool Len = %d, want %d", pc.pool.Len(), defaultConnPoolSize)
	}
	if pc.maxFailures != 3 {
		t.Errorf("maxFailures = %d, want 3", pc.maxFailures)
	}
	if pc.addr != "127.0.0.1:9999" {
		t.Errorf("addr = %q, want %q", pc.addr, "127.0.0.1:9999")
	}
}

func TestPeerConn_ShouldReconnect_FailCount(t *testing.T) {
	p := buildFakePool(t, 1)
	defer p.Close()

	pc := newPeerConn("addr", 3, p)
	if pc.shouldReconnect() {
		t.Error("shouldReconnect() = true on a fresh peerConn, want false")
	}
	// Accumulate failures up to the threshold.
	for i := 0; i < 3; i++ {
		pc.recordFailure()
	}
	if !pc.shouldReconnect() {
		t.Error("shouldReconnect() = false after maxFailures, want true")
	}
}

func TestPeerConn_RecordSuccess_ClearsFailures(t *testing.T) {
	p := buildFakePool(t, 1)
	defer p.Close()

	pc := newPeerConn("addr", 3, p)
	pc.recordFailure()
	pc.recordFailure()
	pc.recordSuccess()
	if pc.shouldReconnect() {
		t.Error("shouldReconnect() = true after recordSuccess, want false")
	}
}

func TestPeerConn_IsHealthy(t *testing.T) {
	p := buildFakePool(t, 1)
	defer p.Close()

	pc := newPeerConn("addr", 2, p)
	if !pc.isHealthy() {
		t.Error("isHealthy() = false on fresh peerConn, want true")
	}
	pc.recordFailure()
	pc.recordFailure()
	if pc.isHealthy() {
		t.Error("isHealthy() = true after maxFailures, want false")
	}
}

// ---------------------------------------------------------------------------
// Auto-reconnect loop
// ---------------------------------------------------------------------------

// TestGrpcTransport_ReconnectStalepeers_ReplacesPool verifies that after a peer
// accumulates enough failures to trigger shouldReconnect, reconnectStalepeers
// replaces the pool so that subsequent calls use a fresh set of connections.
func TestGrpcTransport_ReconnectStalepeers_ReplacesPool(t *testing.T) {
	// We cannot call NewGrpcTransport without real TLS, so we build a minimal
	// transport by hand (same pattern as TestGrpcTransport_PoolSize).
	tr := &GrpcTransport{
		peers:      make(map[raft.ServerID]*peerConn),
		shutdownCh: make(chan struct{}),
	}

	// Install a fake peerConn that is already past the failure threshold.
	oldPool := buildFakePool(t, defaultConnPoolSize)
	pc := newPeerConn("127.0.0.1:1", 3, oldPool)
	// Record enough failures to trigger shouldReconnect.
	pc.recordFailure()
	pc.recordFailure()
	pc.recordFailure()
	if !pc.shouldReconnect() {
		t.Fatal("precondition: shouldReconnect should be true after maxFailures")
	}

	peerID := raft.ServerID("peer1")
	tr.peers[peerID] = pc

	// reconnectStalepeers will try to re-dial.  "127.0.0.1:1" is a blackhole
	// but grpc.Dial is non-blocking (lazy connect), so it will succeed at dial
	// time and the pool entry will be replaced.
	tr.reconnectStalepeers()

	tr.mu.RLock()
	newPC, ok := tr.peers[peerID]
	tr.mu.RUnlock()

	if !ok {
		t.Fatal("peer was removed instead of reconnected")
	}
	if newPC == pc {
		t.Fatal("peerConn was not replaced after reconnect")
	}
	if newPC.pool == oldPool {
		t.Fatal("pool was not replaced after reconnect")
	}
	// The new peerConn should start healthy (failCount reset).
	if !newPC.isHealthy() {
		t.Error("reconnected peerConn is not healthy")
	}

	// Clean up.
	newPC.pool.Close()
	oldPool.Close()
}

// TestGrpcTransport_ReconnectLoop_ExitsOnClose verifies that the background
// reconnect goroutine started by NewGrpcTransport exits cleanly when Close()
// is called (i.e. shutdownCh is closed) and does not cause Close() to hang.
func TestGrpcTransport_ReconnectLoop_ExitsOnClose(t *testing.T) {
	cert := selfSignedCert(t)
	tr, err := NewGrpcTransport(":0", zap.NewNop(), cert, nil)
	if err != nil {
		t.Fatalf("NewGrpcTransport: %v", err)
	}

	done := make(chan struct{})
	go func() {
		tr.Close()
		close(done)
	}()
	select {
	case <-done:
		// Good — Close() returned promptly.
	case <-time.After(5 * time.Second):
		t.Fatal("Close() hung — reconnect goroutine may not have exited on shutdown")
	}
}

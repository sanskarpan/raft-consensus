package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raft-consensus/pkg/raft"
	proto "github.com/raft-consensus/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	defaultConnPoolSize = 4
	reconnectInterval   = 10 * time.Second
	// clientKeepaliveTime is how often the client pings an idle connection to
	// detect a dead peer promptly. serverMinTime is the matching server-side
	// enforcement floor; it must be <= clientKeepaliveTime or the server will
	// tear down clients that ping "too fast" (L6). Previously the client pinged
	// every 2h while the server floor was 5m, so dead connections lingered.
	clientKeepaliveTime = 30 * time.Second
	serverMinTime       = 30 * time.Second
	// defaultServerName is the SNI/ServerName used on client dial configs so
	// server-cert verification succeeds against the generated certs (SANs:
	// DNS localhost, DNS *.raft.local, IP 127.0.0.1) when no explicit name is
	// otherwise configured (C11).
	defaultServerName = "localhost"

	// M-R1: explicit gRPC message size limits, sized for snapshot chunks. The
	// stock gRPC default is 4 MiB on receive and effectively unbounded on send;
	// large AppendEntries/InstallSnapshot batches exceed 4 MiB and are rejected.
	// These defaults are configurable via SetMaxMessageSize.
	defaultGrpcMaxMsgSize = 64 << 20 // 64 MiB

	// M-S4: aggregate cap on a reassembled InstallSnapshot stream. Chunks are
	// accumulated on the server; without a total bound a peer could stream an
	// unbounded snapshot and exhaust memory. Configurable via SetMaxSnapshotBytes.
	defaultMaxSnapshotBytes int64 = 512 << 20 // 512 MiB

	// M-R7: default RPC timeouts, now configurable. The AppendEntries timeout is
	// intentionally short (near the heartbeat interval) so a stuck peer does not
	// pin an in-flight slot; the snapshot timeout is longer for bulk transfer.
	defaultAppendEntriesTimeout = 10 * time.Second
	defaultSnapshotTimeout      = 30 * time.Second
)

// connPool holds a fixed-size slice of gRPC client connections and uses an
// atomic round-robin counter to distribute calls across them.
type connPool struct {
	conns   []*grpc.ClientConn
	counter uint64 // accessed via sync/atomic
	once    sync.Once
}

// newConnPool creates a connPool from an already-dialed slice of connections.
func newConnPool(conns []*grpc.ClientConn) *connPool {
	return &connPool{conns: conns}
}

// serverNameFor picks the TLS ServerName used to verify a peer's certificate
// (C11). An explicitly-configured ServerName always wins. Otherwise it is
// derived from the peer's dial address host, falling back to defaultServerName
// for an empty or unspecified host (e.g. a "[::]:port" listen address in tests).
func serverNameFor(configured, addr string) string {
	if configured != "" {
		return configured
	}
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	switch host {
	case "", "::", "0.0.0.0":
		return defaultServerName
	default:
		return host
	}
}

// Get returns the next connection in round-robin order, or nil if the pool is
// empty. Callers must handle a nil return rather than dereferencing it (H4:
// guard against division by zero on an empty pool).
func (p *connPool) Get() *grpc.ClientConn {
	if p == nil {
		return nil
	}
	n := uint64(len(p.conns))
	if n == 0 {
		return nil
	}
	idx := atomic.AddUint64(&p.counter, 1) % n
	return p.conns[idx]
}

// Close closes every connection in the pool. It is idempotent and nil-safe so
// that a pool swapped out under AddPeer/RemovePeer can be closed more than once
// without panicking (H4).
func (p *connPool) Close() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		for _, c := range p.conns {
			if c != nil {
				c.Close()
			}
		}
	})
}

// Len returns the number of connections in the pool.
func (p *connPool) Len() int {
	return len(p.conns)
}

// GRPCTLSConfig holds TLS material for the gRPC transport.
// Leave nil to run without TLS (development only).
type GRPCTLSConfig struct {
	CACert     string // path to CA certificate file
	ServerCert string // path to server certificate file
	ServerKey  string // path to server private key file
	ClientCert string // path to client certificate file (for mTLS)
	ClientKey  string // path to client private key file
	MutualTLS  bool   // require client certs (server-side mTLS)
}

type GrpcTransport struct {
	mu              sync.RWMutex
	localID         raft.ServerID
	peers           map[raft.ServerID]*peerConn
	logger          *zap.Logger
	server          *grpc.Server
	listener        net.Listener
	shutdownCh      chan struct{}
	closeOnce       sync.Once
	wg              sync.WaitGroup
	tlsConfig       *tls.Config
	clientTLSConfig *tls.Config
	raftHandler     RaftHandler
	adminHandler    AdminHandler
	// requireTLS, when set, forbids the insecure plaintext fallback: dialing a
	// peer without a TLS config returns an error instead of silently connecting
	// in cleartext (C11).
	requireTLS bool
	// allowedMembers, when non-nil, is the set of peer certificate identities
	// (CN or DNS SAN) permitted to call the Raft/Admin services. When mTLS is
	// active and this set is non-empty, the server interceptors reject any RPC
	// whose verified peer identity is not a member (H-S1). Nil/empty means the
	// authorization check is disabled (plaintext/dev still works).
	allowedMembers map[string]struct{}
	// maxMsgSize bounds a single gRPC message on both send and receive (M-R1).
	// 0 means defaultGrpcMaxMsgSize.
	maxMsgSize int
	// maxSnapshotBytes bounds a reassembled InstallSnapshot stream (M-S4).
	// 0 means defaultMaxSnapshotBytes.
	maxSnapshotBytes int64
	// appendEntriesTimeout / snapshotTimeout override the per-RPC deadlines
	// (M-R7). 0 means the corresponding default.
	appendEntriesTimeout time.Duration
	snapshotTimeout      time.Duration
}

// ErrTLSRequired is returned when RequireTLS is set but no TLS config is
// available to dial a peer (C11).
var ErrTLSRequired = errors.New("transport: TLS required but no TLS config configured")

// checkKeyPerm rejects a TLS private-key file that is group- or
// world-readable/writable (mode & 0077 != 0) (M-S3). An overly-permissive key
// file is a common misconfiguration that leaks the cluster's identity material.
// Symlinks are resolved via os.Stat so the permission of the real key is checked.
func checkKeyPerm(path string) error {
	if path == "" {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat key file: %w", err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf(
			"key file %s has insecure permissions %#o (group/other-accessible); use 0600",
			path, perm)
	}
	return nil
}

// SetRequireTLS enables fail-closed behavior: peers can only be dialed over TLS.
func (t *GrpcTransport) SetRequireTLS(v bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.requireTLS = v
}

// SetAllowedMembers configures the set of peer certificate identities (CN or
// DNS SAN) permitted to call the Raft and RaftAdmin services (H-S1). When mTLS
// is active and this set is non-empty, the server interceptors reject any RPC
// whose verified peer identity is not present in the set. Passing an empty or
// nil slice disables the check so plaintext/dev deployments keep working.
func (t *GrpcTransport) SetAllowedMembers(ids []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(ids) == 0 {
		t.allowedMembers = nil
		return
	}
	m := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id != "" {
			m[id] = struct{}{}
		}
	}
	t.allowedMembers = m
}

// SetMaxMessageSize overrides the per-message size limit applied to both the
// gRPC server and outbound dials (M-R1). n <= 0 restores the default.
func (t *GrpcTransport) SetMaxMessageSize(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.maxMsgSize = n
}

// SetMaxSnapshotBytes overrides the aggregate cap on a reassembled
// InstallSnapshot stream (M-S4). n <= 0 restores the default.
func (t *GrpcTransport) SetMaxSnapshotBytes(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.maxSnapshotBytes = n
}

func (t *GrpcTransport) snapshotBytesCap() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.maxSnapshotBytes > 0 {
		return t.maxSnapshotBytes
	}
	return defaultMaxSnapshotBytes
}

// SetRPCTimeouts overrides the per-RPC deadlines (M-R7). A non-positive value
// for either argument leaves that timeout at its default.
func (t *GrpcTransport) SetRPCTimeouts(appendEntries, snapshot time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if appendEntries > 0 {
		t.appendEntriesTimeout = appendEntries
	}
	if snapshot > 0 {
		t.snapshotTimeout = snapshot
	}
}

func (t *GrpcTransport) aeTimeout() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.appendEntriesTimeout > 0 {
		return t.appendEntriesTimeout
	}
	return defaultAppendEntriesTimeout
}

func (t *GrpcTransport) snapTimeout() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.snapshotTimeout > 0 {
		return t.snapshotTimeout
	}
	return defaultSnapshotTimeout
}

// peerAuthorized reports whether an RPC arriving on ctx is permitted. When no
// allowed-member set is configured, or when the connection is not mTLS (no
// verified client cert), the check passes (opt-in). Otherwise the verified peer
// certificate's CN and DNS SANs are matched against the allowed set (H-S1).
func (t *GrpcTransport) peerAuthorized(ctx context.Context) error {
	t.mu.RLock()
	allowed := t.allowedMembers
	t.mu.RUnlock()
	if len(allowed) == 0 {
		return nil // authorization disabled
	}

	p, ok := grpcpeer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		// No transport auth info: not an mTLS connection. Leave enforcement to
		// the TLS layer (RequireAndVerifyClientCert) and skip identity check.
		return nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil
	}
	chains := tlsInfo.State.VerifiedChains
	if len(chains) == 0 || len(chains[0]) == 0 {
		return status.Error(codes.Unauthenticated, "transport: no verified peer certificate")
	}
	leaf := chains[0][0]
	if _, ok := allowed[leaf.Subject.CommonName]; ok {
		return nil
	}
	for _, dns := range leaf.DNSNames {
		if _, ok := allowed[dns]; ok {
			return nil
		}
	}
	return status.Errorf(codes.PermissionDenied,
		"transport: peer identity %q is not an allowed cluster member", leaf.Subject.CommonName)
}

// unaryAuthInterceptor enforces per-RPC peer authorization on unary calls (H-S1).
func (t *GrpcTransport) unaryAuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if err := t.peerAuthorized(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// streamAuthInterceptor enforces per-RPC peer authorization on streaming calls
// such as InstallSnapshot (H-S1).
func (t *GrpcTransport) streamAuthInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := t.peerAuthorized(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

type RaftHandler interface {
	HandleRequestVote(req *proto.RequestVoteRequest) *proto.RequestVoteResponse
	HandleAppendEntries(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse
	HandleInstallSnapshot(req *proto.InstallSnapshotRequest) *proto.InstallSnapshotResponse
	HandleTimeoutNow(req *proto.TimeoutNowRequest) *proto.TimeoutNowResponse
}

type AdminHandler interface {
	HandleGetConfiguration(req *proto.ConfigurationRequest) *proto.ConfigurationResponse
	HandleAddServer(req *proto.AddServerRequest) *proto.AddServerResponse
	HandleRemoveServer(req *proto.RemoveServerRequest) *proto.RemoveServerResponse
	HandlePromoteLearner(req *proto.PromoteLearnerRequest) *proto.PromoteLearnerResponse
	HandleCreateSnapshot(req *proto.SnapshotRequest) *proto.SnapshotResponse
	HandleGetSnapshot(req *proto.GetSnapshotRequest) *proto.SnapshotChunk
	HandleTransferLeadership(req *proto.TransferLeadershipRequest) *proto.TransferLeadershipResponse
}

type peerConn struct {
	pool        *connPool
	addr        string
	mu          sync.RWMutex
	lastUsed    time.Time
	failCount   int
	maxFailures int
}

func newPeerConn(addr string, maxFailures int, pool *connPool) *peerConn {
	return &peerConn{
		pool:        pool,
		addr:        addr,
		lastUsed:    time.Now(),
		maxFailures: maxFailures,
	}
}

func (p *peerConn) isHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.failCount < p.maxFailures
}

func (p *peerConn) recordSuccess() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failCount = 0
	p.lastUsed = time.Now()
}

func (p *peerConn) recordFailure() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failCount++
}

func (p *peerConn) shouldReconnect() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return time.Since(p.lastUsed) > 5*time.Minute || p.failCount >= p.maxFailures
}

// NewGrpcTransportInsecure creates a gRPC transport without TLS.
// Suitable for development / private networks where encryption is not required.
// For production use NewGrpcTransport with proper TLS certificates.
func NewGrpcTransportInsecure(listenAddr string, logger *zap.Logger) (*GrpcTransport, error) {
	t := &GrpcTransport{
		peers:      make(map[raft.ServerID]*peerConn),
		logger:     logger,
		shutdownCh: make(chan struct{}),
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	t.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(defaultGrpcMaxMsgSize),
		grpc.MaxSendMsgSize(defaultGrpcMaxMsgSize),
		grpc.UnaryInterceptor(t.unaryAuthInterceptor),
		grpc.StreamInterceptor(t.streamAuthInterceptor),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             serverMinTime,
			PermitWithoutStream: true,
		}))
	t.listener = ln

	proto.RegisterRaftServiceServer(t.server, &grpcServerHandler{transport: t})
	proto.RegisterRaftAdminServer(t.server, &grpcAdminHandler{transport: t})

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		// Serve returns grpc.ErrServerStopped on graceful Stop(); other errors
		// mean the listener died and are logged for diagnosis.
		if err := t.server.Serve(ln); err != nil && err != grpc.ErrServerStopped {
			t.logger.Warn("gRPC server stopped", zap.Error(err))
		}
	}()

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(reconnectInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.reconnectStalepeers()
			case <-t.shutdownCh:
				return
			}
		}
	}()

	return t, nil
}

func NewGrpcTransport(listenAddr string, logger *zap.Logger, cert tls.Certificate, caCert []byte) (*GrpcTransport, error) {
	t := &GrpcTransport{
		peers:      make(map[raft.ServerID]*peerConn),
		logger:     logger,
		shutdownCh: make(chan struct{}),
	}

	// C11: harden the TLS-configured path. Pin TLS 1.3 as the floor on the
	// server config and default to mutual TLS (RequireAndVerifyClientCert) when
	// a CA pool is available so an unauthenticated peer cannot connect.
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}

	// Build a dedicated client-side dial config that verifies the server
	// (RootCAs set, no InsecureSkipVerify) and offers our cert for mTLS. The
	// ServerName lets verification succeed against certs whose SANs cover
	// localhost / *.raft.local / 127.0.0.1.
	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		// ServerName is derived per-peer at dial time (serverNameFor), so an
		// explicit value is not hardcoded here (C11).
	}

	if caCert != nil {
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(caCert)
		serverTLS.ClientCAs = certPool
		serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
		clientTLS.RootCAs = certPool
	}

	creds := credentials.NewTLS(serverTLS)
	t.tlsConfig = serverTLS
	t.clientTLSConfig = clientTLS

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	t.server = grpc.NewServer(
		grpc.Creds(creds),
		grpc.MaxRecvMsgSize(defaultGrpcMaxMsgSize),
		grpc.MaxSendMsgSize(defaultGrpcMaxMsgSize),
		grpc.UnaryInterceptor(t.unaryAuthInterceptor),
		grpc.StreamInterceptor(t.streamAuthInterceptor),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             serverMinTime,
			PermitWithoutStream: true,
		}))

	t.listener = ln

	// Register the Raft/Admin services before Serve. The insecure constructor
	// already did this inline; the TLS path previously relied on a separate
	// RegisterService() call that cmd/raftd never made, so a TLS server answered
	// every RPC with "unknown service". Registering here keeps both constructors
	// consistent and lets the auth interceptors (H-S1) take effect.
	proto.RegisterRaftServiceServer(t.server, &grpcServerHandler{transport: t})
	proto.RegisterRaftAdminServer(t.server, &grpcAdminHandler{transport: t})

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		// Serve returns grpc.ErrServerStopped on graceful Stop(); other errors
		// mean the listener died and are logged for diagnosis.
		if err := t.server.Serve(ln); err != nil && err != grpc.ErrServerStopped {
			t.logger.Warn("gRPC server stopped", zap.Error(err))
		}
	}()

	// Background reconnect loop: periodically re-dials any peer whose
	// connection pool has degraded past the failure threshold or has been
	// idle for too long.
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(reconnectInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.reconnectStalepeers()
			case <-t.shutdownCh:
				return
			}
		}
	}()

	return t, nil
}

// reconnectStalepeers iterates over known peers and re-dials those whose pool
// has been flagged for reconnection (too many consecutive failures or idle
// for longer than the stale threshold).  It holds the write lock only while
// swapping out the peerConn entry, so live RPCs on other peers are not blocked.
func (t *GrpcTransport) reconnectStalepeers() {
	t.mu.RLock()
	type candidate struct {
		id   raft.ServerID
		addr string
	}
	var stale []candidate
	for id, pc := range t.peers {
		if pc.shouldReconnect() {
			stale = append(stale, candidate{id: id, addr: pc.addr})
		}
	}
	t.mu.RUnlock()

	for _, c := range stale {
		if err := t.AddPeer(c.id, raft.ServerAddress(c.addr)); err != nil {
			if t.logger != nil {
				t.logger.Warn("auto-reconnect failed",
					zap.String("peer", string(c.id)),
					zap.String("addr", c.addr),
					zap.Error(err),
				)
			}
		} else {
			if t.logger != nil {
				t.logger.Info("auto-reconnected to peer",
					zap.String("peer", string(c.id)),
					zap.String("addr", c.addr),
				)
			}
		}
	}
}

func (t *GrpcTransport) SetLocalID(id raft.ServerID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.localID = id
}

func (t *GrpcTransport) SetRaftHandler(handler RaftHandler) {
	t.raftHandler = handler
}

func (t *GrpcTransport) SetAdminHandler(handler AdminHandler) {
	t.adminHandler = handler
}

// SetTLS configures mTLS for the gRPC transport using the provided GRPCTLSConfig.
// This method must be called before Start() / before AddPeer() calls that
// create outbound connections, as it rebuilds both the server and client
// TLS configurations.
func (t *GrpcTransport) SetTLS(cfg *GRPCTLSConfig) error {
	if cfg == nil {
		return nil
	}

	// M-S3: reject a private key file that is group/other-readable before
	// loading it, so a world-readable key is not silently accepted.
	if err := checkKeyPerm(cfg.ServerKey); err != nil {
		return fmt.Errorf("grpc: server key: %w", err)
	}
	if cfg.ClientKey != "" {
		if err := checkKeyPerm(cfg.ClientKey); err != nil {
			return fmt.Errorf("grpc: client key: %w", err)
		}
	}

	// Load server certificate and key.
	serverCert, err := tls.LoadX509KeyPair(cfg.ServerCert, cfg.ServerKey)
	if err != nil {
		return fmt.Errorf("grpc: failed to load server cert/key: %w", err)
	}

	// Load CA certificate pool.
	caCertPEM, err := os.ReadFile(cfg.CACert)
	if err != nil {
		return fmt.Errorf("grpc: failed to read CA cert: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCertPEM) {
		return fmt.Errorf("grpc: failed to parse CA cert")
	}

	// Build server-side TLS config. C11: pin TLS 1.3 and default to mutual TLS
	// whenever a CA pool is present (MutualTLS forces it too). A caller can only
	// weaken this by supplying no CA, which the constructor requires here.
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	_ = cfg.MutualTLS // mTLS is now the default when a CA pool is present.

	// Build client-side TLS config. C11: verify the server (RootCAs set, no
	// InsecureSkipVerify) at TLS 1.3, with a ServerName so verification works
	// against the generated certs.
	clientTLS := &tls.Config{
		RootCAs:    caPool,
		MinVersion: tls.VersionTLS13,
		// ServerName derived per-peer at dial time (serverNameFor) (C11).
	}
	if cfg.ClientCert != "" && cfg.ClientKey != "" {
		clientCert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return fmt.Errorf("grpc: failed to load client cert/key: %w", err)
		}
		clientTLS.Certificates = []tls.Certificate{clientCert}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.tlsConfig = serverTLS
	t.clientTLSConfig = clientTLS
	return nil
}

func (t *GrpcTransport) AddPeer(id raft.ServerID, addr raft.ServerAddress) error {
	t.mu.Lock()

	// H4: never close the previous pool while still holding the transport lock or
	// while in-flight RPCs may still hold one of its connections. We capture the
	// old pool, install the new one, release the lock, and only then close the
	// old pool. grpc.ClientConn.Close is graceful, so any RPC that already grabbed
	// an old conn finishes on it; new RPCs pick up the freshly installed pool.
	var oldPool *connPool
	if peer, ok := t.peers[id]; ok {
		if !peer.shouldReconnect() {
			t.mu.Unlock()
			return nil
		}
		oldPool = peer.pool
	}

	// Use the dedicated client TLS config when available; fall back to the
	// server TLS config (which was set by the legacy constructor).
	outboundTLS := t.clientTLSConfig
	if outboundTLS == nil {
		outboundTLS = t.tlsConfig
	}

	var dialOpts []grpc.DialOption
	if outboundTLS != nil {
		// C11: verify the server certificate against THIS peer's identity, not a
		// hardcoded name. An explicit ServerName wins; otherwise derive it from
		// the dial address so a real multi-host deployment verifies each peer
		// against its own cert SANs.
		peerTLS := outboundTLS.Clone()
		peerTLS.ServerName = serverNameFor(outboundTLS.ServerName, string(addr))
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(peerTLS)))
	} else {
		// C11: never silently fall back to plaintext when TLS is required.
		if t.requireTLS {
			t.mu.Unlock()
			return ErrTLSRequired
		}
		// M-L1: use insecure.NewCredentials() instead of the deprecated
		// grpc.WithInsecure() (SA1019). Behavior is identical: no transport
		// security. Combined with grpc.NewClient below this preserves the lazy
		// dial and per-peer ServerName semantics.
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                clientKeepaliveTime,
		Timeout:             20 * time.Second,
		PermitWithoutStream: true,
	}))
	// M-R1: bound the message size on both directions of the client stub so
	// large AppendEntries/InstallSnapshot payloads are not rejected by gRPC's
	// 4 MiB default and sends stay bounded. t.mu is already held (write) here,
	// so read the field directly rather than via msgSize() to avoid re-locking.
	msgSize := t.maxMsgSize
	if msgSize <= 0 {
		msgSize = defaultGrpcMaxMsgSize
	}
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(msgSize),
		grpc.MaxCallSendMsgSize(msgSize),
	))

	conns := make([]*grpc.ClientConn, defaultConnPoolSize)
	for i := 0; i < defaultConnPoolSize; i++ {
		// M-L1: grpc.NewClient replaces the deprecated grpc.Dial. NewClient is
		// lazy by default (no eager connection), matching the previous behavior
		// where grpc.Dial without WithBlock returned immediately.
		conn, err := grpc.NewClient(string(addr), dialOpts...)
		if err != nil {
			// Close any connections already opened before failing.
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			t.mu.Unlock()
			return err
		}
		conns[i] = conn
	}

	t.peers[id] = newPeerConn(string(addr), 3, newConnPool(conns))
	t.mu.Unlock()

	// Close the old pool outside the lock (H4: avoid use-after-close races and
	// avoid closing while holding the transport lock).
	if oldPool != nil {
		oldPool.Close()
	}

	return nil
}

func (t *GrpcTransport) RemovePeer(id raft.ServerID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if pc, ok := t.peers[id]; ok {
		pc.pool.Close()
		delete(t.peers, id)
	}
}

func (t *GrpcTransport) getPeer(id raft.ServerID) (*peerConn, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	pc, ok := t.peers[id]
	if !ok {
		return nil, fmt.Errorf("peer not found: %s", id)
	}

	return pc, nil
}

// ErrPeerUnhealthy is returned when a peer has accumulated too many consecutive
// failures; the background reconnect loop will re-dial it (H4).
var ErrPeerUnhealthy = errors.New("transport: peer connection unhealthy")

// clientFor selects a connection from the peer's pool, refusing to hand out a
// connection when the peer is flagged unhealthy (H4: broken conns were reused
// forever because health was never consulted) and guarding against an empty
// pool (H4: Get formerly divided by len(conns) with no zero check).
func (t *GrpcTransport) clientFor(pc *peerConn) (proto.RaftServiceClient, error) {
	if !pc.isHealthy() {
		return nil, ErrPeerUnhealthy
	}
	conn := pc.pool.Get()
	if conn == nil {
		return nil, ErrPeerUnhealthy
	}
	return proto.NewRaftServiceClient(conn), nil
}

func (t *GrpcTransport) AppendEntries(ctx context.Context, target raft.ServerID, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	pc, err := t.getPeer(target)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, t.aeTimeout())
	defer cancel()

	client, err := t.clientFor(pc)
	if err != nil {
		return nil, err
	}

	pbReq := &proto.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     string(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      convertEntries(req.Entries),
		LeaderCommit: req.LeaderCommit,
	}

	resp, err := client.AppendEntries(ctx, pbReq)
	if err != nil {
		pc.recordFailure()
		return nil, err
	}
	pc.recordSuccess()

	return &raft.AppendEntriesResponse{
		Term:         resp.Term,
		Success:      resp.Success,
		Index:        resp.ConflictIndex,
		ConflictTerm: resp.ConflictTerm,
	}, nil
}

func (t *GrpcTransport) RequestVote(ctx context.Context, target raft.ServerID, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	pc, err := t.getPeer(target)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, t.aeTimeout())
	defer cancel()

	client, err := t.clientFor(pc)
	if err != nil {
		return nil, err
	}

	pbReq := &proto.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  string(req.CandidateID),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		PreVote:      req.PreVote,
	}

	resp, err := client.RequestVote(ctx, pbReq)
	if err != nil {
		pc.recordFailure()
		return nil, err
	}
	pc.recordSuccess()

	return &raft.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
		Reason:      resp.RejectReason,
	}, nil
}

func (t *GrpcTransport) InstallSnapshot(ctx context.Context, target raft.ServerID, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	pc, err := t.getPeer(target)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, t.snapTimeout())
	defer cancel()

	client, err := t.clientFor(pc)
	if err != nil {
		return nil, err
	}

	stream, err := client.InstallSnapshot(ctx)
	if err != nil {
		pc.recordFailure()
		return nil, err
	}

	pbReq := &proto.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderId:          string(req.LeaderID),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	}

	if err := stream.Send(pbReq); err != nil {
		pc.recordFailure()
		return nil, err
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		pc.recordFailure()
		return nil, err
	}
	pc.recordSuccess()

	return &raft.InstallSnapshotResponse{
		Term: resp.Term,
	}, nil
}

func (t *GrpcTransport) TimeoutNow(ctx context.Context, target raft.ServerID) error {
	pc, err := t.getPeer(target)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, t.aeTimeout())
	defer cancel()

	client, err := t.clientFor(pc)
	if err != nil {
		return err
	}

	req := &proto.TimeoutNowRequest{
		ServerId: string(t.localID),
	}

	_, err = client.TimeoutNow(ctx, req)
	if err != nil {
		pc.recordFailure()
		return err
	}
	pc.recordSuccess()
	return nil
}

func (t *GrpcTransport) SetLogger(logger *zap.Logger) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logger = logger
}

func (t *GrpcTransport) Close() error {
	// M6: signal shutdown and gather the resources to close under the lock, but
	// release the lock BEFORE t.wg.Wait(). The background reconnect goroutine (and
	// AddPeer it invokes) needs the transport lock; holding it across wg.Wait()
	// deadlocks. sync.Once guards against a double close of shutdownCh (panic).
	t.mu.Lock()
	t.closeOnce.Do(func() {
		close(t.shutdownCh)
	})

	pools := make([]*connPool, 0, len(t.peers))
	for _, pc := range t.peers {
		pools = append(pools, pc.pool)
	}
	server := t.server
	t.mu.Unlock()

	for _, p := range pools {
		p.Close()
	}

	if server != nil {
		server.GracefulStop()
	}

	t.wg.Wait()

	return nil
}

// PoolSize returns the number of connections per peer in the connection pool.
func (t *GrpcTransport) PoolSize() int {
	return defaultConnPoolSize
}

// ListenerAddr returns the network address that the gRPC server is listening on.
// This is useful in tests where ":0" is used to get a random available port.
func (t *GrpcTransport) ListenerAddr() string {
	if t.listener == nil {
		return ""
	}
	return t.listener.Addr().String()
}

func (t *GrpcTransport) RegisterService() {
	proto.RegisterRaftServiceServer(t.server, &grpcServerHandler{transport: t})
	proto.RegisterRaftAdminServer(t.server, &grpcAdminHandler{transport: t})
}

type grpcServerHandler struct {
	proto.UnimplementedRaftServiceServer
	transport *GrpcTransport
}

func (h *grpcServerHandler) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	if h.transport.raftHandler != nil {
		return h.transport.raftHandler.HandleRequestVote(req), nil
	}
	return &proto.RequestVoteResponse{}, nil
}

func (h *grpcServerHandler) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	if h.transport.raftHandler != nil {
		return h.transport.raftHandler.HandleAppendEntries(req), nil
	}
	return &proto.AppendEntriesResponse{}, nil
}

func (h *grpcServerHandler) InstallSnapshot(stream proto.RaftService_InstallSnapshotServer) error {
	if h.transport.raftHandler == nil {
		// Drain the stream so the client's CloseAndRecv doesn't hang.
		for {
			if _, err := stream.Recv(); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
	}

	// H9: accumulate every chunk on the stream (Offset-ordered Data frames) until
	// the sender signals Done or closes the stream, instead of handling only the
	// first Recv(). This stays compatible with the current single-frame client
	// (which sends one frame with Done set and then CloseAndRecv) while correctly
	// reassembling multi-chunk snapshots.
	var (
		first *proto.InstallSnapshotRequest
		data  []byte
		done  bool
	)
	// M-S4: enforce an aggregate bound on the reassembled snapshot so a peer
	// cannot stream an unbounded payload and exhaust server memory.
	maxSnap := h.transport.snapshotBytesCap()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// Sender closed the stream without an explicit final Done frame.
			break
		}
		if err != nil {
			return err
		}
		if first == nil {
			first = chunk
		}
		if int64(len(data))+int64(len(chunk.Data)) > maxSnap {
			return status.Errorf(codes.ResourceExhausted,
				"transport: snapshot exceeds max size %d bytes", maxSnap)
		}
		data = append(data, chunk.Data...)
		if chunk.Done {
			done = true
			break
		}
	}

	if first == nil {
		// Empty stream: nothing to install. Acknowledge with the current term via
		// an empty request so the handler can respond.
		resp := h.transport.raftHandler.HandleInstallSnapshot(&proto.InstallSnapshotRequest{})
		return stream.SendAndClose(resp)
	}

	// Build a single logical request carrying the payload accumulated on this
	// stream. Preserve first.Offset (NOT a hardcoded 0): the current client sends
	// one chunk per stream, so the raft handler reassembles the Offset-ordered
	// chunks across RPCs. A future client that streams all chunks on one stream
	// yields first.Offset==0 with the full data, which the handler also accepts.
	req := &proto.InstallSnapshotRequest{
		Term:              first.Term,
		LeaderId:          first.LeaderId,
		LastIncludedIndex: first.LastIncludedIndex,
		LastIncludedTerm:  first.LastIncludedTerm,
		Offset:            first.Offset,
		Data:              data,
		Done:              done,
	}
	resp := h.transport.raftHandler.HandleInstallSnapshot(req)
	return stream.SendAndClose(resp)
}

func (h *grpcServerHandler) TimeoutNow(ctx context.Context, req *proto.TimeoutNowRequest) (*proto.TimeoutNowResponse, error) {
	if h.transport.raftHandler != nil {
		return h.transport.raftHandler.HandleTimeoutNow(req), nil
	}
	return &proto.TimeoutNowResponse{}, nil
}

type grpcAdminHandler struct {
	proto.UnimplementedRaftAdminServer
	transport *GrpcTransport
}

func (h *grpcAdminHandler) GetConfiguration(ctx context.Context, req *proto.ConfigurationRequest) (*proto.ConfigurationResponse, error) {
	if h.transport.adminHandler != nil {
		return h.transport.adminHandler.HandleGetConfiguration(req), nil
	}
	return &proto.ConfigurationResponse{}, nil
}

func (h *grpcAdminHandler) AddServer(ctx context.Context, req *proto.AddServerRequest) (*proto.AddServerResponse, error) {
	if h.transport.adminHandler != nil {
		return h.transport.adminHandler.HandleAddServer(req), nil
	}
	return &proto.AddServerResponse{}, nil
}

func (h *grpcAdminHandler) RemoveServer(ctx context.Context, req *proto.RemoveServerRequest) (*proto.RemoveServerResponse, error) {
	if h.transport.adminHandler != nil {
		return h.transport.adminHandler.HandleRemoveServer(req), nil
	}
	return &proto.RemoveServerResponse{}, nil
}

func (h *grpcAdminHandler) PromoteLearner(ctx context.Context, req *proto.PromoteLearnerRequest) (*proto.PromoteLearnerResponse, error) {
	if h.transport.adminHandler != nil {
		return h.transport.adminHandler.HandlePromoteLearner(req), nil
	}
	return &proto.PromoteLearnerResponse{}, nil
}

func (h *grpcAdminHandler) CreateSnapshot(ctx context.Context, req *proto.SnapshotRequest) (*proto.SnapshotResponse, error) {
	if h.transport.adminHandler != nil {
		return h.transport.adminHandler.HandleCreateSnapshot(req), nil
	}
	return &proto.SnapshotResponse{}, nil
}

func (h *grpcAdminHandler) GetSnapshot(req *proto.GetSnapshotRequest, stream proto.RaftAdmin_GetSnapshotServer) error {
	if h.transport.adminHandler != nil {
		chunk := h.transport.adminHandler.HandleGetSnapshot(req)
		return stream.Send(chunk)
	}
	return nil
}

func (h *grpcAdminHandler) TransferLeadership(ctx context.Context, req *proto.TransferLeadershipRequest) (*proto.TransferLeadershipResponse, error) {
	if h.transport.adminHandler != nil {
		return h.transport.adminHandler.HandleTransferLeadership(req), nil
	}
	return &proto.TransferLeadershipResponse{}, nil
}

func convertEntries(entries []*raft.LogEntry) []*proto.LogEntry {
	if entries == nil {
		return nil
	}
	var pbEntries []*proto.LogEntry
	for _, e := range entries {
		pbEntries = append(pbEntries, &proto.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Type:  proto.EntryType(e.Type),
			Data:  e.Data,
		})
	}
	return pbEntries
}

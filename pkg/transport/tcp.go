package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"go.uber.org/zap"
)

// encBufPool pools *bytes.Buffer values used to marshal outbound RPC payloads
// on the hot send path, avoiding per-request heap allocations.
var encBufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

type message struct {
	// ID is a monotonic per-request correlation ID. The client sets a unique
	// ID on each request; the server echoes it (and the response Type) back so
	// the client can verify the frame it decoded is the response to the request
	// it sent, rather than a stale/mis-ordered frame (C12).
	ID      uint64          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type AppendEntriesReq struct {
	Term         uint64           `json:"term"`
	LeaderID     string           `json:"leader_id"`
	PrevLogIndex uint64           `json:"prev_log_index"`
	PrevLogTerm  uint64           `json:"prev_log_term"`
	Entries      []*raft.LogEntry `json:"entries"`
	LeaderCommit uint64           `json:"leader_commit"`
}

type AppendEntriesResp struct {
	Term         uint64 `json:"term"`
	Success      bool   `json:"success"`
	Index        uint64 `json:"index"`
	ConflictTerm uint64 `json:"conflict_term,omitempty"` // M7: term-skip backup hint
}

type RequestVoteReq struct {
	Term           uint64 `json:"term"`
	CandidateID    string `json:"candidate_id"`
	LastLogIndex   uint64 `json:"last_log_index"`
	LastLogTerm    uint64 `json:"last_log_term"`
	PreVote        bool   `json:"pre_vote"`
	LeaderTransfer bool   `json:"leader_transfer"`
}

type RequestVoteResp struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	Reason      string `json:"reason"`
}

type InstallSnapshotReq struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Offset            uint64 `json:"offset"`
	Data              []byte `json:"data"`
	Done              bool   `json:"done"`
}

type InstallSnapshotResp struct {
	Term uint64 `json:"term"`
}

type TimeoutNowReq struct {
	ServerID string `json:"server_id"`
}

type TimeoutNowResp struct {
}

type tcpTransport struct {
	mu         sync.RWMutex
	localID    raft.ServerID
	peers      map[raft.ServerID]*peer
	logger     *zap.Logger
	timeout    time.Duration
	listener   net.Listener
	handler    MessageHandler
	shutdownCh chan struct{}
	wg         sync.WaitGroup
	// tlsClient is used when dialing peers; nil means plain TCP.
	tlsClient *tls.Config
	// maxMessageBytes bounds the size of a single decoded message so an
	// unauthenticated peer cannot stream an arbitrarily large JSON object and
	// exhaust memory (C12). 0 means use defaultMaxMessageBytes.
	maxMessageBytes int64
	// reqCounter is a monotonic source of per-request correlation IDs (C12).
	// Accessed via sync/atomic; starts at 0 so the first ID handed out is 1.
	reqCounter uint64
	// allowedMembers, when non-nil and non-empty, restricts which verified peer
	// certificate identities (CN or DNS SAN) may drive the transport over TLS
	// (H-S1). Empty/nil disables the check so plaintext/dev keeps working.
	allowedMembers map[string]struct{}
}

// SetAllowedMembers configures the set of verified peer certificate identities
// (CN or DNS SAN) permitted to talk to this TCP transport when TLS is enabled
// (H-S1). An empty/nil slice disables the check (plaintext/dev still works).
func (t *tcpTransport) SetAllowedMembers(ids []string) {
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

// authorizeTLSConn checks the verified peer certificate on a TLS connection
// against the allowed-member set (H-S1). It returns nil (allowed) when the
// check is disabled or the connection is not TLS; enforcement of the presence
// of a client cert itself is left to RequireAndVerifyClientCert.
func (t *tcpTransport) authorizeTLSConn(conn net.Conn) error {
	t.mu.RLock()
	allowed := t.allowedMembers
	t.mu.RUnlock()
	if len(allowed) == 0 {
		return nil
	}
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return nil // not a TLS connection; nothing to authorize here
	}
	// Ensure the handshake has completed so VerifiedChains is populated.
	if err := tlsConn.Handshake(); err != nil {
		return err
	}
	state := tlsConn.ConnectionState()
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return fmt.Errorf("transport: no verified peer certificate")
	}
	leaf := state.VerifiedChains[0][0]
	if _, ok := allowed[leaf.Subject.CommonName]; ok {
		return nil
	}
	for _, dns := range leaf.DNSNames {
		if _, ok := allowed[dns]; ok {
			return nil
		}
	}
	return fmt.Errorf("transport: peer identity %q is not an allowed cluster member", leaf.Subject.CommonName)
}

// nextRequestID returns a fresh, monotonically increasing correlation ID (C12).
func (t *tcpTransport) nextRequestID() uint64 {
	return atomic.AddUint64(&t.reqCounter, 1)
}

// defaultMaxMessageBytes caps a single inbound message. Large enough for a
// snapshot chunk, small enough to limit a pre-auth allocation-DoS (M-S2).
// Snapshots are chunked, so a single frame never needs to be huge; the previous
// 128 MiB default let an unauthenticated peer force a large allocation. Callers
// that legitimately send larger single frames can raise it via
// SetMaxMessageBytes.
const defaultMaxMessageBytes int64 = 16 << 20 // 16 MiB

// maxPeerConns bounds the number of concurrent connections a single peer may
// hold. H-R5: the old design serialized every RPC to a peer behind one mutex
// held across the whole network round-trip, so a slow AppendEntries starved
// heartbeats/votes. We now keep a small pool of idle connections; concurrent
// exchanges each check out their own connection, so they no longer serialize.
// A cap prevents an unbounded connection blow-up under load.
const maxPeerConns = 8

// peer tracks the connection state for one remote server. H-R5: instead of a
// single serialized conn, it maintains a free-list (idle) of ready connections
// plus a count of connections currently checked out, all guarded by mu. The
// request/response exchange itself runs WITHOUT holding mu, so exchanges to the
// same peer proceed concurrently. Per-connection request-ID correlation (C12)
// is preserved because each connection is used by exactly one exchange at a
// time.
type peer struct {
	addr    raft.ServerAddress
	timeout time.Duration

	mu     sync.Mutex
	idle   []net.Conn // ready connections available for checkout
	closed bool       // set by RemovePeer/Close; no further checkouts
}

// checkoutConn returns a ready connection for an exchange, dialing a fresh one
// if the idle pool is empty. The returned conn is owned exclusively by the
// caller until releaseConn/discardConn. mu is held only briefly (M-C2: peer.conn
// state is always mutated under peer.mu).
func (t *tcpTransport) checkoutConn(ctx context.Context, p *peer) (net.Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("peer removed")
	}
	if n := len(p.idle); n > 0 {
		conn := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	// Dial outside the lock so concurrent checkouts do not serialize on dialing.
	return t.dialPeer(ctx, p)
}

// releaseConn returns a healthy connection to the idle pool for reuse. If the
// peer has been removed or the pool is full, the connection is closed instead.
func (t *tcpTransport) releaseConn(p *peer, conn net.Conn) {
	if conn == nil {
		return
	}
	p.mu.Lock()
	if p.closed || len(p.idle) >= maxPeerConns {
		p.mu.Unlock()
		conn.Close()
		return
	}
	p.idle = append(p.idle, conn)
	p.mu.Unlock()
}

// discardConn closes a connection that must not be reused (I/O error or a
// correlation mismatch left it out of sync).
func (t *tcpTransport) discardConn(conn net.Conn) {
	if conn != nil {
		conn.Close()
	}
}

// dialPeer opens a new connection to the peer, honoring the caller ctx deadline.
func (t *tcpTransport) dialPeer(ctx context.Context, p *peer) (net.Conn, error) {
	dialCtx, cancel := t.dialContext(ctx, p.timeout)
	defer cancel()
	if t.tlsClient != nil {
		// C11: verify the server cert against this peer's own address rather
		// than a hardcoded name. An explicit ServerName wins; otherwise it is
		// derived from the peer address host.
		peerTLS := t.tlsClient.Clone()
		peerTLS.ServerName = serverNameFor(t.tlsClient.ServerName, string(p.addr))
		dialer := &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: p.timeout},
			Config:    peerTLS,
		}
		return dialer.DialContext(dialCtx, "tcp", string(p.addr))
	}
	d := &net.Dialer{Timeout: p.timeout}
	return d.DialContext(dialCtx, "tcp", string(p.addr))
}

// TCPTLSConfig holds the paths to TLS material for the TCP transport.
// All three fields must be set together to enable TLS.
type TCPTLSConfig struct {
	// CertFile is the path to the PEM-encoded server certificate (also used
	// as the client certificate when dialing peers).
	CertFile string
	// KeyFile is the path to the PEM-encoded private key for CertFile.
	KeyFile string
	// CAFile is the path to the PEM-encoded CA certificate used to verify
	// peer certificates.  When set, mutual TLS is enforced.
	CAFile string
}

// LoadTLSConfig builds a *tls.Config from the given TCPTLSConfig.
// Returns nil, nil when cfg is nil or all fields are empty (no TLS).
func LoadTLSConfig(cfg *TCPTLSConfig) (*tls.Config, error) {
	if cfg == nil || (cfg.CertFile == "" && cfg.KeyFile == "" && cfg.CAFile == "") {
		return nil, nil
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, fmt.Errorf("tls: CertFile and KeyFile must both be set")
	}

	// M-S3: refuse a key file that is group/other-readable before loading it.
	if err := checkKeyPerm(cfg.KeyFile); err != nil {
		return nil, fmt.Errorf("tls: %w", err)
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: loading cert/key: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}

	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls: reading CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("tls: no valid CA certs in %s", cfg.CAFile)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.RootCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsCfg, nil
}

type MessageHandler interface {
	HandleAppendEntries(req *AppendEntriesReq) *AppendEntriesResp
	HandleRequestVote(req *RequestVoteReq) *RequestVoteResp
	HandleInstallSnapshot(req *InstallSnapshotReq) *InstallSnapshotResp
	HandleTimeoutNow(req *TimeoutNowReq) *TimeoutNowResp
}

func NewTCPTransport(listenAddr string, handler MessageHandler, timeout time.Duration, logger *zap.Logger) (*tcpTransport, error) {
	return NewTCPTransportTLS(listenAddr, handler, timeout, logger, nil)
}

// NewTCPTransportTLS creates a TCP transport with optional TLS.
// Pass a non-nil tlsCfg (from LoadTLSConfig) to enable encrypted inter-node
// communication.  When tlsCfg is nil the transport falls back to plain TCP.
func NewTCPTransportTLS(listenAddr string, handler MessageHandler, timeout time.Duration, logger *zap.Logger, tlsCfg *tls.Config) (*tcpTransport, error) {
	var ln net.Listener
	var err error

	if tlsCfg != nil {
		ln, err = tls.Listen("tcp", listenAddr, tlsCfg)
	} else {
		ln, err = net.Listen("tcp", listenAddr)
	}
	if err != nil {
		return nil, err
	}

	// Build a client-side TLS config for dialing peers:
	// use the same cert/CA as the server, but don't require client certs
	// on the dial side (the server side will do that).
	var clientTLS *tls.Config
	if tlsCfg != nil {
		clientTLS = tlsCfg.Clone()
		clientTLS.ClientAuth = tls.NoClientCert // client-side dial config
		// C11: the dial config must verify the server. LoadTLSConfig already
		// pins MinVersion TLS 1.3 and populates RootCAs; ensure both survive
		// the clone (Clone copies them) and that InsecureSkipVerify stays off.
		clientTLS.InsecureSkipVerify = false
		if clientTLS.MinVersion == 0 {
			clientTLS.MinVersion = tls.VersionTLS13
		}
		// ServerName is derived per-peer at dial time (serverNameFor); an
		// explicit value set by the caller is preserved and takes precedence.
	}

	t := &tcpTransport{
		peers:      make(map[raft.ServerID]*peer),
		handler:    handler,
		timeout:    timeout,
		listener:   ln,
		logger:     logger,
		shutdownCh: make(chan struct{}),
		tlsClient:  clientTLS,
	}

	t.wg.Add(1)
	go t.serve()

	return t, nil
}

func (t *tcpTransport) SetLogger(logger *zap.Logger) {
	t.logger = logger
}

func (t *tcpTransport) SetLocalID(id raft.ServerID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.localID = id
}

// SetMaxMessageBytes overrides the per-message size limit (C12).
func (t *tcpTransport) SetMaxMessageBytes(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.maxMessageBytes = n
}

func (t *tcpTransport) maxMsgBytes() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.maxMessageBytes > 0 {
		return t.maxMessageBytes
	}
	return defaultMaxMessageBytes
}

// ListenerAddr returns the address the transport is listening on.
// Useful in tests where ":0" is used to get an OS-assigned port.
func (t *tcpTransport) ListenerAddr() string {
	if t.listener == nil {
		return ""
	}
	return t.listener.Addr().String()
}

func (t *tcpTransport) serve() {
	defer t.wg.Done()

	// M-R2: bound concurrent connection handlers so a reconnect storm cannot
	// exhaust goroutines/FDs, and back off on persistent accept errors.
	const maxConcurrentConns = 1024
	sem := make(chan struct{}, maxConcurrentConns)
	var backoff time.Duration

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.shutdownCh:
				return
			default:
			}
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else {
				backoff *= 2
			}
			if backoff > time.Second {
				backoff = time.Second
			}
			t.logger.Error("failed to accept connection; backing off",
				zap.Error(err), zap.Duration("delay", backoff))
			select {
			case <-time.After(backoff):
			case <-t.shutdownCh:
				return
			}
			continue
		}
		backoff = 0

		select {
		case sem <- struct{}{}:
		case <-t.shutdownCh:
			conn.Close()
			return
		}
		t.wg.Add(1)
		go func(c net.Conn) {
			defer func() { <-sem }()
			t.handleConn(c)
		}(conn)
	}
}

// idleReadTimeout is the maximum time a server-side connection waits for the
// next message from the peer before being closed.  This bounds goroutine
// lifetime when a peer dies without sending a FIN/RST.
const idleReadTimeout = 60 * time.Second

// idleWriteTimeout bounds how long a server-side connection may block writing a
// single response before the connection is closed (M5).
const idleWriteTimeout = 30 * time.Second

func (t *tcpTransport) handleConn(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()

	// H-S1: when an allowed-member set is configured and this is a TLS
	// connection, reject peers whose verified certificate identity (CN/SAN) is
	// not an expected cluster member before processing any request.
	if err := t.authorizeTLSConn(conn); err != nil {
		if t.logger != nil {
			t.logger.Warn("rejected unauthorized peer", zap.Error(err))
		}
		return
	}

	// C12: bound the bytes read for any single message so an unauthenticated
	// peer cannot exhaust memory with an arbitrarily large JSON object. The
	// limit is reset before each message; a message exceeding it fails to decode
	// and the connection is closed.
	maxBytes := t.maxMsgBytes()
	limited := &io.LimitedReader{R: conn, N: maxBytes}
	decoder := json.NewDecoder(limited)

	for {
		// Refresh the read deadline before each message so idle connections
		// are reaped rather than leaking goroutines indefinitely.
		conn.SetReadDeadline(time.Now().Add(idleReadTimeout)) //nolint:errcheck
		limited.N = maxBytes
		var msg message
		if err := decoder.Decode(&msg); err != nil {
			if err != io.EOF {
				t.logger.Error("failed to decode message", zap.Error(err))
			}
			return
		}

		var resp interface{}
		switch msg.Type {
		case "AppendEntries":
			var req AppendEntriesReq
			if err := json.Unmarshal(msg.Payload, &req); err != nil {
				continue
			}
			if t.handler != nil {
				r := t.handler.HandleAppendEntries(&req)
				resp = r
			}
		case "RequestVote":
			var req RequestVoteReq
			if err := json.Unmarshal(msg.Payload, &req); err != nil {
				continue
			}
			if t.handler != nil {
				r := t.handler.HandleRequestVote(&req)
				resp = r
			}
		case "InstallSnapshot":
			var req InstallSnapshotReq
			if err := json.Unmarshal(msg.Payload, &req); err != nil {
				continue
			}
			if t.handler != nil {
				r := t.handler.HandleInstallSnapshot(&req)
				resp = r
			}
		case "TimeoutNow":
			var req TimeoutNowReq
			if err := json.Unmarshal(msg.Payload, &req); err != nil {
				continue
			}
			if t.handler != nil {
				r := t.handler.HandleTimeoutNow(&req)
				resp = r
			}
		}

		if resp != nil {
			respMsg := message{
				// C12: echo the request's correlation ID and a type derived from
				// the request type so the client can verify this frame is the
				// response to the request it sent.
				ID:      msg.ID,
				Type:    msg.Type + "Response",
				Payload: json.RawMessage{},
			}
			if data, err := json.Marshal(resp); err == nil {
				respMsg.Payload = data
				// M5: bound the time spent writing a response so a stalled/slow
				// peer socket cannot block this goroutine indefinitely.
				conn.SetWriteDeadline(time.Now().Add(idleWriteTimeout)) //nolint:errcheck
				wbuf := encBufPool.Get().(*bytes.Buffer)
				wbuf.Reset()
				encErr := json.NewEncoder(wbuf).Encode(respMsg)
				if encErr == nil {
					_, encErr = conn.Write(wbuf.Bytes())
				}
				encBufPool.Put(wbuf)
				if encErr != nil {
					if t.logger != nil {
						t.logger.Error("failed to write response", zap.Error(encErr))
					}
					return
				}
			}
		}
	}
}

func (t *tcpTransport) AppendEntries(ctx context.Context, target raft.ServerID, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	peer, err := t.getPeer(target)
	if err != nil {
		return nil, err
	}

	pbReq := AppendEntriesReq{
		Term:         req.Term,
		LeaderID:     string(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      req.Entries,
		LeaderCommit: req.LeaderCommit,
	}

	resp, err := t.sendRequest(ctx, peer, "AppendEntries", pbReq)
	if err != nil {
		return nil, err
	}

	var appendResp AppendEntriesResp
	if err := json.Unmarshal(resp, &appendResp); err != nil {
		return nil, err
	}

	return &raft.AppendEntriesResponse{
		Term:         appendResp.Term,
		Success:      appendResp.Success,
		Index:        appendResp.Index,
		ConflictTerm: appendResp.ConflictTerm,
	}, nil
}

func (t *tcpTransport) RequestVote(ctx context.Context, target raft.ServerID, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	peer, err := t.getPeer(target)
	if err != nil {
		return nil, err
	}

	pbReq := RequestVoteReq{
		Term:           req.Term,
		CandidateID:    string(req.CandidateID),
		LastLogIndex:   req.LastLogIndex,
		LastLogTerm:    req.LastLogTerm,
		PreVote:        req.PreVote,
		LeaderTransfer: req.LeaderTransfer,
	}

	resp, err := t.sendRequest(ctx, peer, "RequestVote", pbReq)
	if err != nil {
		return nil, err
	}

	var voteResp RequestVoteResp
	if err := json.Unmarshal(resp, &voteResp); err != nil {
		return nil, err
	}

	return &raft.RequestVoteResponse{
		Term:        voteResp.Term,
		VoteGranted: voteResp.VoteGranted,
		Reason:      voteResp.Reason,
	}, nil
}

func (t *tcpTransport) InstallSnapshot(ctx context.Context, target raft.ServerID, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	peer, err := t.getPeer(target)
	if err != nil {
		return nil, err
	}

	pbReq := InstallSnapshotReq{
		Term:              req.Term,
		LeaderID:          string(req.LeaderID),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	}

	resp, err := t.sendRequest(ctx, peer, "InstallSnapshot", pbReq)
	if err != nil {
		return nil, err
	}

	var snapResp InstallSnapshotResp
	if err := json.Unmarshal(resp, &snapResp); err != nil {
		return nil, err
	}

	return &raft.InstallSnapshotResponse{
		Term: snapResp.Term,
	}, nil
}

func (t *tcpTransport) TimeoutNow(ctx context.Context, target raft.ServerID) error {
	peer, err := t.getPeer(target)
	if err != nil {
		return err
	}

	pbReq := TimeoutNowReq{
		ServerID: string(t.localID),
	}

	_, err = t.sendRequest(ctx, peer, "TimeoutNow", pbReq)
	return err
}

func (t *tcpTransport) getPeer(id raft.ServerID) (*peer, error) {
	t.mu.RLock()
	peer, ok := t.peers[id]
	t.mu.RUnlock()

	if ok {
		return peer, nil
	}

	return nil, fmt.Errorf("peer not found: %s", id)
}

// dialContext returns a context for dialing a peer whose deadline is the sooner
// of the caller ctx's deadline and now+fallback. The caller must invoke the
// returned cancel func (M5).
func (t *tcpTransport) dialContext(ctx context.Context, fallback time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(fallback)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return context.WithDeadline(ctx, deadline)
}

// exchangeDeadline returns the read/write deadline for a single request/response
// exchange: the sooner of the caller ctx's deadline and now+fallback (M5).
func (t *tcpTransport) exchangeDeadline(ctx context.Context, fallback time.Duration) time.Time {
	deadline := time.Now().Add(fallback)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return deadline
}

func (t *tcpTransport) sendRequest(ctx context.Context, peer *peer, msgType string, req interface{}) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// M5: honor the caller's context up front — if it is already canceled or
	// past its deadline, fail fast rather than dialing/writing.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// H-R5: check out a connection dedicated to this exchange instead of holding
	// peer.mu across the whole round-trip. Concurrent RPCs to the same peer each
	// use their own connection and therefore proceed in parallel. Per-connection
	// request-ID correlation (C12) is preserved because a checked-out connection
	// is never shared by two exchanges at once.
	conn, err := t.checkoutConn(ctx, peer)
	if err != nil {
		return nil, err
	}

	// Set a per-exchange deadline so the goroutine cannot block indefinitely
	// when the remote side is killed or unreachable (e.g. RST delayed on macOS).
	// M5: prefer the caller ctx's deadline when it is sooner than the default.
	if err := conn.SetDeadline(t.exchangeDeadline(ctx, peer.timeout)); err != nil {
		t.discardConn(conn)
		return nil, err
	}

	decoder := json.NewDecoder(conn)

	// C12: assign a unique correlation ID for this request. The server echoes
	// it (and a matching response type) so we can detect a stale/mis-ordered
	// frame instead of feeding a mismatched payload into Raft.
	reqID := t.nextRequestID()
	msg := message{
		ID:      reqID,
		Type:    msgType,
		Payload: json.RawMessage{},
	}

	if data, err := json.Marshal(req); err == nil {
		msg.Payload = data
	}

	// Use pooled buffer to encode the outer message envelope.
	wbuf := encBufPool.Get().(*bytes.Buffer)
	wbuf.Reset()
	encErr := json.NewEncoder(wbuf).Encode(msg)
	if encErr == nil {
		_, encErr = conn.Write(wbuf.Bytes())
	}
	encBufPool.Put(wbuf)
	if encErr != nil {
		t.discardConn(conn)
		return nil, encErr
	}

	var resp message
	if err := decoder.Decode(&resp); err != nil {
		t.discardConn(conn)
		return nil, err
	}

	// C12: verify the response echoes the exact request ID and the expected
	// response type. On any mismatch the connection is out of sync (a frame was
	// dropped, duplicated, or reordered); drop the connection and error out
	// rather than returning a payload that may belong to a different request.
	wantType := msgType + "Response"
	if resp.ID != reqID || resp.Type != wantType {
		t.discardConn(conn)
		return nil, fmt.Errorf(
			"transport: response correlation mismatch: got id=%d type=%q, want id=%d type=%q",
			resp.ID, resp.Type, reqID, wantType,
		)
	}

	// Clear the deadline so a future exchange on this connection is not affected
	// by a stale deadline, then return it to the idle pool for reuse.
	conn.SetDeadline(time.Time{}) //nolint:errcheck
	t.releaseConn(peer, conn)

	return resp.Payload, nil
}

func (t *tcpTransport) Close() error {
	close(t.shutdownCh)

	// Close the listener BEFORE waiting for the serve goroutine, otherwise
	// the serve goroutine blocks forever on Accept() and wg.Wait() deadlocks.
	if t.listener != nil {
		t.listener.Close()
	}

	t.wg.Wait()

	// M-C2: take t.mu first, then each peer.mu (consistent t.mu -> peer.mu
	// ordering) so peer connection state is always mutated under peer.mu even
	// during shutdown.
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, p := range t.peers {
		p.mu.Lock()
		p.closed = true
		for _, c := range p.idle {
			c.Close()
		}
		p.idle = nil
		p.mu.Unlock()
	}

	return nil
}

func (t *tcpTransport) AddPeer(id raft.ServerID, addr raft.ServerAddress) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.peers[id]; ok {
		return nil
	}

	t.peers[id] = &peer{
		addr:    addr,
		timeout: t.timeout,
	}

	return nil
}

func (t *tcpTransport) RemovePeer(id raft.ServerID) {
	// M-C2: order is t.mu -> peer.mu. peer connection state (idle pool, closed
	// flag) is mutated ONLY under peer.mu, matching checkoutConn/releaseConn, so
	// there is no data race with a concurrent sendRequest on the same peer.
	t.mu.Lock()
	p, ok := t.peers[id]
	if ok {
		delete(t.peers, id)
	}
	t.mu.Unlock()

	if !ok {
		return
	}
	p.mu.Lock()
	p.closed = true
	for _, c := range p.idle {
		c.Close()
	}
	p.idle = nil
	p.mu.Unlock()
}

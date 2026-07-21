package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // G108: pprof is served only on the auth-gated debug mux (see H12 checks in run()), never on the public API listener.
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sanskarpan/raft-consensus/pkg/fsm"
	"github.com/sanskarpan/raft-consensus/pkg/metrics"
	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"github.com/sanskarpan/raft-consensus/pkg/storage"
	"github.com/sanskarpan/raft-consensus/pkg/tracing"
	"github.com/sanskarpan/raft-consensus/pkg/transport"
	"github.com/sanskarpan/raft-consensus/pkg/version"
	proto "github.com/sanskarpan/raft-consensus/proto"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// roleContextKey is the type used as a context key for the authenticated role.
type roleContextKey struct{}

type Config struct {
	NodeID        string          `yaml:"node_id"`
	ListenAddr    string          `yaml:"listen_addr"`
	HTTPAddr      string          `yaml:"http_addr"`
	DataDir       string          `yaml:"data_dir"`
	Cluster       []ClusterMember `yaml:"cluster"`
	ElectionTick  int             `yaml:"election_tick"`
	HeartbeatTick int             `yaml:"heartbeat_tick"`
	// CheckQuorum makes a leader step down if it cannot reach a quorum of voters
	// within an election timeout, bounding partitioned-minority leadership.
	CheckQuorum bool              `yaml:"check_quorum"`
	AdminToken  string            `yaml:"admin_token"`
	AdminTokens map[string]string `yaml:"admin_tokens"`
	// AllowNoAuth must be set explicitly to run WITHOUT any authentication.
	// When no tokens are configured and this is false, the auth middleware fails
	// closed (rejects every request) rather than silently allowing all callers
	// through — a fake approval point (C9).
	AllowNoAuth bool   `yaml:"allow_no_auth"`
	DebugAddr   string `yaml:"debug_addr"`
	// CORSOrigins is a comma-separated list of allowed origins for CORS.
	// Use "*" (the default when empty) to allow all origins.
	CORSOrigins string `yaml:"cors_origins"`
	// RateLimitRPS caps the number of write requests (Apply) per second.
	// 0 disables rate limiting (default: 500).
	RateLimitRPS int `yaml:"rate_limit_rps"`
	// MaxRequestBodyBytes limits the size of HTTP request bodies for write
	// endpoints. 0 uses the default of 1 MiB.
	MaxRequestBodyBytes int64 `yaml:"max_request_body_bytes"`

	// Transport selects the Raft peer transport: "tcp" (default) or "grpc".
	Transport string `yaml:"transport"`
	// GRPCCompression gzip-compresses inter-node gRPC RPCs (AppendEntries,
	// InstallSnapshot). Interoperates with uncompressed peers.
	GRPCCompression bool `yaml:"grpc_compression"`
	// SnapshotCompression gzip-compresses snapshots on disk. Existing snapshots
	// are read using their recorded compression, so it is safe to toggle.
	SnapshotCompression bool `yaml:"snapshot_compression"`
	// TLS fields for gRPC mTLS (all three must be set together).
	TLSCert string `yaml:"tls_cert"` // path to server certificate
	TLSKey  string `yaml:"tls_key"`  // path to server private key
	TLSCA   string `yaml:"tls_ca"`   // path to CA certificate (for mTLS)
	// RequireTLS makes the gRPC transport fail closed: peers can only be dialed
	// over TLS, never plaintext (C11). Set it in production to guarantee no
	// accidental cleartext inter-node traffic.
	RequireTLS bool `yaml:"require_tls"`
	// BinaryTransport, when true (the default), causes the TCP transport to
	// negotiate binary framing with peers on the hot RPC path.
	// Set to false to force JSON framing (useful for debugging or rollback).
	BinaryTransport bool `yaml:"binary_transport"`
	// WatchIdleTimeout is the maximum time a /v1/watch SSE connection may be
	// idle (no events delivered) before it is closed by the server.
	// Zero uses the default of 5 minutes.
	WatchIdleTimeout time.Duration `yaml:"watch_idle_timeout"`

	// MaxWatchConnections caps the total number of concurrent /v1/watch SSE
	// streams across all clients. Zero uses the default of 1024. (M14)
	MaxWatchConnections int `yaml:"max_watch_connections"`
	// MaxWatchConnectionsPerIP caps concurrent /v1/watch streams from a single
	// client IP. Zero uses the default of 32. (M14)
	MaxWatchConnectionsPerIP int `yaml:"max_watch_connections_per_ip"`

	// HTTPSCert and HTTPSKey enable TLS on the HTTP API server when both are
	// set.  The values are file paths to a PEM-encoded certificate and key.
	HTTPSCert string `yaml:"https_cert"`
	HTTPSKey  string `yaml:"https_key"`

	// OtlpEndpoint, if set, sends traces to the given OTLP/gRPC endpoint.
	// Example: "localhost:4317"
	OtlpEndpoint string `yaml:"otlp_endpoint"`

	// TTLTickInterval is how often the leader proposes a tick command to advance
	// the FSM's virtual clock and sweep expired keys (#207). Default: 1s.
	// Set to 0 to disable TTL expiry (no tick loop is started).
	TTLTickInterval time.Duration `yaml:"ttl_tick_interval"`

	// PerIPRateLimitRPS caps write requests per second per client IP.
	// Zero uses the default of 50 RPS.
	PerIPRateLimitRPS int `yaml:"per_ip_rate_limit_rps"`

	// MetricsAuth, when true, gates the /metrics endpoint behind the read role
	// (M-O3). When false (the default) /metrics is still automatically gated
	// behind auth whenever any admin token is configured; it stays open only in
	// token-less dev mode.
	MetricsAuth bool `yaml:"metrics_auth"`

	// TrustedProxyCIDRs is a list of CIDR blocks (e.g. "10.0.0.0/8") whose
	// X-Forwarded-For / X-Real-IP headers are trusted for per-IP rate
	// limiting.  When a request arrives from an address in one of these
	// ranges the real client IP is extracted from the leftmost
	// X-Forwarded-For entry (or X-Real-IP) instead of RemoteAddr.
	// Leave empty to trust no proxy (use RemoteAddr always).
	TrustedProxyCIDRs []string `yaml:"trusted_proxy_cidrs"`
}

type ClusterMember struct {
	ID          string `yaml:"id"`
	Address     string `yaml:"address"`
	HTTPAddress string `yaml:"http_address"` // HTTP address for leader forwarding
}

// writeLimiter is a simple token-bucket rate limiter for write endpoints.
type writeLimiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
	lastSeen   time.Time // used by per-IP sweep to evict idle entries
}

func newWriteLimiter(rps int) *writeLimiter {
	now := time.Now()
	return &writeLimiter{
		tokens:     float64(rps),
		maxTokens:  float64(rps),
		refillRate: float64(rps),
		lastRefill: now,
		lastSeen:   now,
	}
}

func (l *writeLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	l.tokens += elapsed * l.refillRate
	if l.tokens > l.maxTokens {
		l.tokens = l.maxTokens
	}
	l.lastRefill = now
	l.lastSeen = now
	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

// perIPRateLimitRPS is the default per-client-IP write request cap.
const defaultPerIPRPS = 50

// Default caps on concurrent /v1/watch SSE connections (M14).
const (
	defaultMaxWatchConns      = 1024
	defaultMaxWatchConnsPerIP = 32
)

// acquireWatchSlot reserves a global and per-IP watch slot for ip. It returns a
// release func and true on success, or (nil, false) when either limit is
// exhausted (M14). The caller MUST invoke the release func when the stream ends.
func (s *Server) acquireWatchSlot(ip string) (func(), bool) {
	maxTotal := s.config.MaxWatchConnections
	if maxTotal <= 0 {
		maxTotal = defaultMaxWatchConns
	}
	maxPerIP := s.config.MaxWatchConnectionsPerIP
	if maxPerIP <= 0 {
		maxPerIP = defaultMaxWatchConnsPerIP
	}

	if atomic.AddInt64(&s.watchCount, 1) > int64(maxTotal) {
		atomic.AddInt64(&s.watchCount, -1)
		return nil, false
	}

	ctr := s.watchCounterFor(ip)
	if atomic.AddInt64(ctr, 1) > int64(maxPerIP) {
		atomic.AddInt64(ctr, -1)
		atomic.AddInt64(&s.watchCount, -1)
		return nil, false
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			atomic.AddInt64(ctr, -1)
			atomic.AddInt64(&s.watchCount, -1)
		})
	}, true
}

// watchCounterFor returns the per-IP watch counter, creating it if needed.
func (s *Server) watchCounterFor(ip string) *int64 {
	if v, ok := s.watchPerIP.Load(ip); ok {
		return v.(*int64)
	}
	var zero int64
	actual, _ := s.watchPerIP.LoadOrStore(ip, &zero)
	return actual.(*int64)
}

// clientIP returns the real client IP for rate-limiting purposes.
// When the request comes from a trusted proxy the leftmost X-Forwarded-For
// entry (or X-Real-IP) is used; otherwise r.RemoteAddr is used.
func (s *Server) clientIP(r *http.Request) string {
	remoteHost, _, _ := net.SplitHostPort(r.RemoteAddr)
	remoteIP := net.ParseIP(remoteHost)

	// Only trust forwarded headers when the direct peer is a known proxy.
	trusted := false
	if remoteIP != nil {
		for _, network := range s.trustedNets {
			if network.Contains(remoteIP) {
				trusted = true
				break
			}
		}
	}

	if trusted {
		// X-Forwarded-For: client, proxy1, proxy2 — leftmost is the real client.
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if ip, _, found := strings.Cut(xff, ","); found {
				ip = strings.TrimSpace(ip)
				if parsed := net.ParseIP(ip); parsed != nil {
					return parsed.String()
				}
			} else {
				ip = strings.TrimSpace(xff)
				if parsed := net.ParseIP(ip); parsed != nil {
					return parsed.String()
				}
			}
		}
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			if parsed := net.ParseIP(strings.TrimSpace(xri)); parsed != nil {
				return parsed.String()
			}
		}
	}

	// Default: use RemoteAddr (untrusted proxy or direct connection).
	if remoteIP != nil {
		return remoteIP.String()
	}
	return remoteHost
}

// perIPLimiterFor returns (or lazily creates) the per-IP limiter for addr.
func (s *Server) perIPLimiterFor(addr string) *writeLimiter {
	rps := s.config.PerIPRateLimitRPS
	if rps <= 0 {
		rps = defaultPerIPRPS
	}
	// addr is already a bare IP (no port) from clientIP().
	// Strip port defensively in case called with raw RemoteAddr.
	if host, _, found := strings.Cut(addr, ":"); found {
		addr = host
	}
	if v, ok := s.perIPLimiters.Load(addr); ok {
		return v.(*writeLimiter)
	}
	l := newWriteLimiter(rps)
	actual, _ := s.perIPLimiters.LoadOrStore(addr, l)
	return actual.(*writeLimiter)
}

// sweepPerIPLimiters evicts per-IP limiter entries that have been idle for
// more than 5 minutes, bounding unbounded growth.  It exits when ctx is
// canceled (i.e. when the server shuts down).
func (s *Server) sweepPerIPLimiters(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
		cutoff := time.Now().Add(-5 * time.Minute)
		s.perIPLimiters.Range(func(key, val any) bool {
			l := val.(*writeLimiter)
			l.mu.Lock()
			idle := l.lastSeen.Before(cutoff)
			l.mu.Unlock()
			if idle {
				s.perIPLimiters.Delete(key)
			}
			return true
		})

		// L5: also evict per-IP watch counters that have dropped to zero (no open
		// streams). These are never otherwise removed, so a churn of distinct
		// client IPs would grow the map unbounded. A concurrent acquire that
		// loaded the same counter will simply re-create the entry, so deleting a
		// zeroed counter is safe.
		s.watchPerIP.Range(func(key, val any) bool {
			ctr := val.(*int64)
			if atomic.LoadInt64(ctr) == 0 {
				s.watchPerIP.Delete(key)
			}
			return true
		})
	}
}

type Server struct {
	config          *Config
	raftNode        raft.Raft
	raftTransport   raft.Transport    // kept so Shutdown can send TimeoutNow
	reloadTLS       func() error      // #204: reload TLS certs on SIGHUP (nil if no TLS)
	kv              *fsm.KVStore      // direct FSM reference for stale reads and watches
	watchMgr        *fsm.WatchManager // SSE event fan-out
	watchCtxCancel  context.CancelFunc
	sweepCancel     context.CancelFunc // cancels sweepPerIPLimiters goroutine on shutdown
	tickCancel      context.CancelFunc // cancels leaderTickLoop goroutine on shutdown (#207)
	limiter         *writeLimiter      // global token-bucket rate limiter
	perIPLimiters   sync.Map           // map[string]*writeLimiter — per client-IP limiters
	watchCount      int64              // current number of open /v1/watch SSE streams (atomic)
	watchPerIP      sync.Map           // map[string]*int64 — open watch streams per client IP
	trustedNets     []*net.IPNet       // parsed from config TrustedProxyCIDRs
	tracingProvider *tracing.Provider  // shutdown on exit
	logger          *zap.Logger
	http            *http.Server
	debugServer     *http.Server

	// draining is flipped to 1 at the start of Shutdown so write endpoints
	// return 503 before the drain begins (M-R4).
	draining atomic.Bool

	// watchMu guards watchCancels, the set of per-request cancel funcs for open
	// SSE watch streams. Shutdown cancels them all so http.Shutdown does not
	// block on long-lived streams (M-R3).
	watchMu      sync.Mutex
	watchCancels map[int64]context.CancelFunc
	watchSeq     int64
}

func main() {
	configPath := flag.String("config", "raftd.yaml", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String())
		os.Exit(0)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	config, err := loadConfig(*configPath)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	server, err := NewServer(config, logger)
	if err != nil {
		logger.Fatal("failed to create server", zap.Error(err))
	}

	if err := server.Start(); err != nil {
		logger.Fatal("failed to start server", zap.Error(err))
	}

	logger.Info("server started", zap.String("node_id", config.NodeID))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	for sig := range sigCh {
		if sig == syscall.SIGHUP {
			// #204: reload rotated TLS certificates without restarting.
			if server.reloadTLS != nil {
				if err := server.reloadTLS(); err != nil {
					logger.Error("TLS reload failed", zap.Error(err))
				} else {
					logger.Info("TLS certificates reloaded (SIGHUP)")
				}
			}
			continue
		}
		break // SIGINT / SIGTERM
	}

	logger.Info("shutting down server")
	server.Shutdown()
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Expand ${VAR} / $VAR references from the environment so configs can be
	// templated per-instance without a preprocessing step. This is what lets a
	// Kubernetes StatefulSet set `node_id: "${HOSTNAME}"` (the kubelet sets
	// HOSTNAME to the stable pod name, e.g. "raft-raft-0"). Unset variables
	// expand to "" (which then fails config validation loudly, as intended).
	data = []byte(os.ExpandEnv(string(data)))

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	if config.NodeID == "" {
		return nil, fmt.Errorf("node_id is required")
	}

	// H12: validate the cluster membership before starting. A malformed cluster
	// (empty, missing local node, or duplicate member IDs) leads to a node that
	// can never form or join a quorum, so refuse to start.
	if err := validateCluster(&config); err != nil {
		return nil, err
	}

	if config.ListenAddr == "" {
		config.ListenAddr = ":8080"
	}

	if config.HTTPAddr == "" {
		config.HTTPAddr = ":8081"
	}

	if config.DataDir == "" {
		config.DataDir = "./data"
	}

	if config.ElectionTick == 0 {
		config.ElectionTick = 10
	}

	if config.HeartbeatTick == 0 {
		config.HeartbeatTick = 1
	}

	if config.RateLimitRPS == 0 {
		config.RateLimitRPS = 500
	}

	if config.MaxRequestBodyBytes == 0 {
		config.MaxRequestBodyBytes = 1 << 20 // 1 MiB
	}

	if config.MaxWatchConnections == 0 {
		config.MaxWatchConnections = defaultMaxWatchConns
	}

	if config.MaxWatchConnectionsPerIP == 0 {
		config.MaxWatchConnectionsPerIP = defaultMaxWatchConnsPerIP
	}

	// Validate RBAC roles so a typo can't silently grant no access (rank 0).
	for tok, role := range config.AdminTokens {
		if roleRank(role) == 0 {
			return nil, fmt.Errorf("admin_tokens: token %q has invalid role %q (want read|write|admin)", tok, role)
		}
	}

	return &config, nil
}

// validateCluster enforces the invariants a Raft cluster configuration must
// satisfy (H12): the cluster must be non-empty, the local node_id must appear
// in it, and every member ID must be unique.
func validateCluster(config *Config) error {
	if len(config.Cluster) == 0 {
		return fmt.Errorf("cluster is empty: at least one member is required")
	}
	seen := make(map[string]struct{}, len(config.Cluster))
	localPresent := false
	for _, m := range config.Cluster {
		if m.ID == "" {
			return fmt.Errorf("cluster member has empty id")
		}
		if _, dup := seen[m.ID]; dup {
			return fmt.Errorf("duplicate cluster member id %q", m.ID)
		}
		seen[m.ID] = struct{}{}
		if m.ID == config.NodeID {
			localPresent = true
		}
	}
	if !localPresent {
		return fmt.Errorf("local node_id %q not present in cluster", config.NodeID)
	}
	return nil
}

// debugAddrIsLoopback reports whether addr binds only to the loopback
// interface (or an unspecified host that Go resolves per-interface). Used to
// decide whether an unauthenticated pprof server may be exposed (H12).
func debugAddrIsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// No port separator; treat the whole string as the host.
		host = addr
	}
	if host == "" || host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func NewServer(config *Config, logger *zap.Logger) (*Server, error) {
	s := &Server{
		config:       config,
		logger:       logger,
		limiter:      newWriteLimiter(config.RateLimitRPS),
		watchCancels: make(map[int64]context.CancelFunc),
	}

	// Parse trusted proxy CIDRs so clientIP() can extract real IPs.
	for _, cidr := range config.TrustedProxyCIDRs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted_proxy_cidr %q: %w", cidr, err)
		}
		s.trustedNets = append(s.trustedNets, network)
	}

	// Initialize OpenTelemetry tracing.
	ctx := context.Background()
	if config.OtlpEndpoint != "" {
		tp, err := tracing.NewOTLPProvider(ctx, config.OtlpEndpoint, config.NodeID)
		if err != nil {
			logger.Warn("failed to create OTLP tracing provider; using noop", zap.Error(err))
			s.tracingProvider = tracing.NewNoopProvider()
		} else {
			s.tracingProvider = tp
			logger.Info("OpenTelemetry tracing enabled", zap.String("endpoint", config.OtlpEndpoint))
		}
	} else {
		s.tracingProvider = tracing.NewNoopProvider()
	}

	if err := s.initRaft(); err != nil {
		return nil, err
	}

	s.initHTTP()

	// Launch background goroutine to sweep stale per-IP rate limiter entries.
	// The context is canceled in Shutdown() to stop the goroutine cleanly.
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	s.sweepCancel = sweepCancel
	go s.sweepPerIPLimiters(sweepCtx)

	return s, nil
}

// tlsConfigured reports whether any TLS material is configured for the Raft
// transport, in which case per-peer authorization should be enforced (H-S1).
func (s *Server) tlsConfigured() bool {
	return s.config.TLSCert != "" || s.config.TLSKey != "" || s.config.TLSCA != ""
}

// allowedMemberSetter is implemented by transports that support restricting
// which peer identities may drive consensus (H-S1). Both the gRPC and TCP
// transports satisfy it.
type allowedMemberSetter interface {
	SetAllowedMembers([]string)
}

// applyAllowedMembers passes the configured cluster member IDs to the transport
// so it can reject RPCs from certificates that are not an expected member (H-S1).
func (s *Server) applyAllowedMembers(trans raft.Transport) {
	ids := make([]string, 0, len(s.config.Cluster))
	for _, m := range s.config.Cluster {
		ids = append(ids, m.ID)
	}
	if setter, ok := trans.(allowedMemberSetter); ok {
		setter.SetAllowedMembers(ids)
		s.logger.Info("peer authorization enabled", zap.Int("allowed_members", len(ids)))
	}
}

func (s *Server) initRaft() error {
	dataDir := s.config.DataDir
	nodeDir := fmt.Sprintf("%s/%s", dataDir, s.config.NodeID)

	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		return err
	}

	wal, err := storage.NewWAL(nodeDir+"/wal", nil)
	if err != nil {
		return err
	}

	stable, err := storage.NewStableStore(nodeDir + "/stable.db")
	if err != nil {
		return err
	}

	snapshot, err := storage.NewFileSnapshotStore(nodeDir, 2)
	if err != nil {
		return err
	}
	if s.config.SnapshotCompression {
		snapshot.SetCompression(true)
	}

	var servers []raft.Server
	for _, member := range s.config.Cluster {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(member.ID),
			Address: raft.ServerAddress(member.Address),
		})
	}

	raftConfig := &raft.Config{
		LocalID:       raft.ServerID(s.config.NodeID),
		ElectionTick:  s.config.ElectionTick,
		HeartbeatTick: s.config.HeartbeatTick,
		CheckQuorum:   s.config.CheckQuorum,
		InitialConfiguration: raft.Configuration{
			Servers: servers,
		},
	}

	fsmStore := fsm.NewKVStore()
	s.kv = fsmStore

	// Start WatchManager so SSE clients can subscribe from any node.
	watchCtx, watchCancel := context.WithCancel(context.Background())
	s.watchCtxCancel = watchCancel
	s.watchMgr = fsm.NewWatchManager(fsmStore)
	s.watchMgr.Start(watchCtx)

	// Create handler wrapper first (raftNode will be set after raft node is created).
	wrapper := &raftHandlerWrapper{}

	var trans raft.Transport

	if s.config.Transport == "grpc" {
		// gRPC transport with optional mTLS.
		var (
			gt  *transport.GrpcTransport
			err error
		)
		if s.config.TLSCert != "" && s.config.TLSKey != "" && s.config.TLSCA != "" {
			// mTLS mode: load cert/key/CA from config.
			cert, tlsErr := tls.LoadX509KeyPair(s.config.TLSCert, s.config.TLSKey)
			if tlsErr != nil {
				return fmt.Errorf("grpc: load cert/key: %w", tlsErr)
			}
			ca, caErr := os.ReadFile(s.config.TLSCA)
			if caErr != nil {
				return fmt.Errorf("grpc: read CA cert: %w", caErr)
			}
			gt, err = transport.NewGrpcTransport(s.config.ListenAddr, s.logger, cert, ca)
			if err == nil {
				// #204: register cert paths so a SIGHUP can rotate them at runtime.
				gt.SetCertPaths(s.config.TLSCert, s.config.TLSKey, s.config.TLSCert, s.config.TLSKey)
				s.reloadTLS = gt.ReloadTLS
			}
		} else {
			gt, err = transport.NewGrpcTransportInsecure(s.config.ListenAddr, s.logger)
		}
		if err != nil {
			return fmt.Errorf("grpc transport: %w", err)
		}
		// C11: enforce fail-closed TLS when configured, so a peer can never be
		// dialed in plaintext.
		if s.config.RequireTLS {
			gt.SetRequireTLS(true)
		}
		if s.config.GRPCCompression {
			gt.SetCompression(true)
		}

		grpcWrapper := &grpcHandlerWrapper{raftWrapper: wrapper}
		gt.SetRaftHandler(grpcWrapper)

		for _, member := range s.config.Cluster {
			if member.ID != s.config.NodeID {
				if addErr := gt.AddPeer(raft.ServerID(member.ID), raft.ServerAddress(member.Address)); addErr != nil {
					s.logger.Warn("grpc: failed to add peer",
						zap.String("peer", member.ID), zap.Error(addErr))
				}
			}
		}
		trans = gt
	} else {
		// Default: JSON-over-TCP transport (with optional TLS).
		var tcpTLSCfg *tls.Config
		if s.config.TLSCert != "" || s.config.TLSKey != "" || s.config.TLSCA != "" {
			tcpTLSCfg, err = transport.LoadTLSConfig(&transport.TCPTLSConfig{
				CertFile: s.config.TLSCert,
				KeyFile:  s.config.TLSKey,
				CAFile:   s.config.TLSCA,
			})
			if err != nil {
				return fmt.Errorf("TCP TLS config: %w", err)
			}
			s.logger.Info("TCP transport TLS enabled")
		}
		tcpTrans, err := transport.NewTCPTransportWithConfig(
			s.config.ListenAddr,
			wrapper,
			transport.TCPTransportConfig{
				Timeout:         10 * time.Second,
				Logger:          s.logger,
				TLS:             tcpTLSCfg,
				BinaryTransport: s.config.BinaryTransport,
			},
		)
		if err != nil {
			return err
		}
		for _, member := range s.config.Cluster {
			if member.ID != s.config.NodeID {
				if err := tcpTrans.AddPeer(raft.ServerID(member.ID), raft.ServerAddress(member.Address)); err != nil {
					s.logger.Warn("failed to add static peer",
						zap.String("peer", member.ID), zap.Error(err))
				}
			}
		}
		trans = tcpTrans
	}

	raftNode, err := raft.NewRaft(
		raftConfig,
		raft.ServerID(s.config.NodeID),
		wal,
		stable,
		snapshot,
		fsmStore,
		trans,
	)
	if err != nil {
		return err
	}

	// H-S1: when TLS is configured, restrict which peer certificate identities
	// may drive consensus / RaftAdmin to the configured cluster member IDs, so a
	// cert that merely chains to the CA cannot inject config changes. Left open
	// (no-op) for plaintext/dev deployments.
	if s.tlsConfigured() {
		s.applyAllowedMembers(trans)
	}

	// Wire the raft node into the handler wrapper now that it exists.
	wrapper.raftNode = raftNode

	s.raftNode = raftNode
	s.raftTransport = trans

	// Enable the raft node's internal logger so election/replication events
	// are visible in the test harness output.
	if rl, ok := raftNode.(interface{ SetLogger(*zap.Logger) }); ok {
		rl.SetLogger(s.logger.Named("raft"))
	}

	return nil
}

// corsMiddleware adds CORS headers so the admin UI can reach raftd directly
// from a browser without a reverse proxy.
//
// M13: CORS defaults to DENY. Cross-origin access must be opted into via an
// explicit allowlist in config.CORSOrigins (comma-separated origins, or the
// literal "*" to allow all — which is only honored when set explicitly). When
// the request Origin is not in the allowlist no Access-Control-Allow-Origin
// header is emitted, so the browser blocks the response.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	allowed := parseCORSOrigins(s.config.CORSOrigins)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if allow := corsAllowOrigin(allowed, origin); allow != "" {
			w.Header().Set("Access-Control-Allow-Origin", allow)
			if allow != "*" {
				// Responses vary by Origin when we echo a specific one.
				w.Header().Add("Vary", "Origin")
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}
		// Handle pre-flight requests.
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// parseCORSOrigins splits the configured comma-separated origin allowlist.
func parseCORSOrigins(raw string) []string {
	if raw == "" {
		return nil
	}
	var out []string
	for _, o := range strings.Split(raw, ",") {
		if o = strings.TrimSpace(o); o != "" {
			out = append(out, o)
		}
	}
	return out
}

// corsAllowOrigin returns the value to set for Access-Control-Allow-Origin, or
// "" if the origin is not allowed. An allowlist containing "*" allows any
// origin; otherwise the request Origin must match an allowlisted entry exactly.
func corsAllowOrigin(allowed []string, origin string) string {
	for _, a := range allowed {
		if a == "*" {
			return "*"
		}
		if origin != "" && a == origin {
			return origin
		}
	}
	return ""
}

// rateLimitMiddleware rejects write requests when either the global or the
// per-IP token bucket is empty.
// Read-only endpoints (GET, HEAD) are never rate-limited.
func (s *Server) rateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			// Global limit.
			if !s.limiter.Allow() {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				writeJSON(w, map[string]string{"error": "rate limit exceeded"})
				return
			}
			// Per-IP limit — use real client IP (respects trusted proxy headers).
			if !s.perIPLimiterFor(s.clientIP(r)).Allow() {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				writeJSON(w, map[string]string{"error": "per-IP rate limit exceeded"})
				return
			}
		}
		next(w, r)
	}
}

// waitApplied blocks until the node's FSM has applied at least idx, or ctx
// is canceled.  Used by linearizable reads (ReadIndex) to ensure the local
// FSM reflects the confirmed commit state before serving the response.
//
// L9: this delegates to the raft node's WaitApplied, which is woken by an
// applyIndex-change notification, instead of the previous 5ms busy-poll loop.
func (s *Server) waitApplied(ctx context.Context, idx uint64) error {
	return s.raftNode.WaitApplied(ctx, idx)
}

func (s *Server) initHTTP() {
	s.http = &http.Server{
		Addr:         s.config.HTTPAddr,
		Handler:      s.requestIDMiddleware(s.corsMiddleware(s.buildMux())),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
}

// requestIDHeader is the HTTP header carrying the per-request correlation ID
// (M-O2). It is generated if absent and propagated on the leader-forward hop.
const requestIDHeader = "X-Request-ID"

// requestIDKey is the context key under which the request ID is stored.
type requestIDKey struct{}

// newRequestID returns a random 128-bit hex request ID (M-O2).
func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fall back to a time-based value; correlation is best-effort.
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b[:])
}

// requestIDFromContext returns the request ID stored on ctx, or "".
func requestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey{}).(string); ok {
		return v
	}
	return ""
}

// requestIDMiddleware ensures every request carries an X-Request-ID, generating
// one if the client did not supply it, echoing it in the response, and stashing
// it in the request context so error-path logs and the leader-forward hop can
// correlate a single client request across nodes (M-O2).
func (s *Server) requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = newRequestID()
			r.Header.Set(requestIDHeader, id)
		}
		w.Header().Set(requestIDHeader, id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// instrument wraps a route handler so its latency is recorded in
// raft_http_request_duration_seconds labeled by handler/method/code (M-O1).
func instrument(name string, h http.HandlerFunc) http.HandlerFunc {
	wrapped := promhttp.InstrumentHandlerDuration(
		metrics.HTTPRequestDuration.MustCurryWith(prometheus.Labels{"handler": name}),
		h,
	)
	return wrapped.ServeHTTP
}

// buildMux registers all HTTP routes and returns the mux. Extracted from
// initHTTP so route wiring (including auth gating) is unit-testable.
func (s *Server) buildMux() *http.ServeMux {
	mux := http.NewServeMux()

	// Health/readiness are intentionally unauthenticated.
	mux.HandleFunc("/health", instrument("health", s.handleHealth))
	mux.HandleFunc("/ready", instrument("ready", s.handleReady))
	// C10: /command applies arbitrary FSM writes and MUST require the write role.
	mux.HandleFunc("/command", instrument("command", s.requireRole("write", s.rateLimitMiddleware(s.handleCommand))))
	mux.HandleFunc("/admin/cluster", instrument("admin_cluster", s.authMiddleware(s.handleCluster)))
	mux.HandleFunc("/admin/snapshot", instrument("admin_snapshot", s.requireRole("admin", s.handleSnapshot)))
	// M-O3: /metrics leaks topology/term/leader. Gate it behind the read role
	// whenever any admin token is configured (or metrics_auth is set), so prod
	// deployments require auth while token-less dev stays open.
	mux.HandleFunc("/metrics", instrument("metrics", s.metricsHandler()))

	// Cluster membership management API (write role required).
	// POST   /admin/members            — add a voting server
	// DELETE /admin/members/{id}       — remove a server
	// POST   /admin/members/{id}/promote — promote learner → voter
	// POST   /admin/members/{id}/demote  — demote voter → learner
	// POST   /admin/learners           — add a non-voting learner
	mux.HandleFunc("/admin/members", instrument("admin_members", s.requireRole("admin", s.handleAdminMembers)))
	mux.HandleFunc("/admin/members/", instrument("admin_member_by_id", s.requireRole("admin", s.handleAdminMemberByID)))

	// v1 KV API routes.
	// /v1/kv/{key} — GET (linearizable/stale), PUT, DELETE
	// /v1/kv      — GET ?prefix= for range queries
	mux.HandleFunc("/v1/kv/", instrument("v1_kv", s.authMiddleware(s.rateLimitMiddleware(s.handleV1KV))))
	mux.HandleFunc("/v1/kv", instrument("v1_kv_list", s.authMiddleware(s.rateLimitMiddleware(s.handleV1KVList))))
	mux.HandleFunc("/v1/txn", instrument("v1_txn", s.authMiddleware(s.rateLimitMiddleware(s.handleV1Txn))))
	mux.HandleFunc("/v1/watch", instrument("v1_watch", s.authMiddleware(s.handleV1Watch)))
	mux.HandleFunc("/v1/status", instrument("v1_status", s.authMiddleware(s.handleV1Status)))

	return mux
}

// metricsAuthEnabled reports whether /metrics must be gated behind the read
// role: either metrics_auth was set explicitly, or any admin token is
// configured (prod), leaving token-less dev deployments open (M-O3).
func (s *Server) metricsAuthEnabled() bool {
	if s.config.MetricsAuth {
		return true
	}
	return s.config.AdminToken != "" || len(s.config.AdminTokens) > 0
}

// metricsHandler returns the /metrics handler, gated behind the read role when
// metricsAuthEnabled() (M-O3).
func (s *Server) metricsHandler() http.HandlerFunc {
	if s.metricsAuthEnabled() {
		return s.requireRole("read", s.handleMetrics)
	}
	return s.handleMetrics
}

// leaderTickLoop periodically proposes a committed tick command (#207) so the
// FSM's virtual clock advances even when no client writes are occurring, which
// triggers sweeps of expired TTL keys on all replicas. Only the leader proposes
// ticks; followers detect leadership via State() and skip. The loop exits when
// ctx is canceled (Shutdown).
func (s *Server) leaderTickLoop(ctx context.Context) {
	interval := s.config.TTLTickInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.raftNode.State() != raft.StateLeader {
				continue
			}
			cmd := fsm.EncodeTick(time.Now().UnixMilli())
			tCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, _ = s.raftNode.Apply(tCtx, cmd) // best-effort; log errors if needed
			cancel()
		}
	}
}

func (s *Server) Start() error {
	if err := s.raftNode.Start(); err != nil {
		return err
	}

	// #207: start the leader tick loop so TTL sweeps happen even without writes.
	tickCtx, tickCancel := context.WithCancel(context.Background())
	s.tickCancel = tickCancel
	go s.leaderTickLoop(tickCtx)

	// Validate HTTPS config: both cert and key must be set together.
	if (s.config.HTTPSCert != "") != (s.config.HTTPSKey != "") {
		return fmt.Errorf("https_cert and https_key must be set together (got cert=%q key=%q)",
			s.config.HTTPSCert, s.config.HTTPSKey)
	}

	go func() {
		var err error
		if s.config.HTTPSCert != "" {
			s.logger.Info("HTTPS enabled",
				zap.String("cert", s.config.HTTPSCert),
				zap.String("addr", s.config.HTTPAddr),
			)
			err = s.http.ListenAndServeTLS(s.config.HTTPSCert, s.config.HTTPSKey)
		} else {
			err = s.http.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			s.logger.Error("http server error", zap.Error(err))
		}
	}()

	if s.config.DebugAddr != "" {
		// H12: pprof exposes heap/goroutine dumps. Require auth for it
		// unconditionally. When no tokens are configured (i.e. auth is
		// effectively disabled via AllowNoAuth) refuse to expose it on a
		// non-loopback address so an unauthenticated dump can only be pulled
		// from the local host.
		noAuthConfigured := s.config.AdminToken == "" && len(s.config.AdminTokens) == 0
		if noAuthConfigured && !debugAddrIsLoopback(s.config.DebugAddr) {
			return fmt.Errorf("debug_addr %q exposes pprof without auth: configure admin_token/admin_tokens or bind debug_addr to loopback (127.0.0.1/::1)", s.config.DebugAddr)
		}
		// Wrap pprof routes with the same token auth used by the main server.
		// This prevents anyone with network access from dumping heap/goroutines.
		debugMux := http.NewServeMux()
		for _, path := range []string{
			"/debug/pprof/",
			"/debug/pprof/cmdline",
			"/debug/pprof/profile",
			"/debug/pprof/symbol",
			"/debug/pprof/trace",
		} {
			h := http.DefaultServeMux
			debugMux.Handle(path, s.authMiddleware(h.ServeHTTP))
		}
		s.debugServer = &http.Server{
			Addr:        s.config.DebugAddr,
			Handler:     debugMux,
			ReadTimeout: 30 * time.Second,
			// Allow long-running CPU profiles (up to 5 min).
			WriteTimeout: 5 * time.Minute,
		}
		go func() {
			if err := s.debugServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				s.logger.Error("debug server error", zap.Error(err))
			}
		}()
	}

	return nil
}

func (s *Server) Shutdown() {
	// M-R4: stop accepting new writes before draining so in-flight requests can
	// finish and no new proposals race the leader handoff.
	s.draining.Store(true)

	// M-R3: cancel every open watch stream now so http.Shutdown does not block
	// for the full grace period waiting on long-lived SSE connections. Each
	// stream emits a clean event: shutdown as it unwinds.
	s.cancelAllWatches()

	// Stop WatchManager goroutine.
	if s.watchCtxCancel != nil {
		s.watchCtxCancel()
	}

	// Stop the per-IP limiter sweep goroutine.
	if s.sweepCancel != nil {
		s.sweepCancel()
	}

	// Stop the leader TTL tick loop (#207).
	if s.tickCancel != nil {
		s.tickCancel()
	}

	// Graceful leader transfer: hand off leadership before shutting down so
	// that followers do not need to wait for a full election timeout.
	if s.raftNode != nil && s.raftNode.State() == raft.StateLeader {
		s.transferLeadership()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.http.Shutdown(ctx); err != nil {
		s.logger.Error("http server shutdown error", zap.Error(err))
	}

	if s.debugServer != nil {
		if err := s.debugServer.Shutdown(ctx); err != nil {
			s.logger.Error("debug server shutdown error", zap.Error(err))
		}
	}

	if err := s.raftNode.Shutdown(); err != nil {
		s.logger.Error("raft shutdown error", zap.Error(err))
	}

	// Flush and stop the OpenTelemetry tracer provider.
	if s.tracingProvider != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		if err := s.tracingProvider.Shutdown(shutCtx); err != nil {
			s.logger.Warn("tracing provider shutdown error", zap.Error(err))
		}
	}
}

// transferLeadership picks the most up-to-date follower in the current
// configuration and sends it a TimeoutNow RPC, which makes it immediately
// start a pre-vote / election.  We then wait up to 3 s for this node to step
// down.  On failure we log and continue with the normal shutdown.
func (s *Server) transferLeadership() {
	cfg := s.raftNode.Configuration()
	myID := raft.ServerID(s.config.NodeID)

	// Find a follower to transfer to: pick the one with the highest LastIndex
	// visible via the cluster /admin/cluster endpoint.  Since we are the leader
	// we just pick any peer (we do not have per-follower matchIndex here); the
	// TimeoutNow recipient will win the election when it is most up-to-date.
	var targetID raft.ServerID
	for _, srv := range cfg.Servers {
		if raft.ServerID(srv.ID) != myID {
			targetID = raft.ServerID(srv.ID)
			break
		}
	}
	if targetID == "" {
		// Single-node cluster — nothing to transfer to.
		return
	}

	s.logger.Info("initiating graceful leader transfer", zap.String("to", string(targetID)))

	tCtx, tCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer tCancel()

	if err := s.raftTransport.TimeoutNow(tCtx, targetID); err != nil {
		s.logger.Warn("leader transfer TimeoutNow failed", zap.Error(err))
		return
	}

	// Poll until we are no longer leader or timeout.
	for {
		select {
		case <-tCtx.Done():
			s.logger.Warn("leader transfer: still leader after 3s, proceeding with shutdown")
			return
		default:
		}
		if s.raftNode.State() != raft.StateLeader {
			s.logger.Info("leader transfer: successfully stepped down")
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// constantTimeEqual reports whether a and b are equal using a constant-time
// comparison (M-S1). subtle.ConstantTimeCompare requires equal-length inputs to
// avoid leaking length via an early return, so we hash-independent-length guard
// with a length check that is itself not the discriminating branch: unequal
// lengths always fail, and equal lengths are compared in constant time.
func constantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (s *Server) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// C9: fail closed when no tokens are configured. Only allow unauthenticated
		// access when the operator has explicitly opted in via AllowNoAuth (dev
		// mode), in which case the caller is granted the write role.
		if s.config.AdminToken == "" && len(s.config.AdminTokens) == 0 {
			if s.config.AllowNoAuth {
				// No-auth dev mode grants full access (admin implies write+read).
				ctx := context.WithValue(r.Context(), roleContextKey{}, "admin")
				next(w, r.WithContext(ctx))
				return
			}
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "authentication required: configure admin_token/admin_tokens or set allow_no_auth"})
			return
		}

		// M13: only accept the token from the Authorization header. The
		// previous ?token= query-param fallback leaked credentials into
		// access logs, proxies, and browser history.
		token := r.Header.Get("Authorization")
		// Strip "Bearer " prefix if present.
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}

		// Determine the role for this token. M-S1: use constant-time comparison
		// so an attacker cannot recover a valid token byte-by-byte via timing.
		role := ""
		if s.config.AdminToken != "" && constantTimeEqual(token, s.config.AdminToken) {
			// The legacy single admin_token is the cluster's admin credential and
			// gets the "admin" role (implies write+read), preserving its ability to
			// perform membership/snapshot operations.
			role = "admin"
		} else if s.config.AdminTokens != nil {
			// Constant-time scan: compare against every configured token so the
			// runtime does not depend on which (if any) token matched.
			for candidate, r := range s.config.AdminTokens {
				if constantTimeEqual(token, candidate) {
					role = r
				}
			}
		}

		if role == "" {
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
			return
		}

		// Propagate the role through the request context.
		ctx := context.WithValue(r.Context(), roleContextKey{}, role)
		next(w, r.WithContext(ctx))
	}
}

// roleRank orders the RBAC roles: admin > write > read. An unknown/empty role
// ranks 0 (no access). A caller satisfies a required role when its rank is >=
// the required rank, so admin implies write+read and write implies read.
func roleRank(role string) int {
	switch role {
	case "admin":
		return 3
	case "write":
		return 2
	case "read":
		return 1
	default:
		return 0
	}
}

// requireRole wraps authMiddleware and additionally enforces that the
// authenticated caller has at least the specified role.
// Supported roles: "read", "write", "admin" (admin implies write implies read).
// Membership and snapshot operations require "admin"; data writes require
// "write".
func (s *Server) requireRole(role string, next http.HandlerFunc) http.HandlerFunc {
	return s.authMiddleware(func(w http.ResponseWriter, r *http.Request) {
		userRole, _ := r.Context().Value(roleContextKey{}).(string)
		if roleRank(userRole) < roleRank(role) {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{"error": role + " role required"})
			return
		}
		next(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// healthChecker is the optional interface a raft node implements to report a
// fatal storage/FSM error (H-R2). The concrete *raft node satisfies it via
// Healthy(); stubs may or may not. A node that does not implement it is
// treated as healthy.
type healthChecker interface {
	Healthy() bool
}

// nodeHealthy reports whether the raft node has not hit a fatal storage/FSM
// error. Nodes that do not implement healthChecker are assumed healthy.
func (s *Server) nodeHealthy() bool {
	if hc, ok := s.raftNode.(healthChecker); ok {
		return hc.Healthy()
	}
	return true
}

// handleReady is the readiness probe (H-O1). A node is ready only when it is a
// Follower or Leader, it knows a current leader (Leader() != ""), and it has
// not hit a fatal storage/FSM error (Healthy()). A partitioned follower with no
// known leader — or a node with a fatal disk error — reports 503 so a load
// balancer takes it out of rotation. /health remains a pure liveness probe.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	state := s.raftNode.State()
	stateOK := state == raft.StateLeader || state == raft.StateFollower
	if stateOK && s.raftNode.Leader() != "" && s.nodeHealthy() && !s.draining.Load() {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	w.Write([]byte("not ready"))
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	config := s.raftNode.Configuration()

	response := map[string]interface{}{
		"node_id":    s.config.NodeID,
		"state":      s.raftNode.State().String(),
		"leader":     s.raftNode.Leader(),
		"term":       s.raftNode.Term(),
		"commit_idx": s.raftNode.AppliedIndex(),
		"config":     config,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleCommand(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.StartSpan(r.Context(), "kv", "kv.Command")
	defer span.End()
	r = r.WithContext(ctx)

	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// M-R4: refuse new writes once draining has begun so in-flight requests can
	// finish and the log can be handed off cleanly.
	if s.draining.Load() {
		s.writeDrainingResponse(w)
		return
	}

	leader := s.raftNode.Leader()
	if leader == "" {
		metrics.RecordRejection("not_leader")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "no leader elected"})
		return
	}

	if string(leader) != s.config.NodeID {
		// H6: bound the forwarded body, carry the request context, validate the
		// leader address, and use https:// when TLS is configured.
		r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxRequestBodyBytes)
		// forwardToLeader writes its own error response to w on failure.
		_ = s.forwardToLeader(w, r, string(leader), "/command")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxRequestBodyBytes)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeGenericError(w, http.StatusBadRequest, "invalid request body", err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	result, err := s.raftNode.Apply(ctx, data)
	if err != nil {
		metrics.RecordProposal("error")
		if errors.Is(err, raft.ErrNotLeader) {
			metrics.RecordRejection("not_leader")
		}
		s.logger.Warn("Apply failed",
			zap.Error(err),
			zap.String("state", s.raftNode.State().String()),
			zap.String("leader", string(s.raftNode.Leader())),
			zap.String("request_id", requestIDFromContext(r.Context())),
		)
		s.writeGenericError(w, http.StatusInternalServerError, "internal error", err)
		return
	}
	metrics.RecordProposal("ok")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"result": string(result)})
}

// getMemberAddress returns the HTTP address of the given node for leader
// forwarding.  If http_address is set in the cluster config it is used
// directly; otherwise the raft address is returned as a fallback.
func (s *Server) getMemberAddress(nodeID string) string {
	for _, member := range s.config.Cluster {
		if member.ID == nodeID {
			if member.HTTPAddress != "" {
				return member.HTTPAddress
			}
			return member.Address
		}
	}
	return ""
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		if err := s.raftNode.Snapshot(); err != nil {
			s.writeGenericError(w, http.StatusInternalServerError, "snapshot failed", err)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

// ---------------------------------------------------------------------------
// Cluster membership management API
// ---------------------------------------------------------------------------

// memberRequest is the JSON body for POST /admin/members and
// POST /admin/learners.
type memberRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// handleAdminMembers handles:
//
//	POST /admin/members  — add a voting server (must be leader)
func (s *Server) handleAdminMembers(w http.ResponseWriter, r *http.Request) {
	// GET lists the current members.
	if r.Method == http.MethodGet {
		cfg := s.raftNode.Configuration()
		writeJSON(w, map[string]interface{}{"members": cfg.Servers})
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if s.raftNode.State() != raft.StateLeader {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]string{"error": "not leader"})
		return
	}

	var req memberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" || req.Address == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]string{"error": "id and address are required"})
		return
	}

	// Idempotent: if already a voting member, report conflict rather than
	// issuing a redundant config change.
	cfg := s.raftNode.Configuration()
	if cfg.IsVoter(raft.ServerID(req.ID)) {
		w.WriteHeader(http.StatusConflict)
		writeJSON(w, map[string]string{"status": "already a member", "id": req.ID})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Add the voting member via a single joint-consensus configuration change.
	// (For brand-new nodes that must catch up first, prefer POST /admin/learners
	// followed by POST /admin/members/{id}/promote once caught up — Raft permits
	// only one outstanding configuration change at a time.)
	if err := s.raftNode.AddServer(ctx, raft.ServerID(req.ID), raft.ServerAddress(req.Address)); err != nil {
		if errors.Is(err, raft.ErrConfigChangeInProgress) {
			s.writeGenericError(w, http.StatusConflict, "configuration change already in progress", err)
			return
		}
		s.writeGenericError(w, http.StatusInternalServerError, "failed to add member", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	writeJSON(w, map[string]string{"status": "ok", "id": req.ID})
}

// handleAdminMemberByID handles:
//
//	DELETE /admin/members/{id}          — remove a server
//	POST   /admin/members/{id}/promote  — promote learner → voter
//	POST   /admin/members/{id}/demote   — demote voter → learner
func (s *Server) handleAdminMemberByID(w http.ResponseWriter, r *http.Request) {
	// Path is /admin/members/{id} or /admin/members/{id}/promote|demote
	rest := strings.TrimPrefix(r.URL.Path, "/admin/members/")
	rest = strings.Trim(rest, "/")

	// Split optional action suffix.
	id, action, _ := strings.Cut(rest, "/")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]string{"error": "member id is required"})
		return
	}

	if s.raftNode.State() != raft.StateLeader {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]string{"error": "not leader"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	serverID := raft.ServerID(id)

	switch {
	case r.Method == http.MethodDelete && action == "":
		if err := s.raftNode.RemoveServer(ctx, serverID); err != nil {
			s.writeGenericError(w, http.StatusInternalServerError, "failed to remove member", err)
			return
		}
		writeJSON(w, map[string]string{"status": "removed", "id": id})

	case r.Method == http.MethodPost && action == "promote":
		if err := s.raftNode.PromoteLearner(ctx, serverID); err != nil {
			s.writeGenericError(w, http.StatusInternalServerError, "failed to promote learner", err)
			return
		}
		writeJSON(w, map[string]string{"status": "promoted", "id": id})

	case r.Method == http.MethodPost && action == "demote":
		if err := s.raftNode.Demote(ctx, serverID); err != nil {
			s.writeGenericError(w, http.StatusInternalServerError, "failed to demote member", err)
			return
		}
		writeJSON(w, map[string]string{"status": "demoted", "id": id})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ---------------------------------------------------------------------------
// v1 API handlers
// ---------------------------------------------------------------------------

// ensureLeader checks if this node is the leader. If not, it forwards the
// request to the leader's HTTP address, writes the response, and returns a
// non-nil sentinel error to signal the caller to stop processing.
// Watch endpoints must NOT call ensureLeader (all nodes serve watches).
func (s *Server) ensureLeader(w http.ResponseWriter, r *http.Request, path string) error {
	leader := s.raftNode.Leader()
	if leader == "" {
		metrics.RecordRejection("not_leader")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "no leader elected"})
		return fmt.Errorf("no leader")
	}
	// Tell the client the leader's address so it can route future writes there
	// directly (X-Raft-Leader-Address). Best-effort: only when a usable address
	// is configured for the leader.
	if addr := s.getMemberAddress(string(leader)); addr != "" {
		w.Header().Set("X-Raft-Leader-Address", addr)
	}
	if string(leader) == s.config.NodeID {
		return nil // this node is the leader
	}

	if err := s.forwardToLeader(w, r, string(leader), path); err != nil {
		return err
	}
	// Successfully forwarded — signal the caller to stop processing.
	return errForwarded
}

// errForwarded is a sentinel returned by ensureLeader when the request was
// forwarded to the leader and the response already written.
var errForwarded = errors.New("forwarded to leader")

// buildLeaderScheme returns the URL scheme to use when forwarding to the leader.
// When the HTTP API itself is served over TLS (HTTPSCert set) peers also speak
// https, so forwarding must use https:// to avoid a downgrade (H6).
func (s *Server) leaderScheme() string {
	if s.config.HTTPSCert != "" {
		return "https"
	}
	return "http"
}

// forwardToLeader proxies the current request to the leader's HTTP address.
//
// H6 hardening:
//   - the leader address comes from static config and is validated as a
//     host:port before use (guards against SSRF via a mutable membership
//     address that could otherwise point the forward hop at an arbitrary host);
//   - the scheme follows the local TLS setting (https when HTTPSCert is set)
//     rather than a hard-coded http://, preventing a plaintext downgrade of the
//     forwarded Authorization header;
//   - the caller's request context is propagated so client cancellation aborts
//     the forward hop.
//
// On any failure the appropriate status + generic body is written and a non-nil
// error is returned. On success the leader's response is copied through and nil
// is returned.
func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request, leaderID, path string) error {
	forwardAddr := s.getMemberAddress(leaderID)
	if forwardAddr == "" {
		s.logger.Warn("leader address not found", zap.String("leader", leaderID))
		s.writeGenericError(w, http.StatusServiceUnavailable, "leader address unknown", nil)
		return fmt.Errorf("leader address unknown for %q", leaderID)
	}
	// Validate the forward target: must be a well-formed host:port from config.
	if _, _, err := net.SplitHostPort(forwardAddr); err != nil {
		s.logger.Error("invalid leader forward address",
			zap.String("leader", leaderID),
			zap.String("addr", forwardAddr),
			zap.Error(err),
		)
		s.writeGenericError(w, http.StatusServiceUnavailable, "leader address invalid", err)
		return fmt.Errorf("invalid leader forward address %q: %w", forwardAddr, err)
	}

	forwardURL := s.leaderScheme() + "://" + forwardAddr + path
	if r.URL.RawQuery != "" {
		forwardURL += "?" + r.URL.RawQuery
	}

	//nolint:gosec // G704: forwardURL is built from a config-sourced leader address that is validated with net.SplitHostPort above; not user-controlled.
	req, err := http.NewRequestWithContext(r.Context(), r.Method, forwardURL, r.Body)
	if err != nil {
		s.writeGenericError(w, http.StatusInternalServerError, "internal error", err)
		return err
	}
	// Copy through client headers (including the Authorization header, now only
	// ever sent over the scheme matching our own TLS posture).
	for k, vs := range r.Header {
		req.Header[k] = vs
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	// M-O2: propagate the request ID so the leader's logs correlate with ours.
	if reqID := requestIDFromContext(r.Context()); reqID != "" {
		req.Header.Set(requestIDHeader, reqID)
	}

	client := &http.Client{Timeout: 8 * time.Second}
	//nolint:gosec // G704: req targets the validated, config-sourced leader address (see above); not user-controlled.
	resp, err := client.Do(req)
	if err != nil {
		s.logger.Warn("forwarding to leader failed",
			zap.Error(err),
			zap.String("to", forwardAddr),
			zap.String("leader", leaderID),
			zap.String("request_id", requestIDFromContext(r.Context())),
		)
		metrics.RecordRejection("not_leader")
		s.writeGenericError(w, http.StatusServiceUnavailable, "failed to reach leader", err)
		return err
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		w.Header()[k] = vs
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body) //nolint:errcheck
	return nil
}

// recordApplyRejection records a rejection metric when an Apply failed because
// this node is not the leader (M-O1).
func (s *Server) recordApplyRejection(err error) {
	if errors.Is(err, raft.ErrNotLeader) {
		metrics.RecordRejection("not_leader")
	}
}

// writeDrainingResponse rejects a write with 503 during shutdown drain (M-R4).
func (s *Server) writeDrainingResponse(w http.ResponseWriter) {
	metrics.RecordRejection("draining")
	w.Header().Set("Retry-After", "1")
	w.WriteHeader(http.StatusServiceUnavailable)
	writeJSON(w, map[string]string{"error": "server is shutting down"})
}

// writeJSON is a small helper to set Content-Type and encode a value.
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// Key/value size limits enforced on PUT (L8).
const (
	maxKeySize   = 4 << 10   // 4 KiB
	maxValueSize = 512 << 10 // 512 KiB
)

// validateKeyValue enforces the maximum key/value sizes (L8).
func validateKeyValue(key, value string) error {
	if len(key) > maxKeySize {
		return fmt.Errorf("key too large: %d bytes (max %d)", len(key), maxKeySize)
	}
	if len(value) > maxValueSize {
		return fmt.Errorf("value too large: %d bytes (max %d)", len(value), maxValueSize)
	}
	return nil
}

// isJSONContentType reports whether the Content-Type header denotes JSON,
// ignoring any charset/parameters (L8).
func isJSONContentType(ct string) bool {
	if ct == "" {
		return false
	}
	// Strip parameters like "; charset=utf-8".
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.EqualFold(strings.TrimSpace(ct), "application/json")
}

// parseSeqNum parses the optional X-Seq-Num header. It returns (0, true) when
// the header is absent and (n, true) when it is a valid unsigned integer. On a
// malformed value it writes a 400 and returns (0, false) (L7).
func parseSeqNum(w http.ResponseWriter, s *Server, r *http.Request) (uint64, bool) {
	raw := r.Header.Get("X-Seq-Num")
	if raw == "" {
		return 0, true
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		s.writeGenericError(w, http.StatusBadRequest, "invalid X-Seq-Num", err)
		return 0, false
	}
	return n, true
}

// writeError maps common Raft errors to appropriate HTTP status codes.
//
// M12: internal error detail (dial errors, host:port, wrapped Go errors) is
// logged server-side but never returned to the client. Clients receive a
// generic, status-appropriate message.
func (s *Server) writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		w.WriteHeader(http.StatusMisdirectedRequest)
		writeJSON(w, map[string]string{"error": "not leader"})
	default:
		s.logger.Warn("request failed", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]string{"error": "internal error"})
	}
}

// writeGenericError logs the full error server-side and returns a generic
// message with the given status code, so internal details never reach the
// client (M12).
func (s *Server) writeGenericError(w http.ResponseWriter, status int, msg string, err error) {
	if err != nil {
		s.logger.Warn("request failed",
			zap.Int("status", status),
			zap.String("msg", msg),
			zap.Error(err),
		)
	}
	w.WriteHeader(status)
	writeJSON(w, map[string]string{"error": msg})
}

// handleV1KV dispatches GET/PUT/DELETE for /v1/kv/{key}.
func (s *Server) handleV1KV(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.StartSpan(r.Context(), "kv", "kv."+r.Method)
	defer span.End()
	r = r.WithContext(ctx)

	key := strings.TrimPrefix(r.URL.Path, "/v1/kv/")
	if key == "" {
		http.Error(w, `{"error":"key required"}`, http.StatusBadRequest)
		return
	}
	span.SetAttributes(attribute.String("kv.key", key))

	switch r.Method {
	case http.MethodGet:
		s.handleV1Get(w, r, key)
	case http.MethodPut, http.MethodPost:
		if r.URL.Query().Get("op") == "incr" {
			s.handleV1Incr(w, r, key)
			return
		}
		s.handleV1Put(w, r, key)
	case http.MethodDelete:
		s.handleV1Delete(w, r, key)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleV1Incr atomically adds a signed delta to an integer-valued key and
// returns the updated KeyValue. POST /v1/kv/{key}?op=incr with body {"delta": N}.
func (s *Server) handleV1Incr(w http.ResponseWriter, r *http.Request, key string) {
	if s.draining.Load() {
		s.writeDrainingResponse(w)
		return
	}
	// Pass only the path; forwardToLeader re-appends r.URL.RawQuery (?op=incr),
	// so embedding the query here would double it and break dispatch on the leader.
	if err := s.ensureLeader(w, r, "/v1/kv/"+key); err != nil {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxRequestBodyBytes)
	var body struct {
		Delta int64 `json:"delta"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeGenericError(w, http.StatusBadRequest, "invalid JSON body (expected {\"delta\": <int>})", err)
		return
	}
	if err := validateKeyValue(key, ""); err != nil {
		s.writeGenericError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}

	clientID := r.Header.Get("X-Client-ID")
	seqNum, ok := parseSeqNum(w, s, r)
	if !ok {
		return
	}

	deltaStr := strconv.FormatInt(body.Delta, 10)
	var data []byte
	var err error
	if clientID != "" && seqNum > 0 {
		data, err = fsm.EncodeCommandWithID("incr", key, deltaStr, clientID, seqNum)
	} else {
		data, err = fsm.EncodeCommand("incr", key, deltaStr)
	}
	if err != nil {
		s.writeError(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	result, err := s.raftNode.Apply(ctx, data)
	if err != nil {
		metrics.RecordProposal("error")
		s.recordApplyRejection(err)
		s.writeError(w, err)
		return
	}
	metrics.RecordProposal("ok")

	// An FSM-level rejection (non-integer value/delta, overflow) surfaces as a
	// KvResult error, not a transport error.
	if kvres, derr := fsm.DecodeResult(result); derr == nil && kvres.Error != "" {
		s.writeGenericError(w, http.StatusBadRequest, kvres.Error, nil)
		return
	}
	kv, err := fsm.DecodeKeyValueResult(result)
	if err != nil {
		s.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	writeJSON(w, kv)
}

// handleV1Get serves GET /v1/kv/{key}.
// ?consistency=stale → local FSM read (no Raft round-trip, may be slightly behind).
// default (linearizable) → ReadIndex lease + FSM read (no WAL write, correct).
func (s *Server) handleV1Get(w http.ResponseWriter, r *http.Request, key string) {
	if r.URL.Query().Get("consistency") == "stale" {
		kv, err := s.kv.Get(key)
		if err != nil {
			s.writeError(w, err)
			return
		}
		if kv == nil {
			w.WriteHeader(http.StatusNotFound)
			writeJSON(w, map[string]string{"error": "key not found"})
			return
		}
		writeJSON(w, kv)
		return
	}

	// Linearizable read via ReadIndex (no WAL entry written).
	// 1. Ensure this node is the leader (or forward to leader).
	if err := s.ensureLeader(w, r, "/v1/kv/"+key); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// 2. Confirm quorum via leader lease — returns the safe commit index.
	idx, err := s.raftNode.ReadIndex(ctx)
	if err != nil {
		s.writeError(w, err)
		return
	}

	// 3. Wait until the FSM has applied up to idx so the read reflects
	//    all committed entries.
	if err := s.waitApplied(ctx, idx); err != nil {
		s.writeError(w, err)
		return
	}

	// 4. Serve directly from local FSM — safe because lease proved quorum.
	kv, err := s.kv.Get(key)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if kv == nil {
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]string{"error": "key not found"})
		return
	}
	writeJSON(w, kv)
}

// handleV1Put serves PUT /v1/kv/{key}.
// Body can be a raw string or JSON {"value":"..."}.
func (s *Server) handleV1Put(w http.ResponseWriter, r *http.Request, key string) {
	if s.draining.Load() {
		s.writeDrainingResponse(w)
		return
	}
	if err := s.ensureLeader(w, r, "/v1/kv/"+key); err != nil {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxRequestBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeGenericError(w, http.StatusBadRequest, "invalid request body", err)
		return
	}

	// L8: disambiguate the body by Content-Type instead of "try JSON, fall
	// back to raw", which silently mis-parses a raw value that happens to be
	// valid JSON. Only when Content-Type is application/json do we decode a
	// {"value":"..."} envelope; otherwise the body is the raw value.
	value := string(body)
	var ttlSeconds int64
	if isJSONContentType(r.Header.Get("Content-Type")) {
		var jsonBody struct {
			Value      string `json:"value"`
			TTLSeconds int64  `json:"ttl_seconds,omitempty"` // #207
		}
		if err := json.Unmarshal(body, &jsonBody); err != nil {
			s.writeGenericError(w, http.StatusBadRequest, "invalid JSON body", err)
			return
		}
		value = jsonBody.Value
		ttlSeconds = jsonBody.TTLSeconds
	}
	// Also accept ?ttl_seconds=N query parameter (#207).
	if qs := r.URL.Query().Get("ttl_seconds"); qs != "" && ttlSeconds == 0 {
		if n, e := strconv.ParseInt(qs, 10, 64); e == nil && n > 0 {
			ttlSeconds = n
		}
	}

	// L8: enforce key/value size limits.
	if err := validateKeyValue(key, value); err != nil {
		s.writeGenericError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}

	clientID := r.Header.Get("X-Client-ID")
	seqNum, ok := parseSeqNum(w, s, r)
	if !ok {
		return
	}

	// #207: encode TTL when requested; stamp LeaderTimestampMs at proposal time.
	var data []byte
	if ttlSeconds > 0 {
		leaderTSMs := time.Now().UnixMilli()
		data, err = fsm.EncodeCommandWithTTL("put", key, value, clientID, seqNum, leaderTSMs, ttlSeconds)
	} else if clientID != "" && seqNum > 0 {
		data, err = fsm.EncodeCommandWithID("put", key, value, clientID, seqNum)
	} else {
		data, err = fsm.EncodeCommand("put", key, value)
	}
	if err != nil {
		s.writeError(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	result, err := s.raftNode.Apply(ctx, data)
	if err != nil {
		metrics.RecordProposal("error")
		s.recordApplyRejection(err)
		s.writeError(w, err)
		return
	}
	metrics.RecordProposal("ok")

	kv, err := fsm.DecodeKeyValueResult(result)
	if err != nil {
		s.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	writeJSON(w, kv)
}

// handleV1Delete serves DELETE /v1/kv/{key}.
func (s *Server) handleV1Delete(w http.ResponseWriter, r *http.Request, key string) {
	if s.draining.Load() {
		s.writeDrainingResponse(w)
		return
	}
	if err := s.ensureLeader(w, r, "/v1/kv/"+key); err != nil {
		return
	}

	clientID := r.Header.Get("X-Client-ID")
	seqNum, ok := parseSeqNum(w, s, r)
	if !ok {
		return
	}
	var data []byte
	var err error
	if clientID != "" && seqNum > 0 {
		data, err = fsm.EncodeCommandWithID("delete", key, "", clientID, seqNum)
	} else {
		data, err = fsm.EncodeCommand("delete", key, "")
	}
	if err != nil {
		s.writeError(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	result, err := s.raftNode.Apply(ctx, data)
	if err != nil {
		metrics.RecordProposal("error")
		s.recordApplyRejection(err)
		s.writeError(w, err)
		return
	}
	metrics.RecordProposal("ok")

	res, _ := fsm.DecodeResult(result)
	if res != nil && res.Error != "" {
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]string{"error": res.Error})
		return
	}
	w.WriteHeader(http.StatusOK)
	writeJSON(w, map[string]string{"status": "deleted"})
}

// handleV1KVList serves GET /v1/kv?prefix={p}.
//
// H7: by default this is a LINEARIZABLE range read — the request is forwarded
// to the leader (if we are not it) and gated on a ReadIndex lease so it never
// returns a stale local view. Pass ?consistency=stale to explicitly opt into a
// fast local-FSM read, in which case an X-Consistency: stale response header is
// set so the client knows the result may lag the committed state.
func (s *Server) handleV1KVList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	limit := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 0 {
			s.writeGenericError(w, http.StatusBadRequest, "limit must be a non-negative integer", nil)
			return
		}
		limit = n
	}
	startAfter := r.URL.Query().Get("start_after")

	if r.URL.Query().Get("consistency") == "stale" {
		w.Header().Set("X-Consistency", "stale")
		s.serveRange(w, prefix, limit, startAfter)
		return
	}

	// Linearizable path: ensure leadership (forward otherwise), then confirm
	// quorum via ReadIndex and wait for the FSM to catch up before serving.
	if err := s.ensureLeader(w, r, "/v1/kv"); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	idx, err := s.raftNode.ReadIndex(ctx)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if err := s.waitApplied(ctx, idx); err != nil {
		s.writeError(w, err)
		return
	}
	s.serveRange(w, prefix, limit, startAfter)
}

// serveRange writes the range scan for prefix as a JSON array ([] not null).
// When limit > 0 it paginates: at most limit keys strictly after startAfter are
// returned, and the response carries X-Has-More ("true"/"false") plus
// X-Next-Cursor (the last key, to pass as the next start_after).
func (s *Server) serveRange(w http.ResponseWriter, prefix string, limit int, startAfter string) {
	if limit > 0 {
		kvs, more, err := s.kv.RangePage(prefix, startAfter, limit)
		if err != nil {
			s.writeError(w, err)
			return
		}
		if kvs == nil {
			kvs = []*fsm.KeyValue{}
		}
		w.Header().Set("X-Has-More", strconv.FormatBool(more))
		if len(kvs) > 0 {
			w.Header().Set("X-Next-Cursor", kvs[len(kvs)-1].Key)
		}
		writeJSON(w, kvs)
		return
	}

	kvs, err := s.kv.Range(prefix)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if kvs == nil {
		kvs = []*fsm.KeyValue{} // return [] not null
	}
	writeJSON(w, kvs)
}

// handleV1Txn serves POST /v1/txn — compare-and-swap transaction.
func (s *Server) handleV1Txn(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.StartSpan(r.Context(), "kv", "kv.Txn")
	defer span.End()
	r = r.WithContext(ctx)

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if s.draining.Load() {
		s.writeDrainingResponse(w)
		return
	}

	if err := s.ensureLeader(w, r, "/v1/txn"); err != nil {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxRequestBodyBytes)
	var req fsm.TxnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeGenericError(w, http.StatusBadRequest, "invalid JSON body", err)
		return
	}

	data, err := fsm.EncodeTxn(&req)
	if err != nil {
		s.writeError(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	result, err := s.raftNode.Apply(ctx, data)
	if err != nil {
		metrics.RecordProposal("error")
		s.recordApplyRejection(err)
		s.writeError(w, err)
		return
	}
	metrics.RecordProposal("ok")

	resp, err := fsm.DecodeTxnResult(result)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, resp)
}

// registerWatch stores cancel under a fresh ID so Shutdown can cancel every
// open SSE stream at drain start (M-R3). It returns the ID and a deregister
// func the handler must defer.
func (s *Server) registerWatch(cancel context.CancelFunc) (int64, func()) {
	s.watchMu.Lock()
	s.watchSeq++
	id := s.watchSeq
	if s.watchCancels == nil {
		s.watchCancels = make(map[int64]context.CancelFunc)
	}
	s.watchCancels[id] = cancel
	n := len(s.watchCancels)
	s.watchMu.Unlock()
	metrics.SetWatchConnections(n)
	return id, func() {
		s.watchMu.Lock()
		delete(s.watchCancels, id)
		n := len(s.watchCancels)
		s.watchMu.Unlock()
		metrics.SetWatchConnections(n)
	}
}

// cancelAllWatches cancels every open watch stream's request context so
// http.Shutdown does not block waiting for long-lived SSE connections (M-R3).
func (s *Server) cancelAllWatches() {
	s.watchMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(s.watchCancels))
	for _, c := range s.watchCancels {
		cancels = append(cancels, c)
	}
	s.watchMu.Unlock()
	for _, c := range cancels {
		c()
	}
}

// handleV1Watch serves GET /v1/watch — SSE stream of key change events.
// Supports ?key={key} for exact-key watch or ?prefix={prefix} for prefix watch.
// Optional ?revision={n} (or Last-Event-ID header) replays history from revision n.
// Watch is served from ANY node (no leader forwarding): all nodes apply the
// same committed log entries and thus emit the same sequence of events.
func (s *Server) handleV1Watch(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming not supported"}`, http.StatusInternalServerError)
		return
	}

	// M14: cap concurrent SSE connections globally and per client IP to bound
	// server memory / goroutines against a fan-out DoS.
	release, ok := s.acquireWatchSlot(s.clientIP(r))
	if !ok {
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]string{"error": "too many watch connections"})
		return
	}
	defer release()

	key := r.URL.Query().Get("key")
	prefix := r.URL.Query().Get("prefix")
	revStr := r.URL.Query().Get("revision")
	if revStr == "" {
		revStr = r.Header.Get("Last-Event-ID")
	}

	// L7: a malformed revision / Last-Event-ID must be a client error, not a
	// silent default to 0 (which would replay the entire history unexpectedly).
	var sinceRevision int64
	if revStr != "" {
		n, err := strconv.ParseInt(revStr, 10, 64)
		if err != nil || n < 0 {
			s.writeGenericError(w, http.StatusBadRequest, "invalid revision", err)
			return
		}
		sinceRevision = n
	}

	if key == "" && prefix == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]string{"error": "key or prefix required"})
		return
	}

	// H-R4: an SSE stream is long-lived, but the server's 60s WriteTimeout would
	// otherwise kill it (and make WatchIdleTimeout > 60s a no-op). Clear the
	// per-request write deadline so the stream can run for the full idle window.
	if rc := http.NewResponseController(w); rc != nil {
		if err := rc.SetWriteDeadline(time.Time{}); err != nil {
			// Not fatal — some ResponseWriters (e.g. httptest) don't support it.
			s.logger.Debug("watch: could not clear write deadline", zap.Error(err))
		}
	}

	// Set SSE headers before writing any body.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering

	// Determine idle timeout: default 5 min, overridable via config.
	idleTimeout := s.config.WatchIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 5 * time.Minute
	}

	// M-R3: derive a cancellable context and register it so Shutdown can cancel
	// this stream immediately (rather than blocking http.Shutdown for the full
	// grace period). The stream also emits a clean event: shutdown on cancel.
	ctx, cancelWatch := context.WithCancel(r.Context())
	defer cancelWatch()
	_, deregister := s.registerWatch(cancelWatch)
	defer deregister()

	var (
		ch      <-chan fsm.WatchEvent
		watchID fsm.WatchID
	)

	if prefix != "" {
		ch, watchID = s.watchMgr.WatchPrefix(ctx, prefix, sinceRevision)
	} else {
		ch, watchID = s.watchMgr.Watch(ctx, key, sinceRevision)
	}
	defer s.watchMgr.Cancel(watchID)

	idleTimer := time.NewTimer(idleTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case we, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(we)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "id: %d\ndata: %s\n\n", we.Revision, data)
			flusher.Flush()
			// Reset idle timer on each successfully delivered event.
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(idleTimeout)

		case <-idleTimer.C:
			// Connection has been idle for too long — close it so the client
			// reconnects (it will resume from Last-Event-ID).
			fmt.Fprintf(w, "event: timeout\ndata: {\"error\":\"idle timeout\"}\n\n")
			flusher.Flush()
			return

		case <-ctx.Done():
			// M-R3: if the server is draining, send a clean shutdown event so the
			// client reconnects to another node instead of seeing a truncated
			// stream. Resume is via Last-Event-ID.
			if s.draining.Load() {
				fmt.Fprintf(w, "event: shutdown\ndata: {\"reason\":\"server shutting down\"}\n\n")
				flusher.Flush()
			}
			return
		}
	}
}

// handleV1Status serves GET /v1/status — enhanced cluster status with revision.
func (s *Server) handleV1Status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"node_id":              s.config.NodeID,
		"state":                s.raftNode.State().String(),
		"leader":               string(s.raftNode.Leader()),
		"term":                 s.raftNode.Term(),
		"last_index":           s.raftNode.LastIndex(),
		"applied_index":        s.raftNode.AppliedIndex(),
		"revision":             s.kv.GetRevision(),
		"cluster":              s.raftNode.Configuration(),
		"fsm_dropped_events":   s.kv.DroppedEvents(),
		"watch_dropped_events": s.watchMgr.DroppedEvents(),
	})
}

// ---------------------------------------------------------------------------
// Raft handler wrapper (unchanged)
// ---------------------------------------------------------------------------

type raftHandlerWrapper struct {
	raftNode raft.Raft
}

func (h *raftHandlerWrapper) HandleAppendEntries(req *transport.AppendEntriesReq) *transport.AppendEntriesResp {
	if h.raftNode == nil {
		return &transport.AppendEntriesResp{}
	}

	entries := make([]*raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = &raft.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Type:  raft.EntryType(e.Type),
			Data:  e.Data,
		}
	}

	raftReq := &raft.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     raft.ServerID(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}

	raftResp := h.raftNode.(interface {
		HandleAppendEntriesRPC(*raft.AppendEntriesRequest) *raft.AppendEntriesResponse
	}).HandleAppendEntriesRPC(raftReq)

	return &transport.AppendEntriesResp{
		Term:         raftResp.Term,
		Success:      raftResp.Success,
		Index:        raftResp.Index,
		ConflictTerm: raftResp.ConflictTerm,
	}
}

func (h *raftHandlerWrapper) HandleRequestVote(req *transport.RequestVoteReq) *transport.RequestVoteResp {
	if h.raftNode == nil {
		return &transport.RequestVoteResp{}
	}

	raftReq := &raft.RequestVoteRequest{
		Term:           req.Term,
		CandidateID:    raft.ServerID(req.CandidateID),
		LastLogIndex:   req.LastLogIndex,
		LastLogTerm:    req.LastLogTerm,
		PreVote:        req.PreVote,
		LeaderTransfer: req.LeaderTransfer,
	}

	raftResp := h.raftNode.(interface {
		HandleRequestVoteRPC(*raft.RequestVoteRequest) *raft.RequestVoteResponse
	}).HandleRequestVoteRPC(raftReq)

	return &transport.RequestVoteResp{
		Term:        raftResp.Term,
		VoteGranted: raftResp.VoteGranted,
		Reason:      raftResp.Reason,
	}
}

func (h *raftHandlerWrapper) HandleInstallSnapshot(req *transport.InstallSnapshotReq) *transport.InstallSnapshotResp {
	if h.raftNode == nil {
		return &transport.InstallSnapshotResp{}
	}

	raftReq := &raft.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderID:          raft.ServerID(req.LeaderID),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	}

	raftResp := h.raftNode.(interface {
		HandleInstallSnapshotRPC(*raft.InstallSnapshotRequest) *raft.InstallSnapshotResponse
	}).HandleInstallSnapshotRPC(raftReq)

	return &transport.InstallSnapshotResp{
		Term: raftResp.Term,
	}
}

// timeoutNowHandler is a local interface so we can dispatch TimeoutNow without
// adding HandleTimeoutNowRPC to the public Raft interface.
type timeoutNowHandler interface {
	HandleTimeoutNowRPC()
}

func (h *raftHandlerWrapper) HandleTimeoutNow(req *transport.TimeoutNowReq) *transport.TimeoutNowResp {
	if h.raftNode != nil {
		if handler, ok := h.raftNode.(timeoutNowHandler); ok {
			handler.HandleTimeoutNowRPC()
		}
	}
	return &transport.TimeoutNowResp{}
}

// ---------------------------------------------------------------------------
// grpcHandlerWrapper adapts the gRPC transport's proto-based RaftHandler
// interface to the same raft.Raft methods used by raftHandlerWrapper.
// ---------------------------------------------------------------------------

type grpcHandlerWrapper struct {
	raftWrapper *raftHandlerWrapper
}

func (g *grpcHandlerWrapper) HandleAppendEntries(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
	entries := make([]*raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = &raft.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Type:  raft.EntryType(e.Type),
			Data:  e.Data,
		}
	}
	tcpReq := &transport.AppendEntriesReq{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}
	r := g.raftWrapper.HandleAppendEntries(tcpReq)
	return &proto.AppendEntriesResponse{
		Term:          r.Term,
		Success:       r.Success,
		ConflictIndex: r.Index,
		ConflictTerm:  r.ConflictTerm,
	}
}

func (g *grpcHandlerWrapper) HandleRequestVote(req *proto.RequestVoteRequest) *proto.RequestVoteResponse {
	tcpReq := &transport.RequestVoteReq{
		Term:           req.Term,
		CandidateID:    req.CandidateId,
		LastLogIndex:   req.LastLogIndex,
		LastLogTerm:    req.LastLogTerm,
		PreVote:        req.PreVote,
		LeaderTransfer: req.LeaderTransfer,
	}
	r := g.raftWrapper.HandleRequestVote(tcpReq)
	return &proto.RequestVoteResponse{
		Term:         r.Term,
		VoteGranted:  r.VoteGranted,
		RejectReason: r.Reason,
	}
}

func (g *grpcHandlerWrapper) HandleInstallSnapshot(req *proto.InstallSnapshotRequest) *proto.InstallSnapshotResponse {
	tcpReq := &transport.InstallSnapshotReq{
		Term:              req.Term,
		LeaderID:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	}
	r := g.raftWrapper.HandleInstallSnapshot(tcpReq)
	return &proto.InstallSnapshotResponse{Term: r.Term}
}

func (g *grpcHandlerWrapper) HandleTimeoutNow(_ *proto.TimeoutNowRequest) *proto.TimeoutNowResponse {
	g.raftWrapper.HandleTimeoutNow(nil)
	return &proto.TimeoutNowResponse{}
}

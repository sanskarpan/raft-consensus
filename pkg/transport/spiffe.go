package transport

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"go.uber.org/zap"
)

// SpiffeSource wraps a SPIFFE X.509 source providing auto-rotating TLS configs.
// The underlying workloadapi.X509Source connects to the SPIFFE Workload API
// (typically a SPIRE agent) and receives continuously-rotated X.509 SVIDs.
// Both the server and client TLS configs produced by this type delegate
// certificate selection to the live source, so cert rotation is automatic and
// requires no server restart.
type SpiffeSource struct {
	source *workloadapi.X509Source
	logger *zap.Logger
}

// NewSpiffeSource connects to the SPIFFE Workload API at socketPath and returns
// a SpiffeSource that delivers auto-rotating X.509 SVIDs. The returned source
// MUST be closed with Close() when the caller shuts down.
//
// If socketPath is empty, the library falls back to the SPIFFE_ENDPOINT_SOCKET
// environment variable (the standard SPIFFE/SPIRE convention).
//
// The context controls the initial connection: a deadline or cancellation on ctx
// will abort the handshake with the agent, but does NOT affect the background
// watch for cert updates once established.
func NewSpiffeSource(ctx context.Context, socketPath string, logger *zap.Logger) (*SpiffeSource, error) {
	var opts []workloadapi.X509SourceOption
	if socketPath != "" {
		opts = append(opts, workloadapi.WithClientOptions(workloadapi.WithAddr(socketPath)))
	}
	// workloadapi.NewX509Source blocks until the first SVID is delivered or ctx
	// is canceled.  A canceled context returns an error here, not a nil source.
	src, err := workloadapi.NewX509Source(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("spiffe: connect to workload API: %w", err)
	}
	if logger != nil {
		logger.Info("SPIFFE X.509 source established", zap.String("socket", socketPath))
	}
	return &SpiffeSource{source: src, logger: logger}, nil
}

// ServerTLSConfig returns a *tls.Config suitable for a gRPC/TLS server that
// requires peer (client) certificates and validates them as SPIFFE SVIDs
// belonging to the given trust domain (e.g. "cluster.example").
//
// The returned config is backed directly by the live X.509 source, so
// certificates rotate automatically without restarting the server.
//
// Returns an error if trustDomain is not a valid SPIFFE trust domain name.
func (s *SpiffeSource) ServerTLSConfig(trustDomain string) (*tls.Config, error) {
	if s == nil || s.source == nil {
		return nil, fmt.Errorf("spiffe: source is nil")
	}
	td, err := spiffeid.TrustDomainFromString(trustDomain)
	if err != nil {
		return nil, fmt.Errorf("spiffe: invalid trust domain %q: %w", trustDomain, err)
	}
	cfg := tlsconfig.MTLSServerConfig(s.source, s.source, tlsconfig.AuthorizeMemberOf(td))
	return cfg, nil
}

// ClientTLSConfig returns a *tls.Config for a gRPC/TLS client connecting to a
// peer that presents the given SPIFFE ID (e.g. "spiffe://cluster.example/raft/node2").
//
// The config is backed by the live X.509 source, so client certificates rotate
// without reconnecting.
//
// Returns an error if peerSpiffeID is not a valid SPIFFE ID URI.
func (s *SpiffeSource) ClientTLSConfig(peerSpiffeID string) (*tls.Config, error) {
	if s == nil || s.source == nil {
		return nil, fmt.Errorf("spiffe: source is nil")
	}
	peerID, err := spiffeid.FromString(peerSpiffeID)
	if err != nil {
		return nil, fmt.Errorf("spiffe: invalid peer SPIFFE ID %q: %w", peerSpiffeID, err)
	}
	cfg := tlsconfig.MTLSClientConfig(s.source, s.source, tlsconfig.AuthorizeID(peerID))
	return cfg, nil
}

// Close releases the underlying X.509 source and stops certificate rotation.
// After Close returns, any TLS configs produced by this source will no longer
// be refreshed. It is safe to call Close on a nil SpiffeSource.
func (s *SpiffeSource) Close() error {
	if s == nil || s.source == nil {
		return nil
	}
	if s.logger != nil {
		s.logger.Info("closing SPIFFE X.509 source")
	}
	return s.source.Close()
}

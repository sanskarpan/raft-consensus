package etcdcompat

import (
	"net"

	"github.com/sanskarpan/raft-consensus/pkg/fsm"
	pb "github.com/sanskarpan/raft-consensus/proto/etcdcompat"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// EtcdCompatServer bundles the KV and Watch gRPC services and owns the
// grpc.Server lifecycle. It is wired into cmd/raftd/main.go's Server struct
// so that Start() and Shutdown() manage it alongside the Raft and HTTP servers.
type EtcdCompatServer struct {
	grpcServer *grpc.Server
	kvServer   *KVServer
	watchServer *WatchServer
	logger      *zap.Logger
}

// EtcdCompatConfig carries the parameters needed to create an EtcdCompatServer.
type EtcdCompatConfig struct {
	// ListenAddr is the TCP address to listen on, e.g. ":2379".
	// If empty no server is started.
	ListenAddr string

	// Raft is the Raft node (used by KVServer for linearizable reads/writes).
	Raft RaftApplier

	// KV is the local FSM store (used for stale reads and watch history).
	KV *fsm.KVStore

	// WatchMgr fans out FSM events to watch subscribers.
	WatchMgr *fsm.WatchManager

	// ClusterID and MemberID are echoed in ResponseHeader (optional; default 0).
	ClusterID uint64
	MemberID  uint64

	// GRPCOptions are passed to grpc.NewServer (e.g. credentials, interceptors).
	// If nil, the server is created insecurely.
	GRPCOptions []grpc.ServerOption

	Logger *zap.Logger
}

// NewEtcdCompatServer creates an EtcdCompatServer from cfg. Call Serve() after
// creation to start accepting connections.
func NewEtcdCompatServer(cfg EtcdCompatConfig) *EtcdCompatServer {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	kvSrv := NewKVServer(cfg.Raft, cfg.KV, cfg.ClusterID, cfg.MemberID)
	watchSrv := NewWatchServer(cfg.WatchMgr, cfg.KV, cfg.ClusterID, cfg.MemberID)

	gs := grpc.NewServer(cfg.GRPCOptions...)
	pb.RegisterKVServer(gs, kvSrv)
	pb.RegisterWatchServer(gs, watchSrv)

	return &EtcdCompatServer{
		grpcServer:  gs,
		kvServer:    kvSrv,
		watchServer: watchSrv,
		logger:      logger,
	}
}

// Serve starts the gRPC listener on addr and blocks until the server is
// stopped. It is intended to be called in a goroutine.
func (e *EtcdCompatServer) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	e.logger.Info("etcd-compat gRPC server listening", zap.String("addr", addr))
	return e.grpcServer.Serve(lis)
}

// Stop performs a graceful shutdown: it waits for in-flight RPCs to complete
// before closing the listener. Pending streaming Watch RPCs will observe their
// context being canceled and return.
func (e *EtcdCompatServer) Stop() {
	e.logger.Info("stopping etcd-compat gRPC server")
	e.grpcServer.GracefulStop()
}

// GRPCServer returns the underlying grpc.Server for callers that need to
// register additional services (e.g. the Raft admin service).
func (e *EtcdCompatServer) GRPCServer() *grpc.Server {
	return e.grpcServer
}

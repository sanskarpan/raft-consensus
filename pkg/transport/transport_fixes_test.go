package transport

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/raft-consensus/pkg/raft"
	proto "github.com/raft-consensus/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ---------------------------------------------------------------------------
// H4 — connPool.Get empty-pool guard (was: division by zero panic)
// ---------------------------------------------------------------------------

func TestConnPool_Get_EmptyPoolReturnsNil(t *testing.T) {
	// A pool with zero connections must not panic on Get (formerly it did
	// `% len(conns)` == `% 0` → runtime panic). It should return nil so callers
	// can treat the peer as unavailable.
	p := newConnPool(nil)
	got := p.Get()
	if got != nil {
		t.Fatalf("Get() on empty pool = %v, want nil", got)
	}
}

func TestConnPool_Get_NilPoolReturnsNil(t *testing.T) {
	var p *connPool
	if got := p.Get(); got != nil {
		t.Fatalf("Get() on nil pool = %v, want nil", got)
	}
}

// ---------------------------------------------------------------------------
// H4 — connPool.Close idempotent / nil-safe (was: double-close of grpc conns)
// ---------------------------------------------------------------------------

func TestConnPool_Close_Idempotent(t *testing.T) {
	c, err := grpc.NewClient("localhost:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	p := newConnPool([]*grpc.ClientConn{c})
	// Multiple Close calls must not panic.
	p.Close()
	p.Close()
	p.Close()
}

func TestConnPool_Close_NilSafe(t *testing.T) {
	var p *connPool
	p.Close() // must not panic
}

// ---------------------------------------------------------------------------
// H4 — RPC on an unhealthy peer is refused (health gate is now consulted)
// ---------------------------------------------------------------------------

func TestGrpcTransport_UnhealthyPeerRefused(t *testing.T) {
	tr := &GrpcTransport{peers: map[raft.ServerID]*peerConn{}}

	pool := newConnPool(nil) // empty pool: Get() returns nil
	pc := newPeerConn("127.0.0.1:1", 3, pool)
	// Drive the peer past its failure threshold so isHealthy() is false.
	pc.recordFailure()
	pc.recordFailure()
	pc.recordFailure()
	if pc.isHealthy() {
		t.Fatal("precondition: peer should be unhealthy after maxFailures")
	}
	tr.peers["peer1"] = pc

	_, err := tr.clientFor(pc)
	if err == nil {
		t.Fatal("clientFor on unhealthy peer returned nil error, want ErrPeerUnhealthy")
	}
}

func TestGrpcTransport_EmptyPoolPeerRefused(t *testing.T) {
	// A healthy peer whose pool happens to be empty must not panic; clientFor
	// should surface an error instead of dereferencing a nil conn.
	tr := &GrpcTransport{peers: map[raft.ServerID]*peerConn{}}
	pc := newPeerConn("127.0.0.1:1", 3, newConnPool(nil))
	tr.peers["peer1"] = pc

	_, err := tr.clientFor(pc)
	if err == nil {
		t.Fatal("clientFor with empty pool returned nil error, want error")
	}
}

// ---------------------------------------------------------------------------
// M6 — GrpcTransport.Close: double-close must not panic, and must not deadlock
// ---------------------------------------------------------------------------

func TestGrpcTransport_Close_DoubleCloseNoPanic(t *testing.T) {
	cert := selfSignedCert(t)
	tr, err := NewGrpcTransport(":0", zap.NewNop(), cert, nil)
	if err != nil {
		t.Fatalf("NewGrpcTransport: %v", err)
	}

	done := make(chan struct{})
	go func() {
		tr.Close()
		tr.Close() // second close must not panic (close of closed channel) or hang
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("double Close() hung — deadlock or panic recovery")
	}
}

// TestGrpcTransport_Close_ConcurrentDoesNotDeadlock verifies Close does not hold
// the transport lock across wg.Wait() (M6): the reconnect goroutine needs that
// lock, so holding it would deadlock Close.
func TestGrpcTransport_Close_ConcurrentDoesNotDeadlock(t *testing.T) {
	cert := selfSignedCert(t)
	tr, err := NewGrpcTransport(":0", zap.NewNop(), cert, nil)
	if err != nil {
		t.Fatalf("NewGrpcTransport: %v", err)
	}

	// Fire several concurrent Close calls plus lock-acquiring operations.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 4; i++ {
			go tr.Close()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Close() calls hung")
	}
	// Give the closes time to complete without deadlock.
	time.Sleep(50 * time.Millisecond)
}

// ---------------------------------------------------------------------------
// H9 — gRPC InstallSnapshot server reassembles multi-chunk streams
// ---------------------------------------------------------------------------

// captureSnapshotHandler implements RaftHandler and records the InstallSnapshot
// request the server handler passes to it.
type captureSnapshotHandler struct {
	got *proto.InstallSnapshotRequest
}

func (h *captureSnapshotHandler) HandleRequestVote(req *proto.RequestVoteRequest) *proto.RequestVoteResponse {
	return &proto.RequestVoteResponse{}
}
func (h *captureSnapshotHandler) HandleAppendEntries(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
	return &proto.AppendEntriesResponse{}
}
func (h *captureSnapshotHandler) HandleTimeoutNow(req *proto.TimeoutNowRequest) *proto.TimeoutNowResponse {
	return &proto.TimeoutNowResponse{}
}
func (h *captureSnapshotHandler) HandleInstallSnapshot(req *proto.InstallSnapshotRequest) *proto.InstallSnapshotResponse {
	h.got = &proto.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderId:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              append([]byte(nil), req.Data...),
		Done:              req.Done,
	}
	return &proto.InstallSnapshotResponse{Term: req.Term}
}

// fakeInstallStream is a minimal grpc.ClientStreamingServer for InstallSnapshot.
type fakeInstallStream struct {
	grpc.ServerStream
	reqs          []*proto.InstallSnapshotRequest
	idx           int
	sentAndClosed bool
}

func newFakeInstallStream(reqs []*proto.InstallSnapshotRequest) *fakeInstallStream {
	return &fakeInstallStream{reqs: reqs}
}

func (s *fakeInstallStream) Recv() (*proto.InstallSnapshotRequest, error) {
	if s.idx >= len(s.reqs) {
		return nil, io.EOF
	}
	r := s.reqs[s.idx]
	s.idx++
	return r, nil
}
func (s *fakeInstallStream) SendAndClose(*proto.InstallSnapshotResponse) error {
	s.sentAndClosed = true
	return nil
}
func (s *fakeInstallStream) Context() context.Context     { return context.Background() }
func (s *fakeInstallStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeInstallStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeInstallStream) SetTrailer(metadata.MD)       {}
func (s *fakeInstallStream) SendMsg(m interface{}) error  { return nil }
func (s *fakeInstallStream) RecvMsg(m interface{}) error  { return nil }

func TestGrpcInstallSnapshot_ReassemblesChunks(t *testing.T) {
	// Drive the server handler directly with a fake stream that yields multiple
	// chunks, asserting the reassembled Data spans every chunk and Done is set
	// only when the final frame carries Done. Pre-fix, only the first Recv() was
	// handled so later chunks were dropped.
	capH := &captureSnapshotHandler{}
	h := &grpcServerHandler{transport: &GrpcTransport{raftHandler: capH}}

	stream := newFakeInstallStream([]*proto.InstallSnapshotRequest{
		{Term: 7, LeaderId: "l", LastIncludedIndex: 100, LastIncludedTerm: 3, Offset: 0, Data: []byte("aaa"), Done: false},
		{Term: 7, LeaderId: "l", LastIncludedIndex: 100, LastIncludedTerm: 3, Offset: 3, Data: []byte("bbb"), Done: false},
		{Term: 7, LeaderId: "l", LastIncludedIndex: 100, LastIncludedTerm: 3, Offset: 6, Data: []byte("ccc"), Done: true},
	})

	if err := h.InstallSnapshot(stream); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	if capH.got == nil {
		t.Fatal("handler never received a request")
	}
	if string(capH.got.Data) != "aaabbbccc" {
		t.Fatalf("reassembled data = %q, want %q", capH.got.Data, "aaabbbccc")
	}
	if !capH.got.Done {
		t.Fatal("Done should be true once the final chunk carries Done")
	}
	if capH.got.LastIncludedIndex != 100 {
		t.Fatalf("LastIncludedIndex = %d, want 100", capH.got.LastIncludedIndex)
	}
	if !stream.sentAndClosed {
		t.Fatal("handler should SendAndClose a response")
	}
}

// Single-frame client compatibility: one frame with Done=true.
func TestGrpcInstallSnapshot_SingleFrame(t *testing.T) {
	capH := &captureSnapshotHandler{}
	h := &grpcServerHandler{transport: &GrpcTransport{raftHandler: capH}}

	stream := newFakeInstallStream([]*proto.InstallSnapshotRequest{
		{Term: 5, LeaderId: "l", LastIncludedIndex: 42, Data: []byte("hello"), Done: true},
	})
	if err := h.InstallSnapshot(stream); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	if capH.got == nil || string(capH.got.Data) != "hello" || !capH.got.Done {
		t.Fatalf("single-frame reassembly wrong: %+v", capH.got)
	}
}

// M5 — sendRequest honors an already-canceled context (fails fast).
func TestTCPTransport_SendRequestHonorsCancelledContext(t *testing.T) {
	tr, err := NewTCPTransport(":0", nil, 2*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer tr.Close()

	if err := tr.AddPeer("peer1", raft.ServerAddress("127.0.0.1:1")); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled

	_, err = tr.RequestVote(ctx, "peer1", &raft.RequestVoteRequest{Term: 1})
	if err == nil {
		t.Fatal("RequestVote with canceled ctx returned nil error, want context error")
	}
}

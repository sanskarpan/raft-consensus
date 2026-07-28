package etcdcompat

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/sanskarpan/raft-consensus/pkg/fsm"
	pb "github.com/sanskarpan/raft-consensus/proto/etcdcompat"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WatchServer implements the pb.WatchServer gRPC service interface. It wraps
// fsm.WatchManager to fan out FSM events to gRPC streaming clients.
//
// Protocol summary:
//   - Client sends WatchCreateRequest → server registers a watcher, sends
//     WatchResponse{created:true, watch_id:N}.
//   - Client receives a stream of WatchResponse{events:[...]}.
//   - Client sends WatchCancelRequest{watch_id:N} → server cancels the
//     watcher, sends WatchResponse{canceled:true, watch_id:N}.
//   - Either side closes the stream to end the session.
type WatchServer struct {
	pb.UnimplementedWatchServer

	watchMgr  *fsm.WatchManager
	kv        *fsm.KVStore // for header revision
	clusterID uint64
	memberID  uint64

	// nextWatchID generates monotonic IDs sent to etcd clients.
	// This is separate from the internal fsm.WatchID assigned by WatchManager.
	nextWatchID int64
}

// NewWatchServer creates a WatchServer backed by the given WatchManager.
func NewWatchServer(watchMgr *fsm.WatchManager, kv *fsm.KVStore, clusterID, memberID uint64) *WatchServer {
	return &WatchServer{
		watchMgr:  watchMgr,
		kv:        kv,
		clusterID: clusterID,
		memberID:  memberID,
	}
}

// watchSession tracks a single active subscription within one Watch RPC stream.
type watchSession struct {
	etcdWatchID int64         // ID echoed to the etcd client
	fsmWatchID  fsm.WatchID   // internal ID used by WatchManager.Cancel
	eventCh     <-chan fsm.WatchEvent
}

// Watch implements the Watch.Watch bidirectional streaming RPC.
//
// The server multiplexes multiple create/cancel requests over a single stream.
// For each WatchCreateRequest it registers a watcher with WatchManager and
// starts a goroutine that reads from the fsm event channel and enqueues
// WatchResponse messages onto a shared send channel. A single sender goroutine
// drains sendCh onto the gRPC stream to avoid concurrent stream.Send calls.
func (s *WatchServer) Watch(stream pb.Watch_WatchServer) error {
	ctx := stream.Context()

	// sessions maps etcdWatchID → active watchSession for this stream.
	sessions := make(map[int64]*watchSession)

	// sendCh serialises WatchResponse writes from multiple forwarding goroutines.
	sendCh := make(chan *pb.WatchResponse, 256)

	// errCh captures the first terminal error from the sender goroutine.
	errCh := make(chan error, 1)

	// The sender goroutine writes messages to the stream sequentially.
	go func() {
		for {
			select {
			case msg, ok := <-sendCh:
				if !ok {
					return
				}
				if err := stream.Send(msg); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	defer func() {
		// Cancel every active watcher when the stream ends.
		for _, sess := range sessions {
			s.watchMgr.Cancel(sess.fsmWatchID)
		}
		close(sendCh)
	}()

	for {
		// Check for a terminal error from the sender before blocking on Recv.
		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch v := req.RequestUnion.(type) {
		case *pb.WatchRequest_CreateRequest:
			s.handleCreate(ctx, v.CreateRequest, sessions, sendCh)
		case *pb.WatchRequest_CancelRequest:
			s.handleCancel(v.CancelRequest, sessions, sendCh)
		case *pb.WatchRequest_ProgressRequest:
			// Send an empty progress notification (best-effort, non-blocking).
			select {
			case sendCh <- &pb.WatchResponse{Header: s.header()}:
			default:
			}
		default:
			return status.Errorf(codes.InvalidArgument, "unknown watch request type")
		}
	}
}

// handleCreate registers a new watcher and starts forwarding its events.
func (s *WatchServer) handleCreate(
	ctx context.Context,
	req *pb.WatchCreateRequest,
	sessions map[int64]*watchSession,
	sendCh chan<- *pb.WatchResponse,
) {
	key := string(req.Key)
	rangeEnd := string(req.RangeEnd)
	sinceRevision := req.StartRevision

	// Assign an etcd-facing watch_id.
	var etcdWatchID int64
	if req.WatchId != 0 {
		etcdWatchID = req.WatchId
	} else {
		etcdWatchID = atomic.AddInt64(&s.nextWatchID, 1)
	}

	isPrefix := isRangeEndPrefix(key, rangeEnd)

	var (
		eventCh <-chan fsm.WatchEvent
		fsmWID  fsm.WatchID
	)
	if isPrefix {
		prefix := rangePrefix(key, rangeEnd)
		eventCh, fsmWID = s.watchMgr.WatchPrefix(ctx, prefix, sinceRevision)
	} else {
		eventCh, fsmWID = s.watchMgr.Watch(ctx, key, sinceRevision)
	}

	sess := &watchSession{
		etcdWatchID: etcdWatchID,
		fsmWatchID:  fsmWID,
		eventCh:     eventCh,
	}
	sessions[etcdWatchID] = sess

	// Send the "created" acknowledgement.
	select {
	case sendCh <- &pb.WatchResponse{
		Header:  s.header(),
		WatchId: etcdWatchID,
		Created: true,
	}:
	case <-ctx.Done():
		return
	}

	// Forward FSM events to the gRPC stream until the watcher is canceled or
	// the stream context is done.
	go func() {
		for {
			select {
			case we, ok := <-eventCh:
				if !ok {
					return
				}
				pbEvents := make([]*pb.Event, 0, len(we.Events))
				for _, ev := range we.Events {
					pbEvents = append(pbEvents, fsmEventToPb(ev))
				}
				select {
				case sendCh <- &pb.WatchResponse{
					Header:  s.header(),
					WatchId: etcdWatchID,
					Events:  pbEvents,
				}:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// handleCancel cancels an existing watcher by etcd watch_id.
func (s *WatchServer) handleCancel(
	req *pb.WatchCancelRequest,
	sessions map[int64]*watchSession,
	sendCh chan<- *pb.WatchResponse,
) {
	sess, ok := sessions[req.WatchId]
	if !ok {
		return
	}
	s.watchMgr.Cancel(sess.fsmWatchID)
	delete(sessions, req.WatchId)

	// Confirm the cancellation to the client (best-effort).
	select {
	case sendCh <- &pb.WatchResponse{
		Header:   s.header(),
		WatchId:  req.WatchId,
		Canceled: true,
	}:
	default:
	}
}

// header builds a ResponseHeader with the current FSM revision.
func (s *WatchServer) header() *pb.ResponseHeader {
	return &pb.ResponseHeader{
		ClusterId: s.clusterID,
		MemberId:  s.memberID,
		Revision:  s.kv.GetRevision(),
	}
}

// fsmEventToPb converts a fsm.Event to a proto Event.
func fsmEventToPb(ev fsm.Event) *pb.Event {
	pbEv := &pb.Event{}
	switch ev.Type {
	case fsm.EventPut:
		pbEv.Type = pb.Event_PUT
		if ev.KV != nil {
			pbEv.Kv = fsmKVToPb(ev.KV)
		}
	case fsm.EventDelete:
		pbEv.Type = pb.Event_DELETE
		// For deletes the Kv field carries the deleted key with its mod
		// revision set to the deletion revision, matching etcd semantics.
		pbEv.Kv = &pb.KeyValue{
			Key:         []byte(ev.Key),
			ModRevision: ev.Revision,
		}
	}
	if ev.PrevKV != nil {
		pbEv.PrevKv = fsmKVToPb(ev.PrevKV)
	}
	return pbEv
}

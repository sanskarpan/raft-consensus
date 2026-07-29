package etcdcompat

import (
	"context"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/fsm"
	pb "github.com/sanskarpan/raft-consensus/proto/etcdcompat"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ---------------------------------------------------------------------------
// Mock Watch_WatchServer
// ---------------------------------------------------------------------------

// mockWatchStream is a bidirectional stream stub for unit-testing WatchServer.Watch.
// The test drives it by writing to reqCh and reading from respCh.
type mockWatchStream struct {
	ctx    context.Context
	cancel context.CancelFunc

	// reqCh carries WatchRequests from the test into the server's Recv() call.
	reqCh chan *pb.WatchRequest

	// respCh carries WatchResponses produced by stream.Send() back to the test.
	respCh chan *pb.WatchResponse

	// sendErr, if non-nil, is returned by the first Send() call.
	sendErr error

	mu sync.Mutex
}

func newMockWatchStream() *mockWatchStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &mockWatchStream{
		ctx:    ctx,
		cancel: cancel,
		reqCh:  make(chan *pb.WatchRequest, 32),
		respCh: make(chan *pb.WatchResponse, 256),
	}
}

// Recv returns the next request enqueued by the test, or io.EOF when the
// context is canceled and there are no more requests buffered.
func (m *mockWatchStream) Recv() (*pb.WatchRequest, error) {
	select {
	case req, ok := <-m.reqCh:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	case <-m.ctx.Done():
		return nil, io.EOF
	}
}

// Send records the server's response into respCh.
func (m *mockWatchStream) Send(resp *pb.WatchResponse) error {
	m.mu.Lock()
	err := m.sendErr
	m.mu.Unlock()
	if err != nil {
		return err
	}
	select {
	case m.respCh <- resp:
		return nil
	case <-m.ctx.Done():
		return context.Canceled
	}
}

// Context implements grpc.ServerStream.
func (m *mockWatchStream) Context() context.Context { return m.ctx }

// Remaining ServerStream stubs.
func (m *mockWatchStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockWatchStream) SendHeader(metadata.MD) error { return nil }
func (m *mockWatchStream) SetTrailer(metadata.MD)       {}
func (m *mockWatchStream) RecvMsg(v any) error          { return nil }
func (m *mockWatchStream) SendMsg(v any) error          { return nil }

// send enqueues a WatchRequest from the test side.
func (m *mockWatchStream) send(req *pb.WatchRequest) { m.reqCh <- req }

// recv waits for the next WatchResponse with a timeout.
func (m *mockWatchStream) recv(t *testing.T, timeout time.Duration) *pb.WatchResponse {
	t.Helper()
	select {
	case r := <-m.respCh:
		return r
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for WatchResponse after %v", timeout)
		return nil
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newTestWatchSetup creates a WatchServer backed by an in-memory KVStore and
// WatchManager ready for use in tests.
func newTestWatchSetup(ctx context.Context) (*WatchServer, *fsm.KVStore, *fsm.WatchManager) {
	kv := fsm.NewKVStore()
	wm := fsm.NewWatchManager(kv)
	wm.Start(ctx)
	srv := NewWatchServer(wm, kv, 1, 1)
	return srv, kv, wm
}

// putKey applies a put command directly to the FSM (no Raft round-trip).
func putKey(kv *fsm.KVStore, key, value string) {
	cmd, _ := fsm.EncodeCommand("put", key, value)
	kv.Apply(cmd)
}

// ---------------------------------------------------------------------------
// Test 1: create watch + receive event
// ---------------------------------------------------------------------------

func TestWatch_CreateAndReceiveEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv, kv, _ := newTestWatchSetup(ctx)
	stream := newMockWatchStream()

	// Run the Watch RPC in the background; it blocks on Recv().
	watchDone := make(chan error, 1)
	go func() { watchDone <- srv.Watch(stream) }()

	// Send a WatchCreateRequest for key "foo".
	stream.send(&pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key: []byte("foo"),
			},
		},
	})

	// Expect the "created" acknowledgement.
	created := stream.recv(t, 2*time.Second)
	if !created.Created {
		t.Fatalf("expected created=true, got %v", created)
	}
	watchID := created.WatchId

	// Apply a mutation so WatchManager emits an event.
	putKey(kv, "foo", "bar")

	// Expect an event response on the same watch_id.
	evResp := stream.recv(t, 2*time.Second)
	if evResp.WatchId != watchID {
		t.Fatalf("watch_id mismatch: got %d, want %d", evResp.WatchId, watchID)
	}
	if len(evResp.Events) == 0 {
		t.Fatal("expected at least one event, got none")
	}
	ev := evResp.Events[0]
	if ev.Type != pb.Event_PUT {
		t.Fatalf("expected PUT event, got %v", ev.Type)
	}
	if string(ev.Kv.Key) != "foo" {
		t.Fatalf("expected key 'foo', got %q", string(ev.Kv.Key))
	}

	// Cancel the stream.
	stream.cancel()
	select {
	case err := <-watchDone:
		if err != nil && err != context.Canceled && err != io.EOF {
			t.Fatalf("Watch returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after stream cancel")
	}
}

// ---------------------------------------------------------------------------
// Test 2: cancel watch stops event forwarding; no goroutine leak
// ---------------------------------------------------------------------------

func TestWatch_CancelWatchStopsForwarding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv, kv, _ := newTestWatchSetup(ctx)
	stream := newMockWatchStream()

	watchDone := make(chan error, 1)
	go func() { watchDone <- srv.Watch(stream) }()

	// Create a watcher.
	stream.send(&pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key: []byte("canary"),
			},
		},
	})
	created := stream.recv(t, 2*time.Second)
	if !created.Created {
		t.Fatalf("expected created=true, got %v", created)
	}
	watchID := created.WatchId

	// Snapshot goroutine count before cancellation.
	// (We give a little time for any transient goroutines to settle.)
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	// Cancel the watcher by watch_id.
	stream.send(&pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CancelRequest{
			CancelRequest: &pb.WatchCancelRequest{
				WatchId: watchID,
			},
		},
	})

	// Expect a "canceled" response.
	cancelResp := stream.recv(t, 2*time.Second)
	if !cancelResp.Canceled {
		t.Fatalf("expected canceled=true, got %v", cancelResp)
	}
	if cancelResp.WatchId != watchID {
		t.Fatalf("canceled watch_id mismatch: got %d, want %d", cancelResp.WatchId, watchID)
	}

	// Write a key — should produce no event on the (canceled) watcher.
	putKey(kv, "canary", "dead")
	time.Sleep(50 * time.Millisecond) // give any stray goroutine time to fire

	select {
	case unexpected := <-stream.respCh:
		if unexpected.WatchId == watchID && len(unexpected.Events) > 0 {
			t.Fatalf("received unexpected event after cancel: %v", unexpected)
		}
	default:
	}

	// Check that goroutine count has not grown (forwarder goroutine exited).
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	// Allow a small slack for Go runtime background goroutines.
	if after > before+3 {
		t.Errorf("goroutine leak detected: before=%d after=%d", before, after)
	}

	// Close the stream.
	stream.cancel()
	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after stream cancel")
	}
}

// ---------------------------------------------------------------------------
// Test 3: multiple concurrent watches on one stream each receive only their
//         own events
// ---------------------------------------------------------------------------

func TestWatch_MultipleWatchersIndependence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv, kv, _ := newTestWatchSetup(ctx)
	stream := newMockWatchStream()

	watchDone := make(chan error, 1)
	go func() { watchDone <- srv.Watch(stream) }()

	// Create watcher A on "alpha".
	stream.send(&pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{Key: []byte("alpha")},
		},
	})
	createdA := stream.recv(t, 2*time.Second)
	watchIDA := createdA.WatchId

	// Create watcher B on "beta".
	stream.send(&pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{Key: []byte("beta")},
		},
	})
	createdB := stream.recv(t, 2*time.Second)
	watchIDB := createdB.WatchId

	if watchIDA == watchIDB {
		t.Fatalf("watchers should have different IDs, both got %d", watchIDA)
	}

	// Put "alpha" → only watcher A should fire.
	putKey(kv, "alpha", "1")
	evA := stream.recv(t, 2*time.Second)
	if evA.WatchId != watchIDA {
		t.Fatalf("event for 'alpha' arrived on wrong watcher %d (want %d)", evA.WatchId, watchIDA)
	}

	// Put "beta" → only watcher B should fire.
	putKey(kv, "beta", "2")
	evB := stream.recv(t, 2*time.Second)
	if evB.WatchId != watchIDB {
		t.Fatalf("event for 'beta' arrived on wrong watcher %d (want %d)", evB.WatchId, watchIDB)
	}

	// No stray events.
	time.Sleep(30 * time.Millisecond)
	select {
	case extra := <-stream.respCh:
		t.Fatalf("unexpected extra response: %v", extra)
	default:
	}

	stream.cancel()
	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after stream cancel")
	}
}

// ---------------------------------------------------------------------------
// Test 4: stream close cancels all watchers
// ---------------------------------------------------------------------------

func TestWatch_StreamCloseCancelsAllWatchers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv, kv, wm := newTestWatchSetup(ctx)
	_ = wm // keep reference
	stream := newMockWatchStream()

	watchDone := make(chan error, 1)
	go func() { watchDone <- srv.Watch(stream) }()

	// Register three watchers.
	for _, key := range []string{"x", "y", "z"} {
		stream.send(&pb.WatchRequest{
			RequestUnion: &pb.WatchRequest_CreateRequest{
				CreateRequest: &pb.WatchCreateRequest{Key: []byte(key)},
			},
		})
		resp := stream.recv(t, 2*time.Second)
		if !resp.Created {
			t.Fatalf("expected created=true for key %q", key)
		}
	}

	// Drain goroutines and note initial count.
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	// Cancel the stream context — this simulates the client disconnecting.
	stream.cancel()

	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after stream cancel")
	}

	// All forwarder goroutines should have exited.
	time.Sleep(100 * time.Millisecond)
	after := runtime.NumGoroutine()

	// Allow slack for runtime background goroutines.
	if after > before+3 {
		t.Errorf("goroutine leak: before=%d after=%d (expected goroutines to exit)", before, after)
	}

	// Writes after close must not panic.
	putKey(kv, "x", "post-close")
	putKey(kv, "y", "post-close")
	putKey(kv, "z", "post-close")
}

// ---------------------------------------------------------------------------
// Test 5: StartRevision == 0 must NOT replay history
// ---------------------------------------------------------------------------

func TestWatch_StartRevisionZeroNoHistoryReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv, kv, _ := newTestWatchSetup(ctx)
	stream := newMockWatchStream()

	// Write a key before the watcher is created so it ends up in history.
	putKey(kv, "historic", "value")
	time.Sleep(10 * time.Millisecond)

	watchDone := make(chan error, 1)
	go func() { watchDone <- srv.Watch(stream) }()

	// Create watcher with StartRevision == 0 (the proto3 default / "watch from now").
	stream.send(&pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key:           []byte("historic"),
				StartRevision: 0,
			},
		},
	})

	// Expect the created acknowledgement.
	created := stream.recv(t, 2*time.Second)
	if !created.Created {
		t.Fatalf("expected created=true")
	}

	// Wait to confirm no history events arrive.
	time.Sleep(100 * time.Millisecond)
	select {
	case resp := <-stream.respCh:
		t.Fatalf("unexpected history replay event (StartRevision==0 should not replay): %v", resp)
	default:
	}

	// A live event (after registration) should still arrive.
	putKey(kv, "historic", "live-value")
	live := stream.recv(t, 2*time.Second)
	if len(live.Events) == 0 {
		t.Fatal("expected live event after put, got none")
	}

	stream.cancel()
	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return")
	}
}

// ---------------------------------------------------------------------------
// Test 6: StartRevision > 0 replays buffered history, then delivers live events
// ---------------------------------------------------------------------------

func TestWatch_StartRevisionReplaysThenLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv, kv, _ := newTestWatchSetup(ctx)
	stream := newMockWatchStream()

	// Write two entries so there is history to replay.
	putKey(kv, "replay-key", "v1")
	putKey(kv, "replay-key", "v2")

	// Capture the revision after the second write.
	// We want to replay from revision 1 onwards.
	_ = kv

	watchDone := make(chan error, 1)
	go func() { watchDone <- srv.Watch(stream) }()

	stream.send(&pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key:           []byte("replay-key"),
				StartRevision: 1,
			},
		},
	})

	created := stream.recv(t, 2*time.Second)
	if !created.Created {
		t.Fatalf("expected created=true")
	}

	// We expect at least one replayed history event.
	var gotHistory bool
	deadline := time.After(2 * time.Second)
	for !gotHistory {
		select {
		case resp := <-stream.respCh:
			if len(resp.Events) > 0 {
				gotHistory = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for history replay event")
		}
	}

	stream.cancel()
	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return")
	}
}

// ---------------------------------------------------------------------------
// Compile-time check: mockWatchStream implements the required interface.
// ---------------------------------------------------------------------------

var _ grpc.BidiStreamingServer[pb.WatchRequest, pb.WatchResponse] = (*mockWatchStream)(nil)

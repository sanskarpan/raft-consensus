package transport_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/raft-consensus/pkg/raft"
	"github.com/raft-consensus/pkg/transport"
	proto "github.com/raft-consensus/proto"
)

// TestGrpcInstallSnapshotPreservesChunkOffsets verifies that the gRPC server
// forwards each chunk's original Offset to the raft handler instead of
// collapsing it to 0. The raft sender issues one InstallSnapshot RPC per chunk;
// if the server reported every chunk at Offset 0, the receiver's Offset-ordered
// reassembly would restart on each chunk and a >1 MiB snapshot would restore
// from only its final chunk.
func TestGrpcInstallSnapshotPreservesChunkOffsets(t *testing.T) {
	var mu sync.Mutex
	var got []*proto.InstallSnapshotRequest

	handler := &grpcHandler{
		onInstallSnapshot: func(req *proto.InstallSnapshotRequest) *proto.InstallSnapshotResponse {
			mu.Lock()
			// Copy the fields we assert on; Data is retained by value here.
			got = append(got, &proto.InstallSnapshotRequest{
				Offset: req.Offset,
				Data:   append([]byte(nil), req.Data...),
				Done:   req.Done,
			})
			mu.Unlock()
			return &proto.InstallSnapshotResponse{Term: req.Term}
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

	// Emulate the raft sender: one RPC per 1 MiB chunk, Done only on the last.
	const chunkSize = 1 << 20
	payload := make([]byte, chunkSize*2+123)
	for i := range payload {
		payload[i] = byte((i*7 + 3) % 251)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for off := 0; off < len(payload); off += chunkSize {
		end := off + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		req := &raft.InstallSnapshotRequest{
			Term:              1,
			LeaderID:          "leader",
			LastIncludedIndex: 9,
			LastIncludedTerm:  1,
			Offset:            uint64(off),
			Data:              payload[off:end],
			Done:              end == len(payload),
		}
		if _, err := cli.InstallSnapshot(ctx, raft.ServerID("server"), req); err != nil {
			t.Fatalf("InstallSnapshot(off=%d): %v", off, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("server received %d chunks, want 3", len(got))
	}
	// Offsets must be contiguous (0, 1 MiB, 2 MiB) and the payload reassembles.
	var reasm []byte
	for i, r := range got {
		if r.Offset != uint64(len(reasm)) {
			t.Fatalf("chunk %d: Offset=%d, want %d (offset must be preserved, not collapsed to 0)",
				i, r.Offset, len(reasm))
		}
		reasm = append(reasm, r.Data...)
	}
	if !bytes.Equal(reasm, payload) {
		t.Fatalf("reassembled %d bytes != original %d", len(reasm), len(payload))
	}
	if !got[len(got)-1].Done {
		t.Fatal("final chunk must carry Done")
	}
}

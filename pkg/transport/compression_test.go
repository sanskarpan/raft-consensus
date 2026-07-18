package transport_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"github.com/sanskarpan/raft-consensus/pkg/transport"
	proto "github.com/sanskarpan/raft-consensus/proto"
)

// TestGrpcCompressionRoundTrips verifies that a gzip-compressed AppendEntries
// (client SetCompression) is transparently decompressed by the server and the
// payload arrives intact — mixed-mode interoperability, since the server always
// registers the gzip codec.
func TestGrpcCompressionRoundTrips(t *testing.T) {
	var got []byte
	handler := &grpcHandler{
		onAppendEntries: func(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
			if len(req.Entries) > 0 {
				got = append([]byte(nil), req.Entries[0].Data...)
			}
			return &proto.AppendEntriesResponse{Term: req.Term, Success: true}
		},
	}

	srv, err := transport.NewGrpcTransportInsecure(":0", zap.NewNop())
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	defer srv.Close()
	srv.SetRaftHandler(handler)

	cli, err := transport.NewGrpcTransportInsecure(":0", zap.NewNop())
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cli.Close()
	cli.SetCompression(true) // must be before AddPeer
	if err := cli.AddPeer(raft.ServerID("server"), raft.ServerAddress(srv.ListenerAddr())); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	// A highly compressible 1 MiB payload.
	payload := bytes.Repeat([]byte("raft-consensus "), 70000)
	req := &raft.AppendEntriesRequest{
		Term:    1,
		Entries: []*raft.LogEntry{{Term: 1, Index: 1, Data: payload}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cli.AppendEntries(ctx, raft.ServerID("server"), req)
	if err != nil {
		t.Fatalf("compressed AppendEntries failed: %v", err)
	}
	if !resp.Success {
		t.Fatal("expected success")
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload corrupted: got %d bytes, want %d", len(got), len(payload))
	}
}

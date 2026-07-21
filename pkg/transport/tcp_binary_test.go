package transport_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"github.com/sanskarpan/raft-consensus/pkg/transport"
	"go.uber.org/zap"
)

// TestBinaryHandshake verifies that a raw TCP client sending the magic probe
// to a binary-capable server receives the 4-byte echo back.
func TestBinaryHandshake(t *testing.T) {
	srv, err := transport.NewTCPTransport(":0", &noopHandler{}, 3*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.ListenerAddr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	probe := transport.BinaryMagic()
	if _, err := conn.Write(probe[:]); err != nil {
		t.Fatalf("Write probe: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	echo := make([]byte, 4)
	if _, err := conn.Read(echo); err != nil {
		t.Fatalf("Read echo: %v", err)
	}
	if [4]byte(echo) != probe {
		t.Errorf("echo = %x, want %x", echo, probe)
	}
}

// TestJSONFallback verifies that a client with BinaryTransport: false uses JSON
// framing and a full RequestVote round-trip still works.
func TestJSONFallback(t *testing.T) {
	srv, err := transport.NewTCPTransport(":0", &noopHandler{}, 3*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer srv.Close()

	cli, err := transport.NewTCPTransportWithConfig(":0", &noopHandler{}, transport.TCPTransportConfig{
		Timeout:         3 * time.Second,
		Logger:          zap.NewNop(),
		BinaryTransport: false,
	})
	if err != nil {
		t.Fatalf("NewTCPTransportWithConfig: %v", err)
	}
	defer cli.Close()

	cli.SetLocalID(raft.ServerID("cli"))
	cli.AddPeer(raft.ServerID("srv"), raft.ServerAddress(srv.ListenerAddr())) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = cli.RequestVote(ctx, raft.ServerID("srv"), &raft.RequestVoteRequest{Term: 1})
	if err != nil {
		t.Fatalf("RequestVote (JSON fallback): %v", err)
	}
}

// TestBinaryRoundTripOverPipe exercises a full AppendEntries encode→frame→decode
// between two TCP transports using binary framing.
func TestBinaryRoundTripOverPipe(t *testing.T) {
	var gotReq *transport.AppendEntriesReq
	handler := &callbackAppendHandler{
		onAppendEntries: func(r *transport.AppendEntriesReq) *transport.AppendEntriesResp {
			gotReq = r
			return &transport.AppendEntriesResp{Term: r.Term, Success: true, Index: 42}
		},
	}

	srv, err := transport.NewTCPTransport(":0", handler, 5*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer srv.Close()

	cli, err := transport.NewTCPTransportWithConfig(":0", &noopHandler{}, transport.TCPTransportConfig{
		Timeout:         5 * time.Second,
		Logger:          zap.NewNop(),
		BinaryTransport: true,
	})
	if err != nil {
		t.Fatalf("NewTCPTransportWithConfig: %v", err)
	}
	defer cli.Close()

	cli.SetLocalID(raft.ServerID("cli"))
	cli.AddPeer(raft.ServerID("srv"), raft.ServerAddress(srv.ListenerAddr())) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.AppendEntries(ctx, raft.ServerID("srv"), &raft.AppendEntriesRequest{
		Term:     7,
		LeaderID: "cli",
		Entries: []*raft.LogEntry{
			{Term: 7, Index: 1, Type: raft.EntryNormal, Data: []byte("set x 1")},
		},
		LeaderCommit: 0,
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !resp.Success || resp.Index != 42 {
		t.Errorf("unexpected resp: %+v", resp)
	}
	if gotReq == nil {
		t.Fatal("server handler never called")
	}
	if gotReq.Term != 7 || gotReq.LeaderID != "cli" {
		t.Errorf("server got wrong req: %+v", gotReq)
	}
}

// callbackAppendHandler wraps noopHandler with a configurable AppendEntries hook.
type callbackAppendHandler struct {
	noopHandler
	onAppendEntries func(*transport.AppendEntriesReq) *transport.AppendEntriesResp
}

func (c *callbackAppendHandler) HandleAppendEntries(req *transport.AppendEntriesReq) *transport.AppendEntriesResp {
	if c.onAppendEntries != nil {
		return c.onAppendEntries(req)
	}
	return &transport.AppendEntriesResp{}
}

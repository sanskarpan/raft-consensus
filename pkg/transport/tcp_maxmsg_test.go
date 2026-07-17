package transport_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/transport"
	"go.uber.org/zap"
)

// C12: The TCP transport must bound the size of a single inbound message so an
// unauthenticated peer cannot exhaust memory. A message exceeding the cap must
// be rejected (connection closed) rather than decoded and processed.
func TestTCPTransportRejectsOversizedMessage(t *testing.T) {
	srv, err := transport.NewTCPTransport(":0", &noopHandler{}, 3*time.Second, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTCPTransport: %v", err)
	}
	defer srv.Close()
	srv.SetMaxMessageBytes(1024) // tiny cap for the test

	conn, err := net.Dial("tcp", srv.ListenerAddr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// A valid-JSON message far larger than the 1KiB cap.
	huge := make([]byte, 8192)
	for i := range huge {
		huge[i] = 'a'
	}
	payload := fmt.Sprintf(`{"type":"AppendEntries","payload":{"leader_id":"%s"}}`+"\n", huge)

	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	// The write may not fully complete before the server closes the connection.
	_, _ = conn.Write([]byte(payload))

	// The server must have rejected the message and closed the connection, so a
	// read returns an error (EOF/reset) rather than a valid response.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected connection to be closed after an oversized message, but got a response")
	}
}

package transport

import (
	"errors"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// C11: When RequireTLS is set and no TLS config is available, dialing a peer
// must fail closed (ErrTLSRequired) rather than silently connecting in
// plaintext.
func TestGrpcRequireTLSRefusesPlaintextDial(t *testing.T) {
	tr := &GrpcTransport{peers: map[raft.ServerID]*peerConn{}}
	tr.SetRequireTLS(true)

	err := tr.AddPeer("peer1", "127.0.0.1:12345")
	if !errors.Is(err, ErrTLSRequired) {
		t.Fatalf("AddPeer with RequireTLS and no TLS: got err=%v, want ErrTLSRequired", err)
	}
}

// Without RequireTLS, the legacy plaintext fallback still works (backward compat).
func TestGrpcPlaintextDialAllowedByDefault(t *testing.T) {
	tr := &GrpcTransport{peers: map[raft.ServerID]*peerConn{}}
	// RequireTLS defaults to false.
	if err := tr.AddPeer("peer1", "127.0.0.1:12346"); err != nil {
		t.Fatalf("AddPeer without RequireTLS should succeed (lazy dial): %v", err)
	}
}

package transport_test

import (
	"bytes"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/transport"
)

func TestBinaryFrameEncodeDecode(t *testing.T) {
	payload := []byte("hello binary frame")
	var buf bytes.Buffer
	if err := transport.WriteBinaryFrame(&buf, transport.TagAppendEntriesReq, payload); err != nil {
		t.Fatalf("WriteBinaryFrame: %v", err)
	}
	typeTag, got, err := transport.ReadBinaryFrame(&buf)
	if err != nil {
		t.Fatalf("ReadBinaryFrame: %v", err)
	}
	if typeTag != transport.TagAppendEntriesReq {
		t.Errorf("typeTag = %d, want %d", typeTag, transport.TagAppendEntriesReq)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch: got %q, want %q", got, payload)
	}
}

func TestBinaryFrameZeroPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := transport.WriteBinaryFrame(&buf, transport.TagTimeoutNowResp, []byte{}); err != nil {
		t.Fatalf("WriteBinaryFrame empty: %v", err)
	}
	typeTag, got, err := transport.ReadBinaryFrame(&buf)
	if err != nil {
		t.Fatalf("ReadBinaryFrame empty: %v", err)
	}
	if typeTag != transport.TagTimeoutNowResp {
		t.Errorf("typeTag = %d, want %d", typeTag, transport.TagTimeoutNowResp)
	}
	if len(got) != 0 {
		t.Errorf("expected empty payload, got %d bytes", len(got))
	}
}

func TestBinaryFrameMaxPayload(t *testing.T) {
	payload := make([]byte, 4<<20) // 4 MiB
	for i := range payload {
		payload[i] = byte(i & 0xff)
	}
	var buf bytes.Buffer
	if err := transport.WriteBinaryFrame(&buf, transport.TagInstallSnapshotReq, payload); err != nil {
		t.Fatalf("WriteBinaryFrame 4MiB: %v", err)
	}
	_, got, err := transport.ReadBinaryFrame(&buf)
	if err != nil {
		t.Fatalf("ReadBinaryFrame 4MiB: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("4 MiB payload round-trip mismatch")
	}
}

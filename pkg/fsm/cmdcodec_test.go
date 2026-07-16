package fsm

import (
	"bytes"
	"encoding/json"
	"testing"
)

// M-P4: binary command codec must round-trip and be deterministic (same command
// -> identical bytes on every replica), and still decode legacy JSON commands.
func TestKVCommandBinaryRoundTrip(t *testing.T) {
	in := kvCommand{Op: "put", Key: "k", Value: "v", ClientID: "c1", SeqNum: 42}
	enc := encodeKVCommand(in)
	if enc[0] != cmdBinaryMagic {
		t.Fatalf("expected binary magic, got 0x%x", enc[0])
	}
	out, err := decodeKVCommand(enc)
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round-trip mismatch: %+v != %+v", out, in)
	}
	// Determinism: encoding is a pure function of the command.
	if !bytes.Equal(enc, encodeKVCommand(in)) {
		t.Fatal("binary command encoding is not deterministic")
	}
}

// M-P4: legacy JSON commands (pre-binary, and txn envelopes) must still decode.
func TestKVCommandJSONBackwardCompat(t *testing.T) {
	legacy, _ := json.Marshal(kvCommand{Op: "delete", Key: "old", ClientID: "c", SeqNum: 7})
	if legacy[0] != '{' {
		t.Fatal("expected JSON")
	}
	out, err := decodeKVCommand(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if out.Op != "delete" || out.Key != "old" || out.ClientID != "c" || out.SeqNum != 7 {
		t.Fatalf("legacy JSON decode wrong: %+v", out)
	}
}

// M-P4: a binary-encoded command applied through the FSM behaves identically to
// the JSON path (verifies EncodeCommandWithID -> Apply end to end).
func TestBinaryCommandAppliesCorrectly(t *testing.T) {
	k := NewKVStore()
	put, _ := EncodeCommandWithID("put", "a", "1", "client", 1)
	if put[0] != cmdBinaryMagic {
		t.Fatal("EncodeCommandWithID should now be binary")
	}
	if _, err := k.Apply(put); err != nil {
		t.Fatal(err)
	}
	get, _ := EncodeCommand("get", "a", "")
	res, err := k.Apply(get)
	if err != nil {
		t.Fatal(err)
	}
	var kr KvResult
	if err := json.Unmarshal(res, &kr); err != nil {
		t.Fatalf("result must stay JSON (client-facing): %v", err)
	}
	if kr.Value != "1" {
		t.Fatalf("get returned %q, want 1", kr.Value)
	}
}

func BenchmarkDecodeKVCommandBinary(b *testing.B) {
	enc := encodeKVCommand(kvCommand{Op: "put", Key: "somekey", Value: "someval", ClientID: "client-123", SeqNum: 99})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = decodeKVCommand(enc)
	}
}

func BenchmarkDecodeKVCommandJSON(b *testing.B) {
	enc, _ := json.Marshal(kvCommand{Op: "put", Key: "somekey", Value: "someval", ClientID: "client-123", SeqNum: 99})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var c kvCommand
		_ = json.Unmarshal(enc, &c)
	}
}

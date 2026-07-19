package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
)

// TestStreamingSnapshotRoundTrip verifies the streaming binary snapshot restores
// byte-identically and carries the magic marker (#223).
func TestStreamingSnapshotRoundTrip(t *testing.T) {
	src := NewKVStore()
	for i := 0; i < 200; i++ {
		cmd, _ := EncodeCommandWithID("put", key(i), val(i), "client", uint64(i+1))
		if _, err := src.Apply(cmd); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	r := snap.Reader()
	data, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 || data[0] != snapStreamMagic {
		t.Fatalf("snapshot does not start with the streaming magic byte (got %v)", data)
	}

	dst := NewKVStore()
	if err := dst.Restore(bytes.NewReader(data)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for i := 0; i < 200; i++ {
		kv, _ := dst.Get(key(i))
		if kv == nil || kv.Value != val(i) {
			t.Fatalf("key %s missing/mismatch after restore: %+v", key(i), kv)
		}
	}
	// Idempotency dedup table must survive the snapshot.
	cmd, _ := EncodeCommandWithID("put", key(0), "REPLAYED", "client", 1) // seq 1 already applied
	res, _ := dst.Apply(cmd)
	dr, _ := DecodeResult(res)
	if kv, _ := dst.Get(key(0)); kv.Value == "REPLAYED" {
		t.Fatalf("dedup table not restored: replayed seq 1 was re-applied (%v)", dr)
	}
}

// TestSnapshotBackwardCompatJSON verifies an older JSON snapshot still restores.
func TestSnapshotBackwardCompatJSON(t *testing.T) {
	old := kvSnapshotData{
		Revision: 7,
		Index:    42,
		Data:     map[string]*KeyValue{"legacy": {Key: "legacy", Value: "v", Version: 1}},
	}
	data, _ := json.Marshal(old)

	k := NewKVStore()
	if err := k.Restore(bytes.NewReader(data)); err != nil {
		t.Fatalf("Restore legacy JSON: %v", err)
	}
	kv, _ := k.Get("legacy")
	if kv == nil || kv.Value != "v" {
		t.Fatalf("legacy JSON snapshot did not restore: %+v", kv)
	}
}

func key(i int) string { return "k/" + itoa(i) }
func val(i int) string { return "v" + itoa(i) }
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

package fsm

import (
	"encoding/json"
	"io"
	"sort"
	"testing"
)

// ---------------------------------------------------------------------------
// #207.1: TTL codec + KeyValue/kvCommand fields (backward-compat)
// ---------------------------------------------------------------------------

func TestTTLCommandBinaryRoundTrip(t *testing.T) {
	cmd := kvCommand{
		Op:                "put",
		Key:               "k",
		Value:             "v",
		ClientID:          "c1",
		SeqNum:            99,
		LeaderTimestampMs: 1_700_000_000_000,
		TTLSeconds:        30,
	}
	enc := encodeKVCommand(cmd)
	if enc[0] != cmdBinaryMagic {
		t.Fatalf("expected binary magic, got 0x%x", enc[0])
	}
	got, err := decodeKVCommand(enc)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if got != cmd {
		t.Fatalf("round-trip mismatch:\n  want %+v\n  got  %+v", cmd, got)
	}
	// Determinism: same input → identical bytes.
	enc2 := encodeKVCommand(cmd)
	if string(enc) != string(enc2) {
		t.Fatal("encoding is not deterministic")
	}
}

// Without TTL fields, the encoding must be identical to the pre-TTL format
// so replicas running old code can still decode.
func TestTTLCommandNoTTLBackwardCompat(t *testing.T) {
	cmd := kvCommand{Op: "put", Key: "k", Value: "v", ClientID: "c1", SeqNum: 42}
	enc := encodeKVCommand(cmd)
	got, err := decodeKVCommand(enc)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if got != cmd {
		t.Fatalf("round-trip mismatch: want %+v got %+v", cmd, got)
	}
	if got.LeaderTimestampMs != 0 || got.TTLSeconds != 0 {
		t.Fatalf("TTL fields should be zero for non-TTL command: %+v", got)
	}
}

// Legacy JSON commands (pre-binary) must still decode cleanly.
func TestTTLCommandJSONLegacyDecode(t *testing.T) {
	legacy, _ := json.Marshal(kvCommand{Op: "delete", Key: "old", ClientID: "c", SeqNum: 7})
	if legacy[0] != '{' {
		t.Fatal("expected JSON")
	}
	got, err := decodeKVCommand(legacy)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if got.Op != "delete" || got.Key != "old" || got.LeaderTimestampMs != 0 || got.TTLSeconds != 0 {
		t.Fatalf("legacy JSON decode wrong: %+v", got)
	}
}

// EncodeTick produces a binary command with op="tick" and the given timestamp.
func TestEncodeTickRoundTrip(t *testing.T) {
	ts := int64(1_700_000_000_000)
	enc := EncodeTick(ts)
	if enc[0] != cmdBinaryMagic {
		t.Fatalf("expected binary magic, got 0x%x", enc[0])
	}
	got, err := decodeKVCommand(enc)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if got.Op != "tick" {
		t.Fatalf("expected op=tick, got %q", got.Op)
	}
	if got.LeaderTimestampMs != ts {
		t.Fatalf("LeaderTimestampMs mismatch: want %d got %d", ts, got.LeaderTimestampMs)
	}
}

// EncodeCommandWithTTL produces a binary command with TTL fields set.
func TestEncodeCommandWithTTL(t *testing.T) {
	enc, err := EncodeCommandWithTTL("put", "mykey", "myval", "cid", 1, 1_700_000_000_000, 60)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeKVCommand(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Op != "put" || got.Key != "mykey" || got.Value != "myval" {
		t.Fatalf("wrong op/key/val: %+v", got)
	}
	if got.ClientID != "cid" || got.SeqNum != 1 {
		t.Fatalf("wrong idempotency fields: %+v", got)
	}
	if got.LeaderTimestampMs != 1_700_000_000_000 {
		t.Fatalf("wrong LeaderTimestampMs: %d", got.LeaderTimestampMs)
	}
	if got.TTLSeconds != 60 {
		t.Fatalf("wrong TTLSeconds: %d", got.TTLSeconds)
	}
}

// ---------------------------------------------------------------------------
// #207.2: Apply-time monotonic clock + live/expired read semantics
// ---------------------------------------------------------------------------

func TestApplyTimeAdvancesMonotonically(t *testing.T) {
	k := NewKVStore()

	tick1 := EncodeTick(1000)
	if _, err := k.Apply(tick1); err != nil {
		t.Fatal(err)
	}
	if k.applyTimeMs != 1000 {
		t.Fatalf("applyTimeMs should be 1000, got %d", k.applyTimeMs)
	}

	// A later tick advances it.
	tick2 := EncodeTick(2000)
	if _, err := k.Apply(tick2); err != nil {
		t.Fatal(err)
	}
	if k.applyTimeMs != 2000 {
		t.Fatalf("applyTimeMs should be 2000, got %d", k.applyTimeMs)
	}

	// An earlier tick must NOT regress the clock.
	tickOld := EncodeTick(500)
	if _, err := k.Apply(tickOld); err != nil {
		t.Fatal(err)
	}
	if k.applyTimeMs != 2000 {
		t.Fatalf("applyTimeMs should still be 2000, got %d", k.applyTimeMs)
	}
}

func TestPutWithTTLSetsExpiresAt(t *testing.T) {
	k := NewKVStore()

	// Establish apply time via tick.
	if _, err := k.Apply(EncodeTick(1000)); err != nil {
		t.Fatal(err)
	}

	// Put with TTL=5s → ExpiresAtMs = applyTimeMs + 5000 = 6000
	enc, _ := EncodeCommandWithTTL("put", "ttlkey", "hello", "", 0, 1000, 5)
	if _, err := k.Apply(enc); err != nil {
		t.Fatal(err)
	}

	k.mu.RLock()
	kv := k.data["ttlkey"]
	k.mu.RUnlock()
	if kv == nil {
		t.Fatal("key not stored")
	}
	if kv.ExpiresAtMs != 6000 {
		t.Fatalf("ExpiresAtMs want 6000, got %d", kv.ExpiresAtMs)
	}
}

func TestPutWithoutTTLNoExpiry(t *testing.T) {
	k := NewKVStore()
	if _, err := k.Apply(EncodeTick(1000)); err != nil {
		t.Fatal(err)
	}
	enc, _ := EncodeCommand("put", "noexp", "v")
	if _, err := k.Apply(enc); err != nil {
		t.Fatal(err)
	}
	k.mu.RLock()
	kv := k.data["noexp"]
	k.mu.RUnlock()
	if kv == nil {
		t.Fatal("key not stored")
	}
	if kv.ExpiresAtMs != 0 {
		t.Fatalf("non-TTL key should have ExpiresAtMs=0, got %d", kv.ExpiresAtMs)
	}
}

func TestExpiredKeyAbsentOnGetV2(t *testing.T) {
	k := NewKVStore()

	// Put key with TTL=5s at t=1000 → expires at t=6000.
	if _, err := k.Apply(EncodeTick(1000)); err != nil {
		t.Fatal(err)
	}
	enc, _ := EncodeCommandWithTTL("put", "exp", "val", "", 0, 1000, 5)
	if _, err := k.Apply(enc); err != nil {
		t.Fatal(err)
	}

	// Before expiry (t=5999): key is present.
	if _, err := k.Apply(EncodeTick(5999)); err != nil {
		t.Fatal(err)
	}
	get, _ := EncodeCommand("get_v2", "exp", "")
	res, err := k.Apply(get)
	if err != nil {
		t.Fatal(err)
	}
	kr, _ := DecodeResult(res)
	if kr.Error != "" {
		t.Fatalf("key should be live at t=5999, got error: %s", kr.Error)
	}

	// Advance to t=6000 (exactly at boundary) → expired.
	if _, err := k.Apply(EncodeTick(6000)); err != nil {
		t.Fatal(err)
	}
	res, err = k.Apply(get)
	if err != nil {
		t.Fatal(err)
	}
	kr, _ = DecodeResult(res)
	if kr.Error == "" {
		t.Fatalf("key should be expired at t=6000, got value: %s", kr.Value)
	}
}

func TestExpiredKeyAbsentOnRange(t *testing.T) {
	k := NewKVStore()

	if _, err := k.Apply(EncodeTick(1000)); err != nil {
		t.Fatal(err)
	}

	// Two keys: one with TTL, one without.
	enc1, _ := EncodeCommandWithTTL("put", "pfx/exp", "v1", "", 0, 1000, 5)
	enc2, _ := EncodeCommand("put", "pfx/live", "v2")
	if _, err := k.Apply(enc1); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Apply(enc2); err != nil {
		t.Fatal(err)
	}

	// Advance past expiry.
	if _, err := k.Apply(EncodeTick(7000)); err != nil {
		t.Fatal(err)
	}

	// Range scan: only "pfx/live" should appear.
	rangeEnc, _ := EncodeCommand("range", "pfx/", "")
	res, err := k.Apply(rangeEnc)
	if err != nil {
		t.Fatal(err)
	}
	kvs, err := DecodeKeyValuesResult(res)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range kvs {
		if kv.Key == "pfx/exp" {
			t.Fatal("expired key appeared in range result")
		}
	}
	if len(kvs) != 1 || kvs[0].Key != "pfx/live" {
		t.Fatalf("expected [pfx/live], got %v", kvs)
	}
}

// Two replicas applying the same command stream must reach identical state.
func TestReplicaConsistencyTTL(t *testing.T) {
	r1 := NewKVStore()
	r2 := NewKVStore()

	cmds := [][]byte{
		EncodeTick(1000),
		mustEncode(EncodeCommandWithTTL("put", "a", "1", "", 0, 1000, 5)),
		mustEncode(EncodeCommandWithTTL("put", "b", "2", "", 0, 1000, 10)),
		mustEncode(EncodeCommand("put", "c", "3")),
		EncodeTick(7000), // expires "a" (expiresAt=6000) but not "b" or "c"
	}

	for _, cmd := range cmds {
		if _, err := r1.Apply(cmd); err != nil {
			t.Fatal(err)
		}
		if _, err := r2.Apply(cmd); err != nil {
			t.Fatal(err)
		}
	}

	r1.mu.RLock()
	r2.mu.RLock()
	defer r1.mu.RUnlock()
	defer r2.mu.RUnlock()

	if r1.applyTimeMs != r2.applyTimeMs {
		t.Fatalf("applyTimeMs diverged: r1=%d r2=%d", r1.applyTimeMs, r2.applyTimeMs)
	}
	if len(r1.data) != len(r2.data) {
		t.Fatalf("data length diverged: r1=%d r2=%d", len(r1.data), len(r2.data))
	}
	for k, v1 := range r1.data {
		v2, ok := r2.data[k]
		if !ok || v1.ExpiresAtMs != v2.ExpiresAtMs || v1.Value != v2.Value {
			t.Fatalf("key %q diverged: r1=%+v r2=%+v", k, v1, v2)
		}
	}
}

// ---------------------------------------------------------------------------
// #207.3: Committed tick op + deterministic sweep of expired keys
// ---------------------------------------------------------------------------

func TestTickSweepsExpiredKeys(t *testing.T) {
	k := NewKVStore()

	// Set up: put two keys with different TTLs.
	if _, err := k.Apply(EncodeTick(0)); err != nil {
		t.Fatal(err)
	}
	enc1, _ := EncodeCommandWithTTL("put", "short", "v1", "", 0, 0, 5)  // expires at 5000
	enc2, _ := EncodeCommandWithTTL("put", "longer", "v2", "", 0, 0, 10) // expires at 10000
	if _, err := k.Apply(enc1); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Apply(enc2); err != nil {
		t.Fatal(err)
	}

	// Tick at t=6000: should sweep "short" but not "longer".
	res, err := k.Apply(EncodeTick(6000))
	if err != nil {
		t.Fatal(err)
	}
	kr, _ := DecodeResult(res)
	if kr.Error != "" {
		t.Fatalf("tick returned error: %s", kr.Error)
	}
	// tick result should contain the count of swept keys.
	if kr.Value != "1" {
		t.Fatalf("expected 1 swept key, got %q", kr.Value)
	}

	k.mu.RLock()
	_, shortPresent := k.data["short"]
	_, longerPresent := k.data["longer"]
	k.mu.RUnlock()

	if shortPresent {
		t.Fatal("'short' should have been swept")
	}
	if !longerPresent {
		t.Fatal("'longer' should still be present")
	}
}

func TestTickSweepEmitsDeleteEvents(t *testing.T) {
	k := NewKVStore()

	if _, err := k.Apply(EncodeTick(0)); err != nil {
		t.Fatal(err)
	}
	enc, _ := EncodeCommandWithTTL("put", "evt", "v", "", 0, 0, 1)
	if _, err := k.Apply(enc); err != nil {
		t.Fatal(err)
	}

	// Collect events.
	var gotEvents []Event
	done := make(chan struct{})
	go func() {
		defer close(done)
		for evts := range k.eventCh {
			for _, ev := range evts {
				if ev.Type == EventDelete && ev.Key == "evt" {
					gotEvents = append(gotEvents, ev)
				}
			}
		}
	}()

	// Tick past expiry.
	if _, err := k.Apply(EncodeTick(2000)); err != nil {
		t.Fatal(err)
	}

	// Drain the event channel.
	close(k.eventCh)
	<-done
	k.eventCh = make(chan []Event, defaultEventChanSize) // replace for future use

	if len(gotEvents) == 0 {
		t.Fatal("expected EventDelete for swept key, got none")
	}
	if gotEvents[0].Key != "evt" {
		t.Fatalf("wrong key in delete event: %+v", gotEvents[0])
	}
}

func TestTickSweepIsDeterministicOrder(t *testing.T) {
	k := NewKVStore()

	if _, err := k.Apply(EncodeTick(0)); err != nil {
		t.Fatal(err)
	}
	// Insert keys in non-alphabetical order.
	for _, key := range []string{"z", "a", "m", "b"} {
		enc, _ := EncodeCommandWithTTL("put", key, "v", "", 0, 0, 1)
		if _, err := k.Apply(enc); err != nil {
			t.Fatal(err)
		}
	}

	var sweepOrder []string
	origEmit := k.emitEvents
	_ = origEmit // we'll just check via history

	if _, err := k.Apply(EncodeTick(2000)); err != nil {
		t.Fatal(err)
	}

	// All should be swept. Check that history has them in sorted order.
	k.mu.RLock()
	h := k.GetHistory(0)
	k.mu.RUnlock()

	for _, ev := range h {
		if ev.Type == EventDelete {
			sweepOrder = append(sweepOrder, ev.Key)
		}
	}
	if !sort.StringsAreSorted(sweepOrder) {
		t.Fatalf("sweep order not sorted: %v", sweepOrder)
	}
}

func TestTickSweepIsIdempotent(t *testing.T) {
	k := NewKVStore()

	if _, err := k.Apply(EncodeTick(0)); err != nil {
		t.Fatal(err)
	}
	enc, _ := EncodeCommandWithTTL("put", "once", "v", "", 0, 0, 1)
	if _, err := k.Apply(enc); err != nil {
		t.Fatal(err)
	}

	// First tick past expiry: sweeps 1 key.
	res1, _ := k.Apply(EncodeTick(2000))
	kr1, _ := DecodeResult(res1)
	if kr1.Value != "1" {
		t.Fatalf("first tick should sweep 1 key, got %q", kr1.Value)
	}

	// Second tick at same or later time: nothing to sweep.
	res2, _ := k.Apply(EncodeTick(3000))
	kr2, _ := DecodeResult(res2)
	if kr2.Value != "0" {
		t.Fatalf("second tick should sweep 0 keys, got %q", kr2.Value)
	}
}

// ---------------------------------------------------------------------------
// #207.4: Snapshot/restore of applyTimeMs + ExpiresAtMs
// ---------------------------------------------------------------------------

func TestSnapshotRestorePreservesApplyTimeMs(t *testing.T) {
	src := NewKVStore()
	if _, err := src.Apply(EncodeTick(42000)); err != nil {
		t.Fatal(err)
	}

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := snap.Reader()
	defer r.Close()

	dst := NewKVStore()
	if err := dst.Restore(r); err != nil {
		t.Fatal(err)
	}

	dst.mu.RLock()
	got := dst.applyTimeMs
	dst.mu.RUnlock()

	if got != 42000 {
		t.Fatalf("applyTimeMs not preserved: want 42000, got %d", got)
	}
}

func TestSnapshotRestorePreservesExpiresAtMs(t *testing.T) {
	src := NewKVStore()
	if _, err := src.Apply(EncodeTick(1000)); err != nil {
		t.Fatal(err)
	}
	enc, _ := EncodeCommandWithTTL("put", "ttlkey", "val", "", 0, 1000, 30)
	if _, err := src.Apply(enc); err != nil {
		t.Fatal(err)
	}

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := snap.Reader()
	defer r.Close()

	dst := NewKVStore()
	if err := dst.Restore(r); err != nil {
		t.Fatal(err)
	}

	dst.mu.RLock()
	kv := dst.data["ttlkey"]
	dst.mu.RUnlock()
	if kv == nil {
		t.Fatal("key not restored")
	}
	if kv.ExpiresAtMs != 31000 { // 1000 + 30*1000
		t.Fatalf("ExpiresAtMs not preserved: want 31000, got %d", kv.ExpiresAtMs)
	}
}

// After a restore, TTL expiry still works: a key that expired post-snapshot
// expires correctly once the clock is advanced.
func TestSnapshotRestoreTTLStillExpiresPostRestore(t *testing.T) {
	src := NewKVStore()
	if _, err := src.Apply(EncodeTick(1000)); err != nil {
		t.Fatal(err)
	}
	enc, _ := EncodeCommandWithTTL("put", "willexp", "v", "", 0, 1000, 5)
	if _, err := src.Apply(enc); err != nil {
		t.Fatal(err) // expires at t=6000
	}

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := snap.Reader()
	defer r.Close()

	dst := NewKVStore()
	if err := dst.Restore(r); err != nil {
		t.Fatal(err)
	}

	// Advance past expiry on restored store.
	if _, err := dst.Apply(EncodeTick(7000)); err != nil {
		t.Fatal(err)
	}

	get, _ := EncodeCommand("get_v2", "willexp", "")
	res, err := dst.Apply(get)
	if err != nil {
		t.Fatal(err)
	}
	kr, _ := DecodeResult(res)
	if kr.Error == "" {
		t.Fatalf("key should be expired post-restore, got value: %s", kr.Value)
	}
}

// Legacy JSON snapshot (no TTL fields) restores OK: applyTimeMs=0, ExpiresAtMs=0.
func TestLegacyJSONSnapshotRestoresWithZeroTTL(t *testing.T) {
	raw := `{"revision":5,"index":10,"data":{"k":{"key":"k","value":"v","create_revision":1,"mod_revision":1,"version":1}}}`
	dst := NewKVStore()
	if err := dst.Restore(newStringReader(raw)); err != nil {
		t.Fatal(err)
	}
	dst.mu.RLock()
	defer dst.mu.RUnlock()
	if dst.applyTimeMs != 0 {
		t.Fatalf("applyTimeMs should be 0 for legacy snapshot, got %d", dst.applyTimeMs)
	}
	if kv := dst.data["k"]; kv == nil {
		t.Fatal("key not restored")
	} else if kv.ExpiresAtMs != 0 {
		t.Fatalf("ExpiresAtMs should be 0 for legacy key, got %d", kv.ExpiresAtMs)
	}
}

// ---------------------------------------------------------------------------
// Helper utilities
// ---------------------------------------------------------------------------

func mustEncode(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}

type stringReader struct{ s string; pos int }
func newStringReader(s string) *stringReader { return &stringReader{s: s} }
func (r *stringReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.s) {
		return 0, io.EOF
	}
	n = copy(p, r.s[r.pos:])
	r.pos += n
	return n, nil
}

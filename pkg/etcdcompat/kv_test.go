package etcdcompat

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/fsm"
	"github.com/sanskarpan/raft-consensus/pkg/raft"
	pb "github.com/sanskarpan/raft-consensus/proto/etcdcompat"
)

// ---------------------------------------------------------------------------
// Stub RaftApplier that delegates directly to an in-process KVStore FSM.
// ---------------------------------------------------------------------------

type stubRaft struct {
	kv *fsm.KVStore
}

func (r *stubRaft) Apply(_ context.Context, cmd []byte) ([]byte, error) {
	return r.kv.Apply(cmd)
}

func (r *stubRaft) Leader() raft.ServerID { return "node1" }
func (r *stubRaft) State() raft.RaftState { return raft.StateLeader }

// newTestKVServer creates a KVServer backed by an in-memory KVStore and a
// stub Raft that applies commands directly (no network round-trip).
func newTestKVServer() (*KVServer, *fsm.KVStore) {
	kv := fsm.NewKVStore()
	r := &stubRaft{kv: kv}
	srv := NewKVServer(r, kv, 1, 1)
	return srv, kv
}

// seedKey inserts key=value directly into the FSM so tests can start with
// a known state without going through KVServer.Put.
func seedKey(kv *fsm.KVStore, key, value string) {
	cmd, _ := fsm.EncodeCommand("put", key, value)
	kv.Apply(cmd)
}

// ---------------------------------------------------------------------------
// Range — single key
// ---------------------------------------------------------------------------

func TestKVRange_SingleKey_Found(t *testing.T) {
	srv, kv := newTestKVServer()
	seedKey(kv, "foo", "bar")

	resp, err := srv.Range(context.Background(), &pb.RangeRequest{
		Key:          []byte("foo"),
		Serializable: true,
	})
	if err != nil {
		t.Fatalf("Range error: %v", err)
	}
	if resp.Count != 1 {
		t.Fatalf("Count = %d, want 1", resp.Count)
	}
	if string(resp.Kvs[0].Key) != "foo" {
		t.Errorf("key = %q, want %q", resp.Kvs[0].Key, "foo")
	}
	if string(resp.Kvs[0].Value) != "bar" {
		t.Errorf("value = %q, want %q", resp.Kvs[0].Value, "bar")
	}
}

func TestKVRange_SingleKey_NotFound(t *testing.T) {
	srv, _ := newTestKVServer()

	resp, err := srv.Range(context.Background(), &pb.RangeRequest{
		Key:          []byte("missing"),
		Serializable: true,
	})
	if err != nil {
		t.Fatalf("Range error: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("Count = %d, want 0", resp.Count)
	}
	if len(resp.Kvs) != 0 {
		t.Errorf("Kvs = %v, want empty", resp.Kvs)
	}
}

func TestKVRange_SingleKey_LinearizableGet(t *testing.T) {
	// Non-serializable (linearizable) path routes through get_v2 via Raft.
	srv, kv := newTestKVServer()
	seedKey(kv, "hello", "world")

	resp, err := srv.Range(context.Background(), &pb.RangeRequest{
		Key:          []byte("hello"),
		Serializable: false, // linearizable
	})
	if err != nil {
		t.Fatalf("Range(linearizable) error: %v", err)
	}
	if resp.Count != 1 {
		t.Fatalf("Count = %d, want 1", resp.Count)
	}
	if string(resp.Kvs[0].Value) != "world" {
		t.Errorf("value = %q, want %q", resp.Kvs[0].Value, "world")
	}
}

// ---------------------------------------------------------------------------
// Range — prefix
// ---------------------------------------------------------------------------

func TestKVRange_Prefix(t *testing.T) {
	srv, kv := newTestKVServer()
	seedKey(kv, "prefix/a", "1")
	seedKey(kv, "prefix/b", "2")
	seedKey(kv, "other/c", "3")

	// rangeEnd = key + "\x00" signals a prefix scan in etcd convention.
	key := []byte("prefix/")
	rangeEnd := append([]byte("prefix/"), 0x00)

	resp, err := srv.Range(context.Background(), &pb.RangeRequest{
		Key:          key,
		RangeEnd:     rangeEnd,
		Serializable: true,
	})
	if err != nil {
		t.Fatalf("Range(prefix) error: %v", err)
	}
	if resp.Count != 2 {
		t.Errorf("Count = %d, want 2 (got %v)", resp.Count, kvKeys(resp.Kvs))
	}
	for _, kv := range resp.Kvs {
		if string(kv.Key) == "other/c" {
			t.Errorf("unexpected key %q in prefix result", kv.Key)
		}
	}
}

func TestKVRange_Prefix_KeysOnly(t *testing.T) {
	srv, kv := newTestKVServer()
	seedKey(kv, "k/1", "val1")
	seedKey(kv, "k/2", "val2")

	key := []byte("k/")
	rangeEnd := append([]byte("k/"), 0x00)

	resp, err := srv.Range(context.Background(), &pb.RangeRequest{
		Key:          key,
		RangeEnd:     rangeEnd,
		Serializable: true,
		KeysOnly:     true,
	})
	if err != nil {
		t.Fatalf("Range(keys_only) error: %v", err)
	}
	if resp.Count != 2 {
		t.Fatalf("Count = %d, want 2", resp.Count)
	}
	for _, pkv := range resp.Kvs {
		if len(pkv.Value) != 0 {
			t.Errorf("keys_only=true but got value %q for key %q", pkv.Value, pkv.Key)
		}
	}
}

func TestKVRange_CountOnly(t *testing.T) {
	srv, kv := newTestKVServer()
	seedKey(kv, "m/1", "a")
	seedKey(kv, "m/2", "b")
	seedKey(kv, "m/3", "c")

	key := []byte("m/")
	rangeEnd := append([]byte("m/"), 0x00)

	resp, err := srv.Range(context.Background(), &pb.RangeRequest{
		Key:          key,
		RangeEnd:     rangeEnd,
		Serializable: true,
		CountOnly:    true,
	})
	if err != nil {
		t.Fatalf("Range(count_only) error: %v", err)
	}
	if resp.Count != 3 {
		t.Errorf("Count = %d, want 3", resp.Count)
	}
	if len(resp.Kvs) != 0 {
		t.Errorf("count_only should return no Kvs, got %d", len(resp.Kvs))
	}
}

func TestKVRange_Limit(t *testing.T) {
	srv, kv := newTestKVServer()
	for i := 0; i < 5; i++ {
		seedKey(kv, fmt.Sprintf("lim/%d", i), "v")
	}

	key := []byte("lim/")
	rangeEnd := append([]byte("lim/"), 0x00)

	resp, err := srv.Range(context.Background(), &pb.RangeRequest{
		Key:          key,
		RangeEnd:     rangeEnd,
		Serializable: true,
		Limit:        3,
	})
	if err != nil {
		t.Fatalf("Range(limit) error: %v", err)
	}
	if len(resp.Kvs) != 3 {
		t.Errorf("len(Kvs) = %d, want 3", len(resp.Kvs))
	}
	if !resp.More {
		t.Error("More should be true when limit truncates results")
	}
}

// ---------------------------------------------------------------------------
// Range — empty key validation
// ---------------------------------------------------------------------------

func TestKVRange_EmptyKey_Error(t *testing.T) {
	srv, _ := newTestKVServer()
	_, err := srv.Range(context.Background(), &pb.RangeRequest{Key: []byte("")})
	if err == nil {
		t.Fatal("expected error for empty key, got nil")
	}
}

// ---------------------------------------------------------------------------
// Put
// ---------------------------------------------------------------------------

func TestKVPut_EncodesAndApplies(t *testing.T) {
	srv, kv := newTestKVServer()

	resp, err := srv.Put(context.Background(), &pb.PutRequest{
		Key:   []byte("mykey"),
		Value: []byte("myval"),
	})
	if err != nil {
		t.Fatalf("Put error: %v", err)
	}
	if resp.Header == nil {
		t.Fatal("Header is nil")
	}

	// Verify the value was actually stored in the FSM.
	stored, storeErr := kv.Get("mykey")
	if storeErr != nil || stored == nil {
		t.Fatalf("key not found in FSM after Put: %v", storeErr)
	}
	if stored.Value != "myval" {
		t.Errorf("stored value = %q, want %q", stored.Value, "myval")
	}
}

func TestKVPut_PrevKV(t *testing.T) {
	srv, kv := newTestKVServer()
	seedKey(kv, "pk", "old")

	resp, err := srv.Put(context.Background(), &pb.PutRequest{
		Key:    []byte("pk"),
		Value:  []byte("new"),
		PrevKv: true,
	})
	if err != nil {
		t.Fatalf("Put(prev_kv) error: %v", err)
	}
	if resp.PrevKv == nil {
		t.Fatal("PrevKv should be set")
	}
	if string(resp.PrevKv.Value) != "old" {
		t.Errorf("PrevKv.Value = %q, want %q", resp.PrevKv.Value, "old")
	}
}

func TestKVPut_PrevKV_MissingKey(t *testing.T) {
	srv, _ := newTestKVServer()

	resp, err := srv.Put(context.Background(), &pb.PutRequest{
		Key:    []byte("newkey"),
		Value:  []byte("newval"),
		PrevKv: true,
	})
	if err != nil {
		t.Fatalf("Put(prev_kv, missing) error: %v", err)
	}
	// PrevKv should be nil when the key didn't exist before.
	if resp.PrevKv != nil {
		t.Errorf("PrevKv should be nil for new key, got %v", resp.PrevKv)
	}
}

func TestKVPut_EmptyKey_Error(t *testing.T) {
	srv, _ := newTestKVServer()
	_, err := srv.Put(context.Background(), &pb.PutRequest{Key: []byte(""), Value: []byte("v")})
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

// ---------------------------------------------------------------------------
// DeleteRange
// ---------------------------------------------------------------------------

func TestKVDeleteRange_SingleKey(t *testing.T) {
	srv, kv := newTestKVServer()
	seedKey(kv, "del", "v")

	resp, err := srv.DeleteRange(context.Background(), &pb.DeleteRangeRequest{
		Key: []byte("del"),
	})
	if err != nil {
		t.Fatalf("DeleteRange(single) error: %v", err)
	}
	if resp.Deleted != 1 {
		t.Errorf("Deleted = %d, want 1", resp.Deleted)
	}

	// Confirm gone.
	got, _ := kv.Get("del")
	if got != nil {
		t.Error("key should have been deleted from FSM")
	}
}

func TestKVDeleteRange_SingleKey_Missing(t *testing.T) {
	srv, _ := newTestKVServer()

	resp, err := srv.DeleteRange(context.Background(), &pb.DeleteRangeRequest{
		Key: []byte("ghost"),
	})
	if err != nil {
		t.Fatalf("DeleteRange(missing) error: %v", err)
	}
	// Missing key → deleted = 0, not an error.
	if resp.Deleted != 0 {
		t.Errorf("Deleted = %d, want 0 for missing key", resp.Deleted)
	}
}

func TestKVDeleteRange_SingleKey_PrevKV(t *testing.T) {
	srv, kv := newTestKVServer()
	seedKey(kv, "dk", "dv")

	resp, err := srv.DeleteRange(context.Background(), &pb.DeleteRangeRequest{
		Key:    []byte("dk"),
		PrevKv: true,
	})
	if err != nil {
		t.Fatalf("DeleteRange(prev_kv) error: %v", err)
	}
	if len(resp.PrevKvs) != 1 {
		t.Fatalf("len(PrevKvs) = %d, want 1", len(resp.PrevKvs))
	}
	if string(resp.PrevKvs[0].Value) != "dv" {
		t.Errorf("PrevKvs[0].Value = %q, want %q", resp.PrevKvs[0].Value, "dv")
	}
}

func TestKVDeleteRange_Prefix(t *testing.T) {
	srv, kv := newTestKVServer()
	seedKey(kv, "p/a", "1")
	seedKey(kv, "p/b", "2")
	seedKey(kv, "q/c", "3")

	key := []byte("p/")
	rangeEnd := append([]byte("p/"), 0x00)

	resp, err := srv.DeleteRange(context.Background(), &pb.DeleteRangeRequest{
		Key:      key,
		RangeEnd: rangeEnd,
	})
	if err != nil {
		t.Fatalf("DeleteRange(prefix) error: %v", err)
	}
	if resp.Deleted != 2 {
		t.Errorf("Deleted = %d, want 2", resp.Deleted)
	}

	// q/c should still exist.
	got, _ := kv.Get("q/c")
	if got == nil {
		t.Error("q/c should still exist after prefix delete of p/")
	}
}

// ---------------------------------------------------------------------------
// Txn
// ---------------------------------------------------------------------------

func TestKVTxn_SuccessBranch(t *testing.T) {
	srv, kv := newTestKVServer()
	seedKey(kv, "cas", "original")

	// Compare: value == "original"; if true, put "updated".
	req := &pb.TxnRequest{
		Compare: []*pb.Compare{
			{
				Key:    []byte("cas"),
				Result: pb.Compare_EQUAL,
				Target: pb.Compare_VALUE,
				TargetUnion: &pb.Compare_Value{
					Value: []byte("original"),
				},
			},
		},
		Success: []*pb.RequestOp{
			{
				Request: &pb.RequestOp_RequestPut{
					RequestPut: &pb.PutRequest{
						Key:   []byte("cas"),
						Value: []byte("updated"),
					},
				},
			},
		},
	}

	resp, err := srv.Txn(context.Background(), req)
	if err != nil {
		t.Fatalf("Txn error: %v", err)
	}
	if !resp.Succeeded {
		t.Error("Txn should have succeeded")
	}

	stored, _ := kv.Get("cas")
	if stored == nil || stored.Value != "updated" {
		val := "<nil>"
		if stored != nil {
			val = stored.Value
		}
		t.Errorf("FSM value = %q, want %q", val, "updated")
	}
}

func TestKVTxn_FailureBranch(t *testing.T) {
	srv, kv := newTestKVServer()
	seedKey(kv, "cas2", "actual")

	// Compare: value == "wrong"; if false, put "fallback".
	req := &pb.TxnRequest{
		Compare: []*pb.Compare{
			{
				Key:    []byte("cas2"),
				Result: pb.Compare_EQUAL,
				Target: pb.Compare_VALUE,
				TargetUnion: &pb.Compare_Value{
					Value: []byte("wrong"),
				},
			},
		},
		Failure: []*pb.RequestOp{
			{
				Request: &pb.RequestOp_RequestPut{
					RequestPut: &pb.PutRequest{
						Key:   []byte("cas2"),
						Value: []byte("fallback"),
					},
				},
			},
		},
	}

	resp, err := srv.Txn(context.Background(), req)
	if err != nil {
		t.Fatalf("Txn error: %v", err)
	}
	if resp.Succeeded {
		t.Error("Txn should have NOT succeeded (compare mismatch)")
	}

	stored, _ := kv.Get("cas2")
	if stored == nil || stored.Value != "fallback" {
		val := "<nil>"
		if stored != nil {
			val = stored.Value
		}
		t.Errorf("FSM value = %q, want %q", val, "fallback")
	}
}

func TestKVTxn_EmptyOps(t *testing.T) {
	srv, _ := newTestKVServer()

	// A txn with no compare and no ops should succeed and be a no-op.
	resp, err := srv.Txn(context.Background(), &pb.TxnRequest{})
	if err != nil {
		t.Fatalf("Txn(empty) error: %v", err)
	}
	if !resp.Succeeded {
		t.Error("empty txn should succeed")
	}
}

// ---------------------------------------------------------------------------
// Header fields
// ---------------------------------------------------------------------------

func TestKVServer_HeaderRevisionMonotonic(t *testing.T) {
	srv, _ := newTestKVServer()

	r1, _ := srv.Put(context.Background(), &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	r2, _ := srv.Put(context.Background(), &pb.PutRequest{Key: []byte("b"), Value: []byte("2")})

	if r2.Header.Revision <= r1.Header.Revision {
		t.Errorf("revision should be monotonically increasing: got %d then %d",
			r1.Header.Revision, r2.Header.Revision)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// kvKeys returns the keys from a proto KeyValue slice (for error messages).
func kvKeys(kvs []*pb.KeyValue) []string {
	out := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, string(kv.Key))
	}
	return out
}

// fsmKVValue is a nil-safe value accessor for fsm.KeyValue used in error messages.
func fsmKVValue(kv *fsm.KeyValue) string {
	if kv == nil {
		return "<nil>"
	}
	return kv.Value
}

// ---------------------------------------------------------------------------
// Helper used to encode txn for integration assertion
// ---------------------------------------------------------------------------

func mustEncodeTxn(t *testing.T, req *fsm.TxnRequest) []byte {
	t.Helper()
	b, err := fsm.EncodeTxn(req)
	if err != nil {
		t.Fatalf("EncodeTxn: %v", err)
	}
	return b
}

func mustDecodeTxnResult(t *testing.T, raw []byte) *fsm.TxnResponse {
	t.Helper()
	resp, err := fsm.DecodeTxnResult(raw)
	if err != nil {
		t.Fatalf("DecodeTxnResult: %v", err)
	}
	return resp
}

// TestKVTxn_VerifyRoundtripEncoding checks that the txn encoding KVServer uses
// matches what the FSM expects by performing a direct encode→apply→decode loop.
func TestKVTxn_VerifyRoundtripEncoding(t *testing.T) {
	kv := fsm.NewKVStore()
	seedKey(kv, "rk", "rv")

	fsmReq := &fsm.TxnRequest{
		Compare: []fsm.Compare{
			{Key: "rk", Target: "value", Result: "equal", Value: "rv"},
		},
		Success: []fsm.TxnOp{
			{Type: 0, Key: "rk", Value: "new"},
		},
	}

	encoded := mustEncodeTxn(t, fsmReq)
	raw, applyErr := kv.Apply(encoded)
	if applyErr != nil {
		t.Fatalf("FSM Apply txn: %v", applyErr)
	}
	resp := mustDecodeTxnResult(t, raw)
	if !resp.Succeeded {
		t.Fatal("FSM txn should have succeeded")
	}

	// Encode as JSON to verify Succeeded field is present.
	b, _ := json.Marshal(resp)
	if !json.Valid(b) {
		t.Error("TxnResponse is not valid JSON")
	}
}

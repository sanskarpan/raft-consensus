package fsm

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestKVStoreSet(t *testing.T) {
	kv := NewKVStore()
	cmd, _ := EncodeSet("key1", "value1")
	result, err := kv.Apply(cmd)
	if err != nil {
		t.Fatalf("Apply Set: %v", err)
	}
	res, _ := DecodeResult(result)
	if res.Error != "" {
		t.Errorf("Set returned error: %s", res.Error)
	}
	if res.Value != "value1" {
		t.Errorf("Set returned value %q, want %q", res.Value, "value1")
	}
}

func TestKVStoreGet(t *testing.T) {
	kv := NewKVStore()

	setCmd, _ := EncodeSet("k", "v")
	kv.Apply(setCmd)

	getCmd, _ := EncodeGet("k")
	result, err := kv.Apply(getCmd)
	if err != nil {
		t.Fatalf("Apply Get: %v", err)
	}
	res, _ := DecodeResult(result)
	if res.Value != "v" {
		t.Errorf("Get = %q, want %q", res.Value, "v")
	}
}

func TestKVStoreGetMissing(t *testing.T) {
	kv := NewKVStore()
	cmd, _ := EncodeGet("missing")
	result, _ := kv.Apply(cmd)
	res, _ := DecodeResult(result)
	if res.Error == "" {
		t.Error("expected error for missing key, got none")
	}
}

func TestKVStoreDelete(t *testing.T) {
	kv := NewKVStore()

	setCmd, _ := EncodeSet("key", "val")
	kv.Apply(setCmd)

	delCmd, _ := EncodeDelete("key")
	result, _ := kv.Apply(delCmd)
	res, _ := DecodeResult(result)
	if res.Error != "" {
		t.Errorf("Delete error: %s", res.Error)
	}

	// Second delete should report error.
	result2, _ := kv.Apply(delCmd)
	res2, _ := DecodeResult(result2)
	if res2.Error == "" {
		t.Error("expected error deleting already-deleted key")
	}
}

func TestKVStoreDeleteMissing(t *testing.T) {
	kv := NewKVStore()
	cmd, _ := EncodeDelete("no-key")
	result, _ := kv.Apply(cmd)
	res, _ := DecodeResult(result)
	if res.Error == "" {
		t.Error("expected error for deleting missing key")
	}
}

func TestKVStoreList(t *testing.T) {
	kv := NewKVStore()

	for _, k := range []string{"a", "b", "c"} {
		cmd, _ := EncodeSet(k, k+"-val")
		kv.Apply(cmd)
	}

	listCmd, _ := EncodeCommand("list", "", "")
	result, _ := kv.Apply(listCmd)
	res, _ := DecodeResult(result)
	if res.Error != "" {
		t.Errorf("list error: %s", res.Error)
	}
	// Result should contain all values.
	for _, v := range []string{"a-val", "b-val", "c-val"} {
		if !strings.Contains(res.Value, v) {
			t.Errorf("list result %q missing %q", res.Value, v)
		}
	}
}

func TestKVStoreUnknownOp(t *testing.T) {
	kv := NewKVStore()
	cmd, _ := EncodeCommand("unknown", "k", "v")
	result, _ := kv.Apply(cmd)
	res, _ := DecodeResult(result)
	if res.Error == "" {
		t.Error("expected error for unknown operation")
	}
}

func TestKVStoreSnapshotAndRestore(t *testing.T) {
	kv := NewKVStore()

	for _, pair := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		cmd, _ := EncodeSet(pair[0], pair[1])
		kv.Apply(cmd)
	}

	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	kv2 := NewKVStore()
	reader := snap.Reader()
	defer reader.Close()

	if err := kv2.Restore(reader); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	for _, pair := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		getCmd, _ := EncodeGet(pair[0])
		result, _ := kv2.Apply(getCmd)
		res, _ := DecodeResult(result)
		if res.Value != pair[1] {
			t.Errorf("after restore, get(%q) = %q, want %q", pair[0], res.Value, pair[1])
		}
	}
}

func TestEncodeDecodeUint64(t *testing.T) {
	cases := []uint64{0, 1, 42, 1<<32 - 1, 1<<63 - 1}
	for _, v := range cases {
		b := EncodeUint64(v)
		got := DecodeUint64(b)
		if got != v {
			t.Errorf("EncodeUint64/DecodeUint64(%d) = %d", v, got)
		}
	}
}

func TestKVStoreApplyInvalidJSON(t *testing.T) {
	kv := NewKVStore()
	_, err := kv.Apply([]byte("not-json"))
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

// ---------------------------------------------------------------------------
// v2 tests
// ---------------------------------------------------------------------------

func TestPutOperation(t *testing.T) {
	kv := NewKVStore()
	cmd, _ := EncodeCommand("put", "key1", "value1")
	result, err := kv.Apply(cmd)
	if err != nil {
		t.Fatalf("Apply put: %v", err)
	}
	// Apply returns KvResult{Value: JSON of KeyValue}
	res, _ := DecodeResult(result)
	if res.Error != "" {
		t.Errorf("put returned error: %s", res.Error)
	}
	// Decode the embedded KeyValue JSON.
	var kv2 KeyValue
	if err := json.Unmarshal([]byte(res.Value), &kv2); err != nil {
		t.Fatalf("decode embedded KeyValue: %v", err)
	}
	if kv2.Value != "value1" {
		t.Errorf("KeyValue.Value = %q, want %q", kv2.Value, "value1")
	}
	if kv2.ModRevision != 1 {
		t.Errorf("ModRevision = %d, want 1", kv2.ModRevision)
	}
	if kv2.CreateRevision != 1 {
		t.Errorf("CreateRevision = %d, want 1", kv2.CreateRevision)
	}
	if kv2.Version != 1 {
		t.Errorf("Version = %d, want 1", kv2.Version)
	}
}

func TestRevisionTracking(t *testing.T) {
	kv := NewKVStore()
	if kv.GetRevision() != 0 {
		t.Errorf("initial revision = %d, want 0", kv.GetRevision())
	}

	cmd1, _ := EncodeCommand("put", "k1", "v1")
	kv.Apply(cmd1)
	if kv.GetRevision() != 1 {
		t.Errorf("revision after put = %d, want 1", kv.GetRevision())
	}

	cmd2, _ := EncodeCommand("put", "k2", "v2")
	kv.Apply(cmd2)
	if kv.GetRevision() != 2 {
		t.Errorf("revision after second put = %d, want 2", kv.GetRevision())
	}

	// get_v2 should not increment revision.
	cmd3, _ := EncodeCommand("get_v2", "k1", "")
	kv.Apply(cmd3)
	if kv.GetRevision() != 2 {
		t.Error("linearizable read should not increment revision")
	}

	// set (legacy) should also increment revision.
	cmd4, _ := EncodeSet("k3", "v3")
	kv.Apply(cmd4)
	if kv.GetRevision() != 3 {
		t.Errorf("revision after legacy set = %d, want 3", kv.GetRevision())
	}
}

func TestRangeQuery(t *testing.T) {
	kv := NewKVStore()
	for _, key := range []string{"app/foo", "app/bar", "app/baz", "other/x"} {
		cmd, _ := EncodeCommand("put", key, "val-"+key)
		kv.Apply(cmd)
	}

	results, err := kv.Range("app/")
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Range(app/) returned %d results, want 3", len(results))
	}
	// Results should be sorted by key.
	if results[0].Key != "app/bar" || results[1].Key != "app/baz" || results[2].Key != "app/foo" {
		t.Errorf("Range results not sorted: %v", results)
	}

	// Empty prefix returns all.
	all, _ := kv.Range("")
	if len(all) != 4 {
		t.Errorf("Range(\"\") returned %d results, want 4", len(all))
	}
}

func TestEventEmission(t *testing.T) {
	kv := NewKVStore()
	ch := kv.NotificationChan()

	// put should emit EventPut.
	cmd, _ := EncodeCommand("put", "key1", "value1")
	kv.Apply(cmd)

	select {
	case events := <-ch:
		if len(events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(events))
		}
		if events[0].Type != EventPut {
			t.Errorf("event type = %v, want EventPut", events[0].Type)
		}
		if events[0].Key != "key1" {
			t.Errorf("event key = %q, want 'key1'", events[0].Key)
		}
		if events[0].KV == nil || events[0].KV.Value != "value1" {
			t.Error("event KV is nil or has wrong value")
		}
		if events[0].PrevKV != nil {
			t.Error("PrevKV should be nil for a create")
		}
	default:
		t.Error("expected event in channel after put, got none")
	}

	// delete should emit EventDelete.
	delCmd, _ := EncodeCommand("delete", "key1", "")
	kv.Apply(delCmd)

	select {
	case events := <-ch:
		if events[0].Type != EventDelete {
			t.Errorf("delete event type = %v, want EventDelete", events[0].Type)
		}
		if events[0].PrevKV == nil || events[0].PrevKV.Value != "value1" {
			t.Error("delete event PrevKV is nil or has wrong value")
		}
	default:
		t.Error("expected event in channel after delete")
	}

	// get should NOT emit events.
	getCmd, _ := EncodeCommand("get_v2", "missing", "")
	kv.Apply(getCmd)
	select {
	case <-ch:
		t.Error("unexpected event from read op")
	default:
		// correct — no event
	}
}

func TestTransactionCAS(t *testing.T) {
	kv := NewKVStore()

	// Set initial value.
	cmd, _ := EncodeCommand("put", "balance", "100")
	kv.Apply(cmd)

	// CAS: if value == "100" then set to "90".
	txnReq := &TxnRequest{
		Compare: []Compare{
			{Key: "balance", Target: "value", Result: "equal", Value: "100"},
		},
		Success: []TxnOp{{Type: 0, Key: "balance", Value: "90"}},
		Failure: []TxnOp{},
	}
	data, _ := EncodeTxn(txnReq)
	result, err := kv.Apply(data)
	if err != nil {
		t.Fatalf("Apply txn: %v", err)
	}
	txnResp, err := DecodeTxnResult(result)
	if err != nil {
		t.Fatalf("DecodeTxnResult: %v", err)
	}
	if !txnResp.Succeeded {
		t.Error("txn should have succeeded (value matches)")
	}
	if txnResp.Revision <= 1 {
		t.Errorf("txn revision = %d, want > 1", txnResp.Revision)
	}

	// Verify new value via direct Get.
	entry, _ := kv.Get("balance")
	if entry == nil || entry.Value != "90" {
		t.Errorf("balance = %v, want '90'", entry)
	}

	// CAS with wrong value — should fail (go to Failure branch which is empty).
	txnReq2 := &TxnRequest{
		Compare: []Compare{
			{Key: "balance", Target: "value", Result: "equal", Value: "100"},
		},
		Success: []TxnOp{{Type: 0, Key: "balance", Value: "80"}},
		Failure: []TxnOp{},
	}
	data2, _ := EncodeTxn(txnReq2)
	result2, _ := kv.Apply(data2)
	txnResp2, _ := DecodeTxnResult(result2)
	if txnResp2.Succeeded {
		t.Error("txn should have failed (value mismatch)")
	}

	// Value should still be "90".
	entry2, _ := kv.Get("balance")
	if entry2.Value != "90" {
		t.Errorf("balance after failed CAS = %q, want '90'", entry2.Value)
	}
}

func TestTransactionVersionCompare(t *testing.T) {
	kv := NewKVStore()
	cmd, _ := EncodeCommand("put", "key", "val")
	kv.Apply(cmd)

	// Version should now be 1. Txn comparing version == 1 should succeed.
	txnReq := &TxnRequest{
		Compare: []Compare{
			{Key: "key", Target: "version", Result: "equal", Rev: 1},
		},
		Success: []TxnOp{{Type: 0, Key: "key", Value: "new-val"}},
		Failure: []TxnOp{},
	}
	data, _ := EncodeTxn(txnReq)
	result, _ := kv.Apply(data)
	resp, _ := DecodeTxnResult(result)
	if !resp.Succeeded {
		t.Error("txn with version==1 should succeed after one put")
	}
}

func TestSnapshotWithRevision(t *testing.T) {
	kv := NewKVStore()
	putA, _ := EncodeCommand("put", "a", "1")
	putB, _ := EncodeCommand("put", "b", "2")
	kv.Apply(putA)
	kv.Apply(putB)

	if kv.GetRevision() != 2 {
		t.Errorf("revision = %d, want 2", kv.GetRevision())
	}

	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	kv2 := NewKVStore()
	reader := snap.Reader()
	defer reader.Close()
	if err := kv2.Restore(reader); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if kv2.GetRevision() != 2 {
		t.Errorf("revision after restore = %d, want 2", kv2.GetRevision())
	}

	entry, _ := kv2.Get("a")
	if entry == nil || entry.Value != "1" {
		t.Errorf("restored 'a' = %v, want '1'", entry)
	}
	entry2, _ := kv2.Get("b")
	if entry2 == nil || entry2.Value != "2" {
		t.Errorf("restored 'b' = %v, want '2'", entry2)
	}
}

func TestBackwardCompatOps(t *testing.T) {
	kv := NewKVStore()

	// Legacy "set" should still work and return KvResult{Value: <value>}.
	setCmd, _ := EncodeSet("legacy-key", "legacy-val")
	result, err := kv.Apply(setCmd)
	if err != nil {
		t.Fatalf("Apply set: %v", err)
	}
	res, _ := DecodeResult(result)
	if res.Value != "legacy-val" {
		t.Errorf("legacy set result.Value = %q, want 'legacy-val'", res.Value)
	}

	// Legacy "get" should return the value.
	getCmd, _ := EncodeGet("legacy-key")
	result2, _ := kv.Apply(getCmd)
	res2, _ := DecodeResult(result2)
	if res2.Value != "legacy-val" {
		t.Errorf("legacy get result.Value = %q, want 'legacy-val'", res2.Value)
	}

	// Legacy "delete" should return ok.
	delCmd, _ := EncodeDelete("legacy-key")
	result3, _ := kv.Apply(delCmd)
	res3, _ := DecodeResult(result3)
	if res3.Value != "ok" {
		t.Errorf("legacy delete result.Value = %q, want 'ok'", res3.Value)
	}

	// v2 "put" on same store — data is shared.
	putCmd, _ := EncodeCommand("put", "v2-key", "v2-val")
	kv.Apply(putCmd)
	entry, _ := kv.Get("v2-key")
	if entry == nil || entry.Value != "v2-val" {
		t.Errorf("v2 Get after put = %v, want 'v2-val'", entry)
	}
}

func TestHistoryBuffer(t *testing.T) {
	kv := NewKVStore()

	// Apply 5 puts.
	for i := 0; i < 5; i++ {
		cmd, _ := EncodeCommand("put", strings.Repeat("k", i+1), "v")
		kv.Apply(cmd)
	}

	// GetHistory(0) should return all 5 events.
	history := kv.GetHistory(0)
	if len(history) != 5 {
		t.Errorf("GetHistory(0) returned %d events, want 5", len(history))
	}

	// GetHistory(3) should return revisions 4 and 5.
	history2 := kv.GetHistory(3)
	if len(history2) != 2 {
		t.Errorf("GetHistory(3) returned %d events, want 2", len(history2))
	}
	for _, ev := range history2 {
		if ev.Revision <= 3 {
			t.Errorf("history event revision %d should be > 3", ev.Revision)
		}
	}
}

func TestIdempotencySnapshotRoundtrip(t *testing.T) {
	kv := NewKVStore()

	// Apply a command with idempotency ID.
	cmd, _ := EncodeCommandWithID("put", "snap-key", "snap-val", "snap-client", 42)
	result1, _ := kv.Apply(cmd)
	revAfter := kv.GetRevision()

	// Take snapshot.
	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a fresh store.
	kv2 := NewKVStore()
	reader := snap.Reader()
	defer reader.Close()
	if err := kv2.Restore(reader); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Duplicate apply on restored store must return cached result without bumping revision.
	result2, err := kv2.Apply(cmd)
	if err != nil {
		t.Fatalf("Apply on restored store: %v", err)
	}
	if kv2.GetRevision() != revAfter {
		t.Errorf("revision changed on duplicate after restore: got %d, want %d", kv2.GetRevision(), revAfter)
	}
	if string(result1) != string(result2) {
		t.Errorf("result mismatch after restore:\npre:  %s\npost: %s", result1, result2)
	}
}

func TestIdempotencyDuplicateReturnsCache(t *testing.T) {
	kv := NewKVStore()

	// First apply with clientID+seqNum.
	cmd1, err := EncodeCommandWithID("put", "idempotent-key", "value1", "client-abc", 1)
	if err != nil {
		t.Fatalf("EncodeCommandWithID: %v", err)
	}
	result1, err := kv.Apply(cmd1)
	if err != nil {
		t.Fatalf("Apply first: %v", err)
	}
	rev1 := kv.GetRevision()

	// Duplicate apply — same clientID and seqNum.
	result2, err := kv.Apply(cmd1)
	if err != nil {
		t.Fatalf("Apply duplicate: %v", err)
	}
	rev2 := kv.GetRevision()

	// Revision must not change on duplicate.
	if rev2 != rev1 {
		t.Errorf("revision changed on duplicate apply: %d → %d", rev1, rev2)
	}

	// Both results must be identical bytes (cached response).
	if string(result1) != string(result2) {
		t.Errorf("duplicate apply returned different result:\nfirst:  %s\nsecond: %s", result1, result2)
	}
}

func TestIdempotencyNewSeqNumApplies(t *testing.T) {
	kv := NewKVStore()

	cmd1, _ := EncodeCommandWithID("put", "key", "v1", "client-xyz", 1)
	kv.Apply(cmd1)
	rev1 := kv.GetRevision()

	// Different seqNum — must apply and increment revision.
	cmd2, _ := EncodeCommandWithID("put", "key", "v2", "client-xyz", 2)
	kv.Apply(cmd2)
	rev2 := kv.GetRevision()

	if rev2 != rev1+1 {
		t.Errorf("revision after new seqNum = %d, want %d", rev2, rev1+1)
	}

	entry, _ := kv.Get("key")
	if entry == nil || entry.Value != "v2" {
		t.Errorf("Get(key) = %v, want 'v2'", entry)
	}
}

func TestIdempotencyReadOpsNotTracked(t *testing.T) {
	kv := NewKVStore()

	putCmd, _ := EncodeCommandWithID("put", "ro-key", "val", "client-ro", 1)
	kv.Apply(putCmd)

	// Read op without clientID — must not affect dedup state.
	getCmd, _ := EncodeCommand("get_v2", "ro-key", "")
	r1, _ := kv.Apply(getCmd)
	r2, _ := kv.Apply(getCmd)

	// Both reads should return same value without error.
	res1, _ := DecodeResult(r1)
	res2, _ := DecodeResult(r2)
	if res1.Error != "" || res2.Error != "" {
		t.Errorf("read ops returned errors: %q / %q", res1.Error, res2.Error)
	}
}

func TestIdempotencyDeleteDedup(t *testing.T) {
	kv := NewKVStore()

	// Create key.
	putCmd, _ := EncodeCommand("put", "del-key", "to-delete")
	kv.Apply(putCmd)
	revAfterPut := kv.GetRevision()

	// Delete with idempotency ID.
	delCmd, _ := EncodeCommandWithID("delete", "del-key", "", "client-del", 7)
	result1, _ := kv.Apply(delCmd)
	revAfterDel := kv.GetRevision()

	// Duplicate delete — revision must not change; key must remain absent.
	result2, _ := kv.Apply(delCmd)
	if kv.GetRevision() != revAfterDel {
		t.Errorf("revision changed on duplicate delete: %d → %d", revAfterDel, kv.GetRevision())
	}
	if string(result1) != string(result2) {
		t.Errorf("duplicate delete returned different result")
	}
	_ = revAfterPut

	// Key should still be gone (Get returns nil, nil for missing keys).
	entry, _ := kv.Get("del-key")
	if entry != nil {
		t.Errorf("expected nil for deleted key, got %v", entry)
	}
}

// EncodeCommand with three args — helper so tests don't need to repeat (op,key,"")
func encodeApply(kv *KVStore, op, key, value string) ([]byte, error) {
	cmd, err := EncodeCommand(op, key, value)
	if err != nil {
		return nil, err
	}
	return kv.Apply(cmd)
}

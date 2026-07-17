package fsm

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// ---------------------------------------------------------------------------
// H10: snapshot Index serialized/restored; list op deterministic
// ---------------------------------------------------------------------------

// TestSnapshotIndexRestored verifies that the apply index survives a
// Snapshot/Restore round-trip. Against the pre-fix code kvSnapshotData did not
// serialize k.index, so a restored FSM reported index 0.
func TestSnapshotIndexRestored(t *testing.T) {
	kv := NewKVStore()
	for i := 0; i < 5; i++ {
		cmd, _ := EncodeCommand("set", "k", "v")
		if _, err := kv.Apply(cmd); err != nil {
			t.Fatal(err)
		}
	}

	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	wantIndex := snap.Index()
	if wantIndex == 0 {
		t.Fatal("snapshot index should be non-zero after 5 applies")
	}

	data, err := readAll(snap)
	if err != nil {
		t.Fatal(err)
	}

	restored := NewKVStore()
	if err := restored.Restore(strings.NewReader(string(data))); err != nil {
		t.Fatal(err)
	}

	snap2, err := restored.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// Snapshot() bumps index by one apply? No — Snapshot does not increment
	// index. The restored FSM must report the same index it was snapshotted at.
	if got := snap2.Index(); got != wantIndex {
		t.Fatalf("restored snapshot index = %d, want %d", got, wantIndex)
	}
}

// TestListDeterministicOrder verifies the "list" op returns values in a stable
// (sorted-by-key) order. Against the pre-fix code it iterated the Go map in
// random order, producing non-deterministic Apply output through the log.
func TestListDeterministicOrder(t *testing.T) {
	build := func() string {
		kv := NewKVStore()
		for _, k := range []string{"delta", "alpha", "charlie", "bravo", "echo"} {
			cmd, _ := EncodeCommand("set", k, "val-"+k)
			kv.Apply(cmd)
		}
		cmd, _ := EncodeCommand("list", "", "")
		res, err := kv.Apply(cmd)
		if err != nil {
			t.Fatal(err)
		}
		var r KvResult
		if err := json.Unmarshal(res, &r); err != nil {
			t.Fatal(err)
		}
		return r.Value
	}

	first := build()
	// Sorted by key: alpha, bravo, charlie, delta, echo.
	want := "[val-alpha val-bravo val-charlie val-delta val-echo]"
	if first != want {
		t.Fatalf("list output = %q, want sorted %q", first, want)
	}
	// Repeat many times to catch map-order nondeterminism.
	for i := 0; i < 50; i++ {
		if got := build(); got != first {
			t.Fatalf("list output not deterministic: %q vs %q", got, first)
		}
	}
}

// ---------------------------------------------------------------------------
// M11: txn atomicity — delete-of-missing must not report Succeeded=true
// ---------------------------------------------------------------------------

// TestTxnDeleteMissingKeyNotSucceeded verifies that a transaction whose compare
// branch is satisfied but which contains a delete of a non-existent key does
// NOT report Succeeded=true, and does not partially apply.
func TestTxnDeleteMissingKeyNotSucceeded(t *testing.T) {
	kv := NewKVStore()

	txnReq := &TxnRequest{
		Compare: []Compare{}, // empty compare → success branch
		Success: []TxnOp{
			{Type: 0, Key: "created", Value: "1"}, // put
			{Type: 1, Key: "missing"},             // delete of missing key
		},
		Failure: []TxnOp{},
	}
	data, _ := EncodeTxn(txnReq)
	res, err := kv.Apply(data)
	if err != nil {
		t.Fatal(err)
	}
	var resp TxnResponse
	if err := json.Unmarshal(res, &resp); err != nil {
		t.Fatal(err)
	}

	if resp.Succeeded {
		t.Fatal("txn with delete-of-missing-key must not report Succeeded=true")
	}

	// Atomicity: the put in the same branch must NOT have been applied.
	got, _ := kv.Get("created")
	if got != nil {
		t.Fatalf("txn aborted but put was partially applied: %+v", got)
	}
}

// TestTxnAllPresentSucceeds is a companion sanity check: a txn whose ops all
// succeed still reports Succeeded=true and applies.
func TestTxnAllPresentSucceeds(t *testing.T) {
	kv := NewKVStore()
	set, _ := EncodeCommand("set", "existing", "0")
	kv.Apply(set)

	txnReq := &TxnRequest{
		Compare: []Compare{},
		Success: []TxnOp{
			{Type: 0, Key: "new", Value: "1"},
			{Type: 1, Key: "existing"},
		},
		Failure: []TxnOp{},
	}
	data, _ := EncodeTxn(txnReq)
	res, _ := kv.Apply(data)
	var resp TxnResponse
	json.Unmarshal(res, &resp)
	if !resp.Succeeded {
		t.Fatal("valid txn should report Succeeded=true")
	}
	if got, _ := kv.Get("new"); got == nil || got.Value != "1" {
		t.Fatal("valid txn put not applied")
	}
	if got, _ := kv.Get("existing"); got != nil {
		t.Fatal("valid txn delete not applied")
	}
}

// ---------------------------------------------------------------------------
// L4: revision overflow guard
// ---------------------------------------------------------------------------

// TestRevisionOverflowGuard verifies that once the revision reaches the
// documented ceiling, mutations are refused rather than wrapping negative.
func TestRevisionOverflowGuard(t *testing.T) {
	kv := NewKVStore()
	// Force the counter to the ceiling.
	kv.mu.Lock()
	kv.revision = maxRevision
	kv.mu.Unlock()

	cmd, _ := EncodeCommand("put", "k", "v")
	res, err := kv.Apply(cmd)
	if err != nil {
		t.Fatal(err)
	}
	var r KvResult
	json.Unmarshal(res, &r)
	if r.Error == "" {
		t.Fatal("expected mutation at revision ceiling to error")
	}

	// Revision must not have wrapped negative.
	if rev := kv.GetRevision(); rev < 0 {
		t.Fatalf("revision wrapped negative: %d", rev)
	}
}

// ---------------------------------------------------------------------------
// H11: watch register/snapshot-revision race → no dup / out-of-order
// ---------------------------------------------------------------------------

// TestWatchNoDuplicateAcrossReplayLive applies history, then subscribes with
// sinceRevision=0 (which triggers replay) and applies a new live event. Each
// event's revision must be delivered exactly once. Against the pre-fix code the
// snapshotRevision was read after registration, so an event could be delivered
// both via replay and live.
func TestWatchNoDuplicateAcrossReplayLive(t *testing.T) {
	kv := NewKVStore()
	wm := NewWatchManager(kv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm.Start(ctx)

	// Build history.
	applyPut(kv, "k", "v0")
	applyPut(kv, "k", "v1")

	ch, id := wm.Watch(ctx, "k", 0) // replay all history + live
	defer wm.Cancel(id)

	// Live event.
	applyPut(kv, "k", "v2")

	seen := map[int64]int{}
	deadline := time.After(2 * time.Second)
	for len(seen) < 3 {
		select {
		case we := <-ch:
			for _, ev := range we.Events {
				seen[ev.Revision]++
				if seen[ev.Revision] > 1 {
					t.Fatalf("revision %d delivered more than once (dup)", ev.Revision)
				}
			}
		case <-deadline:
			t.Fatalf("timed out: saw %d distinct revisions, want 3", len(seen))
		}
	}

	// Drain briefly to catch a late duplicate.
	drainDeadline := time.After(150 * time.Millisecond)
	for {
		select {
		case we := <-ch:
			for _, ev := range we.Events {
				seen[ev.Revision]++
				if seen[ev.Revision] > 1 {
					t.Fatalf("revision %d delivered more than once (late dup)", ev.Revision)
				}
			}
		case <-drainDeadline:
			return
		}
	}
}

// TestRestoreDrainsEventCh verifies that events buffered in the FSM's event
// channel before a Restore are dropped and not delivered afterwards (H11).
func TestRestoreDrainsEventCh(t *testing.T) {
	src := NewKVStore()
	applyPut(src, "a", "1")
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	data, err := readAll(snap)
	if err != nil {
		t.Fatal(err)
	}

	// Buffer some events in the target's eventCh WITHOUT anyone consuming.
	target := NewKVStore()
	applyPut(target, "stale1", "x")
	applyPut(target, "stale2", "y")

	if len(target.eventCh) == 0 {
		t.Fatal("expected buffered events before restore")
	}

	if err := target.Restore(strings.NewReader(string(data))); err != nil {
		t.Fatal(err)
	}

	if n := len(target.eventCh); n != 0 {
		t.Fatalf("eventCh not drained after Restore: %d pending", n)
	}
}

// readAll reads a raft.Snapshot's contents fully.
func readAll(snap raft.Snapshot) ([]byte, error) {
	rc := snap.Reader()
	defer rc.Close()
	return io.ReadAll(rc)
}

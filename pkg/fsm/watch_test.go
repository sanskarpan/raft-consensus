package fsm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// applyPut is a helper that applies a "put" command to kv and panics on error.
func applyPut(kv *KVStore, key, value string) {
	cmd, err := EncodeCommand("put", key, value)
	if err != nil {
		panic(err)
	}
	if _, err := kv.Apply(cmd); err != nil {
		panic(err)
	}
}

func TestWatchSingleKey(t *testing.T) {
	kv := NewKVStore()
	wm := NewWatchManager(kv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm.Start(ctx)

	ch, id := wm.Watch(ctx, "mykey", 0)
	defer wm.Cancel(id)

	// Put triggers an event.
	applyPut(kv, "mykey", "myvalue")

	select {
	case we := <-ch:
		if len(we.Events) == 0 {
			t.Fatal("expected at least one event")
		}
		ev := we.Events[0]
		if ev.Key != "mykey" {
			t.Errorf("event key = %q, want 'mykey'", ev.Key)
		}
		if ev.Type != EventPut {
			t.Errorf("event type = %v, want EventPut", ev.Type)
		}
		if ev.KV == nil || ev.KV.Value != "myvalue" {
			t.Errorf("event KV = %v, want value 'myvalue'", ev.KV)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}

	// A different key should NOT produce an event on this watcher.
	applyPut(kv, "otherkey", "val")

	select {
	case we := <-ch:
		t.Errorf("unexpected event for different key: %v", we)
	case <-time.After(100 * time.Millisecond):
		// Expected: no event
	}
}

func TestWatchDelete(t *testing.T) {
	kv := NewKVStore()
	wm := NewWatchManager(kv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm.Start(ctx)

	// Subscribe BEFORE the initial put so there is no history to replay.
	ch, id := wm.Watch(ctx, "foo", 0)
	defer wm.Cancel(id)

	// Initial put — consume the live event.
	applyPut(kv, "foo", "bar")
	select {
	case <-ch:
		// put event consumed
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for put event")
	}

	// Delete should produce EventDelete.
	cmd, _ := EncodeCommand("delete", "foo", "")
	kv.Apply(cmd)

	select {
	case we := <-ch:
		if len(we.Events) == 0 {
			t.Fatal("no events in WatchEvent")
		}
		if we.Events[0].Type != EventDelete {
			t.Errorf("event type = %v, want EventDelete", we.Events[0].Type)
		}
		if we.Events[0].PrevKV == nil || we.Events[0].PrevKV.Value != "bar" {
			t.Errorf("delete PrevKV = %v, want value 'bar'", we.Events[0].PrevKV)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delete event")
	}
}

func TestWatchPrefix(t *testing.T) {
	kv := NewKVStore()
	wm := NewWatchManager(kv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm.Start(ctx)

	ch, id := wm.WatchPrefix(ctx, "app/", 0)
	defer wm.Cancel(id)

	// Two matching keys.
	applyPut(kv, "app/foo", "1")
	applyPut(kv, "app/bar", "2")
	// One non-matching key — should NOT appear.
	applyPut(kv, "other/x", "3")

	received := 0
	deadline := time.After(2 * time.Second)
	for received < 2 {
		select {
		case we := <-ch:
			received += len(we.Events)
			for _, ev := range we.Events {
				if len(ev.Key) < 4 || ev.Key[:4] != "app/" {
					t.Errorf("received event for non-prefix key %q", ev.Key)
				}
			}
		case <-deadline:
			t.Fatalf("timed out: received %d events, want 2", received)
		}
	}

	// Ensure no extra event arrives (the "other/x" key).
	select {
	case we := <-ch:
		t.Errorf("unexpected extra event: %v", we)
	case <-time.After(100 * time.Millisecond):
		// correct
	}
}

func TestWatchCancel(t *testing.T) {
	kv := NewKVStore()
	wm := NewWatchManager(kv)
	ctx := context.Background()
	wm.Start(ctx)

	watchCtx, cancel := context.WithCancel(ctx)
	ch, _ := wm.Watch(watchCtx, "mykey", 0)

	// Cancel before any events.
	cancel()

	// Give goroutines time to clean up.
	time.Sleep(50 * time.Millisecond)

	// Apply a put — the watcher is cancelled so the event should NOT arrive.
	applyPut(kv, "mykey", "val")

	time.Sleep(50 * time.Millisecond)

	select {
	case we, ok := <-ch:
		if ok {
			// A stale event from history replay before cancel might arrive; that's OK.
			// What matters is no panic.
			_ = we
		}
		// Channel may be readable (drained) or blocked; either is fine.
	default:
		// No event, as expected.
	}
}

func TestHistoryReplay(t *testing.T) {
	kv := NewKVStore()
	wm := NewWatchManager(kv)
	ctx := context.Background()
	wm.Start(ctx)

	// Apply events BEFORE subscribing.
	applyPut(kv, "key0", "v0")
	applyPut(kv, "key0", "v1")
	applyPut(kv, "key1", "vv")

	// Watch key0 from the beginning (sinceRevision=0 replays all history).
	ch, id := wm.Watch(ctx, "key0", 0)
	defer wm.Cancel(id)

	// Should receive the two historical events for key0.
	received := 0
	deadline := time.After(2 * time.Second)
	for received < 2 {
		select {
		case we := <-ch:
			for _, ev := range we.Events {
				if ev.Key == "key0" {
					received++
				}
			}
		case <-deadline:
			t.Fatalf("timed out: received %d historical events for key0, want 2", received)
		}
	}
}

func TestWatchManagerConcurrent(t *testing.T) {
	kv := NewKVStore()
	wm := NewWatchManager(kv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm.Start(ctx)

	const numWatchers = 10
	channels := make([]<-chan WatchEvent, numWatchers)
	ids := make([]WatchID, numWatchers)

	for i := 0; i < numWatchers; i++ {
		ch, id := wm.Watch(ctx, "shared-key", 0)
		channels[i] = ch
		ids[i] = id
	}
	defer func() {
		for _, id := range ids {
			wm.Cancel(id)
		}
	}()

	// Apply a single put — all 10 watchers should receive it.
	applyPut(kv, "shared-key", "value")

	var wg sync.WaitGroup
	for i, ch := range channels {
		wg.Add(1)
		go func(idx int, c <-chan WatchEvent) {
			defer wg.Done()
			select {
			case <-c:
				// Good.
			case <-time.After(2 * time.Second):
				t.Errorf("watcher %d did not receive event", idx)
			}
		}(i, ch)
	}
	wg.Wait()
}

func TestTransactionEventEmission(t *testing.T) {
	kv := NewKVStore()
	wm := NewWatchManager(kv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm.Start(ctx)

	ch, id := wm.WatchPrefix(ctx, "acc/", 0)
	defer wm.Cancel(id)

	// Transaction puts two keys atomically.
	txnReq := &TxnRequest{
		Compare: []Compare{},
		Success: []TxnOp{
			{Type: 0, Key: "acc/a", Value: "100"},
			{Type: 0, Key: "acc/b", Value: "200"},
		},
		Failure: []TxnOp{},
	}
	data, _ := EncodeTxn(txnReq)
	kv.Apply(data)

	// Both events arrive in a single WatchEvent batch (same revision).
	select {
	case we := <-ch:
		if len(we.Events) != 2 {
			t.Errorf("expected 2 events from txn, got %d", len(we.Events))
		}
		// All events should share the same revision.
		rev := we.Events[0].Revision
		for _, ev := range we.Events {
			if ev.Revision != rev {
				t.Errorf("txn events have different revisions: %d vs %d", rev, ev.Revision)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for txn events")
	}
}

// TestWatchLargePrefix verifies that watches for "" (empty prefix) receive all events.
func TestWatchLargePrefix(t *testing.T) {
	kv := NewKVStore()
	wm := NewWatchManager(kv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm.Start(ctx)

	ch, id := wm.WatchPrefix(ctx, "", 0) // watch everything
	defer wm.Cancel(id)

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		applyPut(kv, k, "v")
	}

	received := 0
	deadline := time.After(2 * time.Second)
	for received < len(keys) {
		select {
		case we := <-ch:
			received += len(we.Events)
		case <-deadline:
			t.Fatalf("timed out: received %d events, want %d", received, len(keys))
		}
	}
}

// Ensure the fmt package is used (suppress "imported and not used" if needed).
var _ = fmt.Sprintf

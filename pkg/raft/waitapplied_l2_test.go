package raft

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// L2: WaitApplied must not spawn a per-call watcher goroutine. Launch many
// concurrent waiters that all get canceled, then confirm the goroutine count
// returns to (near) baseline — i.e. no leak / no per-call goroutine lingering.
func TestWaitAppliedNoPerCallGoroutine(t *testing.T) {
	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	r, _, _ := makeRaftNode("n1", cfg)
	r.mu.Lock()
	r.applyIndex = 0
	r.mu.Unlock()

	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	base := runtime.NumGoroutine()

	const n = 300
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = r.WaitApplied(ctx, 1_000_000) }() // never applies
	}
	// Let them all park on the wait channel.
	time.Sleep(100 * time.Millisecond)
	cancel() // ctx cancellation must free every waiter with no leftover goroutine
	wg.Wait()

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > base+10 {
		t.Fatalf("goroutine leak after %d WaitApplied calls: base=%d after=%d", n, base, after)
	}
}

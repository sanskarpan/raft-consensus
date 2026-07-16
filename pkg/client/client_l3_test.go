package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// L3 — watch reconnect robustness: jittered backoff, address failover/rotation,
// SSE idle read deadline, and SubmitCommand routed through backoff.
// ---------------------------------------------------------------------------

// TestWatchBackoffIsJittered verifies watchBackoff applies full jitter: two
// computed backoffs for the same attempt differ (with overwhelming probability)
// and every sample stays within [0, base) for that attempt. Pre-fix the loop
// used a fixed exponential value with no jitter.
func TestWatchBackoffIsJittered(t *testing.T) {
	// base for attempt 4 is 100ms<<4 = 1600ms.
	const attempt = 4
	base := 100 * time.Millisecond
	for i := 0; i < attempt; i++ {
		base *= 2
	}

	seen := make(map[time.Duration]struct{})
	for i := 0; i < 64; i++ {
		d := watchBackoff(attempt)
		if d < 0 || d >= base {
			t.Fatalf("watchBackoff(%d) = %v, want in [0, %v)", attempt, d, base)
		}
		seen[d] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("watchBackoff produced no jitter: %d distinct values over 64 samples", len(seen))
	}
}

// TestWatchBackoffCapped verifies the base is capped at 30s even for large
// attempt counts, so the jittered value never exceeds the cap.
func TestWatchBackoffCapped(t *testing.T) {
	for i := 0; i < 32; i++ {
		if d := watchBackoff(20); d < 0 || d >= 30*time.Second {
			t.Fatalf("watchBackoff(20) = %v, want in [0, 30s)", d)
		}
	}
}

// TestWatchRotatesAddresses verifies successive reconnect attempts rotate
// through all configured addresses instead of always hitting addrs[0]. Both
// servers reject the watch (non-200) so the loop keeps reconnecting; we assert
// the second address receives at least one watch request.
func TestWatchRotatesAddresses(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}

	mk := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/v1/watch") {
				mu.Lock()
				hits[name]++
				mu.Unlock()
				// Reject so the client backs off and reconnects (rotating addr).
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			http.NotFound(w, r)
		}))
	}
	s1, s2 := mk("s1"), mk("s2")
	defer s1.Close()
	defer s2.Close()

	addr := func(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }
	c := NewClient(WithAddresses([]string{addr(s1), addr(s2)}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := c.Watch(ctx, "k")
	go func() {
		for range ch {
		}
	}()

	// Wait until the second address has been contacted (rotation happened) or
	// a deadline elapses.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		s2hits := hits["s2"]
		mu.Unlock()
		if s2hits > 0 {
			return // success: rotation reached the second address
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("watch never rotated to second address: hits=%v", hits)
}

// TestWatchFailsOverToSecondAddress verifies that when the first configured
// address is dead (connection refused), the watch fails over to a live second
// address and delivers events from it. Pre-fix the loop only ever dialed
// addrs[0] and would never reach the healthy node.
func TestWatchFailsOverToSecondAddress(t *testing.T) {
	// Live server that streams one SSE event then holds the connection.
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/watch") {
			fl, _ := w.(http.Flusher)
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("data: {\"revision\":42,\"events\":[]}\n\n"))
			if fl != nil {
				fl.Flush()
			}
			// Keep the stream open a moment so the client stays connected.
			select {
			case <-r.Context().Done():
			case <-time.After(500 * time.Millisecond):
			}
			return
		}
		http.NotFound(w, r)
	}))
	defer live.Close()

	// Dead address: bind then immediately close a listener to get a port that
	// refuses connections.
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadAddr := strings.TrimPrefix(dead.URL, "http://")
	dead.Close() // now connections to deadAddr are refused

	liveAddr := strings.TrimPrefix(live.URL, "http://")
	c := NewClient(WithAddresses([]string{deadAddr, liveAddr}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := c.Watch(ctx, "k")

	select {
	case ev := <-ch:
		if ev.Err != nil {
			t.Fatalf("watch event carried error: %v", ev.Err)
		}
		if ev.Revision != 42 {
			t.Fatalf("got revision %d, want 42 (from live second address)", ev.Revision)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("watch never failed over from dead first address to live second address")
	}
}

// TestSubmitCommandBacksOffThroughRetry verifies SubmitCommand is routed
// through doWithRetry: when every node fails, it makes more than one full sweep
// (i.e. it retries with backoff rather than a single pass over the nodes).
// Pre-fix the legacy sweep contacted each node exactly once with no retry.
func TestSubmitCommandBacksOffThroughRetry(t *testing.T) {
	var mu sync.Mutex
	commandHits := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/cluster" {
			writeCluster(w, "n1", 1, nil)
			return
		}
		if r.URL.Path == "/command" {
			mu.Lock()
			commandHits++
			mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	c := NewClient(WithAddresses([]string{addr}))

	start := time.Now()
	if _, err := c.SubmitCommand("k", "v"); err == nil {
		t.Fatal("expected SubmitCommand to fail when all nodes reject")
	}
	elapsed := time.Since(start)

	mu.Lock()
	hits := commandHits
	mu.Unlock()

	// doWithRetry runs v2RetryMax rounds; each round sweeps the cached addr
	// plus the address list. More than one round proves retry/backoff engaged.
	if hits <= 1 {
		t.Fatalf("expected multiple /command attempts via retry, got %d", hits)
	}
	// At least one backoff sleep should have elapsed between rounds.
	if elapsed < 40*time.Millisecond {
		t.Errorf("SubmitCommand returned in %v; expected backoff delay between retry rounds", elapsed)
	}
}

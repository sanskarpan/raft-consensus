package testharness_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/client"
)

// httpAddrForLeader maps a harness node id (node1/2/3) to its HTTP address,
// which is basePort+100+ordinal.
func httpAddrForLeader(leaderID string, basePort int) string {
	ordinal := map[string]int{"node1": 0, "node2": 1, "node3": 2}[leaderID]
	return fmt.Sprintf("localhost:%d", basePort+100+ordinal)
}

// TestE2EAtomicCounter verifies the atomic increment op (#208) against a live
// 3-node cluster: concurrent increments from many goroutines must not lose
// updates, and the final counter must equal the exact number of increments.
func TestE2EAtomicCounter(t *testing.T) {
	const basePort = 22000
	_, _, addrs := setupV1Cluster(t, basePort)
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))

	// Sanity: a fresh key starts at 0, +5 -> 5, -2 -> 3.
	if v, err := c.Increment("ctr/basic", 5); err != nil || v != 5 {
		t.Fatalf("Increment(+5) = (%d, %v), want (5, nil)", v, err)
	}
	if v, err := c.Increment("ctr/basic", -2); err != nil || v != 3 {
		t.Fatalf("Increment(-2) = (%d, %v), want (3, nil)", v, err)
	}

	// Concurrent increments from G independent clients x N each, all +1 on one
	// key. Each goroutine uses its OWN Client (distinct clientID): a single
	// Client is a single-writer because idempotency dedups on a monotonic
	// per-client seqNum (concurrent out-of-order seqNums from one Client would be
	// dropped as stale retries — the realistic pattern is one Client per writer).
	const (
		goroutines = 8
		perG       = 25
	)
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perG)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gc := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))
			for i := 0; i < perG; i++ {
				if _, err := gc.Increment("ctr/concurrent", 1); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Increment failed: %v", err)
	}

	// Linearizable read of the final value: must equal exactly G*N (no lost
	// updates, no double-application).
	kv, err := c.GetKV("ctr/concurrent")
	if err != nil {
		t.Fatalf("GetKV: %v", err)
	}
	got, _ := strconv.ParseInt(kv.Value, 10, 64)
	if want := int64(goroutines * perG); got != want {
		t.Fatalf("final counter = %d, want %d (lost/duplicated increments)", got, want)
	}
}

// TestE2ERangePagination seeds more keys than a page holds and verifies the
// client walks every page in order, with no gaps or duplicates, against a live
// cluster (#206).
func TestE2ERangePagination(t *testing.T) {
	const basePort = 22600
	_, _, addrs := setupV1Cluster(t, basePort)
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))

	const total = 23
	for i := 0; i < total; i++ {
		if _, err := c.Put(fmt.Sprintf("page/%03d", i), "v"); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// A decoy under a different prefix must never appear in the paged results.
	if _, err := c.Put("zzz/decoy", "v"); err != nil {
		t.Fatalf("Put decoy: %v", err)
	}

	var got []string
	cursor := ""
	pages := 0
	for {
		page, next, more, err := c.RangePage("page/", cursor, 10)
		if err != nil {
			t.Fatalf("RangePage: %v", err)
		}
		pages++
		for _, kv := range page {
			got = append(got, kv.Key)
		}
		if !more {
			break
		}
		cursor = next
		if pages > total {
			t.Fatal("pagination not terminating")
		}
	}

	if len(got) != total {
		t.Fatalf("collected %d keys, want %d", len(got), total)
	}
	for i := 0; i < total; i++ {
		want := fmt.Sprintf("page/%03d", i)
		if got[i] != want {
			t.Fatalf("key[%d]=%q want %q (order/gap/dupe)", i, got[i], want)
		}
	}
	if pages != 3 { // 10 + 10 + 3
		t.Fatalf("pages=%d, want 3 for %d keys @ limit 10", pages, total)
	}
}

// TestE2EIncrementErrors verifies FSM-level increment errors surface as
// non-retryable client errors end-to-end.
func TestE2EIncrementErrors(t *testing.T) {
	const basePort = 22200
	_, _, addrs := setupV1Cluster(t, basePort)
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))

	if _, err := c.Put("ctr/text", "not-a-number"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	start := time.Now()
	if _, err := c.Increment("ctr/text", 1); err == nil {
		t.Fatal("expected an error incrementing a non-integer value")
	}
	// Must fail fast (not retried across all nodes for the full backoff budget).
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("increment error took %v; a 4xx should not be retried", elapsed)
	}
}

// TestE2ELeaderRouting verifies that v2 writes (#209) succeed even when the
// client's address list starts with a follower, and that the client converges
// its preferred address onto the leader via the X-Raft-Leader-Address hint.
func TestE2ELeaderRouting(t *testing.T) {
	const basePort = 22400
	_, leaderID, addrs := setupV1Cluster(t, basePort)
	leaderAddr := httpAddrForLeader(leaderID, basePort)

	// Order the address list so a NON-leader is first, forcing the naive path to
	// hit a follower (which forwards). Put the leader last.
	ordered := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if a != leaderAddr {
			ordered = append(ordered, a)
		}
	}
	ordered = append(ordered, leaderAddr)

	c := client.NewClient(client.WithAddresses(ordered), client.WithTimeout(10*time.Second))

	// A write via the follower-first list must still succeed.
	if _, err := c.Put("route/k", "v"); err != nil {
		t.Fatalf("Put via follower-first client: %v", err)
	}

	// After the write, the client should have learned the leader's address and
	// prefer it for subsequent writes.
	if got := c.CurrentAddr(); got != leaderAddr {
		t.Fatalf("client preferred addr = %q, want the leader %q (X-Raft-Leader-Address convergence)",
			got, leaderAddr)
	}

	// A subsequent write now goes to the leader first (writeAddrs prefers it).
	if _, err := c.Increment("route/ctr", 3); err != nil {
		t.Fatalf("Increment after convergence: %v", err)
	}
}

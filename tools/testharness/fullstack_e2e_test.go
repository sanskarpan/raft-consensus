package testharness_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/client"
)

// TestE2EFullStackWithFailover exercises the entire feature surface against one
// live 3-node cluster — CRUD, atomic counters, paginated range, transactions,
// watch, stale reads, status — and then verifies all committed data survives a
// leader kill and that writes recover on the new leader. This is the in-depth
// integration check that everything composes and survives failover.
func TestE2EFullStackWithFailover(t *testing.T) {
	const basePort = 23000
	h, _, addrs := setupV1Cluster(t, basePort)
	c := client.NewClient(client.WithAddresses(addrs), client.WithTimeout(10*time.Second))

	t.Run("CRUD", func(t *testing.T) {
		if _, err := c.Put("fs/k", "hello"); err != nil {
			t.Fatalf("Put: %v", err)
		}
		kv, err := c.GetKV("fs/k")
		if err != nil || kv.Value != "hello" {
			t.Fatalf("GetKV = (%v, %v), want hello", kv, err)
		}
		if err := c.DeleteKV("fs/k"); err != nil {
			t.Fatalf("DeleteKV: %v", err)
		}
		if _, err := c.GetKV("fs/k"); err == nil {
			t.Fatal("GetKV after delete should error (not found)")
		}
	})

	t.Run("AtomicCounter", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			if _, err := c.Increment("fs/ctr", 3); err != nil {
				t.Fatalf("Increment: %v", err)
			}
		}
		v, err := c.Increment("fs/ctr", -5)
		if err != nil || v != 25 { // 10*3 - 5
			t.Fatalf("Increment = (%d, %v), want 25", v, err)
		}
	})

	t.Run("RangePagination", func(t *testing.T) {
		for i := 0; i < 12; i++ {
			if _, err := c.Put(fmt.Sprintf("fs/list/%02d", i), "v"); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		var got int
		cursor := ""
		for {
			page, next, more, err := c.RangePage("fs/list/", cursor, 5)
			if err != nil {
				t.Fatalf("RangePage: %v", err)
			}
			got += len(page)
			if !more {
				break
			}
			cursor = next
		}
		if got != 12 {
			t.Fatalf("paged %d keys, want 12", got)
		}
	})

	t.Run("Txn_CAS", func(t *testing.T) {
		if _, err := c.Put("fs/cas", "v1"); err != nil {
			t.Fatalf("Put: %v", err)
		}
		// Success branch: value == v1 → set to v2.
		resp, err := c.Txn(&client.ClientTxnRequest{
			Compare: []client.TxnCompare{{Key: "fs/cas", Target: "value", Result: "equal", Value: "v1"}},
			Success: []client.ClientTxnOp{{Type: 0, Key: "fs/cas", Value: "v2"}},
			Failure: []client.ClientTxnOp{{Type: 0, Key: "fs/cas", Value: "conflict"}},
		})
		if err != nil || !resp.Succeeded {
			t.Fatalf("Txn success branch = (%+v, %v)", resp, err)
		}
		kv, _ := c.GetKV("fs/cas")
		if kv.Value != "v2" {
			t.Fatalf("after CAS value = %q, want v2", kv.Value)
		}
		// Failure branch: compare v1 (now stale) → failure op runs.
		resp2, err := c.Txn(&client.ClientTxnRequest{
			Compare: []client.TxnCompare{{Key: "fs/cas", Target: "value", Result: "equal", Value: "v1"}},
			Success: []client.ClientTxnOp{{Type: 0, Key: "fs/cas", Value: "shouldnt"}},
			Failure: []client.ClientTxnOp{{Type: 0, Key: "fs/cas", Value: "v3"}},
		})
		if err != nil || resp2.Succeeded {
			t.Fatalf("Txn should take failure branch: (%+v, %v)", resp2, err)
		}
		kv, _ = c.GetKV("fs/cas")
		if kv.Value != "v3" {
			t.Fatalf("after failed CAS value = %q, want v3", kv.Value)
		}
	})

	t.Run("Watch", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		ch, err := c.Watch(ctx, "fs/watched")
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		// Give the watch a moment to register, then write.
		time.Sleep(500 * time.Millisecond)
		if _, err := c.Put("fs/watched", "boom"); err != nil {
			t.Fatalf("Put: %v", err)
		}
		select {
		case we := <-ch:
			if we.Err != nil {
				t.Fatalf("watch event error: %v", we.Err)
			}
			if len(we.Events) == 0 || we.Events[0].Key != "fs/watched" {
				t.Fatalf("unexpected watch event: %+v", we)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for watch event")
		}
	})

	t.Run("StaleRead", func(t *testing.T) {
		if _, err := c.Put("fs/stale", "sv"); err != nil {
			t.Fatalf("Put: %v", err)
		}
		// Stale read may lag briefly; poll.
		deadline := time.Now().Add(5 * time.Second)
		for {
			kv, err := c.GetKVStale("fs/stale")
			if err == nil && kv.Value == "sv" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("stale read never observed the write: (%v)", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("Status", func(t *testing.T) {
		info, err := c.GetClusterInfo()
		if err != nil {
			t.Fatalf("GetClusterInfo: %v", err)
		}
		if info.Leader == "" {
			t.Fatal("status reports no leader")
		}
	})

	// The critical integration scenario: committed data must survive a leader
	// kill, and writes must recover on the newly-elected leader.
	t.Run("LeaderFailover", func(t *testing.T) {
		// Establish a known counter value before the failover.
		before, err := c.Increment("fs/survive", 100)
		if err != nil {
			t.Fatalf("Increment: %v", err)
		}

		leaderID, err := h.WaitForLeader(10 * time.Second)
		if err != nil {
			t.Fatalf("WaitForLeader: %v", err)
		}
		if err := h.StopNode(leaderID); err != nil {
			t.Fatalf("StopNode(leader %s): %v", leaderID, err)
		}

		// A new leader must be elected from the remaining two nodes.
		newLeader, err := h.WaitForLeader(20 * time.Second)
		if err != nil {
			t.Fatalf("no new leader after killing %s: %v", leaderID, err)
		}
		if newLeader == leaderID {
			t.Fatalf("WaitForLeader still returns the killed node %s", leaderID)
		}

		// All committed data must still be readable and correct.
		kv, err := c.GetKV("fs/survive")
		if err != nil {
			t.Fatalf("GetKV after failover: %v", err)
		}
		if got, _ := strconv.ParseInt(kv.Value, 10, 64); got != before {
			t.Fatalf("counter after failover = %v, want %d (committed data lost)", kv.Value, before)
		}
		if kv2, err := c.GetKV("fs/cas"); err != nil || kv2.Value != "v3" {
			t.Fatalf("fs/cas after failover = (%v, %v), want v3", kv2, err)
		}

		// New writes must succeed on the new leader.
		after, err := c.Increment("fs/survive", 1)
		if err != nil {
			t.Fatalf("Increment after failover: %v", err)
		}
		if after != before+1 {
			t.Fatalf("post-failover increment = %d, want %d", after, before+1)
		}
	})
}

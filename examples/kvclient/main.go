// Command kvclient demonstrates the raft-consensus Go client against a running
// cluster. Point it at one or more node HTTP addresses:
//
//	go run ./examples/kvclient -endpoints localhost:8002,localhost:8004,localhost:8006
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/client"
)

func main() {
	endpoints := flag.String("endpoints", "localhost:8002", "comma-separated node HTTP addresses")
	flag.Parse()

	c := client.NewClient(
		client.WithAddresses(strings.Split(*endpoints, ",")),
		client.WithTimeout(5*time.Second),
	)

	// Put / Get.
	if _, err := c.Put("greeting", "hello"); err != nil {
		log.Fatalf("Put: %v", err)
	}
	kv, err := c.GetKV("greeting")
	if err != nil {
		log.Fatalf("GetKV: %v", err)
	}
	fmt.Printf("greeting = %q (rev %d)\n", kv.Value, kv.ModRevision)

	// Atomic counter.
	for i := 0; i < 3; i++ {
		n, err := c.Increment("visits", 1)
		if err != nil {
			log.Fatalf("Increment: %v", err)
		}
		fmt.Printf("visits = %d\n", n)
	}

	// Compare-and-swap transaction.
	resp, err := c.Txn(&client.ClientTxnRequest{
		Compare: []client.TxnCompare{{Key: "greeting", Target: "value", Result: "equal", Value: "hello"}},
		Success: []client.ClientTxnOp{{Type: 0, Key: "greeting", Value: "hi"}},
	})
	if err != nil {
		log.Fatalf("Txn: %v", err)
	}
	fmt.Printf("txn succeeded=%v\n", resp.Succeeded)

	// Seed some keys and page through them.
	for i := 0; i < 12; i++ {
		if _, err := c.Put(fmt.Sprintf("item/%02d", i), "v"); err != nil {
			log.Fatalf("Put: %v", err)
		}
	}
	var total int
	cursor := ""
	for {
		page, next, more, err := c.RangePage("item/", cursor, 5)
		if err != nil {
			log.Fatalf("RangePage: %v", err)
		}
		total += len(page)
		if !more {
			break
		}
		cursor = next
	}
	fmt.Printf("listed %d item/* keys via pagination\n", total)

	// Watch: print the next event for a key.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := c.Watch(ctx, "greeting")
	if err != nil {
		log.Fatalf("Watch: %v", err)
	}
	if _, err := c.Put("greeting", "watched"); err != nil {
		log.Fatalf("Put: %v", err)
	}
	select {
	case ev := <-ch:
		if len(ev.Events) > 0 {
			fmt.Printf("watch event: key=%s rev=%d\n", ev.Events[0].Key, ev.Revision)
		}
	case <-ctx.Done():
		fmt.Println("watch: no event received")
	}
}

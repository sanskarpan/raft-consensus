package client_test

import (
	"fmt"
	"log"

	"github.com/sanskarpan/raft-consensus/pkg/client"
)

// Example shows the basic client workflow against a running cluster. It has no
// Output directive, so it is compiled (and rendered on pkg.go.dev) but not run.
func Example() {
	c := client.NewClient(client.WithAddresses([]string{
		"localhost:8002", "localhost:8004", "localhost:8006",
	}))

	if _, err := c.Put("hello", "world"); err != nil {
		log.Fatal(err)
	}
	kv, err := c.GetKV("hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(kv.Value)

	// Atomic counter.
	n, _ := c.Increment("visits", 1)
	fmt.Println(n)
}

// ExampleClient_RangePage shows cursor pagination over a key prefix.
func ExampleClient_rangePage() {
	c := client.NewClient(client.WithAddresses([]string{"localhost:8002"}))
	cursor := ""
	for {
		page, next, more, err := c.RangePage("users/", cursor, 500)
		if err != nil {
			log.Fatal(err)
		}
		for _, kv := range page {
			fmt.Println(kv.Key, kv.Value)
		}
		if !more {
			break
		}
		cursor = next
	}
}

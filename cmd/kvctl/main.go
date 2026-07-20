// kvctl is a command-line client for the etcd-lite distributed KV store.
//
// Usage:
//
//	kvctl [flags] <command> [args...]
//
// Commands:
//
//	put    <key> <value>           Set a key
//	incr   <key> <delta>           Atomically add delta to an integer key
//	get    <key>                   Get a key (linearizable by default)
//	delete <key>                   Delete a key
//	range  <prefix>                List all keys with prefix
//	txn    <json-file|-|stdin>     Execute a transaction from JSON
//	watch  <key>                   Stream change events for a key (Ctrl-C to stop)
//	status                         Print cluster status and revision
//
// Flags:
//
//	--endpoints  Comma-separated node HTTP addresses (default: localhost:8101)
//	--timeout    Request timeout (default: 10s)
//	--stale      Use stale consistency for get/range
//	--prefix     Used with watch to watch by prefix instead of exact key
//	--revision   Start watch or get-history from this revision
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/client"
)

func main() {
	// Global flags.
	endpoints := flag.String("endpoints", "localhost:8101", "comma-separated node HTTP addresses")
	timeout := flag.Duration("timeout", 10*time.Second, "request timeout")
	stale := flag.Bool("stale", false, "use stale consistency for get/range")
	prefix := flag.Bool("prefix", false, "watch by prefix instead of exact key")
	revision := flag.Int64("revision", 0, "start watch/history from this revision")
	limit := flag.Int("limit", 0, "page size for range (auto-pages through all results when > 0)")
	ttl := flag.Int64("ttl", 0, "TTL in seconds for put (0 = no expiry)")

	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	addrs := strings.Split(*endpoints, ",")
	for i, a := range addrs {
		addrs[i] = strings.TrimSpace(a)
	}

	c := client.NewClient(
		client.WithAddresses(addrs),
		client.WithTimeout(*timeout),
	)

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "put":
		if len(cmdArgs) < 2 {
			fatalf("put requires <key> <value>\n")
		}
		runPut(c, cmdArgs[0], cmdArgs[1], *ttl)

	case "get":
		if len(cmdArgs) < 1 {
			fatalf("get requires <key>\n")
		}
		runGet(c, cmdArgs[0], *stale)

	case "incr":
		if len(cmdArgs) < 2 {
			fatalf("incr requires <key> <delta>\n")
		}
		runIncr(c, cmdArgs[0], cmdArgs[1])

	case "delete":
		if len(cmdArgs) < 1 {
			fatalf("delete requires <key>\n")
		}
		runDelete(c, cmdArgs[0])

	case "range":
		pfx := ""
		if len(cmdArgs) >= 1 {
			pfx = cmdArgs[0]
		}
		runRange(c, pfx, *limit)

	case "txn":
		src := "-"
		if len(cmdArgs) >= 1 {
			src = cmdArgs[0]
		}
		runTxn(c, src)

	case "watch":
		if len(cmdArgs) < 1 && !*prefix {
			fatalf("watch requires <key> (or --prefix <prefix>)\n")
		}
		keyOrPrefix := ""
		if len(cmdArgs) >= 1 {
			keyOrPrefix = cmdArgs[0]
		}
		runWatch(c, keyOrPrefix, *prefix, *revision)

	case "status":
		runStatus(c)

	default:
		fatalf("unknown command %q\n", cmd)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `kvctl — etcd-lite distributed KV store client

Usage:
  kvctl [flags] <command> [args...]

Commands:
  put    <key> <value>     Set a key (--ttl=N sets TTL in seconds)
  incr   <key> <delta>     Atomically add delta (may be negative) to an integer key
  get    <key>             Get a key (linearizable by default; --stale for local FSM read)
  delete <key>             Delete a key
  range  [prefix]          List all keys (optionally filtered by prefix)
  txn    [file|-]          Execute a transaction from JSON file or stdin
  watch  <key>             Stream change events (--prefix for prefix watch, --revision=N to replay)
  status                   Print cluster status and revision

Flags:
`)
	flag.PrintDefaults()
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "kvctl: "+format, args...)
	os.Exit(1)
}

func prettyJSON(v interface{}) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------

func runPut(c *client.Client, key, value string, ttlSeconds int64) {
	kv, err := c.PutWithTTL(key, value, ttlSeconds)
	if err != nil {
		fatalf("put failed: %v\n", err)
	}
	fmt.Println(prettyJSON(kv))
}

func runIncr(c *client.Client, key, deltaStr string) {
	delta, err := strconv.ParseInt(deltaStr, 10, 64)
	if err != nil {
		fatalf("incr: delta must be an integer: %v\n", err)
	}
	v, err := c.Increment(key, delta)
	if err != nil {
		fatalf("incr failed: %v\n", err)
	}
	fmt.Println(v)
}

func runGet(c *client.Client, key string, stale bool) {
	var (
		kv  *client.KVPair
		err error
	)
	if stale {
		kv, err = c.GetKVStale(key)
	} else {
		kv, err = c.GetKV(key)
	}
	if err != nil {
		fatalf("get failed: %v\n", err)
	}
	fmt.Println(prettyJSON(kv))
}

func runDelete(c *client.Client, key string) {
	if err := c.DeleteKV(key); err != nil {
		fatalf("delete failed: %v\n", err)
	}
	fmt.Printf("deleted %q\n", key)
}

func runRange(c *client.Client, prefix string, limit int) {
	var kvs []*client.KVPair
	if limit > 0 {
		// Auto-page through all results (bounded per request); avoids the
		// single-shot 10k-key cap.
		cursor := ""
		for {
			page, next, more, err := c.RangePage(prefix, cursor, limit)
			if err != nil {
				fatalf("range failed: %v\n", err)
			}
			kvs = append(kvs, page...)
			if !more {
				break
			}
			cursor = next
		}
	} else {
		var err error
		if kvs, err = c.Range(prefix); err != nil {
			fatalf("range failed: %v\n", err)
		}
	}
	if len(kvs) == 0 {
		fmt.Println("(empty)")
		return
	}
	fmt.Println(prettyJSON(kvs))
}

func runTxn(c *client.Client, src string) {
	var r io.Reader
	if src == "-" || src == "stdin" {
		r = os.Stdin
	} else {
		f, err := os.Open(src)
		if err != nil {
			fatalf("open %s: %v\n", src, err)
		}
		defer f.Close()
		r = f
	}

	var req client.ClientTxnRequest
	if err := json.NewDecoder(r).Decode(&req); err != nil {
		fatalf("decode txn JSON: %v\n", err)
	}

	resp, err := c.Txn(&req)
	if err != nil {
		fatalf("txn failed: %v\n", err)
	}
	fmt.Println(prettyJSON(resp))
}

func runWatch(c *client.Client, keyOrPrefix string, byPrefix bool, sinceRevision int64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl-C gracefully.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	opts := []client.WatchOption{}
	if sinceRevision > 0 {
		opts = append(opts, client.WithRevision(sinceRevision))
	}

	var (
		ch  <-chan client.ClientWatchEvent
		err error
	)
	if byPrefix {
		ch, err = c.WatchPrefix(ctx, keyOrPrefix, opts...)
	} else {
		ch, err = c.Watch(ctx, keyOrPrefix, opts...)
	}
	if err != nil {
		fatalf("watch failed: %v\n", err)
	}

	fmt.Fprintf(os.Stderr, "watching %q (revision>%d) — press Ctrl-C to stop\n", keyOrPrefix, sinceRevision)

	for we := range ch {
		if we.Err != nil {
			fmt.Fprintf(os.Stderr, "watch error: %v\n", we.Err)
			continue
		}
		fmt.Println(prettyJSON(we))
	}
}

func runStatus(c *client.Client) {
	info, err := c.GetClusterInfo()
	if err != nil {
		fatalf("status failed: %v\n", err)
	}
	fmt.Println(prettyJSON(info))
}

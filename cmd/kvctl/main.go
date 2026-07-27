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
	"net/http"
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
	leader := flag.String("leader", "", "leader HTTP address for backup/restore (e.g. localhost:8012)")

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

	case "backup":
		dst := ""
		if len(cmdArgs) >= 1 {
			dst = cmdArgs[0]
		}
		runBackup(*leader, dst)

	case "restore":
		if len(cmdArgs) < 1 {
			fatalf("restore requires <file>\n")
		}
		runRestore(*leader, cmdArgs[0])

	case "completion":
		shell := "bash"
		if len(cmdArgs) >= 1 {
			shell = cmdArgs[0]
		}
		runCompletion(shell)

	case "help":
		usage()
		os.Exit(0)

	default:
		fatalf("unknown command %q\n", cmd)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `kvctl — etcd-lite distributed KV store client

Usage:
  kvctl [flags] <command> [args...]

Commands:
  put        <key> <value>     Set a key (--ttl=N sets TTL in seconds)
  incr       <key> <delta>     Atomically add delta (may be negative) to an integer key
  get        <key>             Get a key (linearizable by default; --stale for local FSM read)
  delete     <key>             Delete a key
  range      [prefix]          List all keys (optionally filtered by prefix)
  txn        [file|-]          Execute a transaction from JSON file or stdin
  watch      <key>             Stream change events (--prefix for prefix watch, --revision=N to replay)
  status                       Print cluster status and revision
  backup     [output-file]     Download a snapshot from the leader (requires --leader)
  restore    <file>            Restore a snapshot to the leader (requires --leader)
  completion [bash|zsh|fish]   Print shell completion script (default: bash)
  help                         Print this help message

Flags:
`)
	flag.PrintDefaults()
}

// completionCommands is the canonical list of subcommands, used by shell completion scripts.
var completionCommands = []string{
	"put", "incr", "get", "delete", "range", "txn",
	"watch", "status", "backup", "restore", "completion", "help",
}

func runCompletion(shell string) {
	switch shell {
	case "bash":
		fmt.Print(`# kvctl bash completion
# Source this file or add to ~/.bashrc:
#   source <(kvctl completion bash)
_kvctl_completions() {
  local cur prev
  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"
  local cmds="put incr get delete range txn watch status backup restore completion help"
  local flags="--endpoints --timeout --stale --prefix --revision --limit --ttl --leader"
  if [[ ${COMP_CWORD} -eq 1 ]]; then
    COMPREPLY=( $(compgen -W "${cmds}" -- "${cur}") )
  elif [[ "${cur}" == -* ]]; then
    COMPREPLY=( $(compgen -W "${flags}" -- "${cur}") )
  elif [[ "${prev}" == "completion" ]]; then
    COMPREPLY=( $(compgen -W "bash zsh fish" -- "${cur}") )
  fi
}
complete -F _kvctl_completions kvctl
`)
	case "zsh":
		fmt.Print(`#compdef kvctl
# kvctl zsh completion
# Add to ~/.zshrc:
#   source <(kvctl completion zsh)
_kvctl() {
  local -a cmds flags
  cmds=(
    'put:Set a key'
    'incr:Atomically add delta to an integer key'
    'get:Get a key'
    'delete:Delete a key'
    'range:List all keys with prefix'
    'txn:Execute a transaction from JSON'
    'watch:Stream change events for a key'
    'status:Print cluster status and revision'
    'backup:Download a snapshot from the leader'
    'restore:Restore a snapshot to the leader'
    'completion:Print shell completion script'
    'help:Print help'
  )
  flags=(
    '--endpoints[Comma-separated node HTTP addresses]:addr'
    '--timeout[Request timeout]:duration'
    '--stale[Use stale consistency for get/range]'
    '--prefix[Watch by prefix instead of exact key]'
    '--revision[Start watch/history from revision]:int'
    '--limit[Page size for range]:int'
    '--ttl[TTL in seconds for put]:int'
    '--leader[Leader HTTP address for backup/restore]:addr'
  )
  if (( CURRENT == 2 )); then
    _describe 'command' cmds
  else
    _arguments ${flags[@]}
  fi
}
_kvctl
`)
	case "fish":
		fmt.Print(`# kvctl fish completion
# Copy to ~/.config/fish/completions/kvctl.fish
#   kvctl completion fish > ~/.config/fish/completions/kvctl.fish
set -l commands put incr get delete range txn watch status backup restore completion help
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a put        -d 'Set a key'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a incr       -d 'Atomically add delta to an integer key'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a get        -d 'Get a key'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a delete     -d 'Delete a key'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a range      -d 'List all keys with prefix'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a txn        -d 'Execute a transaction from JSON'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a watch      -d 'Stream change events for a key'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a status     -d 'Print cluster status'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a backup     -d 'Download a snapshot from the leader'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a restore    -d 'Restore a snapshot to the leader'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a completion -d 'Print shell completion script'
complete -c kvctl -f -n "not __fish_seen_subcommand_from $commands" -a help       -d 'Print help'
complete -c kvctl -f -n "__fish_seen_subcommand_from completion" -a "bash zsh fish"
complete -c kvctl -l endpoints -d 'Comma-separated node HTTP addresses'
complete -c kvctl -l timeout   -d 'Request timeout'
complete -c kvctl -l stale     -d 'Stale consistency for get/range'
complete -c kvctl -l prefix    -d 'Watch by prefix'
complete -c kvctl -l revision  -d 'Start watch from revision'
complete -c kvctl -l limit     -d 'Page size for range'
complete -c kvctl -l ttl       -d 'TTL in seconds for put'
complete -c kvctl -l leader    -d 'Leader HTTP address for backup/restore'
`)
	default:
		fatalf("unknown shell %q — supported: bash, zsh, fish\n", shell)
	}
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

// runBackup downloads a snapshot from the leader and saves it to dst.
// If dst is empty, a timestamped filename is generated automatically.
func runBackup(leaderAddr, dst string) {
	if leaderAddr == "" {
		fatalf("backup requires --leader=<addr>\n")
	}
	if dst == "" {
		dst = fmt.Sprintf("backup-%d.snap", time.Now().Unix())
	}
	hc := &http.Client{Timeout: 60 * time.Second}
	resp, err := hc.Get(fmt.Sprintf("http://%s/admin/snapshot/download", leaderAddr))
	if err != nil {
		fatalf("backup GET failed: %v\n", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fatalf("backup: server returned %d: %s\n", resp.StatusCode, body)
	}
	f, err := os.Create(dst)
	if err != nil {
		fatalf("create %s: %v\n", dst, err)
	}
	defer f.Close()
	n, err := io.Copy(f, resp.Body)
	if err != nil {
		fatalf("write %s: %v\n", dst, err)
	}
	fmt.Printf("backup saved to %s (%d bytes)\n", dst, n)
}

// runRestore uploads a local snapshot file to the leader via PUT /admin/restore.
func runRestore(leaderAddr, path string) {
	if leaderAddr == "" {
		fatalf("restore requires --leader=<addr>\n")
	}
	f, err := os.Open(path)
	if err != nil {
		fatalf("open %s: %v\n", path, err)
	}
	defer f.Close()
	hc := &http.Client{Timeout: 60 * time.Second}
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/admin/restore", leaderAddr), f)
	if err != nil {
		fatalf("build request: %v\n", err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		fatalf("restore PUT failed: %v\n", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fatalf("restore: server returned %d: %s\n", resp.StatusCode, body)
	}
	fmt.Println("restore complete")
}

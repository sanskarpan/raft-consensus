package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/fsm"
	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// ErrKeyNotFound is returned by GetKV/GetKVStale when the key does not exist
// (HTTP 404). It is authoritative and not retried.
var ErrKeyNotFound = errors.New("key not found")

// errClientRequest wraps a 4xx response (e.g. incrementing a non-integer value).
// It is a client error, not retryable across nodes.
var errClientRequest = errors.New("client request rejected")

// retryBackoff returns the wait duration before retry attempt n (0-indexed).
// 50ms → 100ms → 200ms → 400ms → 800ms → 1600ms → 2000ms (capped).
// Adds ±10% random jitter to spread thundering-herd retries.
func retryBackoff(n int) time.Duration {
	d := 50 * time.Millisecond
	for i := 0; i < n; i++ {
		d *= 2
		if d >= 2*time.Second {
			d = 2 * time.Second
			break
		}
	}
	// ±10% jitter
	tenPct := int64(d / 10)
	if tenPct < 1 {
		tenPct = 1
	}
	jitter := time.Duration(mathrand.Int63n(tenPct*2+1)) - time.Duration(tenPct)
	return d + jitter
}

// Watch stream deadlines (L3). watchResponseHeaderTimeout bounds the initial
// connect + header wait so a black-holed first address fails over quickly;
// watchIdleReadTimeout bounds the gap between reads on an established stream so
// a silent half-open connection is detected rather than hanging on the OS TCP
// timeout. It must comfortably exceed the server's SSE keepalive interval.
const (
	watchResponseHeaderTimeout = 10 * time.Second
	watchIdleReadTimeout       = 90 * time.Second
)

// watchBackoff returns the full-jitter wait before reconnect attempt n
// (0-indexed). The base grows exponentially 100ms → 200ms → … capped at 30s;
// the returned value is uniformly random in [0, base) ("full jitter") to spread
// a thundering herd of clients reconnecting after a shared outage. Because the
// result is random, callers should not assume monotonic growth between calls.
func watchBackoff(n int) time.Duration {
	base := 100 * time.Millisecond
	for i := 0; i < n; i++ {
		base *= 2
		if base >= 30*time.Second {
			base = 30 * time.Second
			break
		}
	}
	// Full jitter: uniform in [0, base).
	return time.Duration(mathrand.Int63n(int64(base)))
}

// v2RetryMax is the number of full retry rounds for v2 API calls.
const v2RetryMax = 4

// doWithRetry runs fn up to v2RetryMax times, sleeping with exponential
// backoff between attempts. Suitable for idempotent operations.
func (c *Client) doWithRetry(fn func() error) error {
	var err error
	for i := 0; i < v2RetryMax; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if errors.Is(err, ErrKeyNotFound) || errors.Is(err, errClientRequest) {
			return err // authoritative client error, non-retryable
		}
		if i < v2RetryMax-1 {
			time.Sleep(retryBackoff(i))
		}
	}
	return err
}

// idempotencyHeader names the HTTP request headers carrying idempotency info.
const (
	headerClientID   = "X-Client-ID"
	headerSeqNum     = "X-Seq-Num"
	headerLeaderAddr = "X-Raft-Leader-Address"
)

// defaultMaxLeaseDuration is the fallback leader-lease window used for
// ReadLease consistency when no explicit duration is configured. Without a
// non-zero value the lease is always considered expired (defeating its
// purpose), so we default to a conservative sub-election-timeout window.
const defaultMaxLeaseDuration = 500 * time.Millisecond

type Client struct {
	addrs      []string
	httpClient *http.Client
	timeout    time.Duration

	// mu guards the mutable leader-tracking fields below, which are read and
	// written both by foreground calls and by background Watch goroutines.
	mu               sync.Mutex
	currentAddr      string
	leader           string
	leaderTerm       uint64
	leaseExpiry      time.Time
	maxLeaseDuration time.Duration

	// Idempotency: clientID uniquely identifies this client instance;
	// seqNum is atomically incremented for every write so the FSM can
	// deduplicate retried commands that already committed.
	clientID string
	seqNum   atomic.Uint64
}

type ClusterInfo struct {
	NodeID    string             `json:"node_id"`
	State     string             `json:"state"`
	Leader    string             `json:"leader"`
	Term      uint64             `json:"term"`
	CommitIdx uint64             `json:"commit_idx"`
	Config    raft.Configuration `json:"config"`
}

type ClientOption func(*Client)

func WithAddresses(addrs []string) ClientOption {
	return func(c *Client) {
		c.addrs = addrs
	}
}

func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithMaxLeaseDuration sets the leader-lease window used for ReadLease
// consistency. Must be shorter than the cluster's election timeout to remain
// safe. A non-positive value falls back to defaultMaxLeaseDuration.
func WithMaxLeaseDuration(d time.Duration) ClientOption {
	return func(c *Client) {
		if d > 0 {
			c.maxLeaseDuration = d
		}
	}
}

func NewClient(opts ...ClientOption) *Client {
	c := &Client{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		timeout:          10 * time.Second,
		clientID:         newClientID(),
		maxLeaseDuration: defaultMaxLeaseDuration,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// newClientID generates a random 128-bit hex string to uniquely identify this
// client instance across retries.
func newClientID() string {
	var b [16]byte
	rand.Read(b[:]) //nolint:errcheck — crypto/rand never errors on standard platforms
	return hex.EncodeToString(b[:])
}

// nextSeqNum returns a monotonically increasing sequence number for the next
// write command, used together with clientID for idempotency deduplication.
func (c *Client) nextSeqNum() uint64 {
	return c.seqNum.Add(1)
}

// getCurrentAddr returns the last-known good address under lock. It may be
// empty before the first successful GetClusterInfo call.
func (c *Client) getCurrentAddr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentAddr
}

func (c *Client) setCurrentAddr(addr string) {
	c.mu.Lock()
	c.currentAddr = addr
	c.mu.Unlock()
}

// CurrentAddr returns the address the client currently prefers for writes (the
// last-known leader, learned from the X-Raft-Leader-Address response header).
// Empty until the first successful cluster contact.
func (c *Client) CurrentAddr() string {
	return c.getCurrentAddr()
}

// writeAddrs returns the configured addresses with the last-known-good address
// first (currentAddr, which is kept pointed at the leader via the
// X-Raft-Leader-Address response header). This makes v2 writes prefer the leader
// and avoid a follower→leader forward hop, instead of always starting at addrs[0].
func (c *Client) writeAddrs() []string {
	cur := c.getCurrentAddr()
	if cur == "" {
		return c.addrs
	}
	ordered := make([]string, 0, len(c.addrs)+1)
	ordered = append(ordered, cur)
	for _, a := range c.addrs {
		if a != cur {
			ordered = append(ordered, a)
		}
	}
	return ordered
}

// noteLeaderFromResponse updates the preferred address from the leader hint the
// server attaches to v2 write responses, so the client converges on the leader.
func (c *Client) noteLeaderFromResponse(resp *http.Response) {
	if la := resp.Header.Get(headerLeaderAddr); la != "" {
		c.setCurrentAddr(la)
	}
}

func (c *Client) GetClusterInfo() (*ClusterInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	for _, addr := range c.addrs {
		url := fmt.Sprintf("http://%s/admin/cluster", addr)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			var info ClusterInfo
			if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
				continue
			}

			c.mu.Lock()
			c.currentAddr = addr
			if info.Leader != "" && info.Leader != c.leader {
				c.leader = info.Leader
				c.leaderTerm = info.Term
				c.leaseExpiry = time.Now().Add(c.maxLeaseDuration)
			}
			c.mu.Unlock()

			return &info, nil
		}
	}

	return nil, fmt.Errorf("failed to get cluster info")
}

func (c *Client) GetLeader() (string, error) {
	info, err := c.GetClusterInfo()
	if err != nil {
		return "", err
	}

	if info.Leader == "" {
		return "", fmt.Errorf("no leader elected")
	}

	return info.Leader, nil
}

func (c *Client) SubmitCommand(key, value string) (string, error) {
	info, err := c.GetClusterInfo()
	if err != nil {
		return "", err
	}

	if info.Leader == "" {
		return "", fmt.Errorf("no leader")
	}

	data, err := fsm.EncodeSet(key, value)
	if err != nil {
		return "", err
	}

	// SubmitCommand is a mutating write and is retried across nodes below.
	// Attach a single idempotency key (clientID + seqNum) so the FSM can
	// deduplicate retries that already committed — never retry a write
	// without one (H5).
	seq := c.nextSeqNum()

	// Route the multi-node sweep through doWithRetry so a shared outage backs
	// off (jittered exponential) between rounds instead of hammering every node
	// in a tight loop (L3). Try the last-known-good address first, then the full
	// address list; the seq is generated once so all retries share one
	// idempotency key (H5).
	var resp []byte
	err = c.doWithRetry(func() error {
		if cur := c.getCurrentAddr(); cur != "" {
			url := fmt.Sprintf("http://%s/command", cur)
			if r, e := c.sendCommandIdem(url, data, seq); e == nil {
				resp = r
				return nil
			}
		}
		for _, addr := range c.addrs {
			url := fmt.Sprintf("http://%s/command", addr)
			r, e := c.sendCommandIdem(url, data, seq)
			if e == nil {
				resp = r
				return nil
			}
		}
		return fmt.Errorf("SubmitCommand: all nodes failed")
	})
	if err != nil {
		return "", err
	}

	var result fsm.KvResult
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", err
	}

	if result.Error != "" {
		return "", fmt.Errorf("%s", result.Error)
	}

	return result.Value, nil
}

func (c *Client) sendCommand(url string, data []byte) ([]byte, error) {
	return c.sendCommandIdem(url, data, 0)
}

// sendCommandIdem POSTs an encoded FSM command to the /command endpoint.
// When seqNum > 0 the request carries idempotency headers (X-Client-ID +
// X-Seq-Num) so mutating commands that are retried across nodes are
// deduplicated by the FSM. Reads pass seqNum == 0 and send no headers.
func (c *Client) sendCommandIdem(url string, data []byte, seqNum uint64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(data)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if seqNum > 0 {
		req.Header.Set(headerClientID, c.clientID)
		req.Header.Set(headerSeqNum, fmt.Sprintf("%d", seqNum))
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("command failed: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (c *Client) GetValue(key string) (string, error) {
	data, err := fsm.EncodeGet(key)
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("http://%s/command", c.getCurrentAddr())
	resp, err := c.sendCommand(url, data)
	if err != nil {
		return "", err
	}

	var result fsm.KvResult
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", err
	}

	if result.Error != "" {
		return "", fmt.Errorf("%s", result.Error)
	}

	return result.Value, nil
}

func (c *Client) DeleteValue(key string) error {
	data, err := fsm.EncodeDelete(key)
	if err != nil {
		return err
	}

	// Delete is a mutating write; attach an idempotency key (H5).
	seq := c.nextSeqNum()
	url := fmt.Sprintf("http://%s/command", c.getCurrentAddr())
	_, err = c.sendCommandIdem(url, data, seq)
	return err
}

type ReadConsistency int

const (
	ReadDefault ReadConsistency = iota
	ReadQuorum
	ReadStale
	ReadLease
)

func (c *Client) GetValueWithConsistency(key string, consistency ReadConsistency) (string, error) {
	switch consistency {
	case ReadQuorum:
		return c.getValueQuorum(key)
	case ReadStale:
		return c.getValueStale(key)
	case ReadLease:
		return c.getValueLease(key)
	default:
		return c.getValue(key)
	}
}

func (c *Client) getValueLease(key string) (string, error) {
	c.mu.Lock()
	expired := time.Now().After(c.leaseExpiry)
	c.mu.Unlock()

	if expired {
		info, err := c.GetClusterInfo()
		if err != nil {
			return "", err
		}
		c.mu.Lock()
		if info.Term != c.leaderTerm {
			c.leaseExpiry = time.Time{}
			c.mu.Unlock()
			return "", fmt.Errorf("leader lease expired")
		}
		c.leaseExpiry = time.Now().Add(c.maxLeaseDuration)
		c.mu.Unlock()
	}

	return c.getValue(key)
}

func (c *Client) getValue(key string) (string, error) {
	info, err := c.GetClusterInfo()
	if err != nil {
		return "", err
	}

	if info.Leader == "" {
		return "", fmt.Errorf("no leader")
	}

	data, err := fsm.EncodeGet(key)
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("http://%s/command", c.getCurrentAddr())
	resp, err := c.sendCommand(url, data)
	if err != nil {
		return "", err
	}

	return string(resp), nil
}

func (c *Client) getValueQuorum(key string) (string, error) {
	info, err := c.GetClusterInfo()
	if err != nil {
		return "", err
	}

	if info.Leader == "" {
		return "", fmt.Errorf("no leader")
	}

	// Only count non-learner (voting) servers for quorum.
	voters := 0
	for _, s := range info.Config.Servers {
		if !s.Learner {
			voters++
		}
	}
	// If the configuration is empty (e.g. not reported), fall back to the
	// number of configured addresses so a majority can still be required.
	if voters == 0 {
		voters = len(c.addrs)
	}
	quorum := voters/2 + 1

	data, err := fsm.EncodeGet(key)
	if err != nil {
		return "", err
	}

	// A quorum read is only meaningful if a majority of nodes agree on the
	// same value. Counting bare HTTP 200s (the old behavior) returns the
	// first reachable node's answer even if a majority disagree, which is
	// not a quorum read at all (H7). Tally identical responses and only
	// return a value once one distinct response reaches quorum.
	counts := make(map[string]int)
	replies := 0
	for _, addr := range c.addrs {
		url := fmt.Sprintf("http://%s/command", addr)
		resp, err := c.sendCommand(url, data)
		if err != nil {
			continue
		}
		replies++
		v := string(resp)
		counts[v]++
		if counts[v] >= quorum {
			return v, nil
		}
	}

	if replies < quorum {
		return "", fmt.Errorf("quorum not reached: %d/%d nodes responded", replies, quorum)
	}
	return "", fmt.Errorf("quorum not reached: no value agreed by %d nodes (%d distinct responses from %d replies)", quorum, len(counts), replies)
}

func (c *Client) getValueStale(key string) (string, error) {
	data, err := fsm.EncodeGet(key)
	if err != nil {
		return "", err
	}

	for _, addr := range c.addrs {
		url := fmt.Sprintf("http://%s/command", addr)
		resp, err := c.sendCommand(url, data)
		if err == nil {
			return string(resp), nil
		}
	}

	return "", fmt.Errorf("all nodes failed")
}

func (c *Client) HealthCheck() error {
	for _, addr := range c.addrs {
		rawURL := fmt.Sprintf("http://%s/health", addr)
		resp, err := c.httpClient.Get(rawURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			return nil
		}
	}
	return fmt.Errorf("all nodes unhealthy")
}

// ---------------------------------------------------------------------------
// v2 types
// ---------------------------------------------------------------------------

// KVPair is the client-facing representation of a versioned key-value entry.
type KVPair struct {
	Key            string `json:"key"`
	Value          string `json:"value"`
	CreateRevision int64  `json:"create_revision"`
	ModRevision    int64  `json:"mod_revision"`
	Version        int64  `json:"version"`
	ExpiresAtMs    int64  `json:"expires_at_ms,omitempty"` // #207: zero means no expiry
}

// TxnCompare is a single condition in a client transaction.
type TxnCompare struct {
	Key    string `json:"key"`
	Target string `json:"target"` // "value"|"version"|"create_revision"|"mod_revision"
	Result string `json:"result"` // "equal"|"not_equal"|"greater"|"less"
	Value  string `json:"value,omitempty"`
	Rev    int64  `json:"rev,omitempty"`
}

// ClientTxnOp is a single operation (put or delete) within a transaction.
type ClientTxnOp struct {
	Type  int    `json:"type"` // 0=put, 1=delete
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// ClientTxnRequest is the transaction payload sent to /v1/txn.
type ClientTxnRequest struct {
	Compare []TxnCompare  `json:"compare"`
	Success []ClientTxnOp `json:"success"`
	Failure []ClientTxnOp `json:"failure"`
}

// ClientTxnResponse is the response from /v1/txn.
type ClientTxnResponse struct {
	Succeeded bool     `json:"succeeded"`
	Results   []KVPair `json:"results,omitempty"`
	Revision  int64    `json:"revision"`
}

// ClientKVEvent is a single key change event delivered to Watch subscribers.
type ClientKVEvent struct {
	Type     int     `json:"type"` // 0=put, 1=delete
	Key      string  `json:"key"`
	KV       *KVPair `json:"kv,omitempty"`
	PrevKV   *KVPair `json:"prev_kv,omitempty"`
	Revision int64   `json:"revision"`
}

// ClientWatchEvent is what the Watch channel delivers to callers.
type ClientWatchEvent struct {
	Events   []ClientKVEvent `json:"events"`
	Revision int64           `json:"revision"`
	Err      error
}

// WatchOption is a functional option for Watch and WatchPrefix.
type WatchOption func(*watchConfig)

type watchConfig struct {
	sinceRevision int64
}

// WithRevision makes the watch replay history from the given revision.
func WithRevision(rev int64) WatchOption {
	return func(cfg *watchConfig) {
		cfg.sinceRevision = rev
	}
}

// ---------------------------------------------------------------------------
// v2 methods
// ---------------------------------------------------------------------------

// Put sets key to value via PUT /v1/kv/{key}.
// Retries up to v2RetryMax times with exponential backoff.
// The seqNum is generated once so all retries carry the same idempotency key.
func (c *Client) Put(key, value string) (*KVPair, error) {
	return c.PutWithTTL(key, value, 0)
}

// PutWithTTL sets key to value with an optional TTL (#207).
// ttlSeconds == 0 means no expiry (behaves identically to Put).
func (c *Client) PutWithTTL(key, value string, ttlSeconds int64) (*KVPair, error) {
	seq := c.nextSeqNum()
	var result *KVPair
	err := c.doWithRetry(func() error {
		for _, addr := range c.writeAddrs() {
			rawURL := fmt.Sprintf("http://%s/v1/kv/%s", addr, url.PathEscape(key))
			kv, err := c.doPutKV(rawURL, []byte(value), seq, ttlSeconds)
			if err == nil {
				result = kv
				return nil
			}
		}
		return fmt.Errorf("PutWithTTL: all nodes failed")
	})
	return result, err
}

// Increment atomically adds delta (which may be negative) to the integer value
// stored at key and returns the new value. A missing key starts at 0. It is a
// linearizable write routed to the leader.
//
// Concurrency: a Client is a single-writer. Its idempotency scheme dedups on a
// monotonic per-client sequence number, so overlapping writes from one Client
// can be dropped as stale retries. Use one Client per concurrent writer.
func (c *Client) Increment(key string, delta int64) (int64, error) {
	seq := c.nextSeqNum()
	body, _ := json.Marshal(map[string]int64{"delta": delta})
	var newVal int64
	err := c.doWithRetry(func() error {
		var lastErr error
		for _, addr := range c.writeAddrs() {
			rawURL := fmt.Sprintf("http://%s/v1/kv/%s?op=incr", addr, url.PathEscape(key))
			kv, err := c.doIncr(rawURL, body, seq)
			if err == nil {
				v, perr := strconv.ParseInt(kv.Value, 10, 64)
				if perr != nil {
					return perr // server returned a non-integer; not retryable
				}
				newVal = v
				return nil
			}
			if errors.Is(err, errClientRequest) {
				return err // 4xx (e.g. non-integer/overflow): don't retry
			}
			lastErr = err
		}
		if lastErr != nil {
			return lastErr
		}
		return fmt.Errorf("Increment: all nodes failed")
	})
	return newVal, err
}

// GetKV performs a linearizable GET via /v1/kv/{key}.
// Retries up to v2RetryMax times with exponential backoff.
func (c *Client) GetKV(key string) (*KVPair, error) {
	var result *KVPair
	err := c.doWithRetry(func() error {
		for _, addr := range c.addrs {
			rawURL := fmt.Sprintf("http://%s/v1/kv/%s", addr, url.PathEscape(key))
			kv, err := c.doGetKV(rawURL)
			if err == nil {
				result = kv
				return nil
			}
			if errors.Is(err, ErrKeyNotFound) {
				return err // authoritative 404 from the leader; stop
			}
		}
		return fmt.Errorf("GetKV: all nodes failed")
	})
	return result, err
}

// GetKVStale performs a stale (local FSM) GET via /v1/kv/{key}?consistency=stale.
// Retries up to v2RetryMax times with exponential backoff.
func (c *Client) GetKVStale(key string) (*KVPair, error) {
	var result *KVPair
	err := c.doWithRetry(func() error {
		for _, addr := range c.addrs {
			rawURL := fmt.Sprintf("http://%s/v1/kv/%s?consistency=stale", addr, url.PathEscape(key))
			kv, err := c.doGetKV(rawURL)
			if err == nil {
				result = kv
				return nil
			}
			if errors.Is(err, ErrKeyNotFound) {
				return err
			}
		}
		return fmt.Errorf("GetKVStale: all nodes failed")
	})
	return result, err
}

// DeleteKV deletes key via DELETE /v1/kv/{key}.
// Retries up to v2RetryMax times with exponential backoff.
func (c *Client) DeleteKV(key string) error {
	seq := c.nextSeqNum()
	return c.doWithRetry(func() error {
		for _, addr := range c.writeAddrs() {
			rawURL := fmt.Sprintf("http://%s/v1/kv/%s", addr, url.PathEscape(key))
			if err := c.doDeleteKV(rawURL, seq); err == nil {
				return nil
			}
		}
		return fmt.Errorf("DeleteKV: all nodes failed")
	})
}

// Range returns all keys with prefix via GET /v1/kv?prefix={prefix}.
// Retries up to v2RetryMax times with exponential backoff.
func (c *Client) Range(prefix string) ([]*KVPair, error) {
	var result []*KVPair
	err := c.doWithRetry(func() error {
		for _, addr := range c.addrs {
			rawURL := fmt.Sprintf("http://%s/v1/kv?prefix=%s", addr, url.QueryEscape(prefix))
			kvs, err := c.doGetKVList(rawURL)
			if err == nil {
				result = kvs
				return nil
			}
		}
		return fmt.Errorf("Range: all nodes failed")
	})
	return result, err
}

// RangePage returns up to limit keys with the given prefix, ordered by key and
// strictly after startAfter (pass "" for the first page). It returns the page,
// the cursor to pass as the next startAfter, and whether more keys remain. This
// bounds the response size regardless of how many keys match.
//
// Example (iterate everything under "users/"):
//
//	cur := ""
//	for {
//	    page, next, more, err := c.RangePage("users/", cur, 500)
//	    if err != nil { ... }
//	    process(page)
//	    if !more { break }
//	    cur = next
//	}
func (c *Client) RangePage(prefix, startAfter string, limit int) (kvs []*KVPair, nextCursor string, more bool, err error) {
	err = c.doWithRetry(func() error {
		for _, addr := range c.addrs {
			u := fmt.Sprintf("http://%s/v1/kv?prefix=%s&limit=%d", addr, url.QueryEscape(prefix), limit)
			if startAfter != "" {
				u += "&start_after=" + url.QueryEscape(startAfter)
			}
			page, cur, m, e := c.doGetKVListPaged(u)
			if e == nil {
				kvs, nextCursor, more = page, cur, m
				return nil
			}
		}
		return fmt.Errorf("RangePage: all nodes failed")
	})
	return kvs, nextCursor, more, err
}

// Txn submits a transaction via POST /v1/txn.
// Retries up to v2RetryMax times with exponential backoff.
func (c *Client) Txn(req *ClientTxnRequest) (*ClientTxnResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	// A transaction is a mutating write that doWithRetry replays across
	// nodes; generate one idempotency key so all retries deduplicate to a
	// single application (H5).
	seq := c.nextSeqNum()
	var result *ClientTxnResponse
	err = c.doWithRetry(func() error {
		for _, addr := range c.writeAddrs() {
			rawURL := fmt.Sprintf("http://%s/v1/txn", addr)
			resp, err := c.doPost(rawURL, data, seq)
			if err == nil {
				var txnResp ClientTxnResponse
				if err := json.Unmarshal(resp, &txnResp); err == nil {
					result = &txnResp
					return nil
				}
			}
		}
		return fmt.Errorf("Txn: all nodes failed")
	})
	return result, err
}

// Watch subscribes to SSE events for the given key.
// Returns a channel that delivers events; close the context to stop.
// Reconnects automatically using Last-Event-ID on disconnect.
func (c *Client) Watch(ctx context.Context, key string, opts ...WatchOption) (<-chan ClientWatchEvent, error) {
	cfg := &watchConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	ch := make(chan ClientWatchEvent, 64)
	go c.watchLoop(ctx, fmt.Sprintf("/v1/watch?key=%s", url.QueryEscape(key)), cfg.sinceRevision, ch)
	return ch, nil
}

// WatchPrefix subscribes to SSE events for all keys with prefix.
func (c *Client) WatchPrefix(ctx context.Context, prefix string, opts ...WatchOption) (<-chan ClientWatchEvent, error) {
	cfg := &watchConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	ch := make(chan ClientWatchEvent, 64)
	go c.watchLoop(ctx, fmt.Sprintf("/v1/watch?prefix=%s", url.QueryEscape(prefix)), cfg.sinceRevision, ch)
	return ch, nil
}

// watchLoop connects to an SSE endpoint and pushes events to ch.
// On disconnect it reconnects with the last seen revision as Last-Event-ID.
// Backoff between reconnects is full-jittered exponential (base 100ms → … → 30s)
// to avoid a thundering herd on a shared outage, and successive attempts rotate
// through every configured address so a dead first node fails over (L3).
func (c *Client) watchLoop(ctx context.Context, path string, sinceRevision int64, ch chan<- ClientWatchEvent) {
	defer close(ch)
	lastRevision := sinceRevision
	reconnects := 0
	attempt := 0 // rotates the starting address across reconnects

	for {
		if ctx.Err() != nil {
			return
		}

		// Rotate through all configured addresses so a dead first node fails
		// over to a second on the next attempt instead of retrying addrs[0].
		addrs := c.addrs
		var addr string
		if len(addrs) > 0 {
			addr = addrs[attempt%len(addrs)]
		}
		attempt++
		rawURL := fmt.Sprintf("http://%s%s", addr, path)

		err := c.streamSSE(ctx, rawURL, lastRevision, ch, &lastRevision)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			// Full-jitter exponential backoff to spread reconnects.
			d := watchBackoff(reconnects)
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
			}
			reconnects++
		} else {
			reconnects = 0 // clean disconnect; reset backoff
		}
	}
}

// streamSSE connects to an SSE URL and pushes parsed events to ch.
// Updates *lastRevision as events arrive for reconnect continuity.
func (c *Client) streamSSE(
	ctx context.Context,
	rawURL string,
	sinceRevision int64,
	ch chan<- ClientWatchEvent,
	lastRevision *int64,
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	if sinceRevision > 0 {
		req.Header.Set("Last-Event-ID", fmt.Sprintf("%d", sinceRevision))
	}

	// A silent half-open connection (peer vanished, no FIN/RST) would otherwise
	// hang until the OS TCP timeout — potentially minutes. We bound how long we
	// wait for response headers, and grab the underlying net.Conn via a custom
	// DialContext so we can set a per-read idle deadline that is reset on each
	// received event: a healthy long-lived stream stays open while a stalled one
	// is torn down (L3). The client has no overall Timeout so an active stream is
	// never cut off mid-flight.
	var connMu sync.Mutex
	var streamConn net.Conn
	baseDial := (&net.Dialer{Timeout: watchResponseHeaderTimeout}).DialContext
	streamClient := &http.Client{
		Transport: &http.Transport{
			ResponseHeaderTimeout: watchResponseHeaderTimeout,
			DialContext: func(dctx context.Context, network, addr string) (net.Conn, error) {
				conn, derr := baseDial(dctx, network, addr)
				if derr == nil {
					connMu.Lock()
					streamConn = conn
					connMu.Unlock()
				}
				return conn, derr
			},
		},
	}
	resp, err := streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	// setReadDeadline pushes a fresh idle deadline on the captured conn; when it
	// elapses with no data the Read fails and the loop exits, triggering a
	// reconnect rather than hanging on a dead conn.
	setReadDeadline := func() {
		connMu.Lock()
		conn := streamConn
		connMu.Unlock()
		if conn != nil {
			_ = conn.SetReadDeadline(time.Now().Add(watchIdleReadTimeout))
		}
	}

	scanner := bufio.NewScanner(resp.Body)
	var dataLines []string

	for {
		// Reset the idle deadline before each line read.
		setReadDeadline()
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()

		if line == "" {
			// Empty line dispatches the accumulated event.
			if len(dataLines) > 0 {
				raw := strings.Join(dataLines, "\n")
				var we ClientWatchEvent
				if err := json.Unmarshal([]byte(raw), &we); err == nil {
					if we.Revision > *lastRevision {
						*lastRevision = we.Revision
					}
					select {
					case ch <- we:
					case <-ctx.Done():
						return nil
					}
				}
				dataLines = dataLines[:0]
			}
			continue
		}

		if after, ok := strings.CutPrefix(line, "data: "); ok {
			dataLines = append(dataLines, after)
		}
		// id: and event: lines are ignored; we track revision from JSON payload
	}

	return scanner.Err()
}

// ---------------------------------------------------------------------------
// Internal HTTP helpers for v2 API
// ---------------------------------------------------------------------------

func (c *Client) doPutKV(rawURL string, rawValue []byte, seqNum uint64, ttlSeconds int64) (*KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	var body []byte
	var contentType string
	if ttlSeconds > 0 {
		// #207: wrap in JSON to carry ttl_seconds.
		envelope := struct {
			Value      string `json:"value"`
			TTLSeconds int64  `json:"ttl_seconds"`
		}{Value: string(rawValue), TTLSeconds: ttlSeconds}
		var err error
		if body, err = json.Marshal(envelope); err != nil {
			return nil, err
		}
		contentType = "application/json"
	} else {
		body = rawValue
		contentType = "text/plain"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, rawURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set(headerClientID, c.clientID)
	req.Header.Set(headerSeqNum, fmt.Sprintf("%d", seqNum))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	c.noteLeaderFromResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("PUT %s: status %d", rawURL, resp.StatusCode)
	}

	var kv KVPair
	if err := json.NewDecoder(resp.Body).Decode(&kv); err != nil {
		return nil, err
	}
	return &kv, nil
}

// doIncr POSTs an atomic increment. A 4xx (non-integer value/delta, overflow) is
// returned wrapped in errClientRequest so callers do not retry it across nodes.
func (c *Client) doIncr(rawURL string, body []byte, seqNum uint64) (*KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerClientID, c.clientID)
	req.Header.Set(headerSeqNum, fmt.Sprintf("%d", seqNum))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	c.noteLeaderFromResponse(resp)

	if resp.StatusCode == http.StatusOK {
		var kv KVPair
		if err := json.NewDecoder(resp.Body).Decode(&kv); err != nil {
			return nil, err
		}
		return &kv, nil
	}
	// Only a 400 Bad Request is a hard, non-retryable client error (non-integer
	// value/delta, overflow). 429 (rate limit) and 5xx are transient and are
	// left retryable so doWithRetry backs off and tries again.
	if resp.StatusCode == http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: incr %s: %s", errClientRequest, rawURL, strings.TrimSpace(string(b)))
	}
	return nil, fmt.Errorf("incr %s: status %d", rawURL, resp.StatusCode)
}

func (c *Client) doGetKV(rawURL string) (*KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrKeyNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", rawURL, resp.StatusCode)
	}

	var kv KVPair
	if err := json.NewDecoder(resp.Body).Decode(&kv); err != nil {
		return nil, err
	}
	return &kv, nil
}

func (c *Client) doDeleteKV(rawURL string, seqNum uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set(headerClientID, c.clientID)
	req.Header.Set(headerSeqNum, fmt.Sprintf("%d", seqNum))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	c.noteLeaderFromResponse(resp)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE %s: status %d", rawURL, resp.StatusCode)
	}
	return nil
}

func (c *Client) doGetKVList(rawURL string) ([]*KVPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET list %s: status %d", rawURL, resp.StatusCode)
	}

	var kvs []*KVPair
	if err := json.NewDecoder(resp.Body).Decode(&kvs); err != nil {
		return nil, err
	}
	return kvs, nil
}

// doGetKVListPaged is doGetKVList that also returns pagination metadata from the
// X-Next-Cursor / X-Has-More response headers.
func (c *Client) doGetKVListPaged(rawURL string) (kvs []*KVPair, nextCursor string, more bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", false, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", false, fmt.Errorf("GET list %s: status %d", rawURL, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&kvs); err != nil {
		return nil, "", false, err
	}
	nextCursor = resp.Header.Get("X-Next-Cursor")
	more = resp.Header.Get("X-Has-More") == "true"
	return kvs, nextCursor, more, nil
}

// doPost sends a mutating POST (currently transactions). When seqNum > 0 it
// attaches idempotency headers so retries across nodes are deduplicated by the
// FSM (H5); a mutating write must never be retried without one.
func (c *Client) doPost(rawURL string, body []byte, seqNum uint64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if seqNum > 0 {
		req.Header.Set(headerClientID, c.clientID)
		req.Header.Set(headerSeqNum, fmt.Sprintf("%d", seqNum))
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	c.noteLeaderFromResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("POST %s: status %d", rawURL, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

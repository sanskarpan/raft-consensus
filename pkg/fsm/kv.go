package fsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// ---------------------------------------------------------------------------
// Core event types
// ---------------------------------------------------------------------------

// EventType distinguishes put (create or update) from delete events.
type EventType int

const (
	EventPut EventType = iota
	EventDelete
)

// KeyValue is the etcd-style versioned entry stored for each key.
type KeyValue struct {
	Key            string `json:"key"`
	Value          string `json:"value"`
	CreateRevision int64  `json:"create_revision"`
	ModRevision    int64  `json:"mod_revision"`
	Version        int64  `json:"version"`      // number of modifications to this key
	ExpiresAtMs    int64  `json:"expires_at_ms,omitempty"` // Unix ms; 0 = no expiry (#207)
}

// Event represents a single change notification emitted after a committed Apply.
type Event struct {
	Type     EventType `json:"type"`
	Key      string    `json:"key"`
	KV       *KeyValue `json:"kv,omitempty"`      // nil for deletes
	PrevKV   *KeyValue `json:"prev_kv,omitempty"` // nil for creates
	Revision int64     `json:"revision"`
}

// ---------------------------------------------------------------------------
// KVStore
// ---------------------------------------------------------------------------

// maxDedupEntries caps the number of unique client IDs tracked for
// idempotency. When the cap is exceeded the entry with the lowest Order (oldest
// deterministic insertion) is evicted. Each entry is ~200 bytes; 10 000 entries
// ≈ 2 MB. It is a var (not const) only so tests can lower it to exercise
// eviction. Its value must be identical on every replica.
var maxDedupEntries = 10_000

const (
	defaultEventChanSize = 512
	defaultHistorySize   = 1024
	// maxRangeResults caps the number of keys returned by a single Range call
	// to prevent memory exhaustion from a wildcard query.
	maxRangeResults = 10_000

	// maxRevision is the documented ceiling for the monotonic revision/version
	// counters (L4). int64 gives ~9.2e18 headroom; at one mutation per
	// nanosecond that is still ~292 years, so this is only a defense against
	// pathological/adversarial input rather than an expected limit. We stop well
	// short of math.MaxInt64 so the counters can never wrap to a negative value,
	// which would break the monotonicity that watch/range/dedup rely on. When the
	// ceiling is reached a mutation returns an error instead of corrupting state.
	maxRevision = int64(1) << 62

	// errRevisionExhausted is the KvResult.Error returned by a mutation once the
	// revision counter has reached maxRevision (L4).
	errRevisionExhausted = "revision counter exhausted"
)

// KVStore is the FSM for the distributed key-value store.
// It implements raft.FSM and is safe for concurrent reads from any goroutine,
// while Apply() is called exclusively from the raft run() goroutine.
type KVStore struct {
	mu       sync.RWMutex
	data     map[string]*KeyValue // all keys (both legacy and v2 ops share this)
	revision int64                // global cluster revision; incremented on mutations only
	index    uint64               // incremented on every Apply call; used by Snapshot.Index()

	// eventCh is written non-blocking by Apply() and drained by WatchManager.
	eventCh chan []Event

	// droppedEvents counts non-blocking sends to eventCh that were dropped
	// because the channel was full. Observable via DroppedEvents().
	droppedEvents uint64 // accessed atomically

	// history is a ring buffer of past events for late-subscriber replay.
	history    []Event
	historyPos int // next write position (wraps around)
	historyLen int // number of valid entries (capped at defaultHistorySize)

	// dedupTable stores the last applied seqNum and result per client.
	// This prevents double-application when a client retries after a network
	// failure where the command committed but the response was lost.
	dedupTable map[string]dedupEntry

	// applyTimeMs is the FSM's virtual monotonic clock (#207). It is advanced
	// from LeaderTimestampMs on every command that carries one (tick + TTL puts).
	// All replicas apply the same log entries in the same order, so this field
	// is identical on every live replica. Never use time.Now() here.
	applyTimeMs int64
}

// DroppedEvents returns the total number of event notifications that were
// silently dropped because the WatchManager was not consuming fast enough.
// A non-zero value means some watch subscribers may have missed live events
// and had to reconnect using history replay.
func (k *KVStore) DroppedEvents() uint64 {
	return atomic.LoadUint64(&k.droppedEvents)
}

// NewKVStore creates a ready-to-use KVStore.
func NewKVStore() *KVStore {
	return &KVStore{
		data:       make(map[string]*KeyValue),
		eventCh:    make(chan []Event, defaultEventChanSize),
		history:    make([]Event, defaultHistorySize),
		dedupTable: make(map[string]dedupEntry),
	}
}

// ---------------------------------------------------------------------------
// kvCommand — internal JSON wire format shared by all ops
// ---------------------------------------------------------------------------

type kvCommand struct {
	Op    string      `json:"op"`
	Key   string      `json:"key"`
	Value string      `json:"value"`
	Txn   *TxnRequest `json:"txn,omitempty"` // only present for op=="txn"

	// Idempotency fields.  When ClientID is set, Apply() deduplicates repeated
	// commands from the same client (e.g. after a network retry that committed
	// on the server but whose response was lost in transit).
	// Read-only ops (get, get_v2, list, range) do not set these fields.
	ClientID string `json:"client_id,omitempty"`
	SeqNum   uint64 `json:"seq_num,omitempty"`

	// TTL fields (#207). LeaderTimestampMs is the leader's wall-clock time
	// (Unix milliseconds) at proposal time, used to advance the FSM's
	// deterministic virtual clock (applyTimeMs) identically on every replica.
	// TTLSeconds > 0 causes the key to expire TTLSeconds after LeaderTimestampMs.
	// Both fields are omitted (zero) for non-TTL commands and for JSON-encoded
	// commands — the binary codec appends them only when non-zero.
	LeaderTimestampMs int64 `json:"leader_timestamp_ms,omitempty"`
	TTLSeconds        int64 `json:"ttl_seconds,omitempty"`
}

// dedupEntry caches the last applied result per client for idempotency.
type dedupEntry struct {
	SeqNum uint64 `json:"seq_num"`
	Result []byte `json:"result"`
	// Order is the apply index at which this entry was last stored. It gives a
	// deterministic, replica-consistent basis for eviction (C8).
	Order uint64 `json:"order"`
}

// KvResult is the response type for legacy and simple v2 operations.
type KvResult struct {
	Value string `json:"value"`
	Error string `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// Apply — called serially from raft run() goroutine
// ---------------------------------------------------------------------------

func (k *KVStore) Apply(entry []byte) (result []byte, err error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	cmd, err := decodeKVCommand(entry)
	if err != nil {
		return nil, err
	}

	k.index++

	// #207: advance the FSM's virtual monotonic clock from the leader-stamped
	// timestamp. This is monotonic: we never regress applyTimeMs.
	if cmd.LeaderTimestampMs > k.applyTimeMs {
		k.applyTimeMs = cmd.LeaderTimestampMs
	}

	// Idempotency deduplication: if we have seen this (clientID, seqNum) pair
	// before, return the cached result without re-applying.
	if cmd.ClientID != "" {
		// C8: dedup on monotonic seq. Any command whose seq is <= the highest
		// already applied for this client is a duplicate/stale retry and must not
		// be re-applied (return the cached result rather than mutating again).
		if entry, ok := k.dedupTable[cmd.ClientID]; ok && entry.SeqNum >= cmd.SeqNum {
			return entry.Result, nil
		}
	}

	var res []byte
	var applyErr error
	isMutation := false

	switch cmd.Op {
	// ------------------------------------------------------------------
	// #207: tick — advance virtual clock and sweep expired keys
	// ------------------------------------------------------------------

	case "tick":
		// applyTimeMs already advanced above. Now sweep keys whose TTL has passed.
		n := k.sweepExpiredLocked()
		res, applyErr = json.Marshal(KvResult{Value: strconv.Itoa(n)})

	// ------------------------------------------------------------------
	// Legacy ops — preserved for 100 % backward compatibility
	// ------------------------------------------------------------------

	case "set":
		if k.revisionExhausted() {
			res, applyErr = json.Marshal(KvResult{Error: errRevisionExhausted})
			break
		}
		prev := k.data[cmd.Key]
		kv := k.applyPutLocked(cmd.Key, cmd.Value, prev, 0)
		k.emitEvents([]Event{{
			Type:     EventPut,
			Key:      cmd.Key,
			KV:       kvClone(kv),
			PrevKV:   kvClone(prev),
			Revision: k.revision,
		}})
		res, applyErr = json.Marshal(KvResult{Value: cmd.Value})
		isMutation = true

	case "get":
		kv := k.data[cmd.Key]
		if kv == nil || !k.isLive(kv) {
			res, applyErr = json.Marshal(KvResult{Error: "key not found"})
		} else {
			res, applyErr = json.Marshal(KvResult{Value: kv.Value})
		}

	case "delete":
		kv := k.data[cmd.Key]
		if kv == nil || !k.isLive(kv) {
			res, applyErr = json.Marshal(KvResult{Error: "key not found"})
		} else if k.revisionExhausted() {
			res, applyErr = json.Marshal(KvResult{Error: errRevisionExhausted})
		} else {
			k.revision++
			delete(k.data, cmd.Key)
			k.emitEvents([]Event{{
				Type:     EventDelete,
				Key:      cmd.Key,
				PrevKV:   kvClone(kv),
				Revision: k.revision,
			}})
			res, applyErr = json.Marshal(KvResult{Value: "ok"})
			isMutation = true
		}

	case "list":
		// Iterate keys in sorted order so the Apply output is deterministic
		// across replicas (a Go map iterates in random order). Mirrors "range".
		keys := make([]string, 0, len(k.data))
		for key, kv := range k.data {
			if k.isLive(kv) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		values := make([]string, 0, len(keys))
		for _, key := range keys {
			values = append(values, k.data[key].Value)
		}
		res, applyErr = json.Marshal(KvResult{Value: fmt.Sprintf("%v", values)})

	// ------------------------------------------------------------------
	// v2 ops
	// ------------------------------------------------------------------

	case "put":
		if k.revisionExhausted() {
			res, applyErr = json.Marshal(KvResult{Error: errRevisionExhausted})
			break
		}
		// #207: compute ExpiresAtMs from TTLSeconds + current applyTimeMs.
		var expiresAt int64
		if cmd.TTLSeconds > 0 {
			expiresAt = k.applyTimeMs + cmd.TTLSeconds*1000
		}
		prev := k.data[cmd.Key]
		kv := k.applyPutLocked(cmd.Key, cmd.Value, prev, expiresAt)
		k.emitEvents([]Event{{
			Type:     EventPut,
			Key:      cmd.Key,
			KV:       kvClone(kv),
			PrevKV:   kvClone(prev),
			Revision: k.revision,
		}})
		kvJSON, err := json.Marshal(kv)
		if err != nil {
			return nil, err
		}
		res, applyErr = json.Marshal(KvResult{Value: string(kvJSON)})
		isMutation = true

	case "get_v2":
		// Linearizable: this op travels through the Raft log so the read
		// is guaranteed to reflect all preceding committed writes.
		kv := k.data[cmd.Key]
		if kv == nil || !k.isLive(kv) {
			res, applyErr = json.Marshal(KvResult{Error: "key not found"})
		} else {
			kvJSON, err := json.Marshal(kv)
			if err != nil {
				return nil, err
			}
			res, applyErr = json.Marshal(KvResult{Value: string(kvJSON)})
		}

	case "range":
		// Prefix scan — no mutation, no revision bump. #207: filter expired keys.
		var kvs []*KeyValue
		for key, kv := range k.data {
			if strings.HasPrefix(key, cmd.Key) && k.isLive(kv) {
				kvs = append(kvs, kvClone(kv))
			}
		}
		sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
		rangeJSON, err := json.Marshal(kvs)
		if err != nil {
			return nil, err
		}
		res, applyErr = json.Marshal(KvResult{Value: string(rangeJSON)})

	case "incr":
		// Atomic counter: interpret the current value and the delta (carried in
		// cmd.Value) as base-10 int64s, add them, and store the result. A missing
		// key starts at 0. Determinism holds because Apply runs serially and the
		// arithmetic is a pure function of the log.
		if k.revisionExhausted() {
			res, applyErr = json.Marshal(KvResult{Error: errRevisionExhausted})
			break
		}
		delta, derr := strconv.ParseInt(cmd.Value, 10, 64)
		if derr != nil {
			res, applyErr = json.Marshal(KvResult{Error: "incr: delta is not a base-10 int64"})
			break
		}
		prev := k.data[cmd.Key]
		var cur int64
		if prev != nil && k.isLive(prev) {
			cur, derr = strconv.ParseInt(prev.Value, 10, 64)
			if derr != nil {
				res, applyErr = json.Marshal(KvResult{Error: "incr: existing value is not a base-10 int64"})
				break
			}
		} else {
			prev = nil // treat expired as absent
		}
		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			res, applyErr = json.Marshal(KvResult{Error: "incr: int64 overflow"})
			break
		}
		kv := k.applyPutLocked(cmd.Key, strconv.FormatInt(cur+delta, 10), prev, 0)
		k.emitEvents([]Event{{
			Type:     EventPut,
			Key:      cmd.Key,
			KV:       kvClone(kv),
			PrevKV:   kvClone(prev),
			Revision: k.revision,
		}})
		kvJSON, err := json.Marshal(kv)
		if err != nil {
			return nil, err
		}
		res, applyErr = json.Marshal(KvResult{Value: string(kvJSON)})
		isMutation = true

	case "txn":
		if cmd.Txn == nil {
			res, applyErr = json.Marshal(KvResult{Error: "missing txn payload"})
		} else {
			resp := k.applyTxnLocked(cmd.Txn)
			res, applyErr = json.Marshal(resp)
			isMutation = true
		}

	default:
		res, applyErr = json.Marshal(KvResult{Error: "unknown operation"})
	}

	// Record the result in the dedup table for mutation ops so that client
	// retries receive the cached response rather than double-applying.
	if isMutation && cmd.ClientID != "" && applyErr == nil {
		prev, ok := k.dedupTable[cmd.ClientID]
		if !ok || cmd.SeqNum >= prev.SeqNum {
			k.dedupTable[cmd.ClientID] = dedupEntry{SeqNum: cmd.SeqNum, Result: res, Order: k.index}
			k.evictDedupIfNeeded()
		}
	}

	return res, applyErr
}

// revisionExhausted reports whether the revision counter has reached its
// documented ceiling (L4). Callers must refuse further mutations when true so
// the int64 counters never wrap to a negative value.
func (k *KVStore) revisionExhausted() bool {
	return k.revision >= maxRevision
}

// applyPutLocked creates or updates a key. Caller must hold mu.Lock().
// expiresAt is the absolute Unix-millisecond expiry time; 0 means no expiry (#207).
// Returns the updated KeyValue (stored in k.data).
func (k *KVStore) applyPutLocked(key, value string, prev *KeyValue, expiresAt int64) *KeyValue {
	k.revision++
	var cr, ver int64
	if prev != nil {
		cr = prev.CreateRevision
		ver = prev.Version + 1
	} else {
		cr = k.revision
		ver = 1
	}
	kv := &KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: cr,
		ModRevision:    k.revision,
		Version:        ver,
		ExpiresAtMs:    expiresAt,
	}
	k.data[key] = kv
	return kv
}

// isLive reports whether kv is present and not yet expired (#207). Caller must
// hold at least mu.RLock(). The check is against the FSM's virtual clock
// (applyTimeMs), which is identical on every replica, so expiry is deterministic.
func (k *KVStore) isLive(kv *KeyValue) bool {
	return kv.ExpiresAtMs == 0 || kv.ExpiresAtMs > k.applyTimeMs
}

// sweepExpiredLocked deletes all keys whose TTL has lapsed (ExpiresAtMs > 0
// && ExpiresAtMs <= applyTimeMs) in deterministic sorted key order and emits
// an EventDelete for each. Called only from the "tick" Apply path. Caller must
// hold mu.Lock().
func (k *KVStore) sweepExpiredLocked() int {
	var expired []string
	for key, kv := range k.data {
		if kv.ExpiresAtMs > 0 && kv.ExpiresAtMs <= k.applyTimeMs {
			expired = append(expired, key)
		}
	}
	if len(expired) == 0 {
		return 0
	}
	sort.Strings(expired) // deterministic deletion order across replicas
	if k.revisionExhausted() {
		return 0
	}
	k.revision++
	sweepRev := k.revision
	events := make([]Event, 0, len(expired))
	for _, key := range expired {
		prev := k.data[key]
		delete(k.data, key)
		events = append(events, Event{
			Type:     EventDelete,
			Key:      key,
			PrevKV:   kvClone(prev),
			Revision: sweepRev,
		})
	}
	k.emitEvents(events)
	return len(expired)
}

// applyTxnLocked evaluates and executes a transaction. Caller must hold mu.Lock().
func (k *KVStore) applyTxnLocked(req *TxnRequest) TxnResponse {
	// Evaluate all conditions.
	succeeded := true
	for _, cmp := range req.Compare {
		if !k.evalCompare(cmp) {
			succeeded = false
			break
		}
	}

	ops := req.Success
	if !succeeded {
		ops = req.Failure
	}

	if len(ops) == 0 {
		return TxnResponse{Succeeded: succeeded, Revision: k.revision}
	}

	// L4: refuse to mutate once the revision ceiling is reached rather than
	// wrapping the int64 counter negative. The txn is reported as not succeeded
	// and no state is changed.
	if k.revisionExhausted() {
		return TxnResponse{Succeeded: false, Revision: k.revision}
	}

	// M11: transactions are atomic. Pre-validate every op before mutating any
	// state so a failing op (e.g. delete of a missing key) aborts the whole txn
	// without partial application. On abort nothing is written, no revision is
	// consumed, no events are emitted, and Succeeded is reported false so the
	// caller is never told the branch succeeded when an op did not.
	for _, op := range ops {
		if op.Type == 1 && k.data[op.Key] == nil { // delete of a missing key
			results := make([]TxnResult, len(ops))
			for i := range ops {
				if ops[i].Type == 1 && k.data[ops[i].Key] == nil {
					results[i] = TxnResult{Error: "key not found"}
				}
			}
			return TxnResponse{Succeeded: false, Results: results, Revision: k.revision}
		}
	}

	// Execute ops. All mutations share a single revision increment for the txn.
	k.revision++
	txnRev := k.revision

	var results []TxnResult
	var events []Event

	for _, op := range ops {
		switch op.Type {
		case 0: // put
			prev := k.data[op.Key]
			var cr, ver int64
			if prev != nil {
				cr = prev.CreateRevision
				ver = prev.Version + 1
			} else {
				cr = txnRev
				ver = 1
			}
			kv := &KeyValue{
				Key:            op.Key,
				Value:          op.Value,
				CreateRevision: cr,
				ModRevision:    txnRev,
				Version:        ver,
			}
			k.data[op.Key] = kv
			events = append(events, Event{
				Type:     EventPut,
				Key:      op.Key,
				KV:       kvClone(kv),
				PrevKV:   kvClone(prev),
				Revision: txnRev,
			})
			results = append(results, TxnResult{KV: kvClone(kv)})

		case 1: // delete
			// Pre-validated above: prev is guaranteed non-nil here.
			prev := k.data[op.Key]
			delete(k.data, op.Key)
			events = append(events, Event{
				Type:     EventDelete,
				Key:      op.Key,
				PrevKV:   kvClone(prev),
				Revision: txnRev,
			})
			results = append(results, TxnResult{KV: kvClone(prev)})
		}
	}

	if len(events) > 0 {
		k.emitEvents(events)
	}

	return TxnResponse{Succeeded: succeeded, Results: results, Revision: txnRev}
}

// evalCompare evaluates a single Compare condition. Caller must hold mu.Lock() or mu.RLock().
func (k *KVStore) evalCompare(c Compare) bool {
	kv := k.data[c.Key]
	switch c.Target {
	case "value":
		if kv == nil {
			return c.Result == "not_equal"
		}
		switch c.Result {
		case "equal":
			return kv.Value == c.Value
		case "not_equal":
			return kv.Value != c.Value
		}
		return false
	case "version":
		var ver int64
		if kv != nil {
			ver = kv.Version
		}
		return compareInt64(ver, c.Rev, c.Result)
	case "create_revision":
		var cr int64
		if kv != nil {
			cr = kv.CreateRevision
		}
		return compareInt64(cr, c.Rev, c.Result)
	case "mod_revision":
		var mr int64
		if kv != nil {
			mr = kv.ModRevision
		}
		return compareInt64(mr, c.Rev, c.Result)
	}
	return false
}

// emitEvents writes events to the history ring buffer and pushes them to eventCh
// non-blocking. Caller must hold mu.Lock().
func (k *KVStore) emitEvents(events []Event) {
	for _, ev := range events {
		k.history[k.historyPos] = ev
		k.historyPos = (k.historyPos + 1) % len(k.history)
		if k.historyLen < len(k.history) {
			k.historyLen++
		}
	}
	select {
	case k.eventCh <- events:
	default:
		// WatchManager is not keeping up; drop the live notification.
		// Late subscribers reconnect using GetHistory(sinceRevision).
		// Increment the dropped-events counter so operators can alert on this.
		atomic.AddUint64(&k.droppedEvents, 1)
	}
}

// drainEventCh non-blockingly empties any pending event batches from eventCh.
// Caller must hold mu.Lock(). Used by Restore so pre-snapshot events buffered
// in the channel are not delivered live after a snapshot install (H11).
func (k *KVStore) drainEventCh() {
	for {
		select {
		case <-k.eventCh:
		default:
			return
		}
	}
}

// kvClone returns a deep copy of kv, or nil if kv is nil.
func kvClone(kv *KeyValue) *KeyValue {
	if kv == nil {
		return nil
	}
	cp := *kv
	return &cp
}

// ---------------------------------------------------------------------------
// Public read methods (safe for concurrent HTTP goroutines)
// ---------------------------------------------------------------------------

// Get returns the KeyValue for key, or nil if the key does not exist.
func (k *KVStore) Get(key string) (*KeyValue, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	kv := k.data[key]
	if kv == nil {
		return nil, nil
	}
	return kvClone(kv), nil
}

// Range returns all KeyValues whose key has the given prefix, sorted by key.
// At most maxRangeResults entries are returned; if the result set would exceed
// this limit an error is returned to avoid memory exhaustion from wildcard queries.
func (k *KVStore) Range(prefix string) ([]*KeyValue, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	var result []*KeyValue
	for key, kv := range k.data {
		if len(result) >= maxRangeResults {
			return nil, fmt.Errorf("range result exceeds limit of %d keys; use a more specific prefix", maxRangeResults)
		}
		if strings.HasPrefix(key, prefix) {
			result = append(result, kvClone(kv))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

// RangePage returns up to limit KeyValues whose key has the given prefix and is
// lexicographically greater than startAfter (an exclusive cursor; "" starts at
// the beginning), sorted by key. The bool reports whether more matching keys
// exist beyond this page — the caller pages by passing the last returned key as
// the next startAfter. limit is clamped to (0, maxRangeResults]. This bounds the
// response size regardless of how many keys match, unlike Range.
func (k *KVStore) RangePage(prefix, startAfter string, limit int) ([]*KeyValue, bool, error) {
	if limit <= 0 || limit > maxRangeResults {
		limit = maxRangeResults
	}
	k.mu.RLock()
	defer k.mu.RUnlock()

	var keys []string
	for key := range k.data {
		if strings.HasPrefix(key, prefix) && key > startAfter {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	more := false
	if len(keys) > limit {
		keys = keys[:limit]
		more = true
	}
	result := make([]*KeyValue, 0, len(keys))
	for _, key := range keys {
		result = append(result, kvClone(k.data[key]))
	}
	return result, more, nil
}

// GetRevision returns the current global cluster revision.
func (k *KVStore) GetRevision() int64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.revision
}

// NotificationChan returns the read-only channel on which Apply emits events.
// It should be consumed exclusively by a single WatchManager goroutine.
func (k *KVStore) NotificationChan() <-chan []Event {
	return k.eventCh
}

// GetHistory returns all buffered events with Revision > sinceRevision,
// in ascending revision order.
func (k *KVStore) GetHistory(sinceRevision int64) []Event {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if k.historyLen == 0 {
		return nil
	}

	// Walk the ring buffer in insertion order (oldest → newest).
	start := (k.historyPos - k.historyLen + len(k.history)) % len(k.history)
	var result []Event
	for i := 0; i < k.historyLen; i++ {
		ev := k.history[(start+i)%len(k.history)]
		if ev.Revision > sinceRevision {
			result = append(result, ev)
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Snapshot / Restore
// ---------------------------------------------------------------------------

// kvSnapshotData is the JSON envelope written into snapshot files.
type kvSnapshotData struct {
	Revision int64 `json:"revision"`
	// Index is the apply index (k.index) at the moment the snapshot was taken.
	// It must be serialized and restored so Snapshot().Index() is correct after a
	// Restore (H10): otherwise a restored FSM reports index 0 and the raft layer
	// can mis-compute the snapshot/log boundary.
	Index       uint64                `json:"index"`
	Data        map[string]*KeyValue  `json:"data"`
	DedupTable  map[string]dedupEntry `json:"dedup_table,omitempty"`
	ApplyTimeMs int64                 `json:"apply_time_ms,omitempty"` // #207 virtual clock
}

// evictDedupIfNeeded removes entries when dedupTable exceeds maxDedupEntries.
// Eviction is DETERMINISTIC (C8): it removes the entry with the lowest Order
// (oldest deterministic insertion), breaking ties by client ID. Because the
// dedup table is serialized into snapshots, a non-deterministic (e.g.
// map-iteration-order) eviction would make replicas' snapshots diverge.
// Called under mu.Lock (held by Apply).
func (k *KVStore) evictDedupIfNeeded() {
	for len(k.dedupTable) > maxDedupEntries {
		var victimID string
		var victimOrder uint64
		first := true
		for id, e := range k.dedupTable {
			if first || e.Order < victimOrder || (e.Order == victimOrder && id < victimID) {
				victimID = id
				victimOrder = e.Order
				first = false
			}
		}
		delete(k.dedupTable, victimID)
	}
}

// snapStreamMagic tags the streaming binary snapshot format (#223). It is
// deliberately not '{' (JSON) or '[' (legacy array) so Restore can distinguish
// it from the older JSON snapshots by peeking the first byte.
const snapStreamMagic = 0x02

func (k *KVStore) Snapshot() (raft.Snapshot, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	// Clone the state under the read lock (consistency), but do NOT marshal it
	// into a single []byte here (#223): the clone is stream-encoded on demand by
	// Reader(), avoiding a second full-size copy and reflection-based json.Marshal.
	data := make(map[string]*KeyValue, len(k.data))
	for key, kv := range k.data {
		data[key] = kvClone(kv)
	}
	dedup := make(map[string]dedupEntry, len(k.dedupTable))
	for clientID, entry := range k.dedupTable {
		dedup[clientID] = entry
	}
	return &kvSnapshot{
		revision:    k.revision,
		index:       k.index,
		applyTimeMs: k.applyTimeMs,
		data:        data,
		dedup:       dedup,
	}, nil
}

// writeSnapshotStream serializes the FSM state to w in a compact, length-prefixed
// binary form, record by record (no full-size intermediate buffer).
// Format version 2 adds applyTimeMs (#207) after index, and ExpiresAtMs after
// each key's Version field. Readers check the version and skip absent fields.
func writeSnapshotStream(w io.Writer, revision int64, index uint64, applyTimeMs int64, data map[string]*KeyValue, dedup map[string]dedupEntry) error {
	bw := bufio.NewWriter(w)
	var scratch [binary.MaxVarintLen64]byte
	putUvarint := func(v uint64) error {
		n := binary.PutUvarint(scratch[:], v)
		_, err := bw.Write(scratch[:n])
		return err
	}
	putVarint := func(v int64) error {
		n := binary.PutVarint(scratch[:], v)
		_, err := bw.Write(scratch[:n])
		return err
	}
	putStr := func(s string) error {
		if err := putUvarint(uint64(len(s))); err != nil {
			return err
		}
		_, err := bw.WriteString(s)
		return err
	}
	if err := bw.WriteByte(snapStreamMagic); err != nil {
		return err
	}
	if err := putUvarint(2); err != nil { // format version 2 (#207)
		return err
	}
	if err := putVarint(revision); err != nil {
		return err
	}
	if err := putUvarint(index); err != nil {
		return err
	}
	if err := putVarint(applyTimeMs); err != nil { // v2: virtual clock (#207)
		return err
	}
	if err := putUvarint(uint64(len(data))); err != nil {
		return err
	}
	for key, kv := range data {
		if err := putStr(key); err != nil {
			return err
		}
		if err := putStr(kv.Value); err != nil {
			return err
		}
		if err := putVarint(kv.CreateRevision); err != nil {
			return err
		}
		if err := putVarint(kv.ModRevision); err != nil {
			return err
		}
		if err := putVarint(kv.Version); err != nil {
			return err
		}
		if err := putVarint(kv.ExpiresAtMs); err != nil { // v2: TTL expiry (#207)
			return err
		}
	}
	if err := putUvarint(uint64(len(dedup))); err != nil {
		return err
	}
	for clientID, e := range dedup {
		if err := putStr(clientID); err != nil {
			return err
		}
		if err := putUvarint(e.SeqNum); err != nil {
			return err
		}
		if err := putUvarint(uint64(len(e.Result))); err != nil {
			return err
		}
		if _, err := bw.Write(e.Result); err != nil {
			return err
		}
		if err := putUvarint(e.Order); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// readSnapshotStream decodes the binary snapshot written by writeSnapshotStream.
// The magic byte has already been consumed by the caller. Returns applyTimeMs
// (#207) which is non-zero only in v2+ snapshots.
func readSnapshotStream(br *bufio.Reader) (revision int64, index uint64, applyTimeMs int64, data map[string]*KeyValue, dedup map[string]dedupEntry, err error) {
	readStr := func() (string, error) {
		n, e := binary.ReadUvarint(br)
		if e != nil {
			return "", e
		}
		buf := make([]byte, n)
		if _, e := io.ReadFull(br, buf); e != nil {
			return "", e
		}
		return string(buf), nil
	}
	var version uint64
	if version, err = binary.ReadUvarint(br); err != nil { // format version
		return
	}
	if revision, err = binary.ReadVarint(br); err != nil {
		return
	}
	if index, err = binary.ReadUvarint(br); err != nil {
		return
	}
	if version >= 2 { // v2: virtual clock (#207)
		if applyTimeMs, err = binary.ReadVarint(br); err != nil {
			return
		}
	}
	var n uint64
	if n, err = binary.ReadUvarint(br); err != nil {
		return
	}
	data = make(map[string]*KeyValue, n)
	for i := uint64(0); i < n; i++ {
		var key, val string
		if key, err = readStr(); err != nil {
			return
		}
		if val, err = readStr(); err != nil {
			return
		}
		kv := &KeyValue{Key: key, Value: val}
		if kv.CreateRevision, err = binary.ReadVarint(br); err != nil {
			return
		}
		if kv.ModRevision, err = binary.ReadVarint(br); err != nil {
			return
		}
		if kv.Version, err = binary.ReadVarint(br); err != nil {
			return
		}
		if version >= 2 { // v2: per-key TTL expiry (#207)
			if kv.ExpiresAtMs, err = binary.ReadVarint(br); err != nil {
				return
			}
		}
		data[key] = kv
	}
	var dn uint64
	if dn, err = binary.ReadUvarint(br); err != nil {
		return
	}
	dedup = make(map[string]dedupEntry, dn)
	for i := uint64(0); i < dn; i++ {
		var clientID string
		if clientID, err = readStr(); err != nil {
			return
		}
		var e dedupEntry
		if e.SeqNum, err = binary.ReadUvarint(br); err != nil {
			return
		}
		var rl uint64
		if rl, err = binary.ReadUvarint(br); err != nil {
			return
		}
		e.Result = make([]byte, rl)
		if _, err = io.ReadFull(br, e.Result); err != nil {
			return
		}
		if e.Order, err = binary.ReadUvarint(br); err != nil {
			return
		}
		dedup[clientID] = e
	}
	return
}

func (k *KVStore) Restore(reader io.Reader) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	// Reset watch history: stale pre-snapshot events must not be delivered
	// to reconnecting watchers after the snapshot is installed.
	k.history = make([]Event, defaultHistorySize)
	k.historyPos = 0
	k.historyLen = 0

	// Drain any events buffered in eventCh (H11): pre-snapshot events must not be
	// delivered live to watchers after a restore, or they'd be duplicated /
	// out-of-order relative to the restored state.
	k.drainEventCh()

	// New streaming binary format (#223): identified by the magic first byte.
	if len(data) > 0 && data[0] == snapStreamMagic {
		br := bufio.NewReader(bytes.NewReader(data[1:]))
		rev, idx, applyTs, d, dd, derr := readSnapshotStream(br)
		if derr != nil {
			return derr
		}
		k.data = d
		k.revision = rev
		k.index = idx
		k.applyTimeMs = applyTs // #207: restore virtual clock
		if dd != nil {
			k.dedupTable = dd
		} else {
			k.dedupTable = make(map[string]dedupEntry)
		}
		return nil
	}

	// Try the older versioned JSON format (backward compatible).
	var snap kvSnapshotData
	if err := json.Unmarshal(data, &snap); err == nil && snap.Data != nil {
		k.data = snap.Data
		k.revision = snap.Revision
		k.index = snap.Index
		k.applyTimeMs = snap.ApplyTimeMs // #207: zero for old snapshots
		if snap.DedupTable != nil {
			k.dedupTable = snap.DedupTable
		} else {
			k.dedupTable = make(map[string]dedupEntry)
		}
		return nil
	}

	// Fall back to old map[string]string format (rolling upgrade path).
	var legacy map[string]string
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}
	k.data = make(map[string]*KeyValue, len(legacy))
	for key, val := range legacy {
		k.data[key] = &KeyValue{Key: key, Value: val}
	}
	k.revision = 0
	k.dedupTable = make(map[string]dedupEntry)
	return nil
}

type kvSnapshot struct {
	revision    int64
	index       uint64
	applyTimeMs int64 // #207: virtual clock snapshot
	data        map[string]*KeyValue
	dedup       map[string]dedupEntry
}

func (s *kvSnapshot) Index() uint64 { return s.index }
func (s *kvSnapshot) Term() uint64  { return 0 }

// Reader streams the snapshot's binary encoding on demand via an io.Pipe, so the
// snapshot store consumes it record-by-record without materializing the whole
// serialized payload in memory (#223).
func (s *kvSnapshot) Reader() io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(writeSnapshotStream(pw, s.revision, s.index, s.applyTimeMs, s.data, s.dedup))
	}()
	return pr
}

// ---------------------------------------------------------------------------
// Command encoding helpers (public API, backward compatible)
// ---------------------------------------------------------------------------

type CommandType uint8

const (
	CommandSet CommandType = iota
	CommandGet
	CommandDelete
	CommandList
)

func EncodeCommand(op string, key, value string) ([]byte, error) {
	return encodeKVCommand(kvCommand{Op: op, Key: key, Value: value}), nil
}

// EncodeCommandWithID encodes a kvCommand that includes idempotency fields so
// the FSM can deduplicate retried writes from the same client.
func EncodeCommandWithID(op, key, value, clientID string, seqNum uint64) ([]byte, error) {
	return encodeKVCommand(kvCommand{
		Op:       op,
		Key:      key,
		Value:    value,
		ClientID: clientID,
		SeqNum:   seqNum,
	}), nil
}

// cmdBinaryMagic tags a binary-encoded kvCommand. It is deliberately not '{'
// (0x7b) so decodeKVCommand can distinguish binary from the legacy/txn JSON
// encoding by peeking the first byte (M-P4).
const cmdBinaryMagic = 0x01

// encodeKVCommand serializes a non-txn kvCommand into a compact, deterministic
// binary form (M-P4): a magic byte, then length-prefixed op/key/value/clientID,
// a uvarint seqNum, and an optional TTL extension (#207) appended only when
// LeaderTimestampMs or TTLSeconds is non-zero (zero-extend for old decoders that
// stop reading after SeqNum — backward compatible). The encoding is a pure
// function of the command so replicas produce byte-identical entries.
func encodeKVCommand(cmd kvCommand) []byte {
	// Txn commands are never routed here (EncodeTxn handles them); fall back to
	// JSON defensively if a Txn is somehow present so nothing is silently lost.
	if cmd.Txn != nil {
		b, _ := json.Marshal(cmd)
		return b
	}
	size := 1 + 5*4 + len(cmd.Op) + len(cmd.Key) + len(cmd.Value) + len(cmd.ClientID) + binary.MaxVarintLen64
	if cmd.LeaderTimestampMs != 0 || cmd.TTLSeconds != 0 {
		size += 2 * binary.MaxVarintLen64
	}
	buf := make([]byte, 0, size)
	buf = append(buf, cmdBinaryMagic)
	writeLenPrefixed := func(b []byte, s string) []byte {
		b = binary.AppendUvarint(b, uint64(len(s)))
		return append(b, s...)
	}
	buf = writeLenPrefixed(buf, cmd.Op)
	buf = writeLenPrefixed(buf, cmd.Key)
	buf = writeLenPrefixed(buf, cmd.Value)
	buf = writeLenPrefixed(buf, cmd.ClientID)
	buf = binary.AppendUvarint(buf, cmd.SeqNum)
	// TTL extension (#207): appended only when present so old decoders (which
	// stop after SeqNum) decode cleanly — their trailing-byte-ignoring behavior
	// is load-bearing for rolling upgrades.
	if cmd.LeaderTimestampMs != 0 || cmd.TTLSeconds != 0 {
		buf = binary.AppendVarint(buf, cmd.LeaderTimestampMs)
		buf = binary.AppendUvarint(buf, uint64(cmd.TTLSeconds))
	}
	return buf
}

// decodeKVCommand decodes a kvCommand from either the binary form (magic byte)
// or the legacy/txn JSON form (starts with '{'), so it is backward-compatible
// with entries written before M-P4 and with txn commands (M-P4). The binary
// decoder reads the optional TTL extension (#207) when remaining bytes exist
// after SeqNum.
func decodeKVCommand(data []byte) (kvCommand, error) {
	var cmd kvCommand
	if len(data) == 0 {
		return cmd, fmt.Errorf("empty command")
	}
	if data[0] != cmdBinaryMagic {
		// Legacy JSON command or a txn envelope.
		err := json.Unmarshal(data, &cmd)
		return cmd, err
	}
	b := data[1:]
	readStr := func() (string, error) {
		n, m := binary.Uvarint(b)
		if m <= 0 {
			return "", fmt.Errorf("corrupt command: bad length prefix")
		}
		b = b[m:]
		if uint64(len(b)) < n {
			return "", fmt.Errorf("corrupt command: truncated field")
		}
		s := string(b[:n])
		b = b[n:]
		return s, nil
	}
	var err error
	if cmd.Op, err = readStr(); err != nil {
		return cmd, err
	}
	if cmd.Key, err = readStr(); err != nil {
		return cmd, err
	}
	if cmd.Value, err = readStr(); err != nil {
		return cmd, err
	}
	if cmd.ClientID, err = readStr(); err != nil {
		return cmd, err
	}
	seq, m := binary.Uvarint(b)
	if m <= 0 {
		return cmd, fmt.Errorf("corrupt command: bad seqnum")
	}
	cmd.SeqNum = seq
	b = b[m:]
	// TTL extension (#207): present only when the encoder appended it.
	if len(b) > 0 {
		lts, m2 := binary.Varint(b)
		if m2 <= 0 {
			return cmd, fmt.Errorf("corrupt command: bad leader_timestamp_ms")
		}
		cmd.LeaderTimestampMs = lts
		b = b[m2:]
		ttl, m3 := binary.Uvarint(b)
		if m3 <= 0 {
			return cmd, fmt.Errorf("corrupt command: bad ttl_seconds")
		}
		cmd.TTLSeconds = int64(ttl)
	}
	return cmd, nil
}

// EncodeTick encodes a committed tick command that carries the leader's
// wall-clock time and triggers a deterministic sweep of expired keys (#207).
func EncodeTick(leaderTimestampMs int64) []byte {
	return encodeKVCommand(kvCommand{Op: "tick", LeaderTimestampMs: leaderTimestampMs})
}

// EncodeCommandWithTTL encodes a kvCommand with TTL fields for write ops that
// should expire (#207). leaderTsMs is the leader's Unix-millisecond timestamp
// at proposal time; ttlSeconds is the desired lifetime in seconds.
func EncodeCommandWithTTL(op, key, value, clientID string, seqNum uint64, leaderTsMs, ttlSeconds int64) ([]byte, error) {
	return encodeKVCommand(kvCommand{
		Op:                op,
		Key:               key,
		Value:             value,
		ClientID:          clientID,
		SeqNum:            seqNum,
		LeaderTimestampMs: leaderTsMs,
		TTLSeconds:        ttlSeconds,
	}), nil
}

func EncodeSet(key, value string) ([]byte, error) { return EncodeCommand("set", key, value) }
func EncodeGet(key string) ([]byte, error)        { return EncodeCommand("get", key, "") }
func EncodeDelete(key string) ([]byte, error)     { return EncodeCommand("delete", key, "") }

func DecodeResult(data []byte) (*KvResult, error) {
	var res KvResult
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// DecodeKeyValueResult decodes a *KeyValue from the JSON string embedded in
// KvResult.Value — used by HTTP handlers and the client library for v2 ops.
func DecodeKeyValueResult(data []byte) (*KeyValue, error) {
	res, err := DecodeResult(data)
	if err != nil {
		return nil, err
	}
	if res.Error != "" {
		return nil, fmt.Errorf("%s", res.Error)
	}
	var kv KeyValue
	if err := json.Unmarshal([]byte(res.Value), &kv); err != nil {
		return nil, err
	}
	return &kv, nil
}

// DecodeKeyValuesResult decodes a []*KeyValue from the JSON string embedded in
// KvResult.Value — used by HTTP handlers for range ops.
func DecodeKeyValuesResult(data []byte) ([]*KeyValue, error) {
	res, err := DecodeResult(data)
	if err != nil {
		return nil, err
	}
	if res.Error != "" {
		return nil, fmt.Errorf("%s", res.Error)
	}
	var kvs []*KeyValue
	if err := json.Unmarshal([]byte(res.Value), &kvs); err != nil {
		return nil, err
	}
	return kvs, nil
}

func EncodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func DecodeUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

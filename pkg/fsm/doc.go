// Package fsm provides the replicated key/value state machine applied on top of
// the raft log. KVStore is a linearizable KV store with MVCC-style revisions,
// etcd-style mini-transactions (compare/success/failure), atomic counters,
// prefix range scans with cursor pagination, idempotency deduplication, and
// snapshot/restore. WatchManager delivers ordered change events to SSE watchers.
package fsm

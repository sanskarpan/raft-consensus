// Package storage provides durable persistence for the raft node: a segmented,
// CRC-checked, fsync-durable write-ahead log (WAL) with torn-tail recovery, an
// atomic checksummed file snapshot store, and a BoltDB-backed StableStore for
// term/vote/commit metadata.
package storage

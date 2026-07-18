// Package client is the Go client for a raftd cluster's HTTP API. It provides
// linearizable and stale reads, writes (Put/DeleteKV/Increment), compare-and-swap
// transactions, cursor-paginated ranges, and change watches, with leader-aware
// routing, idempotent retries, and configurable read consistency.
//
// A Client is a single writer: its idempotency uses a monotonic per-client
// sequence number, so concurrent writers should each use their own Client.
package client

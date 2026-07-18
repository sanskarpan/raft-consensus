// Package transport implements the inter-node RPC transports for raft — a gRPC
// transport and a JSON-over-TCP transport — carrying RequestVote, AppendEntries,
// InstallSnapshot, and TimeoutNow. Both support TLS 1.3 / mutual TLS with
// per-peer server-name verification, peer-identity authorization, connection
// pooling, and bounded message sizes.
package transport

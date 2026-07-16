package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	TracerRaft      = "raft"
	TracerTransport = "transport"
)

// SpanRequestVote creates a span for a RequestVote RPC.
// Usage: ctx, span := tracing.SpanRequestVote(ctx, "n1", 5); defer span.End()
func SpanRequestVote(ctx context.Context, nodeID string, term uint64) (context.Context, trace.Span) {
	ctx, span := StartSpan(ctx, TracerRaft, "raft.RequestVote")
	span.SetAttributes(
		attribute.String("raft.node_id", nodeID),
		attribute.Int64("raft.term", int64(term)),
	)
	return ctx, span
}

// SpanAppendEntries creates a span for an AppendEntries RPC.
func SpanAppendEntries(ctx context.Context, nodeID string, term uint64, entryCount int) (context.Context, trace.Span) {
	ctx, span := StartSpan(ctx, TracerTransport, "raft.AppendEntries")
	span.SetAttributes(
		attribute.String("raft.node_id", nodeID),
		attribute.Int64("raft.term", int64(term)),
		attribute.Int("raft.entry_count", entryCount),
	)
	return ctx, span
}

// SpanSnapshot creates a span for a snapshot operation.
func SpanSnapshot(ctx context.Context, nodeID string, index uint64) (context.Context, trace.Span) {
	ctx, span := StartSpan(ctx, TracerRaft, "raft.Snapshot")
	span.SetAttributes(
		attribute.String("raft.node_id", nodeID),
		attribute.Int64("raft.snapshot_index", int64(index)),
	)
	return ctx, span
}

// RecordError records an error on a span.
func RecordError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

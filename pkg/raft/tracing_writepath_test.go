package raft

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestWritePathSpanEmitted verifies that a client Apply with an active trace
// context produces a child "raft.commit_apply" span that ends when the entry is
// applied (#213).
func TestWritePathSpanEmitted(t *testing.T) {
	// Install an in-memory span recorder as the global tracer provider.
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	r, _, _ := makeRaftNode("n1", cfg)
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	defer r.Shutdown()
	waitState(t, r, StateLeader, 5*time.Second)

	// Apply under a root span so the write-path span has a parent to attach to.
	ctx, root := tp.Tracer("test").Start(context.Background(), "client.write")
	if _, err := r.Apply(ctx, []byte("payload")); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	root.End()

	// Give the async apply a beat, then check the recorded spans.
	deadline := time.Now().Add(3 * time.Second)
	var found bool
	for time.Now().Before(deadline) {
		for _, s := range rec.Ended() {
			if s.Name() == "raft.commit_apply" {
				found = true
				// It must be a child of our root span (same trace).
				if s.Parent().TraceID() != root.SpanContext().TraceID() {
					t.Fatalf("write-path span trace %v != root trace %v",
						s.Parent().TraceID(), root.SpanContext().TraceID())
				}
			}
		}
		if found {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !found {
		t.Fatal("no raft.commit_apply span was recorded for a traced Apply")
	}
}

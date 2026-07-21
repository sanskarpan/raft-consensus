//go:build integration

package transport

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestSpiffeSource_Integration verifies that NewSpiffeSource fails gracefully
// when the Workload API socket does not exist. This tests the error-wrapping
// path so callers can identify SPIFFE init failures by their "spiffe: connect
// to workload API:" prefix.
//
// Requires the "integration" build tag: go test -tags integration ./pkg/transport/...
func TestSpiffeSource_Integration(t *testing.T) {
	// Point at a socket path that cannot exist.
	socketPath := "unix:///nonexistent/spire/agent.sock"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := NewSpiffeSource(ctx, socketPath, nil)
	if err == nil {
		t.Fatal("NewSpiffeSource: expected error for non-existent socket, got nil")
	}
	const wantPrefix = "spiffe: connect to workload API:"
	if !strings.HasPrefix(err.Error(), wantPrefix) {
		t.Errorf("NewSpiffeSource error = %q; want prefix %q", err.Error(), wantPrefix)
	}
	t.Logf("got expected error: %v", err)
}

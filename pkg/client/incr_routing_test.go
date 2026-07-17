package client

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestWriteAddrsPrefersCurrentAddr checks the leader-preference ordering.
func TestWriteAddrsPrefersCurrentAddr(t *testing.T) {
	c := NewClient(WithAddresses([]string{"a", "b", "c"}))

	// No known leader yet → original order.
	if got := c.writeAddrs(); got[0] != "a" || len(got) != 3 {
		t.Fatalf("writeAddrs before hint = %v, want [a b c]", got)
	}

	// After learning the leader is "c", it must be tried first, others follow.
	c.setCurrentAddr("c")
	got := c.writeAddrs()
	if got[0] != "c" || len(got) != 3 {
		t.Fatalf("writeAddrs after hint = %v, want c first", got)
	}
	// No duplicates.
	seen := map[string]bool{}
	for _, a := range got {
		if seen[a] {
			t.Fatalf("duplicate addr in %v", got)
		}
		seen[a] = true
	}
}

// TestIncrementParsesAndConverges verifies Increment returns the server's new
// value and converges CurrentAddr onto the X-Raft-Leader-Address hint.
func TestIncrementParsesAndConverges(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("op") != "incr" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("X-Raft-Leader-Address", "leader:9999")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"key":"ctr","value":"42","create_revision":1,"mod_revision":1,"version":1}`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	c := NewClient(WithAddresses([]string{addr}))
	v, err := c.Increment("ctr", 7)
	if err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if v != 42 {
		t.Fatalf("Increment returned %d, want 42", v)
	}
	if got := c.CurrentAddr(); got != "leader:9999" {
		t.Fatalf("CurrentAddr = %q, want leader:9999 (from X-Raft-Leader-Address)", got)
	}
}

// TestIncrementBadRequestNotRetried verifies a 400 surfaces immediately and is
// not retried across the node list.
func TestIncrementBadRequestNotRetried(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"incr: existing value is not a base-10 int64"}`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	c := NewClient(WithAddresses([]string{addr}))
	if _, err := c.Increment("ctr", 1); err == nil {
		t.Fatal("expected a client error for a 400 response")
	}
	if calls != 1 {
		t.Fatalf("server was called %d times; a 400 must not be retried", calls)
	}
}

// TestIncrementRateLimitRetried verifies a 429 IS retried (transient).
func TestIncrementRateLimitRetried(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"key":"ctr","value":"1","version":1}`))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	c := NewClient(WithAddresses([]string{addr}))
	v, err := c.Increment("ctr", 1)
	if err != nil {
		t.Fatalf("Increment should succeed after a 429 retry: %v", err)
	}
	if v != 1 || calls < 2 {
		t.Fatalf("value=%d calls=%d; expected retry after 429", v, calls)
	}
}

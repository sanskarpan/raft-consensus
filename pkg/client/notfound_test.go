package client

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// E2E finding: a GET on a missing key must return ErrKeyNotFound, not the
// misleading "all nodes failed" (which conflated a 404 with a connection error).
func TestGetKVMissingKeyReturnsErrKeyNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewClient(WithAddresses([]string{srv.Listener.Addr().String()}))
	_, err := c.GetKV("nope")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("GetKV on a missing key: got %v, want ErrKeyNotFound", err)
	}
}

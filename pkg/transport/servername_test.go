package transport

import "testing"

// C11: serverNameFor must respect an explicitly-configured ServerName, derive
// from the peer address otherwise, and fall back to the default for empty or
// unspecified listen addresses.
func TestServerNameFor(t *testing.T) {
	cases := []struct {
		name       string
		configured string
		addr       string
		want       string
	}{
		{"explicit wins", "peer.example.com", "10.0.0.5:8001", "peer.example.com"},
		{"derive host", "", "node2.internal:7000", "node2.internal"},
		{"derive ip", "", "127.0.0.1:8001", "127.0.0.1"},
		{"unspecified v6 falls back", "", "[::]:8001", defaultServerName},
		{"unspecified v4 falls back", "", "0.0.0.0:8001", defaultServerName},
		{"no port", "", "localhost", "localhost"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := serverNameFor(c.configured, c.addr); got != c.want {
				t.Fatalf("serverNameFor(%q,%q)=%q, want %q", c.configured, c.addr, got, c.want)
			}
		})
	}
}

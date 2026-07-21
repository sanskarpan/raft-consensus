package transport

import (
	"testing"
)

// TestSpiffeSourceClose_nilSafe verifies that Close() on a SpiffeSource whose
// inner workloadapi.X509Source is nil does not panic. This guards the shutdown
// path where the SPIFFE source may not have been initialized (e.g. no socket
// configured) but Shutdown() still calls Close() unconditionally.
func TestSpiffeSourceClose_nilSafe(t *testing.T) {
	t.Parallel()

	// A zero-value SpiffeSource has a nil source.
	s := &SpiffeSource{}
	if err := s.Close(); err != nil {
		t.Errorf("Close on nil-source SpiffeSource returned error: %v", err)
	}

	// A nil SpiffeSource pointer must also not panic.
	var nilSrc *SpiffeSource
	if err := nilSrc.Close(); err != nil {
		t.Errorf("Close on nil *SpiffeSource returned error: %v", err)
	}
}

// TestClientTLSConfig_invalidID verifies that ClientTLSConfig returns an error
// (and does not panic) when given a string that is not a valid SPIFFE URI.
// The internal spiffeid.FromString parser requires the "spiffe://" scheme and a
// non-empty trust domain, so bare strings, HTTP URIs, and empty strings must
// all be rejected.
func TestClientTLSConfig_invalidID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		id   string
	}{
		{"empty string", ""},
		{"not a uri", "not-a-spiffe-uri"},
		{"http scheme", "http://example.com/path"},
		{"missing path", "spiffe://"},
		{"bare hostname", "example.com"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// source is nil — the SPIFFE ID parsing error should fire first.
			s := &SpiffeSource{}
			_, err := s.ClientTLSConfig(tc.id)
			if err == nil {
				t.Errorf("ClientTLSConfig(%q): expected error, got nil", tc.id)
			}
		})
	}
}

// TestServerTLSConfig_invalidTrustDomain verifies that ServerTLSConfig returns
// an error when given a string that is not a valid SPIFFE trust domain.
// A trust domain must be a valid DNS name with only lower-case letters, digits,
// hyphens, and dots — not a full URI and not empty.
func TestServerTLSConfig_invalidTrustDomain(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		domain string
	}{
		{"empty string", ""},
		{"full spiffe uri", "spiffe://cluster.example/path"},
		{"upper case", "CLUSTER.EXAMPLE"},
		{"spaces", "cluster example"},
		{"slash", "cluster/example"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &SpiffeSource{}
			_, err := s.ServerTLSConfig(tc.domain)
			if err == nil {
				t.Errorf("ServerTLSConfig(%q): expected error, got nil", tc.domain)
			}
		})
	}
}


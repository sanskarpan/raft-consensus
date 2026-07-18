package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// roleTestServer builds a leader server with read/write/admin tokens and a
// working membership handler (stubRaft).
func roleTestServer(t *testing.T) *Server {
	t.Helper()
	s := &Server{
		config: &Config{
			AdminTokens: map[string]string{"rtok": "read", "wtok": "write", "atok": "admin"},
		},
		logger:   zap.NewNop(),
		limiter:  newWriteLimiter(1000),
		raftNode: &stubRaft{state: raft.StateLeader},
	}
	s.initHTTP()
	return s
}

func doReq(t *testing.T, s *Server, method, path, token, body string) int {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, r)
	return w.Code
}

// TestAdminRoleRequiredForMembership verifies membership ops require the admin
// role: a write token is forbidden, an admin token gets past the auth gate.
func TestAdminRoleRequiredForMembership(t *testing.T) {
	s := roleTestServer(t)
	body := `{"id":"x","address":"1.2.3.4:1"}`

	// write token: forbidden on membership (admin-only).
	if code := doReq(t, s, http.MethodPost, "/admin/members", "wtok", body); code != http.StatusForbidden {
		t.Fatalf("write token on /admin/members = %d, want 403", code)
	}
	// read token: also forbidden.
	if code := doReq(t, s, http.MethodPost, "/admin/members", "rtok", body); code != http.StatusForbidden {
		t.Fatalf("read token on /admin/members = %d, want 403", code)
	}
	// admin token: passes the auth gate (handler may 200/4xx/5xx, but not 401/403).
	if code := doReq(t, s, http.MethodPost, "/admin/members", "atok", body); code == http.StatusForbidden || code == http.StatusUnauthorized {
		t.Fatalf("admin token on /admin/members = %d, want NOT 401/403", code)
	}
}

// TestWriteRoleStillWritesButNotAdmin verifies the hierarchy: write can write
// data but cannot do admin ops; read cannot write.
func TestWriteRoleHierarchy(t *testing.T) {
	s := roleTestServer(t)

	// read token cannot POST /command (write-gated).
	if code := doReq(t, s, http.MethodPost, "/command", "rtok", "{}"); code != http.StatusForbidden {
		t.Fatalf("read token on /command = %d, want 403", code)
	}
	// write token is NOT forbidden on /command (passes auth; handler processes).
	if code := doReq(t, s, http.MethodPost, "/command", "wtok", "{}"); code == http.StatusForbidden || code == http.StatusUnauthorized {
		t.Fatalf("write token on /command = %d, want NOT 401/403", code)
	}
	// admin token also allowed on /command (admin implies write).
	if code := doReq(t, s, http.MethodPost, "/command", "atok", "{}"); code == http.StatusForbidden || code == http.StatusUnauthorized {
		t.Fatalf("admin token on /command = %d, want NOT 401/403", code)
	}
	// admin token forbidden on nothing it's allowed; snapshot is admin-only:
	if code := doReq(t, s, http.MethodPost, "/admin/snapshot", "wtok", ""); code != http.StatusForbidden {
		t.Fatalf("write token on /admin/snapshot = %d, want 403 (admin-only)", code)
	}
}

// TestLoadConfigRejectsBadRole verifies role validation at config load.
func TestLoadConfigRejectsBadRole(t *testing.T) {
	// exercised via loadConfig-equivalent validation: build a config and run the
	// same check used in loadConfig.
	roles := map[string]string{"t": "superuser"}
	var bad bool
	for _, role := range roles {
		if roleRank(role) == 0 {
			bad = true
		}
	}
	if !bad {
		t.Fatal("expected 'superuser' to be an invalid role")
	}
}

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// C9: When no tokens are configured, the auth middleware must FAIL CLOSED
// (reject) rather than allow every request through. Only an explicit AllowNoAuth
// opt-in permits unauthenticated access.
func TestAuthFailsClosedWhenNoTokensConfigured(t *testing.T) {
	s := bareServer("") // no admin token, AllowNoAuth defaults to false

	handler := s.authMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(http.MethodGet, "/admin/cluster", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated request allowed through: status=%d, want 401", w.Code)
	}

	// With AllowNoAuth explicitly set, dev-mode pass-through works (write role).
	s.config.AllowNoAuth = true
	w2 := httptest.NewRecorder()
	handler(w2, httptest.NewRequest(http.MethodGet, "/admin/cluster", nil))
	if w2.Code != http.StatusOK {
		t.Fatalf("AllowNoAuth pass-through: status=%d, want 200", w2.Code)
	}
}

// C10: /command applies arbitrary FSM writes and must require authentication
// even when tokens are configured (previously it had no auth at all).
func TestCommandEndpointRequiresAuth(t *testing.T) {
	s := bareServer("tok") // token configured => auth enforced

	mux := s.buildMux()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/command", strings.NewReader("{}")))

	if w.Code != http.StatusUnauthorized && w.Code != http.StatusForbidden {
		t.Fatalf("/command without a token: status=%d, want 401 or 403", w.Code)
	}
}

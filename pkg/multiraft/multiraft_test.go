package multiraft

import (
	"testing"
)

// twoGroupCfgs returns two non-overlapping GroupConfigs covering the full keyspace:
//   group 1: ["", "m")
//   group 2: ["m", "")
func twoGroupCfgs() []GroupConfig {
	return []GroupConfig{
		{ID: 1, KeyRangeStart: "", KeyRangeEnd: "m"},
		{ID: 2, KeyRangeStart: "m", KeyRangeEnd: ""},
	}
}

// ── RangeRouter unit tests ────────────────────────────────────────────────────

func TestNewRangeRouter_TwoGroups(t *testing.T) {
	router, err := NewRangeRouter(twoGroupCfgs())
	if err != nil {
		t.Fatalf("NewRangeRouter: %v", err)
	}
	if router == nil {
		t.Fatal("expected non-nil router")
	}
}

func TestNewRangeRouter_OverlapError(t *testing.T) {
	cfgs := []GroupConfig{
		{ID: 1, KeyRangeStart: "a", KeyRangeEnd: "z"},
		{ID: 2, KeyRangeStart: "m", KeyRangeEnd: ""},
	}
	_, err := NewRangeRouter(cfgs)
	if err == nil {
		t.Fatal("expected overlap error, got nil")
	}
}

func TestNewRangeRouter_SingleGroupAllKeys(t *testing.T) {
	cfgs := []GroupConfig{
		{ID: 1, KeyRangeStart: "", KeyRangeEnd: ""},
	}
	router, err := NewRangeRouter(cfgs)
	if err != nil {
		t.Fatalf("NewRangeRouter: %v", err)
	}
	for _, key := range []string{"", "a", "apple", "z", "zzz"} {
		id, ok := router.GroupIDFor(key)
		if !ok {
			t.Errorf("GroupIDFor(%q): expected ok=true", key)
		}
		if id != 1 {
			t.Errorf("GroupIDFor(%q): got group %d, want 1", key, id)
		}
	}
}

func TestGroupIDFor_BasicRouting(t *testing.T) {
	router, err := NewRangeRouter(twoGroupCfgs())
	if err != nil {
		t.Fatalf("NewRangeRouter: %v", err)
	}

	cases := []struct {
		key     string
		wantID  GroupID
		wantOK  bool
	}{
		{"apple", 1, true},   // "apple" < "m" → group 1
		{"mango", 2, true},   // "mango" >= "m" → group 2
		{"m", 2, true},       // exactly at boundary (start is inclusive)
		{"l", 1, true},       // just before boundary
		{"zzz", 2, true},     // well past "m"
		{"", 1, true},        // empty key → first group (start="")
		{"banana", 1, true},  // "banana" < "m"
		{"noodle", 2, true},  // "noodle" > "m"
	}

	for _, tc := range cases {
		id, ok := router.GroupIDFor(tc.key)
		if ok != tc.wantOK {
			t.Errorf("GroupIDFor(%q): ok=%v, want %v", tc.key, ok, tc.wantOK)
			continue
		}
		if ok && id != tc.wantID {
			t.Errorf("GroupIDFor(%q): id=%d, want %d", tc.key, id, tc.wantID)
		}
	}
}

func TestGroupIDFor_KeyOutsideAllRanges(t *testing.T) {
	// Partial coverage: only ["c", "f") is configured.
	cfgs := []GroupConfig{
		{ID: 1, KeyRangeStart: "c", KeyRangeEnd: "f"},
	}
	router, err := NewRangeRouter(cfgs)
	if err != nil {
		t.Fatalf("NewRangeRouter: %v", err)
	}

	// Keys before range.
	if _, ok := router.GroupIDFor("a"); ok {
		t.Error("GroupIDFor(\"a\"): expected ok=false (before range)")
	}
	if _, ok := router.GroupIDFor(""); ok {
		t.Error("GroupIDFor(\"\"): expected ok=false (before range)")
	}

	// Keys after range.
	if _, ok := router.GroupIDFor("g"); ok {
		t.Error("GroupIDFor(\"g\"): expected ok=false (after range)")
	}
	if _, ok := router.GroupIDFor("z"); ok {
		t.Error("GroupIDFor(\"z\"): expected ok=false (after range)")
	}

	// Key exactly at end boundary (exclusive) → not in range.
	if _, ok := router.GroupIDFor("f"); ok {
		t.Error("GroupIDFor(\"f\"): expected ok=false (end is exclusive)")
	}

	// Keys inside range.
	for _, key := range []string{"c", "d", "e", "ca", "cb"} {
		if _, ok := router.GroupIDFor(key); !ok {
			t.Errorf("GroupIDFor(%q): expected ok=true (inside range)", key)
		}
	}
}

func TestGroupIDFor_ThreeGroups(t *testing.T) {
	cfgs := []GroupConfig{
		{ID: 1, KeyRangeStart: "", KeyRangeEnd: "g"},
		{ID: 2, KeyRangeStart: "g", KeyRangeEnd: "n"},
		{ID: 3, KeyRangeStart: "n", KeyRangeEnd: ""},
	}
	router, err := NewRangeRouter(cfgs)
	if err != nil {
		t.Fatalf("NewRangeRouter: %v", err)
	}

	cases := []struct {
		key    string
		wantID GroupID
	}{
		{"apple", 1},
		{"g", 2},     // start of group 2 (inclusive)
		{"hello", 2},
		{"n", 3},     // start of group 3 (inclusive)
		{"nope", 3},
		{"zzz", 3},
		{"", 1},
	}
	for _, tc := range cases {
		id, ok := router.GroupIDFor(tc.key)
		if !ok {
			t.Errorf("GroupIDFor(%q): expected ok=true", tc.key)
			continue
		}
		if id != tc.wantID {
			t.Errorf("GroupIDFor(%q): got group %d, want %d", tc.key, id, tc.wantID)
		}
	}
}

func TestGroupsForPrefix_SingleGroup(t *testing.T) {
	router, err := NewRangeRouter(twoGroupCfgs())
	if err != nil {
		t.Fatalf("NewRangeRouter: %v", err)
	}

	// Prefix "app" is entirely within group 1's range (< "m").
	ids := router.GroupsForPrefix("app")
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("GroupsForPrefix(\"app\"): got %v, want [1]", ids)
	}

	// Prefix "na" is entirely within group 2's range (>= "m").
	ids = router.GroupsForPrefix("na")
	if len(ids) != 1 || ids[0] != 2 {
		t.Errorf("GroupsForPrefix(\"na\"): got %v, want [2]", ids)
	}
}

func TestGroupsForPrefix_SpansBoundary(t *testing.T) {
	router, err := NewRangeRouter(twoGroupCfgs())
	if err != nil {
		t.Fatalf("NewRangeRouter: %v", err)
	}

	// Empty prefix "" → all keys → all groups.
	ids := router.GroupsForPrefix("")
	if len(ids) != 2 {
		t.Errorf("GroupsForPrefix(\"\"): got %v, want 2 groups", ids)
	}
}

func TestNewRangeRouter_Empty(t *testing.T) {
	router, err := NewRangeRouter(nil)
	if err != nil {
		t.Fatalf("NewRangeRouter(nil): %v", err)
	}
	_, ok := router.GroupIDFor("any")
	if ok {
		t.Error("GroupIDFor on empty router should return ok=false")
	}
	ids := router.GroupsForPrefix("any")
	if len(ids) != 0 {
		t.Errorf("GroupsForPrefix on empty router: got %v, want []", ids)
	}
}

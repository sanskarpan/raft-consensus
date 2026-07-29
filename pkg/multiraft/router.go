package multiraft

import (
	"fmt"
	"sort"
)

// rangeEntry maps a key half-open interval [start, end) to a group.
// An empty end means +infinity (covers all keys from start onward).
type rangeEntry struct {
	start   string  // inclusive; "" = beginning of keyspace
	end     string  // exclusive; "" = +infinity (beyond all keys)
	groupID GroupID
}

// RangeRouter maps keys to Raft groups via a sorted slice of rangeEntry values.
// Sort.Search gives O(log n) lookup; the number of groups is expected to be
// small (typically <100) so this is faster to allocate and more GC-friendly
// than a B-tree.
type RangeRouter struct {
	entries []rangeEntry // sorted ascending by start
}

// NewRangeRouter builds a RangeRouter from a slice of GroupConfig values.
// It validates that no two ranges overlap. Gaps (regions not covered by any
// group) are allowed — GroupIDFor simply returns false for uncovered keys.
func NewRangeRouter(cfgs []GroupConfig) (*RangeRouter, error) {
	entries := make([]rangeEntry, 0, len(cfgs))
	for _, cfg := range cfgs {
		entries = append(entries, rangeEntry{
			start:   cfg.KeyRangeStart,
			end:     cfg.KeyRangeEnd,
			groupID: cfg.ID,
		})
	}

	// Sort by start so binary search and overlap detection both work correctly.
	sort.Slice(entries, func(i, j int) bool {
		// Empty start sorts before all non-empty starts.
		if entries[i].start == entries[j].start {
			return false
		}
		if entries[i].start == "" {
			return true
		}
		if entries[j].start == "" {
			return false
		}
		return entries[i].start < entries[j].start
	})

	// Validate: no two entries may overlap.
	// Entry A overlaps entry B (B.start > A.start after sort) when
	//   A.end == "" (infinite)  OR  B.start < A.end.
	for i := 1; i < len(entries); i++ {
		prev := entries[i-1]
		curr := entries[i]
		if prev.end == "" {
			return nil, fmt.Errorf(
				"multiraft: group %d range [%q, +∞) overlaps group %d range [%q, %q)",
				prev.groupID, prev.start, curr.groupID, curr.start, curr.end)
		}
		if curr.start < prev.end {
			return nil, fmt.Errorf(
				"multiraft: group %d range [%q, %q) overlaps group %d range [%q, %q)",
				prev.groupID, prev.start, prev.end, curr.groupID, curr.start, curr.end)
		}
	}

	return &RangeRouter{entries: entries}, nil
}

// GroupIDFor returns the GroupID whose range contains key.
// ok is false when no configured group covers the key.
//
// Binary-search to find the last entry whose start <= key, then verify the
// key falls within the entry's upper bound.
func (r *RangeRouter) GroupIDFor(key string) (GroupID, bool) {
	if len(r.entries) == 0 {
		return 0, false
	}

	// Find the rightmost entry whose start <= key.
	// sort.Search returns the smallest index i such that entries[i].start > key.
	// We want the entry just before that, i.e. index i-1.
	i := sort.Search(len(r.entries), func(idx int) bool {
		s := r.entries[idx].start
		if s == "" {
			// "" sorts before all real keys, so the condition s > key is only
			// true when key is impossible — never. Return false so that all
			// entries with start=="" are considered candidates.
			return false
		}
		return s > key
	})
	// i is now the index of the first entry with start > key (or len if none).
	// The candidate is entries[i-1], if it exists.
	if i == 0 {
		return 0, false
	}
	candidate := r.entries[i-1]

	// Check upper bound: end == "" means +infinity so any key qualifies.
	if candidate.end != "" && key >= candidate.end {
		return 0, false
	}

	return candidate.groupID, true
}

// GroupsForPrefix returns the IDs of all groups whose range overlaps the
// prefix scan range [prefix, prefix+"\xff"...], i.e. all keys that could
// start with prefix. A group overlaps the prefix if its range contains at
// least one key with that prefix.
//
// A group's range [start, end) overlaps [prefix, ∞) when
//   end == "" OR end > prefix.
// And it overlaps [prefix, ∞) only up to prefix+\xff... but since we are
// looking for groups that might contain ANY key with the prefix, we check:
//   group.start <= (prefix+last possible suffix)  AND
//   (group.end == "" OR group.end > prefix).
// In practice: group.end > prefix (any non-empty end that is lex-greater than
// prefix) ensures at least one key starting with prefix lies in [start, end).
func (r *RangeRouter) GroupsForPrefix(prefix string) []GroupID {
	var result []GroupID
	for _, e := range r.entries {
		// Does this group's range overlap [prefix, ∞)?
		//   Lower overlap check: e.start <= prefix OR e.start starts with prefix
		//     (i.e. e.start has prefix as a prefix, so some e.start key matches).
		//   Upper overlap check: e.end == "" OR e.end > prefix.
		upperOK := e.end == "" || e.end > prefix
		if !upperOK {
			continue
		}
		// e.start must not be greater than the "last" possible key with this prefix.
		// Since we don't know the last key, we check whether e.start has prefix as
		// a prefix (range overlaps from inside) OR e.start <= prefix (range starts
		// before or at the prefix boundary).
		startOK := e.start <= prefix || hasPrefix(e.start, prefix)
		if startOK {
			result = append(result, e.groupID)
		}
	}
	return result
}

// hasPrefix reports whether s begins with prefix.
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

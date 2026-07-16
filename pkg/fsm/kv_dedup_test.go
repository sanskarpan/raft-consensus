package fsm

import (
	"fmt"
	"reflect"
	"testing"
)

// C8: The dedup table is included in snapshots, so its contents must be a
// deterministic function of the applied log. Two replicas applying the same
// commands (including eviction) must end with identical dedup tables; otherwise
// their snapshots diverge and retried commands are deduplicated on one replica
// but re-applied on another.
func TestDedupTableIsDeterministicAcrossReplicas(t *testing.T) {
	old := maxDedupEntries
	maxDedupEntries = 5
	defer func() { maxDedupEntries = old }()

	apply := func() map[string]dedupEntry {
		k := NewKVStore()
		for i := 0; i < 20; i++ {
			cmd, err := EncodeCommandWithID("put", fmt.Sprintf("k%d", i), "v", fmt.Sprintf("client%d", i), 1)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.Apply(cmd); err != nil {
				t.Fatal(err)
			}
		}
		return k.dedupTable
	}

	a := apply()
	b := apply()
	if len(a) != 5 {
		t.Fatalf("dedup table size=%d, want 5 (cap enforced)", len(a))
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("dedup tables diverged between replicas:\n a=%v\n b=%v", a, b)
	}
}

// C8: A retried command with a sequence number at or below the highest already
// applied for that client must be deduplicated (not re-applied), even if a
// newer command from the same client has since been applied.
func TestDedupRejectsReorderedRetry(t *testing.T) {
	k := NewKVStore()

	cmd1, _ := EncodeCommandWithID("put", "a", "1", "client", 1)
	cmd2, _ := EncodeCommandWithID("put", "a", "2", "client", 2)
	if _, err := k.Apply(cmd1); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Apply(cmd2); err != nil {
		t.Fatal(err)
	}
	revAfter2 := k.revision

	// A delayed retry of seq 1 must be deduplicated and must NOT mutate state.
	if _, err := k.Apply(cmd1); err != nil {
		t.Fatal(err)
	}
	if k.revision != revAfter2 {
		t.Fatalf("reordered retry of seq 1 re-applied: revision %d -> %d", revAfter2, k.revision)
	}
}

package fsm

import (
	"fmt"
	"testing"
)

func seedKeys(t *testing.T, kv *KVStore, prefix string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		cmd, _ := EncodeCommand("put", fmt.Sprintf("%s%03d", prefix, i), "v")
		if _, err := kv.Apply(cmd); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
}

// TestRangePagePagesInOrderWithoutGapsOrDupes walks every page and checks the
// union is exactly the seeded set, in sorted order, with no gaps or overlaps.
func TestRangePagePagesInOrderWithoutGapsOrDupes(t *testing.T) {
	kv := NewKVStore()
	const total = 25
	seedKeys(t, kv, "p/", total)
	// A decoy under a different prefix must never appear.
	dc, _ := EncodeCommand("put", "other/x", "v")
	kv.Apply(dc)

	var got []string
	cursor := ""
	pages := 0
	for {
		page, more, err := kv.RangePage("p/", cursor, 10)
		if err != nil {
			t.Fatalf("RangePage: %v", err)
		}
		pages++
		for _, kvp := range page {
			got = append(got, kvp.Key)
		}
		if !more {
			break
		}
		if len(page) == 0 {
			t.Fatal("more=true but empty page (would loop forever)")
		}
		cursor = page[len(page)-1].Key
		if pages > total+2 {
			t.Fatal("too many pages; pagination not terminating")
		}
	}

	if len(got) != total {
		t.Fatalf("collected %d keys across pages, want %d", len(got), total)
	}
	for i := 0; i < total; i++ {
		want := fmt.Sprintf("p/%03d", i)
		if got[i] != want {
			t.Fatalf("key[%d] = %q, want %q (order/gap/dupe)", i, got[i], want)
		}
	}
	if pages != 3 { // 10 + 10 + 5
		t.Fatalf("pages = %d, want 3 for 25 keys @ limit 10", pages)
	}
}

func TestRangePageMoreFlag(t *testing.T) {
	kv := NewKVStore()
	seedKeys(t, kv, "k/", 5)

	page, more, _ := kv.RangePage("k/", "", 3)
	if len(page) != 3 || !more {
		t.Fatalf("first page len=%d more=%v, want 3/true", len(page), more)
	}
	page2, more2, _ := kv.RangePage("k/", page[2].Key, 3)
	if len(page2) != 2 || more2 {
		t.Fatalf("second page len=%d more=%v, want 2/false", len(page2), more2)
	}
}

func TestRangePageCursorIsExclusive(t *testing.T) {
	kv := NewKVStore()
	seedKeys(t, kv, "c/", 4) // c/000..c/003
	page, _, _ := kv.RangePage("c/", "c/001", 10)
	// Only keys strictly greater than c/001.
	if len(page) != 2 || page[0].Key != "c/002" || page[1].Key != "c/003" {
		t.Fatalf("page = %v, want [c/002 c/003]", keyList(page))
	}
}

func keyList(kvs []*KeyValue) []string {
	out := make([]string, len(kvs))
	for i, k := range kvs {
		out[i] = k.Key
	}
	return out
}

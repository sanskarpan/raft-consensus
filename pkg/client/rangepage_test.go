package client

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// TestRangePageReadsCursorHeaders verifies the client parses X-Next-Cursor /
// X-Has-More and passes start_after/limit correctly across pages, driving a
// mock server that returns 10 sorted keys in pages of 4.
func TestRangePageReadsCursorHeaders(t *testing.T) {
	// Server holds 10 keys k000..k009 and paginates by start_after/limit.
	all := make([]string, 10)
	for i := range all {
		all[i] = fmt.Sprintf("k%03d", i)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		limit, _ := strconv.Atoi(q.Get("limit"))
		startAfter := q.Get("start_after")
		var page []string
		for _, k := range all {
			if k > startAfter {
				page = append(page, k)
			}
		}
		more := false
		if len(page) > limit {
			page = page[:limit]
			more = true
		}
		w.Header().Set("X-Has-More", strconv.FormatBool(more))
		if len(page) > 0 {
			w.Header().Set("X-Next-Cursor", page[len(page)-1])
		}
		w.Header().Set("Content-Type", "application/json")
		var sb strings.Builder
		sb.WriteString("[")
		for i, k := range page {
			if i > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, `{"key":%q,"value":"v","version":1}`, k)
		}
		sb.WriteString("]")
		w.Write([]byte(sb.String()))
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	c := NewClient(WithAddresses([]string{addr}))

	var got []string
	cursor := ""
	pages := 0
	for {
		page, next, more, err := c.RangePage("k", cursor, 4)
		if err != nil {
			t.Fatalf("RangePage: %v", err)
		}
		pages++
		for _, kv := range page {
			got = append(got, kv.Key)
		}
		if !more {
			break
		}
		cursor = next
		if pages > 5 {
			t.Fatal("pagination not terminating")
		}
	}

	if len(got) != 10 {
		t.Fatalf("collected %d keys, want 10", len(got))
	}
	for i, k := range got {
		if k != all[i] {
			t.Fatalf("key[%d]=%q want %q", i, k, all[i])
		}
	}
	if pages != 3 { // 4 + 4 + 2
		t.Fatalf("pages=%d, want 3", pages)
	}
}

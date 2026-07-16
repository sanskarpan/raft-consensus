package storage

import (
	"fmt"
	"testing"

	"github.com/raft-consensus/pkg/raft"
)

// M-P5: encodeRecord must allocate a single buffer (the result), so ~1 alloc/op.
func BenchmarkEncodeRecord(b *testing.B) {
	rec := &logRecord{
		term:     7,
		index:    42,
		data:     []byte("hello world, a modestly sized payload"),
		recordTy: 1,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := encodeRecord(rec); err != nil {
			b.Fatal(err)
		}
	}
}

// TestEncodeRecordSingleAlloc asserts M-P5's allocation goal directly so it is
// enforced in the normal test run, not just observable in a benchmark.
func TestEncodeRecordSingleAlloc(t *testing.T) {
	rec := &logRecord{term: 1, index: 1, data: []byte("payload"), recordTy: 1}
	allocs := testing.AllocsPerRun(100, func() {
		if _, err := encodeRecord(rec); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 1 {
		t.Fatalf("encodeRecord allocates %.1f/op, want ~1", allocs)
	}
}

func benchWAL(b *testing.B, n uint64) *WAL {
	b.Helper()
	dir := b.TempDir()
	w, err := NewWAL(dir, nil)
	if err != nil {
		b.Fatal(err)
	}
	for i := uint64(1); i <= n; i++ {
		data := []byte(fmt.Sprintf("entry-payload-%d", i))
		if err := w.Append([]*raft.LogEntry{{Index: i, Term: 1, Data: data}}); err != nil {
			b.Fatal(err)
		}
	}
	return w
}

// H-P2: BenchmarkWALGet measures the read hot path (positional ReadAt on a
// shared fd, no per-read open/seek/close). Sequential variant.
func BenchmarkWALGet(b *testing.B) {
	const n = 1000
	w := benchWAL(b, n)
	defer w.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := uint64((i % n) + 1)
		if _, err := w.Get(idx); err != nil {
			b.Fatal(err)
		}
	}
}

// H-P2: parallel variant — exercises concurrent readers on the shared fd.
func BenchmarkWALGetParallel(b *testing.B) {
	const n = 1000
	w := benchWAL(b, n)
	defer w.Close()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			i++
			idx := (i % n) + 1
			if _, err := w.Get(idx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

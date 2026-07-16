package fsm

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// ---------------------------------------------------------------------------
// M-P3: reduced Apply write-lock critical section
// ---------------------------------------------------------------------------
//
// The result json.Marshal and dedup recording were moved out of the primary
// Apply write-lock so concurrent readers (Get/Range) block for less time. The
// tests below assert that:
//   1. correctness/determinism is unchanged (byte-identical wire output and a
//      deterministic dedup table) — see also the C8/H10/H11/M11 tests; and
//   2. Get is race-free while a writer runs Apply concurrently (run under
//      `go test -race ./pkg/fsm/`).

// TestApplyResultBytesUnchanged pins the exact wire bytes produced by each op so
// the M-P3 refactor (deferred marshaling) cannot silently alter the format that
// snapshots, dedup entries, and cross-replica determinism depend on.
func TestApplyResultBytesUnchanged(t *testing.T) {
	kv := NewKVStore()

	// put: KvResult whose Value is a nested KeyValue JSON string.
	putCmd, _ := EncodeCommand("put", "k", "v")
	putRes, err := kv.Apply(putCmd)
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct the expected bytes exactly as the pre-refactor code would.
	wantKV := &KeyValue{Key: "k", Value: "v", CreateRevision: 1, ModRevision: 1, Version: 1}
	kvJSON, _ := json.Marshal(wantKV)
	wantPut, _ := json.Marshal(KvResult{Value: string(kvJSON)})
	if string(putRes) != string(wantPut) {
		t.Fatalf("put wire bytes changed:\n got %s\nwant %s", putRes, wantPut)
	}

	// get_v2: same nested-KeyValue shape.
	getCmd, _ := EncodeCommand("get_v2", "k", "")
	getRes, _ := kv.Apply(getCmd)
	if string(getRes) != string(wantPut) {
		t.Fatalf("get_v2 wire bytes changed:\n got %s\nwant %s", getRes, wantPut)
	}

	// range: KvResult whose Value is a nested []*KeyValue JSON string.
	rangeCmd, _ := EncodeCommand("range", "k", "")
	rangeRes, _ := kv.Apply(rangeCmd)
	rangeJSON, _ := json.Marshal([]*KeyValue{wantKV})
	wantRange, _ := json.Marshal(KvResult{Value: string(rangeJSON)})
	if string(rangeRes) != string(wantRange) {
		t.Fatalf("range wire bytes changed:\n got %s\nwant %s", rangeRes, wantRange)
	}

	// range with no matches must still serialize as null (nil slice), unchanged.
	emptyCmd, _ := EncodeCommand("range", "zzz", "")
	emptyRes, _ := kv.Apply(emptyCmd)
	wantEmpty, _ := json.Marshal(KvResult{Value: "null"})
	if string(emptyRes) != string(wantEmpty) {
		t.Fatalf("empty range wire bytes changed:\n got %s\nwant %s", emptyRes, wantEmpty)
	}

	// txn: TxnResponse marshaled directly.
	txnRes, _ := kv.Apply(mustTxn(t, &TxnRequest{
		Success: []TxnOp{{Type: 0, Key: "t", Value: "1"}},
	}))
	var resp TxnResponse
	if err := json.Unmarshal(txnRes, &resp); err != nil {
		t.Fatalf("txn result not a TxnResponse: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("txn should have succeeded")
	}
}

// TestGetRaceFreeUnderApply drives concurrent Get/Range readers against a stream
// of Apply mutations and asserts (under -race) that the reduced critical section
// introduced no data race and that every observed value is internally consistent.
func TestGetRaceFreeUnderApply(t *testing.T) {
	kv := NewKVStore()

	const keys = 32
	// Seed keys so readers always find something.
	for i := 0; i < keys; i++ {
		cmd, _ := EncodeCommand("put", fmt.Sprintf("k%02d", i), "seed")
		if _, err := kv.Apply(cmd); err != nil {
			t.Fatal(err)
		}
	}

	var stop atomic.Bool
	var writerWG, readerWG sync.WaitGroup

	// One writer goroutine driving Apply. (Apply is serial in production; a
	// single writer preserves that invariant while exercising reader contention.)
	// It runs until the readers signal stop, so it lives in its own WaitGroup —
	// otherwise waiting on the readers would also wait on this never-ending loop.
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		seq := 0
		for !stop.Load() {
			cmd, _ := EncodeCommand("put", fmt.Sprintf("k%02d", seq%keys), fmt.Sprintf("v%d", seq))
			if _, err := kv.Apply(cmd); err != nil {
				t.Errorf("apply: %v", err)
				return
			}
			seq++
		}
	}()

	// Several concurrent readers, each with a bounded iteration count.
	for r := 0; r < 4; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for i := 0; i < 5000; i++ {
				k := fmt.Sprintf("k%02d", i%keys)
				got, err := kv.Get(k)
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if got != nil && got.Key != k {
					t.Errorf("Get returned mismatched key: want %s got %s", k, got.Key)
					return
				}
				if _, err := kv.Range("k"); err != nil {
					t.Errorf("range: %v", err)
					return
				}
			}
		}()
	}

	// Let readers run to completion, then stop the writer and join it.
	readerWG.Wait()
	stop.Store(true)
	writerWG.Wait()
}

func mustTxn(t *testing.T, req *TxnRequest) []byte {
	t.Helper()
	data, err := EncodeTxn(req)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// BenchmarkKVGetUnderApply measures Get latency for concurrent readers while a
// single background writer runs Apply continuously. It is the primary M-P3
// benchmark: with result marshaling moved out of the Apply write lock the
// parallel Get path should contend less with the writer.
func BenchmarkKVGetUnderApply(b *testing.B) {
	kv := NewKVStore()
	const keys = 256
	for i := 0; i < keys; i++ {
		cmd, _ := EncodeCommand("put", fmt.Sprintf("k%03d", i), "seed-value-of-moderate-length")
		if _, err := kv.Apply(cmd); err != nil {
			b.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		seq := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			cmd, _ := EncodeCommand("put", fmt.Sprintf("k%03d", seq%keys), fmt.Sprintf("value-%d", seq))
			_, _ = kv.Apply(cmd)
			seq++
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = kv.Get(fmt.Sprintf("k%03d", i%keys))
			i++
		}
	})
	b.StopTimer()

	close(stop)
	writerWG.Wait()
}

// BenchmarkFSMApplyPut documents the baseline allocation/latency of the Apply
// path for a single put (reflection-based json.Unmarshal + json.Marshal, the
// largest allocator — M-P4). Run with -benchmem to see allocs/op.
func BenchmarkFSMApplyPut(b *testing.B) {
	kv := NewKVStore()
	cmds := make([][]byte, 64)
	for i := range cmds {
		cmds[i], _ = EncodeCommand("put", fmt.Sprintf("k%02d", i), "a-value-of-representative-length")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kv.Apply(cmds[i%len(cmds)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFSMApplyTxn documents the baseline for a small transaction (two puts).
func BenchmarkFSMApplyTxn(b *testing.B) {
	kv := NewKVStore()
	cmds := make([][]byte, 64)
	for i := range cmds {
		cmds[i], _ = EncodeTxn(&TxnRequest{
			Success: []TxnOp{
				{Type: 0, Key: fmt.Sprintf("a%02d", i), Value: "v1"},
				{Type: 0, Key: fmt.Sprintf("b%02d", i), Value: "v2"},
			},
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kv.Apply(cmds[i%len(cmds)]); err != nil {
			b.Fatal(err)
		}
	}
}

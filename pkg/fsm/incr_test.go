package fsm

import (
	"strconv"
	"testing"
)

func applyIncr(t *testing.T, kv *KVStore, key string, delta int64) (*KeyValue, string) {
	t.Helper()
	cmd, err := EncodeCommand("incr", key, strconv.FormatInt(delta, 10))
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}
	result, err := kv.Apply(cmd)
	if err != nil {
		t.Fatalf("Apply incr: %v", err)
	}
	res, _ := DecodeResult(result)
	if res.Error != "" {
		return nil, res.Error
	}
	got, err := DecodeKeyValueResult(result)
	if err != nil {
		t.Fatalf("DecodeKeyValueResult: %v", err)
	}
	return got, ""
}

func TestIncrOnMissingKeyStartsAtZero(t *testing.T) {
	kv := NewKVStore()
	got, errStr := applyIncr(t, kv, "counter", 5)
	if errStr != "" {
		t.Fatalf("incr error: %s", errStr)
	}
	if got.Value != "5" {
		t.Fatalf("value = %q, want 5", got.Value)
	}
}

func TestIncrAccumulatesAndDecrements(t *testing.T) {
	kv := NewKVStore()
	applyIncr(t, kv, "c", 10)
	applyIncr(t, kv, "c", 7)
	got, _ := applyIncr(t, kv, "c", -3)
	if got.Value != "14" { // 10 + 7 - 3
		t.Fatalf("value = %q, want 14", got.Value)
	}
	if got.Version != 3 {
		t.Fatalf("version = %d, want 3", got.Version)
	}
}

func TestIncrOnNonIntegerValueErrors(t *testing.T) {
	kv := NewKVStore()
	setCmd, _ := EncodeCommand("put", "k", "not-a-number")
	kv.Apply(setCmd)

	_, errStr := applyIncr(t, kv, "k", 1)
	if errStr == "" {
		t.Fatal("expected an error incrementing a non-integer value")
	}
}

func TestIncrRejectsNonIntegerDelta(t *testing.T) {
	kv := NewKVStore()
	cmd, _ := EncodeCommand("incr", "k", "3.5")
	result, err := kv.Apply(cmd)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	res, _ := DecodeResult(result)
	if res.Error == "" {
		t.Fatal("expected an error for a non-integer delta")
	}
}

func TestIncrOverflowRejected(t *testing.T) {
	kv := NewKVStore()
	applyIncr(t, kv, "c", 9000000000000000000) // near MaxInt64
	_, errStr := applyIncr(t, kv, "c", 1000000000000000000)
	if errStr == "" {
		t.Fatal("expected an int64-overflow error")
	}
}

// TestIncrIsDeterministic verifies two independent stores replaying the same
// sequence reach byte-identical results (replica consistency).
func TestIncrIsDeterministic(t *testing.T) {
	a, b := NewKVStore(), NewKVStore()
	for i := int64(1); i <= 50; i++ {
		ca, _ := EncodeCommand("incr", "c", strconv.FormatInt(i, 10))
		a.Apply(ca)
		cb, _ := EncodeCommand("incr", "c", strconv.FormatInt(i, 10))
		b.Apply(cb)
	}
	va, _ := a.Get("c")
	vb, _ := b.Get("c")
	if va.Value != vb.Value || va.Value != "1275" { // sum 1..50
		t.Fatalf("nondeterministic: a=%q b=%q want 1275", va.Value, vb.Value)
	}
}

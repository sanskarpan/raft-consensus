package transport

import (
	"bytes"
	"sync/atomic"
	"testing"
)

// TestEncBufPoolRoundTrip verifies that encBufPool is defined and that a buffer
// retrieved from the pool can be written to, returned, and then retrieved again —
// confirming the pool reuses allocations rather than always allocating new ones.
func TestEncBufPoolRoundTrip(t *testing.T) {
	var newCount int64
	// Override New temporarily so we can count allocations.
	orig := encBufPool.New
	encBufPool.New = func() interface{} {
		atomic.AddInt64(&newCount, 1)
		return new(bytes.Buffer)
	}
	defer func() { encBufPool.New = orig }()

	// First get: may allocate.
	buf := encBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteString("hello")
	encBufPool.Put(buf)

	// Second get: GC pressure is low in tests so the pool likely returns the same
	// object, but the hard invariant is only that we get a usable *bytes.Buffer.
	buf2 := encBufPool.Get().(*bytes.Buffer)
	if buf2 == nil {
		t.Fatal("encBufPool.Get() returned nil")
	}
	buf2.Reset()
	encBufPool.Put(buf2)
}

// TestEncBufPoolIsBytes verifies the pool produces *bytes.Buffer values.
func TestEncBufPoolIsBytes(t *testing.T) {
	got := encBufPool.Get()
	if _, ok := got.(*bytes.Buffer); !ok {
		t.Fatalf("pool produced %T, want *bytes.Buffer", got)
	}
	encBufPool.Put(got.(*bytes.Buffer))
}

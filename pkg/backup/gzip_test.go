package backup_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/backup"
)

func TestCompressRoundTrip(t *testing.T) {
	original := strings.Repeat("raft-snapshot-data-line\n", 500)

	// Compress
	compressed := backup.CompressReader(strings.NewReader(original))
	defer compressed.Close()

	compressedData, err := io.ReadAll(compressed)
	if err != nil {
		t.Fatalf("reading compressed: %v", err)
	}

	// Compressed should be smaller than original for repetitive input
	if len(compressedData) >= len(original) {
		t.Errorf("expected compression: compressed=%d original=%d", len(compressedData), len(original))
	}

	// Decompress and verify
	gr, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gr.Close()

	got, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}

	if string(got) != original {
		t.Errorf("roundtrip mismatch: got %d bytes, want %d bytes", len(got), len(original))
	}
}

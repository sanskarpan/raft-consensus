package backup_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/backup"
)

func TestManifestJSON(t *testing.T) {
	m := backup.Manifest{
		Version:      1,
		Name:         "snap-001.gz",
		SHA256:       "abc123",
		SizeBytes:    4096,
		Compressed:   true,
		SnapshotIdx:  42,
		SnapshotTerm: 3,
		NodeID:       "node-1",
		CreatedAt:    1700000000000,
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got backup.Manifest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != m {
		t.Errorf("roundtrip mismatch: got %+v, want %+v", got, m)
	}
}

func TestComputeSHA256(t *testing.T) {
	// echo -n "hello" | sha256sum => 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
	input := "hello"
	digest, n, err := backup.ComputeSHA256(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ComputeSHA256: %v", err)
	}
	want := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if digest != want {
		t.Errorf("digest: got %s, want %s", digest, want)
	}
	if n != int64(len(input)) {
		t.Errorf("bytes: got %d, want %d", n, len(input))
	}
	// Empty reader
	digest2, n2, err := backup.ComputeSHA256(bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("ComputeSHA256 empty: %v", err)
	}
	if n2 != 0 {
		t.Errorf("empty bytes: got %d, want 0", n2)
	}
	if len(digest2) != 64 {
		t.Errorf("empty digest length: got %d, want 64", len(digest2))
	}
}

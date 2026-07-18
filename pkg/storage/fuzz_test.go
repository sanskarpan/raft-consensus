package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// FuzzReadRecordAtBounded ensures WAL record parsing never panics on arbitrary
// bytes/offsets (bounds-checked length, truncated headers, etc.).
func FuzzReadRecordAtBounded(f *testing.F) {
	f.Add([]byte("short"), int64(0))
	f.Add([]byte{}, int64(0))
	f.Add(make([]byte, 64), int64(-1))
	f.Add(make([]byte, 64), int64(1<<40))

	f.Fuzz(func(t *testing.T, data []byte, offset int64) {
		r := bytes.NewReader(data)
		_, _ = readRecordAtBounded(r, offset, int64(len(data))) // must not panic
	})
}

// FuzzVerifySnapChecksum ensures snapshot footer verification never panics on
// arbitrary file contents.
func FuzzVerifySnapChecksum(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("SNAP\x00\x00\x00\x00"))
	f.Add(make([]byte, 12))

	f.Fuzz(func(t *testing.T, data []byte) {
		tmp, err := os.CreateTemp(t.TempDir(), "snap")
		if err != nil {
			t.Skip()
		}
		defer tmp.Close()
		_, _ = tmp.Write(data)
		_, _ = verifysnapChecksum(tmp, true) // must not panic
	})
}

// FuzzWALRecovery ensures opening a WAL over a corrupt segment file never panics
// (it must return an error or recover a valid prefix, never crash).
func FuzzWALRecovery(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 25))
	f.Add([]byte("garbage segment data that is not a valid record"))

	f.Fuzz(func(t *testing.T, seg []byte) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "00000000000000000001.wal"), seg, 0o600); err != nil {
			t.Skip()
		}
		w, err := NewWAL(dir, nil) // must not panic
		if err == nil {
			_ = w.Close()
		}
	})
}

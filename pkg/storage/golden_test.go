package storage

import (
	"encoding/hex"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var updateGolden = flag.Bool("update", false, "update .golden files")

func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	h := hex.EncodeToString(got)
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(h+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (generate with: go test ./pkg/storage -run %s -update): %v", path, t.Name(), err)
	}
	if strings.TrimSpace(string(want)) != h {
		t.Fatalf("%s: WAL record byte format changed!\n got:  %s\n want: %s\nIf intentional, run: go test ./pkg/storage -run %s -update", name, h, strings.TrimSpace(string(want)), t.Name())
	}
}

// TestGoldenWALRecordEncoding locks the on-disk WAL record byte layout.
func TestGoldenWALRecordEncoding(t *testing.T) {
	rec, err := encodeRecord(&logRecord{term: 7, index: 42, recordTy: 0, data: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	checkGolden(t, "wal_record", rec)

	empty, err := encodeRecord(&logRecord{term: 1, index: 1, recordTy: 1, data: nil})
	if err != nil {
		t.Fatal(err)
	}
	checkGolden(t, "wal_record_empty", empty)
}

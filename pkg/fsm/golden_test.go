package fsm

import (
	"encoding/hex"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var updateGolden = flag.Bool("update", false, "update .golden files")

// checkGolden compares got against testdata/<name>.golden (hex-encoded). On
// -update it (re)writes the golden file. A mismatch means the on-disk byte
// format changed — a breaking change unless intentional.
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
		t.Fatalf("read golden %s (generate with: go test ./pkg/fsm -run %s -update): %v", path, t.Name(), err)
	}
	if strings.TrimSpace(string(want)) != h {
		t.Fatalf("%s: command byte format changed!\n got:  %s\n want: %s\nIf intentional, run: go test ./pkg/fsm -run %s -update", name, h, strings.TrimSpace(string(want)), t.Name())
	}
}

// TestGoldenKVCommandEncoding locks the binary command wire format.
func TestGoldenKVCommandEncoding(t *testing.T) {
	checkGolden(t, "cmd_put", encodeKVCommand(kvCommand{Op: "put", Key: "hello", Value: "world"}))
	checkGolden(t, "cmd_incr_idem", encodeKVCommand(kvCommand{Op: "incr", Key: "ctr", Value: "-5", ClientID: "abc123", SeqNum: 42}))
	checkGolden(t, "cmd_delete", encodeKVCommand(kvCommand{Op: "delete", Key: "gone"}))
}

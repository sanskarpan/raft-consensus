package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

func writeSnapshot(t *testing.T, store *FileSnapshotStore, term, index uint64, cfg raft.Configuration) {
	t.Helper()
	sink, err := store.Create(1, index, term, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write([]byte("snapshot-data")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

// M10: with retainCount N, pruning must delete the OLDEST snapshots, never a
// newer one. Creating 10, 20, 30 with retain=2 must keep {20, 30}.
func TestSnapshotPruneKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileSnapshotStore(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	writeSnapshot(t, store, 1, 10, raft.Configuration{})
	writeSnapshot(t, store, 1, 20, raft.Configuration{})
	writeSnapshot(t, store, 1, 30, raft.Configuration{})

	list, _ := store.List()
	got := map[uint64]bool{}
	for _, m := range list {
		got[m.Index] = true
	}
	if got[10] || !got[20] || !got[30] {
		t.Fatalf("retained indices = %v, want {20,30} (oldest pruned)", got)
	}
}

// C14: a snapshot's Configuration (cluster membership) must survive a process
// restart. It is persisted in a durable sidecar and reloaded intact.
func TestSnapshotConfigurationSurvivesReload(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	cfg := raft.Configuration{Servers: []raft.Server{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}
	writeSnapshot(t, store, 2, 5, cfg)

	// Simulate a restart by opening a fresh store over the same directory.
	store2, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	list, _ := store2.List()
	if len(list) != 1 {
		t.Fatalf("want 1 snapshot after reload, got %d", len(list))
	}
	if len(list[0].Configuration.Servers) != 3 {
		t.Fatalf("configuration lost on reload: %+v", list[0].Configuration)
	}
}

// C14: Close must be atomic — no temp files may be left behind in the snapshot
// directory after a successful snapshot.
func TestSnapshotCloseLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	writeSnapshot(t, store, 1, 7, raft.Configuration{Servers: []raft.Server{{ID: "n1"}}})

	entries, err := os.ReadDir(filepath.Join(dir, snapshotDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), tmpSuffix) {
			t.Fatalf("temp file left behind after Close: %s", e.Name())
		}
	}
}

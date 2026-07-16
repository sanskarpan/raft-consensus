package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/raft-consensus/pkg/raft"
)

// L6(c): a real error from removing the snapshot sidecar (e.g. it is a
// non-empty directory instead of a file, so os.Remove fails with a non-NotExist
// error) must be surfaced by Delete, not swallowed. A missing sidecar (the
// legacy case) is still ignored. Pre-fix, os.Remove's error was discarded
// unconditionally.
func TestSnapshotDeleteSurfacesSidecarRemoveError(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileSnapshotStore(dir, 5)
	if err != nil {
		t.Fatal(err)
	}
	writeSnapshot(t, store, 1, 10, raft.Configuration{})

	list, _ := store.List()
	if len(list) != 1 {
		t.Fatalf("want 1 snapshot, got %d", len(list))
	}
	id := list[0].ID

	// Replace the sidecar FILE with a non-empty DIRECTORY of the same name.
	// os.Remove on a non-empty directory fails with a real (non-NotExist)
	// error, which the fix must surface.
	sidecar := filepath.Join(dir, snapshotDir, id+snapMetaExt)
	if err := os.Remove(sidecar); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(sidecar, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sidecar, "blocker"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete(id); err == nil {
		t.Fatal("Delete must surface a real sidecar-removal error, not swallow it")
	}
}

// L6(c) counterpart: a missing sidecar (legacy snapshot) must NOT cause Delete
// to fail — os.IsNotExist is still ignored.
func TestSnapshotDeleteIgnoresMissingSidecar(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileSnapshotStore(dir, 5)
	if err != nil {
		t.Fatal(err)
	}
	writeSnapshot(t, store, 1, 10, raft.Configuration{})

	list, _ := store.List()
	id := list[0].ID

	// Remove the sidecar entirely so deleteLocked hits os.IsNotExist.
	sidecar := filepath.Join(dir, snapshotDir, id+snapMetaExt)
	if err := os.Remove(sidecar); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete(id); err != nil {
		t.Fatalf("Delete must ignore a missing sidecar, got: %v", err)
	}
}

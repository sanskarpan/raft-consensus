package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// M15: a snapshot recorded as checksummed in its durable meta MUST be rejected
// when its CRC32 footer is missing/truncated. Before the fix, stripping the
// footer made the trailing bytes fail the magic check and verification was
// silently skipped ("legacy" path), so a corrupt snapshot opened successfully.
func TestChecksummedSnapshotRejectsMissingFooter(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}

	sink, err := store.Create(raft.SnapshotVersionMax, 9, 2, raft.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write([]byte("real-fsm-state")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	snaps, _ := store.List()
	if len(snaps) == 0 {
		t.Fatal("no snapshots")
	}
	id := snaps[0].ID
	snapPath := filepath.Join(dir, snapshotDir, id+snapshotExt)

	// Truncate off the 8-byte footer to simulate a torn write / corruption.
	fi, err := os.Stat(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(snapPath, fi.Size()-snapFooterSize); err != nil {
		t.Fatal(err)
	}

	// Reload from disk so the store reads the durable (checksummed=true) meta.
	store2, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store2.Open(id); err == nil {
		t.Fatal("Open succeeded on a checksummed snapshot with a stripped footer; want error")
	} else if !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("unexpected error %v; want a corruption error", err)
	}
}

// M15: the durable meta records checksummed=true, so it survives a store
// reload; a valid snapshot still opens after restart.
func TestChecksummedFlagPersistsAcrossReload(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	sink, _ := store.Create(raft.SnapshotVersionMax, 3, 1, raft.Configuration{})
	sink.Write([]byte("payload"))
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	snaps, _ := store.List()
	id := snaps[0].ID

	store2, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !store2.checksummed[id] {
		t.Fatal("checksummed flag not persisted/loaded from durable meta")
	}
	if _, _, err := store2.Open(id); err != nil {
		t.Fatalf("Open of intact checksummed snapshot after reload: %v", err)
	}
}

// M15: a genuinely legacy snapshot (no sidecar declaring a checksum, no footer)
// must still open without verification so we don't break backward compat.
func TestLegacySnapshotWithoutFooterStillOpens(t *testing.T) {
	dir := tempDir(t)
	snapDir := filepath.Join(dir, snapshotDir)
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		t.Fatal(err)
	}
	// Write a raw legacy .snap named "{term}-{index}.snap" with no footer and
	// no sidecar.
	legacyPath := filepath.Join(snapDir, "4-8"+snapshotExt)
	if err := os.WriteFile(legacyPath, []byte("legacy-body-no-footer"), 0644); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileSnapshotStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	snaps, _ := store.List()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 legacy snapshot, got %d", len(snaps))
	}
	if store.checksummed[snaps[0].ID] {
		t.Fatal("legacy snapshot should not be marked checksummed")
	}
	if _, _, err := store.Open(snaps[0].ID); err != nil {
		t.Fatalf("legacy snapshot should open without verification: %v", err)
	}
}

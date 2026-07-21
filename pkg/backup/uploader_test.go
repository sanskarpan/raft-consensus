package backup_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/backup"
	"go.uber.org/zap"
)

func TestNoOpUploaderUploadAndList(t *testing.T) {
	u := &backup.NoOpUploader{Logger: zap.NewNop()}
	ctx := context.Background()

	// Upload should not error.
	if err := u.Upload(ctx, "snap-001", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// List returns empty (noop).
	names, err := u.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("expected empty list from noop, got %v", names)
	}
}

func TestNoOpUploaderDownloadErrors(t *testing.T) {
	u := &backup.NoOpUploader{Logger: zap.NewNop()}
	ctx := context.Background()

	rc, err := u.Download(ctx, "snap-001")
	if err == nil {
		rc.Close()
		t.Fatal("expected error from noop Download, got nil")
	}
}

// Compile-time check: NoOpUploader implements Uploader.
var _ backup.Uploader = (*backup.NoOpUploader)(nil)

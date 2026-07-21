// Package backup provides a pluggable interface for uploading and downloading
// Raft snapshots to durable object storage. The only built-in implementation
// is NoOpUploader; real S3/GCS uploaders can be added by third-party packages
// without modifying this repo (no cloud SDKs required).
package backup

import (
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"
)

// Uploader is the interface that wraps basic snapshot transfer operations.
// Implementations must be safe for concurrent use.
type Uploader interface {
	// Upload streams r to the object store under the given name.
	Upload(ctx context.Context, name string, r io.Reader) error
	// Download retrieves the named object and returns a streaming reader.
	// The caller must close the reader.
	Download(ctx context.Context, name string) (io.ReadCloser, error)
	// List returns the names of all stored backups, newest first when the
	// implementation can determine ordering.
	List(ctx context.Context) ([]string, error)
}

// NoOpUploader satisfies Uploader without contacting any remote service.
// It logs the upload name and discards the data. Download always returns an
// error. Suitable for development and testing.
type NoOpUploader struct {
	Logger *zap.Logger
}

// Upload logs the upload and drains the reader (satisfies the Uploader contract
// so callers do not need to special-case noop).
func (u *NoOpUploader) Upload(ctx context.Context, name string, r io.Reader) error {
	if u.Logger != nil {
		u.Logger.Info("backup upload (noop)", zap.String("name", name))
	}
	_, _ = io.Copy(io.Discard, r) // drain so callers don't block on pipe reads
	return nil
}

// Download always returns an error because the noop uploader never stores data.
func (u *NoOpUploader) Download(_ context.Context, name string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("noop uploader: no stored backups (requested %q)", name)
}

// List always returns an empty slice.
func (u *NoOpUploader) List(_ context.Context) ([]string, error) {
	return nil, nil
}

//go:build integration
// +build integration

package backup_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/api/option"

	"github.com/sanskarpan/raft-consensus/pkg/backup"
)

const fakeGCSImage = "fsouza/fake-gcs-server:latest"

// startFakeGCS launches a fake-gcs-server container with no auth and returns
// the HTTP base URL (http://host:port) and a cleanup func.
func startFakeGCS(t *testing.T) (baseURL string, cleanup func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        fakeGCSImage,
		ExposedPorts: []string{"4443/tcp"},
		Cmd:          []string{"-scheme", "http", "-port", "4443"},
		WaitingFor:   wait.ForHTTP("/").WithPort("4443").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("skipping GCS integration test: failed to start fake-gcs-server container: %v", err)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "4443")
	base := fmt.Sprintf("http://%s:%s", host, port.Port())

	cleanup = func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate fake-gcs-server container: %v", err)
		}
	}
	return base, cleanup
}

// newTestGCSClient returns a *storage.Client pointed at the fake-gcs-server.
func newTestGCSClient(t *testing.T, baseURL string) *storage.Client {
	t.Helper()
	ctx := context.Background()
	client, err := storage.NewClient(ctx,
		option.WithEndpoint(baseURL+"/storage/v1/"),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{}),
	)
	if err != nil {
		t.Fatalf("storage.NewClient: %v", err)
	}
	return client
}

// newTestGCSUploader creates a GCSUploader pointed at the fake-gcs-server.
func newTestGCSUploader(t *testing.T, baseURL string, compress bool) *backup.GCSUploader {
	t.Helper()
	ctx := context.Background()
	u, err := backup.NewGCSUploader(ctx, backup.GCSConfig{
		Bucket:   "test-gcs-backups",
		Compress: compress,
		NodeID:   "test-node",
		Retry: backup.RetryConfig{
			MaxAttempts: 2,
			InitialWait: 50 * time.Millisecond,
		},
		TestEndpoint: baseURL + "/storage/v1/",
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("NewGCSUploader: %v", err)
	}
	return u
}

// TestGCSRoundTrip uploads data, downloads it, and verifies content integrity.
func TestGCSRoundTrip(t *testing.T) {
	baseURL, cleanup := startFakeGCS(t)
	defer cleanup()

	uploader := newTestGCSUploader(t, baseURL, true /*compress*/)
	ctx := context.Background()

	data := strings.Repeat("raft-gcs-snapshot-data", 1000)
	if err := uploader.Upload(ctx, "snap-gcs-001", strings.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := uploader.Download(ctx, "snap-gcs-001")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != data {
		t.Fatalf("roundtrip mismatch: got %d bytes, want %d bytes", len(got), len(data))
	}
}

// TestGCSRoundTripUncompressed exercises the uncompressed upload/download path.
func TestGCSRoundTripUncompressed(t *testing.T) {
	baseURL, cleanup := startFakeGCS(t)
	defer cleanup()

	uploader := newTestGCSUploader(t, baseURL, false /*compress*/)
	ctx := context.Background()

	data := "plain-gcs-snapshot-data"
	if err := uploader.Upload(ctx, "snap-gcs-plain", strings.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := uploader.Download(ctx, "snap-gcs-plain")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != data {
		t.Fatalf("roundtrip mismatch: got %q, want %q", string(got), data)
	}
}

// TestGCSList uploads several backups and verifies List returns them all,
// newest-first.
func TestGCSList(t *testing.T) {
	baseURL, cleanup := startFakeGCS(t)
	defer cleanup()

	uploader := newTestGCSUploader(t, baseURL, true)
	ctx := context.Background()

	names := []string{"snap-a", "snap-b", "snap-c"}
	for _, name := range names {
		if err := uploader.Upload(ctx, name, strings.NewReader("data-"+name)); err != nil {
			t.Fatalf("Upload %s: %v", name, err)
		}
		// Small sleep so LastModified timestamps are distinct.
		time.Sleep(10 * time.Millisecond)
	}

	listed, err := uploader.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 3 {
		t.Fatalf("expected 3 objects, got %d: %v", len(listed), listed)
	}

	// Verify all names are present.
	nameSet := make(map[string]bool)
	for _, n := range listed {
		nameSet[n] = true
	}
	for _, want := range names {
		if !nameSet[want] {
			t.Errorf("expected %q in list, got: %v", want, listed)
		}
	}

	// Newest-first: snap-c uploaded last, should be first.
	if listed[0] != "snap-c" {
		t.Errorf("expected snap-c first (newest), got: %v", listed)
	}
}

// TestGCSSHA256Corruption verifies that corrupted object data is detected on download.
func TestGCSSHA256Corruption(t *testing.T) {
	baseURL, cleanup := startFakeGCS(t)
	defer cleanup()

	uploader := newTestGCSUploader(t, baseURL, false /*compress*/)
	ctx := context.Background()

	// Upload a valid backup.
	data := "gcs-integrity-check-data"
	if err := uploader.Upload(ctx, "snap-gcs-integrity", strings.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Overwrite the data object with corrupted bytes, leaving the manifest
	// untouched. Uses the raw GCS client to bypass the uploader.
	rawClient := newTestGCSClient(t, baseURL)
	defer rawClient.Close()

	wc := rawClient.Bucket("test-gcs-backups").Object("snap-gcs-integrity").NewWriter(ctx)
	if _, err := io.Copy(wc, strings.NewReader("CORRUPTED DATA")); err != nil {
		t.Fatalf("corrupt write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("corrupt write close: %v", err)
	}

	// Download should fail with a SHA-256 mismatch.
	_, dlErr := uploader.Download(ctx, "snap-gcs-integrity")
	if dlErr == nil {
		t.Fatal("expected Download to fail after corruption, got nil")
	}
	if !strings.Contains(dlErr.Error(), "SHA-256 mismatch") {
		t.Errorf("expected SHA-256 mismatch error, got: %v", dlErr)
	}
}

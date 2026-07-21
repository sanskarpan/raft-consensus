//go:build integration
// +build integration

package backup_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"

	"github.com/sanskarpan/raft-consensus/pkg/backup"
)

func startMinIO(t *testing.T) (endpoint, accessKey, secretKey string, cleanup func()) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "minio/minio:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     "minioadmin",
			"MINIO_ROOT_PASSWORD": "minioadmin",
		},
		Cmd:        []string{"server", "/data"},
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("skipping integration test: failed to start MinIO container: %v", err)
	}
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "9000")
	ep := fmt.Sprintf("%s:%s", host, port.Port())
	cleanup = func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate MinIO container: %v", err)
		}
	}
	return ep, "minioadmin", "minioadmin", cleanup
}

func newTestUploader(t *testing.T, endpoint, accessKey, secretKey string, compress bool) *backup.MinIOUploader {
	t.Helper()
	ctx := context.Background()
	u, err := backup.NewMinIOUploader(ctx, backup.MinIOConfig{
		Endpoint:  endpoint,
		Bucket:    "test-backups",
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    false,
		Compress:  compress,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("NewMinIOUploader: %v", err)
	}
	return u
}

func TestMinIOUploaderRoundTrip(t *testing.T) {
	endpoint, accessKey, secretKey, cleanup := startMinIO(t)
	defer cleanup()

	uploader := newTestUploader(t, endpoint, accessKey, secretKey, true)
	ctx := context.Background()

	data := strings.Repeat("raft-snapshot-data", 1000)
	if err := uploader.Upload(ctx, "snap-001", strings.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := uploader.Download(ctx, "snap-001")
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

func TestMinIOUploaderRoundTripUncompressed(t *testing.T) {
	endpoint, accessKey, secretKey, cleanup := startMinIO(t)
	defer cleanup()

	uploader := newTestUploader(t, endpoint, accessKey, secretKey, false)
	ctx := context.Background()

	data := "plain-text-snapshot"
	if err := uploader.Upload(ctx, "snap-plain", strings.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	rc, err := uploader.Download(ctx, "snap-plain")
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

func TestMinIOUploaderList(t *testing.T) {
	endpoint, accessKey, secretKey, cleanup := startMinIO(t)
	defer cleanup()

	uploader := newTestUploader(t, endpoint, accessKey, secretKey, true)
	ctx := context.Background()

	for _, name := range []string{"snap-a", "snap-b", "snap-c"} {
		if err := uploader.Upload(ctx, name, strings.NewReader("data-"+name)); err != nil {
			t.Fatalf("Upload %s: %v", name, err)
		}
	}

	names, err := uploader.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 3 {
		t.Fatalf("expected 3 objects, got %d: %v", len(names), names)
	}
	// All three names should be present (order may vary based on timestamps).
	nameSet := map[string]bool{}
	for _, n := range names {
		nameSet[n] = true
	}
	for _, want := range []string{"snap-a", "snap-b", "snap-c"} {
		if !nameSet[want] {
			t.Errorf("expected %q in list, got: %v", want, names)
		}
	}
}

func TestMinIOUploaderSHA256Verification(t *testing.T) {
	endpoint, accessKey, secretKey, cleanup := startMinIO(t)
	defer cleanup()

	ctx := context.Background()
	uploader := newTestUploader(t, endpoint, accessKey, secretKey, false)

	// Upload a legitimate object.
	data := "integrity-check-data"
	if err := uploader.Upload(ctx, "snap-integrity", strings.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Overwrite only the data object with corrupted bytes, leaving manifest untouched.
	// This simulates bit-rot or tampering.
	client := uploader.ClientForTest()
	_, err := client.PutObject(ctx, "test-backups", "snap-integrity",
		strings.NewReader("CORRUPTED DATA"), 14,
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	if err != nil {
		t.Fatalf("corrupt PutObject: %v", err)
	}

	// Download should fail with a SHA-256 mismatch.
	_, dlErr := uploader.Download(ctx, "snap-integrity")
	if dlErr == nil {
		t.Fatal("expected Download to fail after corruption, got nil")
	}
	if !strings.Contains(dlErr.Error(), "SHA-256 mismatch") {
		t.Errorf("expected SHA-256 mismatch error, got: %v", dlErr)
	}
}

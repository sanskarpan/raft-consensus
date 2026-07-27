package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// fakeMinioStorage is an in-memory implementation of minioStorage for unit tests.
type fakeMinioStorage struct {
	buckets map[string]bool
	objects map[string][]byte // keyed by "bucket/key"
	// getObjectErr, if non-nil, is returned by GetObject for any key.
	getObjectErr error
}

func newFakeMinioStorage() *fakeMinioStorage {
	return &fakeMinioStorage{
		buckets: make(map[string]bool),
		objects: make(map[string][]byte),
	}
}

func (f *fakeMinioStorage) storeKey(bucket, key string) string {
	return bucket + "/" + key
}

func (f *fakeMinioStorage) BucketExists(_ context.Context, bucket string) (bool, error) {
	return f.buckets[bucket], nil
}

func (f *fakeMinioStorage) MakeBucket(_ context.Context, bucket string, _ minio.MakeBucketOptions) error {
	f.buckets[bucket] = true
	return nil
}

func (f *fakeMinioStorage) PutObject(_ context.Context, bucket, key string, r io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	f.objects[f.storeKey(bucket, key)] = data
	return minio.UploadInfo{Size: int64(len(data))}, nil
}

func (f *fakeMinioStorage) GetObject(_ context.Context, bucket, key string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	if f.getObjectErr != nil {
		return nil, f.getObjectErr
	}
	data, ok := f.objects[f.storeKey(bucket, key)]
	if !ok {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeMinioStorage) ListObjects(_ context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo, 64)
	go func() {
		defer close(ch)
		prefix := opts.Prefix
		for k := range f.objects {
			// k is "bucket/key"
			bkt := bucket + "/"
			if !strings.HasPrefix(k, bkt) {
				continue
			}
			objKey := strings.TrimPrefix(k, bkt)
			if prefix != "" && !strings.HasPrefix(objKey, prefix) {
				continue
			}
			ch <- minio.ObjectInfo{
				Key:          objKey,
				LastModified: time.Now(),
			}
		}
	}()
	return ch
}

// newFakeMinIOUploader creates a MinIOUploader backed by fakeMinioStorage,
// bypassing the NewMinIOUploader constructor entirely.
func newFakeMinIOUploader(cfg MinIOConfig) *MinIOUploader {
	fake := newFakeMinioStorage()
	// Pre-create the bucket so Upload/Download work without real connectivity.
	fake.buckets[cfg.Bucket] = true
	return &MinIOUploader{
		storage: fake,
		cfg:     cfg,
		logger:  nil,
	}
}

// fakeBucket is the bucket name used in all unit tests.
const fakeBucket = "unit-test-bucket"

func TestMinIOUploaderUploadDownloadRoundTrip(t *testing.T) {
	cfg := MinIOConfig{
		Endpoint: "localhost:9000",
		Bucket:   fakeBucket,
		Retry:    RetryConfig{MaxAttempts: 1},
	}
	u := newFakeMinIOUploader(cfg)
	ctx := context.Background()

	payload := "hello-world"
	if err := u.Upload(ctx, "snap-001", strings.NewReader(payload)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := u.Download(ctx, "snap-001")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != payload {
		t.Errorf("content mismatch: got %q, want %q", string(got), payload)
	}
}

func TestMinIOUploaderUploadDownloadCompressed(t *testing.T) {
	cfg := MinIOConfig{
		Endpoint: "localhost:9000",
		Bucket:   fakeBucket,
		Compress: true,
		Retry:    RetryConfig{MaxAttempts: 1},
	}
	u := newFakeMinIOUploader(cfg)
	ctx := context.Background()

	payload := strings.Repeat("raft-snapshot-data", 500)
	if err := u.Upload(ctx, "snap-compressed", strings.NewReader(payload)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := u.Download(ctx, "snap-compressed")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != payload {
		t.Errorf("decompressed content mismatch: got %d bytes, want %d bytes", len(got), len(payload))
	}
}

func TestMinIOUploaderSHA256Mismatch(t *testing.T) {
	cfg := MinIOConfig{
		Endpoint: "localhost:9000",
		Bucket:   fakeBucket,
		Retry:    RetryConfig{MaxAttempts: 1},
	}
	u := newFakeMinIOUploader(cfg)
	ctx := context.Background()

	if err := u.Upload(ctx, "snap-integrity", strings.NewReader("original data")); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Corrupt the data object in the fake backend by changing a byte.
	fake := u.storage.(*fakeMinioStorage)
	dataKey := fakeBucket + "/snap-integrity"
	original := fake.objects[dataKey]
	corrupted := make([]byte, len(original))
	copy(corrupted, original)
	corrupted[0] ^= 0xFF
	fake.objects[dataKey] = corrupted

	_, err := u.Download(ctx, "snap-integrity")
	if err == nil {
		t.Fatal("expected Download to fail after corruption, got nil")
	}

	var pe *PermanentError
	if !errors.As(err, &pe) {
		t.Errorf("expected *PermanentError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Errorf("expected SHA-256 mismatch in error, got: %v", err)
	}
}

func TestMinIOUploaderList(t *testing.T) {
	cfg := MinIOConfig{
		Endpoint: "localhost:9000",
		Bucket:   fakeBucket,
		Retry:    RetryConfig{MaxAttempts: 1},
	}
	u := newFakeMinIOUploader(cfg)
	ctx := context.Background()

	for _, name := range []string{"snap-a", "snap-b", "snap-c"} {
		if err := u.Upload(ctx, name, strings.NewReader("data-"+name)); err != nil {
			t.Fatalf("Upload %s: %v", name, err)
		}
	}

	names, err := u.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	// Should have 3 entries (manifests are filtered).
	if len(names) != 3 {
		t.Fatalf("expected 3 names, got %d: %v", len(names), names)
	}

	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}
	for _, want := range []string{"snap-a", "snap-b", "snap-c"} {
		if !nameSet[want] {
			t.Errorf("expected %q in list, got: %v", want, names)
		}
	}
}

func TestMinIOUploaderPrefix(t *testing.T) {
	const prefix = "node1/snapshots"
	cfg := MinIOConfig{
		Endpoint: "localhost:9000",
		Bucket:   fakeBucket,
		Prefix:   prefix,
		Retry:    RetryConfig{MaxAttempts: 1},
	}
	u := newFakeMinIOUploader(cfg)
	ctx := context.Background()

	if err := u.Upload(ctx, "snap-prefixed", strings.NewReader("some data")); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	fake := u.storage.(*fakeMinioStorage)
	expectedKey := fakeBucket + "/" + prefix + "/snap-prefixed"
	if _, ok := fake.objects[expectedKey]; !ok {
		t.Errorf("expected object at key %q in fake storage; stored keys: %v",
			expectedKey, keys(fake.objects))
	}
}

func TestMinIOUploaderGetObjectError(t *testing.T) {
	cfg := MinIOConfig{
		Endpoint: "localhost:9000",
		Bucket:   fakeBucket,
		Retry:    RetryConfig{MaxAttempts: 1},
	}
	u := newFakeMinIOUploader(cfg)
	ctx := context.Background()

	// Upload normally first so the manifest exists.
	if err := u.Upload(ctx, "snap-err", strings.NewReader("some data")); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Inject error into GetObject.
	injectedErr := errors.New("injected GetObject failure")
	fake := u.storage.(*fakeMinioStorage)
	fake.getObjectErr = injectedErr

	_, err := u.Download(ctx, "snap-err")
	if err == nil {
		t.Fatal("expected Download to fail, got nil")
	}
	if !strings.Contains(err.Error(), "injected GetObject failure") {
		t.Errorf("expected injected error in message, got: %v", err)
	}
}

// keys returns the map keys as a slice (for error messages).
func keys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestMinIOUploaderMissingEndpoint verifies NewMinIOUploader returns error when Endpoint is empty.
func TestMinIOUploaderMissingEndpoint(t *testing.T) {
	_, err := NewMinIOUploader(context.Background(), MinIOConfig{Bucket: "b"}, nil)
	if err == nil {
		t.Fatal("expected error for missing Endpoint")
	}
	if !strings.Contains(err.Error(), "Endpoint is required") {
		t.Errorf("expected 'Endpoint is required' error, got: %v", err)
	}
}

// TestMinIOUploaderMissingBucket verifies NewMinIOUploader returns error when Bucket is empty.
func TestMinIOUploaderMissingBucket(t *testing.T) {
	_, err := NewMinIOUploader(context.Background(), MinIOConfig{Endpoint: "localhost:9000"}, nil)
	if err == nil {
		t.Fatal("expected error for missing Bucket")
	}
	if !strings.Contains(err.Error(), "Bucket is required") {
		t.Errorf("expected 'Bucket is required' error, got: %v", err)
	}
}

// TestMinIOUploaderObjectKeyWithPrefix covers the objectKey path when Prefix is set.
func TestMinIOUploaderObjectKeyWithPrefix(t *testing.T) {
	u := &MinIOUploader{cfg: MinIOConfig{Prefix: "myprefix"}}
	got := u.objectKey("snap-001")
	want := "myprefix/snap-001"
	if got != want {
		t.Errorf("objectKey = %q, want %q", got, want)
	}
}

// TestMinIOUploaderObjectKeyNoPrefix covers the no-prefix path.
func TestMinIOUploaderObjectKeyNoPrefix(t *testing.T) {
	u := &MinIOUploader{cfg: MinIOConfig{}}
	got := u.objectKey("snap-001")
	if got != "snap-001" {
		t.Errorf("objectKey = %q, want %q", got, "snap-001")
	}
}

// TestMinIOUploaderClientForTest verifies ClientForTest returns nil for non-adapter storage.
func TestMinIOUploaderClientForTest(t *testing.T) {
	cfg := MinIOConfig{
		Endpoint: "localhost:9000",
		Bucket:   fakeBucket,
	}
	u := newFakeMinIOUploader(cfg)
	// fakeMinioStorage is not *minioClientAdapter, so ClientForTest should return nil.
	got := u.ClientForTest()
	if got != nil {
		t.Errorf("ClientForTest on fake storage: expected nil, got %v", got)
	}
}

// TestMinIOUploaderListError verifies that List propagates errors from ListObjects.
func TestMinIOUploaderListError(t *testing.T) {
	cfg := MinIOConfig{
		Endpoint: "localhost:9000",
		Bucket:   fakeBucket,
		Retry:    RetryConfig{MaxAttempts: 1},
	}
	u := newFakeMinIOUploader(cfg)

	// Replace storage with one that returns an error channel.
	u.storage = &errorListStorage{bucket: fakeBucket}

	_, err := u.List(context.Background())
	if err == nil {
		t.Fatal("expected List to fail with error, got nil")
	}
}

// errorListStorage is a minioStorage whose ListObjects channel sends an error.
type errorListStorage struct {
	bucket string
}

func (e *errorListStorage) BucketExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}
func (e *errorListStorage) MakeBucket(_ context.Context, _ string, _ minio.MakeBucketOptions) error {
	return nil
}
func (e *errorListStorage) PutObject(_ context.Context, _, _ string, r io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	return minio.UploadInfo{}, nil
}
func (e *errorListStorage) GetObject(_ context.Context, _, _ string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
func (e *errorListStorage) ListObjects(_ context.Context, _ string, _ minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo, 1)
	ch <- minio.ObjectInfo{Err: errors.New("list failure")}
	close(ch)
	return ch
}

// TestPermanentErrorUnwrap verifies PermanentError.Unwrap returns the cause.
func TestPermanentErrorUnwrap(t *testing.T) {
	cause := errors.New("original cause")
	pe := &PermanentError{Cause: cause}
	if pe.Unwrap() != cause {
		t.Errorf("Unwrap() = %v, want %v", pe.Unwrap(), cause)
	}
}

// TestRetryConfigDefaults verifies RetryConfig.maxAttempts returns default 5 when zero.
func TestRetryConfigDefaults(t *testing.T) {
	rc := RetryConfig{}
	if rc.maxAttempts() != 5 {
		t.Errorf("maxAttempts() = %d, want 5", rc.maxAttempts())
	}
}

// TestMinIOUploaderDownloadMissingManifest verifies Download fails when the manifest is absent.
func TestMinIOUploaderDownloadMissingManifest(t *testing.T) {
	cfg := MinIOConfig{
		Endpoint: "localhost:9000",
		Bucket:   fakeBucket,
		Retry:    RetryConfig{MaxAttempts: 1},
	}
	u := newFakeMinIOUploader(cfg)
	ctx := context.Background()

	// No upload — manifest doesn't exist.
	_, err := u.Download(ctx, "snap-nonexistent")
	if err == nil {
		t.Fatal("expected Download to fail for missing manifest, got nil")
	}
}

// TestFakeMinioStorageBucketLifecycle tests fakeMinioStorage bucket operations directly.
func TestFakeMinioStorageBucketLifecycle(t *testing.T) {
	ctx := context.Background()
	f := newFakeMinioStorage()

	// Bucket doesn't exist initially.
	exists, err := f.BucketExists(ctx, "mybucket")
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if exists {
		t.Error("expected bucket to not exist")
	}

	// Create it.
	if err := f.MakeBucket(ctx, "mybucket", minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}

	// Now it should exist.
	exists, err = f.BucketExists(ctx, "mybucket")
	if err != nil {
		t.Fatalf("BucketExists after create: %v", err)
	}
	if !exists {
		t.Error("expected bucket to exist after MakeBucket")
	}
}

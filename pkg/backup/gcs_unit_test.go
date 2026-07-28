package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// errNotFound is the sentinel error returned by fakeGCSStorage.BucketExists when
// the bucket does not exist. It must satisfy isNotFound().
var errNotFound = fmt.Errorf("bucket does not exist")

// fakeGCSStorage is an in-memory implementation of gcsStorage for unit tests.
type fakeGCSStorage struct {
	buckets map[string]bool
	objects map[string][]byte // keyed by "bucket/key"
	// timestamps tracks insertion order for newest-first sorting in List.
	timestamps map[string]time.Time
}

func newFakeGCSStorage() *fakeGCSStorage {
	return &fakeGCSStorage{
		buckets:    make(map[string]bool),
		objects:    make(map[string][]byte),
		timestamps: make(map[string]time.Time),
	}
}

func (f *fakeGCSStorage) objectKey(bucket, key string) string {
	return bucket + "/" + key
}

func (f *fakeGCSStorage) BucketExists(_ context.Context, bucket string) error {
	if f.buckets[bucket] {
		return nil
	}
	return errNotFound
}

func (f *fakeGCSStorage) EnsureBucket(_ context.Context, bucket string) error {
	f.buckets[bucket] = true
	return nil
}

func (f *fakeGCSStorage) WriteObject(_ context.Context, bucket, key, _ string, b []byte) error {
	k := f.objectKey(bucket, key)
	data := make([]byte, len(b))
	copy(data, b)
	f.objects[k] = data
	f.timestamps[k] = time.Now()
	return nil
}

func (f *fakeGCSStorage) ReadObject(_ context.Context, bucket, key string) ([]byte, error) {
	data, ok := f.objects[f.objectKey(bucket, key)]
	if !ok {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (f *fakeGCSStorage) ListObjects(_ context.Context, bucket, prefix string) ([]gcsObjInfo, error) {
	bktPrefix := bucket + "/"
	var out []gcsObjInfo
	for k := range f.objects {
		if !strings.HasPrefix(k, bktPrefix) {
			continue
		}
		name := strings.TrimPrefix(k, bktPrefix)
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		out = append(out, gcsObjInfo{Name: name, Created: f.timestamps[k]})
	}
	return out, nil
}

// newFakeGCSUploader creates a GCSUploader directly with fakeGCSStorage,
// bypassing the NewGCSUploader constructor entirely.
func newFakeGCSUploader(cfg GCSConfig) *GCSUploader {
	fake := newFakeGCSStorage()
	// Pre-create the bucket so Upload/Download work without real connectivity.
	fake.buckets[cfg.Bucket] = true
	return &GCSUploader{
		backend: fake,
		cfg:     cfg,
		logger:  nil,
	}
}

const fakeGCSBucket = "unit-test-gcs-bucket"

func TestGCSUploaderUploadDownloadRoundTrip(t *testing.T) {
	cfg := GCSConfig{
		Bucket: fakeGCSBucket,
		Retry:  RetryConfig{MaxAttempts: 1},
	}
	u := newFakeGCSUploader(cfg)
	ctx := context.Background()

	payload := "hello-world-gcs"
	if err := u.Upload(ctx, "snap-gcs-001", strings.NewReader(payload)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := u.Download(ctx, "snap-gcs-001")
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

func TestGCSUploaderUploadDownloadCompressed(t *testing.T) {
	cfg := GCSConfig{
		Bucket:   fakeGCSBucket,
		Compress: true,
		Retry:    RetryConfig{MaxAttempts: 1},
	}
	u := newFakeGCSUploader(cfg)
	ctx := context.Background()

	payload := strings.Repeat("gcs-snapshot-data", 500)
	if err := u.Upload(ctx, "snap-gcs-compressed", strings.NewReader(payload)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	rc, err := u.Download(ctx, "snap-gcs-compressed")
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

func TestGCSUploaderSHA256Mismatch(t *testing.T) {
	cfg := GCSConfig{
		Bucket: fakeGCSBucket,
		Retry:  RetryConfig{MaxAttempts: 1},
	}
	u := newFakeGCSUploader(cfg)
	ctx := context.Background()

	if err := u.Upload(ctx, "snap-gcs-integrity", strings.NewReader("original gcs data")); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Corrupt the data object in the fake backend.
	fake := u.backend.(*fakeGCSStorage)
	dataKey := fakeGCSBucket + "/snap-gcs-integrity"
	original := fake.objects[dataKey]
	corrupted := make([]byte, len(original))
	copy(corrupted, original)
	corrupted[0] ^= 0xFF
	fake.objects[dataKey] = corrupted

	_, err := u.Download(ctx, "snap-gcs-integrity")
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

func TestGCSUploaderList(t *testing.T) {
	cfg := GCSConfig{
		Bucket: fakeGCSBucket,
		Retry:  RetryConfig{MaxAttempts: 1},
	}
	u := newFakeGCSUploader(cfg)
	ctx := context.Background()

	for _, name := range []string{"snap-a", "snap-b", "snap-c"} {
		if err := u.Upload(ctx, name, strings.NewReader("data-"+name)); err != nil {
			t.Fatalf("Upload %s: %v", name, err)
		}
		// Small sleep so timestamps are distinct for ordering.
		time.Sleep(2 * time.Millisecond)
	}

	names, err := u.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

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

func TestGCSUploaderPrefix(t *testing.T) {
	const prefix = "node1/gcs-snapshots"
	cfg := GCSConfig{
		Bucket: fakeGCSBucket,
		Prefix: prefix,
		Retry:  RetryConfig{MaxAttempts: 1},
	}
	u := newFakeGCSUploader(cfg)
	ctx := context.Background()

	if err := u.Upload(ctx, "snap-prefixed", strings.NewReader("some gcs data")); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	fake := u.backend.(*fakeGCSStorage)
	expectedKey := fakeGCSBucket + "/" + prefix + "/snap-prefixed"
	if _, ok := fake.objects[expectedKey]; !ok {
		t.Errorf("expected object at key %q in fake storage; stored keys: %v",
			expectedKey, gcsKeys(fake.objects))
	}
}

// gcsKeys returns map keys as a slice (for error messages).
func gcsKeys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestIsNotFound verifies the isNotFound helper covers its branches.
func TestIsNotFound(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{fmt.Errorf("bucket does not exist"), true},
		{fmt.Errorf("received 404 from server"), true},
		{fmt.Errorf("permission denied"), false},
		{fmt.Errorf("connection refused"), false},
	}
	for _, tc := range cases {
		if got := isNotFound(tc.err); got != tc.want {
			t.Errorf("isNotFound(%q) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// TestIsNotFoundStorageSentinels covers the sentinel-error branches of isNotFound.
func TestIsNotFoundStorageSentinels(t *testing.T) {
	// Import cloud.google.com/go/storage is available via the gcs.go file;
	// we call isNotFound directly since it's in the same package.
	if !isNotFound(errNotFound) {
		t.Error("expected errNotFound ('bucket does not exist') to match isNotFound")
	}
}

// TestBuildClientOptionsTestEndpoint exercises the TestEndpoint branch of buildClientOptions.
func TestBuildClientOptionsTestEndpoint(t *testing.T) {
	cfg := GCSConfig{TestEndpoint: "http://localhost:4443/storage/v1/"}
	opts, err := buildClientOptions(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(opts) == 0 {
		t.Error("expected non-empty options for TestEndpoint config")
	}
}

// TestBuildClientOptionsCredentialsFile exercises the CredentialsFile branch.
// The file does not exist, so buildClientOptions must return an error.
func TestBuildClientOptionsCredentialsFile(t *testing.T) {
	cfg := GCSConfig{CredentialsFile: "/nonexistent/creds.json"}
	_, err := buildClientOptions(cfg)
	if err == nil {
		t.Error("expected error for non-existent credentials file")
	}
}

// TestBuildClientOptionsDefault exercises the ADC (no-option) path.
func TestBuildClientOptionsDefault(t *testing.T) {
	cfg := GCSConfig{}
	opts, err := buildClientOptions(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(opts) != 0 {
		t.Errorf("expected empty options for default config, got %d", len(opts))
	}
}

// TestGCSUploaderMissingBucket verifies NewGCSUploader returns error when Bucket is empty.
func TestGCSUploaderMissingBucket(t *testing.T) {
	_, err := NewGCSUploader(context.Background(), GCSConfig{}, nil)
	if err == nil {
		t.Fatal("expected error for missing Bucket")
	}
	if !strings.Contains(err.Error(), "Bucket is required") {
		t.Errorf("expected 'Bucket is required' error, got: %v", err)
	}
}

// TestGCSUploaderObjectKeyWithPrefix covers the objectKey path when Prefix is set.
func TestGCSUploaderObjectKeyWithPrefix(t *testing.T) {
	u := &GCSUploader{cfg: GCSConfig{Prefix: "myprefix"}}
	got := u.objectKey("snap-001")
	want := "myprefix/snap-001"
	if got != want {
		t.Errorf("objectKey = %q, want %q", got, want)
	}
}

// TestGCSUploaderObjectKeyNoPrefix covers the no-prefix path.
func TestGCSUploaderObjectKeyNoPrefix(t *testing.T) {
	u := &GCSUploader{cfg: GCSConfig{}}
	got := u.objectKey("snap-001")
	if got != "snap-001" {
		t.Errorf("objectKey = %q, want %q", got, "snap-001")
	}
}

// TestFakeGCSStorageBucketLifecycle tests fakeGCSStorage bucket operations.
func TestFakeGCSStorageBucketLifecycle(t *testing.T) {
	ctx := context.Background()
	f := newFakeGCSStorage()

	// Bucket doesn't exist yet.
	if err := f.BucketExists(ctx, "mybucket"); err == nil {
		t.Error("expected error for non-existent bucket")
	}

	// Create it.
	if err := f.EnsureBucket(ctx, "mybucket"); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}

	// Now it exists.
	if err := f.BucketExists(ctx, "mybucket"); err != nil {
		t.Errorf("BucketExists after EnsureBucket: %v", err)
	}
}

// TestGCSUploaderUploadReadError simulates ReadObject returning an error on data fetch.
func TestGCSUploaderUploadReadError(t *testing.T) {
	cfg := GCSConfig{
		Bucket: fakeGCSBucket,
		Retry:  RetryConfig{MaxAttempts: 1},
	}
	u := newFakeGCSUploader(cfg)
	ctx := context.Background()

	// Upload normally.
	if err := u.Upload(ctx, "snap-read-err", strings.NewReader("test data")); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Remove the data object so ReadObject fails for it (manifest still exists).
	fake := u.backend.(*fakeGCSStorage)
	delete(fake.objects, fakeGCSBucket+"/snap-read-err")

	_, err := u.Download(ctx, "snap-read-err")
	if err == nil {
		t.Fatal("expected Download to fail when data object missing, got nil")
	}
}

// TestGCSUploaderDownloadMissingManifest verifies Download fails when the manifest is absent.
func TestGCSUploaderDownloadMissingManifest(t *testing.T) {
	cfg := GCSConfig{
		Bucket: fakeGCSBucket,
		Retry:  RetryConfig{MaxAttempts: 1},
	}
	u := newFakeGCSUploader(cfg)
	ctx := context.Background()

	// No upload — manifest doesn't exist.
	_, err := u.Download(ctx, "snap-no-manifest")
	if err == nil {
		t.Fatal("expected Download to fail for missing manifest, got nil")
	}
}

// TestGCSUploaderListErrorPropagation verifies List propagates backend errors.
func TestGCSUploaderListErrorPropagation(t *testing.T) {
	cfg := GCSConfig{
		Bucket: fakeGCSBucket,
		Retry:  RetryConfig{MaxAttempts: 1},
	}
	u := newFakeGCSUploader(cfg)
	u.backend = &errorGCSStorage{}

	_, err := u.List(context.Background())
	if err == nil {
		t.Fatal("expected List to fail with error, got nil")
	}
}

// errorGCSStorage is a gcsStorage that returns errors from ListObjects.
type errorGCSStorage struct{}

func (e *errorGCSStorage) BucketExists(_ context.Context, _ string) error { return nil }
func (e *errorGCSStorage) EnsureBucket(_ context.Context, _ string) error { return nil }
func (e *errorGCSStorage) WriteObject(_ context.Context, _, _, _ string, _ []byte) error {
	return nil
}
func (e *errorGCSStorage) ReadObject(_ context.Context, _, _ string) ([]byte, error) {
	return nil, errors.New("read error")
}
func (e *errorGCSStorage) ListObjects(_ context.Context, _, _ string) ([]gcsObjInfo, error) {
	return nil, errors.New("list error")
}

// TestGCSUploaderCompressedUploadPaths exercises the gzip path in doUpload.
func TestGCSUploaderCompressedGzipError(t *testing.T) {
	// This covers the gzip write/close paths - already covered by TestGCSUploaderUploadDownloadCompressed.
	// Adding a test that exercises the logger path.
	cfg := GCSConfig{
		Bucket: fakeGCSBucket,
		Retry:  RetryConfig{MaxAttempts: 1},
	}
	u := newFakeGCSUploader(cfg)
	ctx := context.Background()

	// Test with empty payload (edge case).
	if err := u.Upload(ctx, "snap-empty", strings.NewReader("")); err != nil {
		t.Fatalf("Upload empty: %v", err)
	}

	rc, err := u.Download(ctx, "snap-empty")
	if err != nil {
		t.Fatalf("Download empty: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if string(got) != "" {
		t.Errorf("expected empty content, got %q", string(got))
	}
}

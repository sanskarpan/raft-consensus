package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

// MinIOConfig configures the MinIO (or S3-compatible) backend.
type MinIOConfig struct {
	Endpoint  string // e.g. "localhost:9000" or "s3.amazonaws.com"
	Bucket    string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Region    string // optional; used for AWS S3
	Prefix    string // optional key prefix for all objects
	Compress  bool   // gzip-compress uploads (recommended)
	NodeID    string // optional; recorded in manifest for provenance
	Retry     RetryConfig
}

// MinIOUploader uploads/downloads Raft snapshots to MinIO or any S3-compatible
// object store. It implements the Uploader interface.
type MinIOUploader struct {
	client *minio.Client
	cfg    MinIOConfig
	logger *zap.Logger
}

// NewMinIOUploader creates and validates a MinIOUploader. It creates the bucket
// if it does not already exist.
func NewMinIOUploader(ctx context.Context, cfg MinIOConfig, logger *zap.Logger) (*MinIOUploader, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("MinIOConfig.Endpoint is required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("MinIOConfig.Bucket is required")
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("minio.New: %w", err)
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("checking bucket %q: %w", cfg.Bucket, err)
	}
	if !exists {
		opts := minio.MakeBucketOptions{Region: cfg.Region}
		if err := client.MakeBucket(ctx, cfg.Bucket, opts); err != nil {
			return nil, fmt.Errorf("creating bucket %q: %w", cfg.Bucket, err)
		}
		if logger != nil {
			logger.Info("created MinIO bucket", zap.String("bucket", cfg.Bucket))
		}
	}

	return &MinIOUploader{client: client, cfg: cfg, logger: logger}, nil
}

// ClientForTest exposes the underlying minio.Client for integration test
// manipulation (e.g. injecting corruption). Not for production use.
func (u *MinIOUploader) ClientForTest() *minio.Client {
	return u.client
}

// objectKey returns the full object key for the given name, applying the configured prefix.
func (u *MinIOUploader) objectKey(name string) string {
	if u.cfg.Prefix == "" {
		return name
	}
	return path.Join(u.cfg.Prefix, name)
}

// Upload streams r to object storage. If cfg.Compress is true the data is
// gzip-compressed before upload. A manifest JSON sidecar is uploaded alongside.
// The reader is fully buffered upfront so retry attempts can re-read it.
func (u *MinIOUploader) Upload(ctx context.Context, name string, r io.Reader) error {
	start := time.Now()

	// Buffer entire payload upfront so retry attempts can re-read it.
	raw, err := io.ReadAll(r)
	if err != nil {
		uploadErrors.Inc()
		return fmt.Errorf("reading snapshot data: %w", err)
	}

	err = u.cfg.Retry.Do(ctx, func() error {
		return u.doUpload(ctx, name, raw)
	})
	uploadDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		uploadErrors.Inc()
		if u.logger != nil {
			u.logger.Error("backup upload failed", zap.String("name", name), zap.Error(err))
		}
		return err
	}
	if u.logger != nil {
		u.logger.Info("backup uploaded", zap.String("name", name), zap.Duration("elapsed", time.Since(start)))
	}
	return nil
}

func (u *MinIOUploader) doUpload(ctx context.Context, name string, raw []byte) error {
	var payload []byte
	compressed := u.cfg.Compress
	if compressed {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(raw); err != nil {
			return fmt.Errorf("gzip write: %w", err)
		}
		if err := gz.Close(); err != nil {
			return fmt.Errorf("gzip close: %w", err)
		}
		payload = buf.Bytes()
	} else {
		payload = raw
	}

	// Compute SHA-256 of the (possibly compressed) payload.
	digest, n, err := ComputeSHA256(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("sha256: %w", err)
	}

	// Build manifest.
	dataKey := u.objectKey(name)
	manifest := Manifest{
		Version:    1,
		Name:       dataKey,
		SHA256:     digest,
		SizeBytes:  n,
		Compressed: compressed,
		NodeID:     u.cfg.NodeID,
		CreatedAt:  time.Now().UnixMilli(),
		// SnapshotIdx and SnapshotTerm are not available at Upload() time;
		// callers that need provenance should set them on the Manifest directly.
	}

	// Upload data object. Pass exact size so MinIO doesn't need to buffer again.
	opts := minio.PutObjectOptions{
		ContentType: "application/octet-stream",
		PartSize:    64 * 1024 * 1024, // 64 MiB parts for multipart
	}
	info, err := u.client.PutObject(ctx, u.cfg.Bucket, dataKey, bytes.NewReader(payload), n, opts)
	if err != nil {
		return fmt.Errorf("PutObject %q: %w", dataKey, err)
	}
	uploadBytes.Add(float64(info.Size))

	// Upload manifest sidecar.
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	manifestKey := dataKey + ".manifest.json"
	manifestSize := int64(len(manifestData))
	_, err = u.client.PutObject(ctx, u.cfg.Bucket, manifestKey,
		bytes.NewReader(manifestData), manifestSize,
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	if err != nil {
		return fmt.Errorf("PutObject manifest %q: %w", manifestKey, err)
	}

	return nil
}

// Download retrieves the named object, verifies its SHA-256 against the manifest,
// and returns a reader over the decompressed content.
func (u *MinIOUploader) Download(ctx context.Context, name string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := u.doDownload(ctx, name)
	downloadDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		restoreErrors.Inc()
		if u.logger != nil {
			u.logger.Error("backup download failed", zap.String("name", name), zap.Error(err))
		}
		return nil, err
	}
	return rc, nil
}

func (u *MinIOUploader) doDownload(ctx context.Context, name string) (io.ReadCloser, error) {
	dataKey := u.objectKey(name)
	manifestKey := dataKey + ".manifest.json"

	// Fetch manifest with retry.
	var manifestData []byte
	if retryErr := u.cfg.Retry.Do(ctx, func() error {
		mObj, err := u.client.GetObject(ctx, u.cfg.Bucket, manifestKey, minio.GetObjectOptions{})
		if err != nil {
			return fmt.Errorf("GetObject manifest %q: %w", manifestKey, err)
		}
		defer mObj.Close()
		manifestData, err = io.ReadAll(mObj)
		if err != nil {
			return fmt.Errorf("reading manifest %q: %w", manifestKey, err)
		}
		return nil
	}); retryErr != nil {
		return nil, retryErr
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	// Fetch data object with retry.
	var dataPayload []byte
	retryErr := u.cfg.Retry.Do(ctx, func() error {
		obj, err := u.client.GetObject(ctx, u.cfg.Bucket, dataKey, minio.GetObjectOptions{})
		if err != nil {
			return fmt.Errorf("GetObject %q: %w", dataKey, err)
		}
		defer obj.Close()
		dataPayload, err = io.ReadAll(obj)
		return err
	})
	if retryErr != nil {
		return nil, retryErr
	}

	// Verify SHA-256.
	digest, _, err := ComputeSHA256(bytes.NewReader(dataPayload))
	if err != nil {
		return nil, fmt.Errorf("sha256 verify: %w", err)
	}
	if digest != manifest.SHA256 {
		return nil, &PermanentError{
			Cause: fmt.Errorf("SHA-256 mismatch for %q: stored=%s computed=%s", dataKey, manifest.SHA256, digest),
		}
	}

	// Decompress if needed.
	if manifest.Compressed {
		gr, err := gzip.NewReader(bytes.NewReader(dataPayload))
		if err != nil {
			return nil, fmt.Errorf("gzip.NewReader: %w", err)
		}
		return gr, nil
	}
	return io.NopCloser(bytes.NewReader(dataPayload)), nil
}

// List returns the names of all stored backup data objects (not manifests),
// newest first by object last-modified time.
func (u *MinIOUploader) List(ctx context.Context) ([]string, error) {
	prefix := u.cfg.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	type entry struct {
		name    string
		modTime time.Time
	}
	var entries []entry

	for obj := range u.client.ListObjects(ctx, u.cfg.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("listing objects: %w", obj.Err)
		}
		// Skip manifest sidecars.
		if strings.HasSuffix(obj.Key, ".manifest.json") {
			continue
		}
		// Strip prefix to return bare names.
		n := obj.Key
		if prefix != "" {
			n = strings.TrimPrefix(n, prefix)
		}
		entries = append(entries, entry{name: n, modTime: obj.LastModified})
	}

	// Sort newest-first.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].modTime.After(entries[j].modTime)
	})

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.name
	}
	return names, nil
}

// Compile-time check: MinIOUploader implements Uploader.
var _ Uploader = (*MinIOUploader)(nil)

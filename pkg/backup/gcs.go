package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	authcreds "cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/storage"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSConfig configures the Google Cloud Storage backup backend.
type GCSConfig struct {
	// Bucket is the GCS bucket name (required).
	Bucket string
	// Prefix is an optional key prefix for all objects stored in the bucket.
	Prefix string
	// CredentialsFile is the path to a service account JSON key file.
	// When empty, Application Default Credentials (ADC) are used — the
	// recommended approach for GKE Workload Identity, Cloud Run, and other
	// GCP-managed environments.
	CredentialsFile string
	// Compress enables gzip compression of uploaded payloads (recommended).
	Compress bool
	// NodeID is recorded in the manifest sidecar for provenance.
	NodeID string
	// Retry controls upload/download retry behavior.
	Retry RetryConfig

	// TestEndpoint is an internal field used only by integration tests to redirect
	// requests to a fake-gcs-server instance. It is not exposed in YAML config.
	// When non-empty, authentication is also disabled.
	TestEndpoint string
}

// gcsObjInfo carries the name and creation time of a GCS object.
type gcsObjInfo struct {
	Name    string
	Created time.Time
}

// gcsStorage is the interface GCSUploader uses to interact with GCS.
// The production implementation wraps *storage.Client; tests use a fake.
type gcsStorage interface {
	// BucketExists returns nil if the bucket exists, storage.ErrBucketNotExist
	// (or an equivalent error) if it does not, and another error otherwise.
	BucketExists(ctx context.Context, bucket string) error
	// EnsureBucket creates the bucket.
	EnsureBucket(ctx context.Context, bucket string) error
	WriteObject(ctx context.Context, bucket, key, contentType string, b []byte) error
	ReadObject(ctx context.Context, bucket, key string) ([]byte, error)
	ListObjects(ctx context.Context, bucket, prefix string) ([]gcsObjInfo, error)
}

// gcsClientAdapter wraps *storage.Client to implement gcsStorage.
type gcsClientAdapter struct{ c *storage.Client }

func (a *gcsClientAdapter) BucketExists(ctx context.Context, bucket string) error {
	_, err := a.c.Bucket(bucket).Attrs(ctx)
	return err
}

func (a *gcsClientAdapter) EnsureBucket(ctx context.Context, bucket string) error {
	return a.c.Bucket(bucket).Create(ctx, "", nil)
}

func (a *gcsClientAdapter) WriteObject(ctx context.Context, bucket, key, contentType string, b []byte) error {
	wc := a.c.Bucket(bucket).Object(key).NewWriter(ctx)
	wc.ContentType = contentType
	if _, err := wc.Write(b); err != nil {
		_ = wc.Close()
		return err
	}
	return wc.Close()
}

func (a *gcsClientAdapter) ReadObject(ctx context.Context, bucket, key string) ([]byte, error) {
	rc, err := a.c.Bucket(bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (a *gcsClientAdapter) ListObjects(ctx context.Context, bucket, prefix string) ([]gcsObjInfo, error) {
	it := a.c.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	var out []gcsObjInfo
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, gcsObjInfo{Name: attrs.Name, Created: attrs.Created})
	}
	return out, nil
}

// GCSUploader uploads and downloads Raft snapshots to Google Cloud Storage.
// It implements the Uploader interface and is safe for concurrent use.
type GCSUploader struct {
	backend gcsStorage
	cfg     GCSConfig
	logger  *zap.Logger
}

// NewGCSUploader creates and validates a GCSUploader. It ensures the target
// bucket exists and is accessible, creating it if it does not already exist
// (requires storage.buckets.create permission — fine for integration tests;
// in production the bucket is typically pre-created via Terraform/gcloud).
func NewGCSUploader(ctx context.Context, cfg GCSConfig, logger *zap.Logger) (*GCSUploader, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("GCSConfig.Bucket is required")
	}

	opts, err := buildClientOptions(cfg)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %w", err)
	}

	u := &GCSUploader{backend: &gcsClientAdapter{c: client}, cfg: cfg, logger: logger}

	// Verify (and optionally create) the bucket. In production environments the
	// bucket is pre-provisioned; this is mostly a connectivity check.
	if err := u.backend.BucketExists(ctx, cfg.Bucket); err != nil {
		if isNotFound(err) {
			// Attempt to create the bucket. This succeeds in integration tests
			// (fake-gcs-server) and on GCP when the SA has the right IAM role.
			if createErr := u.backend.EnsureBucket(ctx, cfg.Bucket); createErr != nil {
				client.Close()
				return nil, fmt.Errorf("bucket %q does not exist and creation failed: %w", cfg.Bucket, createErr)
			}
			if logger != nil {
				logger.Info("created GCS bucket", zap.String("bucket", cfg.Bucket))
			}
		} else {
			client.Close()
			return nil, fmt.Errorf("checking bucket %q: %w", cfg.Bucket, err)
		}
	}

	return u, nil
}

// buildClientOptions returns the GCS client options for the given config.
func buildClientOptions(cfg GCSConfig) ([]option.ClientOption, error) {
	var opts []option.ClientOption
	if cfg.TestEndpoint != "" {
		// Integration test mode: use the fake-gcs-server with no auth.
		opts = append(opts,
			option.WithEndpoint(cfg.TestEndpoint),
			option.WithoutAuthentication(),
			option.WithHTTPClient(&http.Client{}),
		)
		return opts, nil
	}
	if cfg.CredentialsFile != "" {
		creds, err := authcreds.DetectDefault(&authcreds.DetectOptions{
			Scopes:          []string{storage.ScopeReadWrite, storage.ScopeFullControl},
			CredentialsFile: cfg.CredentialsFile,
		})
		if err != nil {
			return nil, fmt.Errorf("loading credentials from %q: %w", cfg.CredentialsFile, err)
		}
		opts = append(opts, option.WithAuthCredentials(creds))
	}
	// No options = ADC (Application Default Credentials). This is the
	// recommended path for GKE Workload Identity and Cloud Run.
	return opts, nil
}

// isNotFound reports whether the GCS error indicates a 404 / object-not-found.
func isNotFound(err error) bool {
	if err == storage.ErrBucketNotExist || err == storage.ErrObjectNotExist {
		return true
	}
	// The GCS client also wraps HTTP 404 errors as googleapi.Error; check string
	// as a fallback so this remains robust across SDK versions.
	return strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "does not exist")
}

// objectKey returns the full GCS object key for the given name, applying the
// configured prefix.
func (u *GCSUploader) objectKey(name string) string {
	if u.cfg.Prefix == "" {
		return name
	}
	return path.Join(u.cfg.Prefix, name)
}

// Upload streams r to GCS. If cfg.Compress is true the data is gzip-compressed
// before upload. A manifest JSON sidecar is written alongside.
// The reader is fully buffered so retry attempts can re-read it.
func (u *GCSUploader) Upload(ctx context.Context, name string, r io.Reader) error {
	start := time.Now()

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
			u.logger.Error("GCS backup upload failed", zap.String("name", name), zap.Error(err))
		}
		return err
	}
	if u.logger != nil {
		u.logger.Info("GCS backup uploaded",
			zap.String("name", name),
			zap.Duration("elapsed", time.Since(start)),
		)
	}
	return nil
}

func (u *GCSUploader) doUpload(ctx context.Context, name string, raw []byte) error {
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

	dataKey := u.objectKey(name)
	manifest := Manifest{
		Version:    1,
		Name:       dataKey,
		SHA256:     digest,
		SizeBytes:  n,
		Compressed: compressed,
		NodeID:     u.cfg.NodeID,
		CreatedAt:  time.Now().UnixMilli(),
	}

	// Upload data object.
	if err := u.backend.WriteObject(ctx, u.cfg.Bucket, dataKey, "application/octet-stream", payload); err != nil {
		return fmt.Errorf("write object %q: %w", dataKey, err)
	}
	uploadBytes.Add(float64(n))

	// Upload manifest sidecar.
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	manifestKey := dataKey + ".manifest.json"
	if err := u.backend.WriteObject(ctx, u.cfg.Bucket, manifestKey, "application/json", manifestData); err != nil {
		return fmt.Errorf("write manifest %q: %w", manifestKey, err)
	}

	return nil
}

// Download retrieves the named object, verifies its SHA-256 against the
// manifest, and returns a reader over the decompressed content.
func (u *GCSUploader) Download(ctx context.Context, name string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := u.doDownload(ctx, name)
	downloadDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		restoreErrors.Inc()
		if u.logger != nil {
			u.logger.Error("GCS backup download failed", zap.String("name", name), zap.Error(err))
		}
		return nil, err
	}
	return rc, nil
}

func (u *GCSUploader) doDownload(ctx context.Context, name string) (io.ReadCloser, error) {
	dataKey := u.objectKey(name)
	manifestKey := dataKey + ".manifest.json"

	// Fetch manifest with retry.
	var manifestData []byte
	if retryErr := u.cfg.Retry.Do(ctx, func() error {
		data, err := u.backend.ReadObject(ctx, u.cfg.Bucket, manifestKey)
		if err != nil {
			return fmt.Errorf("read manifest %q: %w", manifestKey, err)
		}
		manifestData = data
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
	if retryErr := u.cfg.Retry.Do(ctx, func() error {
		data, err := u.backend.ReadObject(ctx, u.cfg.Bucket, dataKey)
		if err != nil {
			return fmt.Errorf("read object %q: %w", dataKey, err)
		}
		dataPayload = data
		return nil
	}); retryErr != nil {
		return nil, retryErr
	}

	// Verify SHA-256.
	digest, _, err := ComputeSHA256(bytes.NewReader(dataPayload))
	if err != nil {
		return nil, fmt.Errorf("sha256 verify: %w", err)
	}
	if digest != manifest.SHA256 {
		return nil, &PermanentError{
			Cause: fmt.Errorf("SHA-256 mismatch for %q: stored=%s computed=%s",
				dataKey, manifest.SHA256, digest),
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
// newest first by object creation time.
func (u *GCSUploader) List(ctx context.Context) ([]string, error) {
	prefix := u.cfg.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	type entry struct {
		name    string
		created time.Time
	}
	var entries []entry

	objs, err := u.backend.ListObjects(ctx, u.cfg.Bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("listing GCS objects: %w", err)
	}
	for _, obj := range objs {
		// Skip manifest sidecars.
		if strings.HasSuffix(obj.Name, ".manifest.json") {
			continue
		}
		// Strip prefix to return bare names.
		n := obj.Name
		if prefix != "" {
			n = strings.TrimPrefix(n, prefix)
		}
		entries = append(entries, entry{name: n, created: obj.Created})
	}

	// Sort newest-first.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].created.After(entries[j].created)
	})

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.name
	}
	return names, nil
}

// Compile-time check: GCSUploader implements Uploader.
var _ Uploader = (*GCSUploader)(nil)

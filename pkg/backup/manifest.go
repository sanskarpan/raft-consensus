package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// Manifest is written alongside each backup object as <name>.manifest.json.
// It records enough metadata to verify integrity before restore.
type Manifest struct {
	Version      int    `json:"version"`      // always 1
	Name         string `json:"name"`         // object key of the data file
	SHA256       string `json:"sha256"`       // hex-encoded SHA-256 of the compressed data
	SizeBytes    int64  `json:"size_bytes"`
	Compressed   bool   `json:"compressed"`   // true if gzip-compressed
	SnapshotIdx  uint64 `json:"snapshot_idx"`
	SnapshotTerm uint64 `json:"snapshot_term"`
	NodeID       string `json:"node_id"`
	CreatedAt    int64  `json:"created_at"` // Unix milliseconds
}

// ComputeSHA256 reads r, computes SHA-256, and returns (hexDigest, bytesRead, error).
// The caller must re-open or re-buffer r for the actual upload since this drains it.
func ComputeSHA256(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

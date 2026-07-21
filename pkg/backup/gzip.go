package backup

import (
	"compress/gzip"
	"io"
)

// compressReader wraps a gzip.Writer behind a pipe so callers read compressed
// bytes from the returned ReadCloser without buffering the entire payload.
type compressReader struct {
	pr    *io.PipeReader
	pw    *io.PipeWriter
	gz    *gzip.Writer
	errCh chan error
}

// CompressReader wraps r with gzip compression and returns the compressed reader.
// The caller is responsible for closing the returned ReadCloser.
// The gzip writer runs in a background goroutine; errors surface on Read.
func CompressReader(r io.Reader) io.ReadCloser {
	pr, pw := io.Pipe()
	gz := gzip.NewWriter(pw)
	errCh := make(chan error, 1)

	go func() {
		_, err := io.Copy(gz, r)
		if err != nil {
			gz.Close()
			pw.CloseWithError(err)
			errCh <- err
			return
		}
		if err := gz.Close(); err != nil {
			pw.CloseWithError(err)
			errCh <- err
			return
		}
		pw.Close()
		errCh <- nil
	}()

	return &compressReader{pr: pr, pw: pw, gz: gz, errCh: errCh}
}

func (c *compressReader) Read(p []byte) (int, error) {
	return c.pr.Read(p)
}

func (c *compressReader) Close() error {
	// Drain errCh if background goroutine already finished; don't block.
	select {
	case <-c.errCh:
	default:
	}
	return c.pr.Close()
}

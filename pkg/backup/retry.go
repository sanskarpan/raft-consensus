package backup

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// PermanentError wraps an error that should not be retried. When RetryConfig.Do
// encounters a PermanentError it stops immediately and returns the error.
type PermanentError struct {
	Cause error
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("permanent error (no retry): %v", e.Cause)
}

func (e *PermanentError) Unwrap() error { return e.Cause }

// RetryConfig controls how Upload/Download retries on transient errors.
type RetryConfig struct {
	MaxAttempts int           // default 5 when zero
	InitialWait time.Duration // default 200ms when zero
	MaxWait     time.Duration // default 30s when zero
	Multiplier  float64       // default 2.0 when zero
}

func (rc RetryConfig) maxAttempts() int {
	if rc.MaxAttempts <= 0 {
		return 5
	}
	return rc.MaxAttempts
}

func (rc RetryConfig) initialWait() time.Duration {
	if rc.InitialWait <= 0 {
		return 200 * time.Millisecond
	}
	return rc.InitialWait
}

func (rc RetryConfig) maxWait() time.Duration {
	if rc.MaxWait <= 0 {
		return 30 * time.Second
	}
	return rc.MaxWait
}

func (rc RetryConfig) multiplier() float64 {
	if rc.Multiplier <= 0 {
		return 2.0
	}
	return rc.Multiplier
}

// Do calls fn up to MaxAttempts times, waiting exponentially between attempts.
// Context cancellation and PermanentError stop retries immediately.
func (rc RetryConfig) Do(ctx context.Context, fn func() error) error {
	wait := rc.initialWait()
	maxW := rc.maxWait()
	mult := rc.multiplier()
	maxAttempts := rc.maxAttempts()

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		// Stop immediately on permanent errors.
		var pe *PermanentError
		if errors.As(lastErr, &pe) {
			return lastErr
		}
		// Last attempt: don't sleep, just return.
		if attempt == maxAttempts-1 {
			break
		}
		// Wait with context cancellation support.
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		// Advance backoff.
		next := time.Duration(float64(wait) * mult)
		if next > maxW {
			next = maxW
		}
		wait = next
	}
	return lastErr
}

package backup_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/backup"
)

func TestRetrySucceedsOnThirdAttempt(t *testing.T) {
	attempts := 0
	rc := backup.RetryConfig{
		MaxAttempts: 5,
		InitialWait: time.Millisecond,
		MaxWait:     10 * time.Millisecond,
		Multiplier:  2.0,
	}
	err := rc.Do(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("transient error attempt %d", attempts)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryAbortsPermanentError(t *testing.T) {
	attempts := 0
	rc := backup.RetryConfig{
		MaxAttempts: 5,
		InitialWait: time.Millisecond,
		MaxWait:     10 * time.Millisecond,
		Multiplier:  2.0,
	}
	permErr := &backup.PermanentError{Cause: errors.New("fatal: object not found")}
	err := rc.Do(context.Background(), func() error {
		attempts++
		return permErr
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var pe *backup.PermanentError
	if !errors.As(err, &pe) {
		t.Errorf("expected PermanentError, got %T: %v", err, err)
	}
	if attempts != 1 {
		t.Errorf("expected exactly 1 attempt for permanent error, got %d", attempts)
	}
}

func TestRetryRespectsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	rc := backup.RetryConfig{
		MaxAttempts: 10,
		InitialWait: 50 * time.Millisecond, // longer than context timeout
		MaxWait:     time.Second,
		Multiplier:  2.0,
	}
	attempts := 0
	err := rc.Do(ctx, func() error {
		attempts++
		return errors.New("transient")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got: %v", err)
	}
	// Should have attempted once (on first call) before waiting
	if attempts > 2 {
		t.Errorf("expected at most 2 attempts before context expired, got %d", attempts)
	}
}

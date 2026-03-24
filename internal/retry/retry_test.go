package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDo_SucceedsFirstAttempt(t *testing.T) {
	calls := 0
	err := Do(context.Background(), DefaultOptions, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestDo_SucceedsAfterRetries(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Options{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	}, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestDo_ExhaustsRetries(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Options{
		MaxAttempts: 2,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	}, func() error {
		calls++
		return errors.New("permanent")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestDo_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Do(ctx, DefaultOptions, func() error {
		return errors.New("should not reach")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

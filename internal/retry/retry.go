package retry

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// Options configures retry behavior.
type Options struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// DefaultOptions is a sensible default for transient failures.
var DefaultOptions = Options{
	MaxAttempts: 3,
	BaseDelay:   200 * time.Millisecond,
	MaxDelay:    10 * time.Second,
}

// Do executes fn up to opts.MaxAttempts times with exponential backoff
// and jitter between attempts. Returns nil on the first successful call,
// or the last error if all attempts fail.
func Do(ctx context.Context, opts Options, fn func() error) error {
	var lastErr error
	for attempt := range opts.MaxAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if attempt < opts.MaxAttempts-1 {
			delay := time.Duration(float64(opts.BaseDelay) * math.Pow(2, float64(attempt)))
			if delay > opts.MaxDelay {
				delay = opts.MaxDelay
			}
			// Jitter: 50–150% of calculated delay.
			jitter := time.Duration(float64(delay) * (0.5 + rand.Float64()))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(jitter):
			}
		}
	}
	return lastErr
}

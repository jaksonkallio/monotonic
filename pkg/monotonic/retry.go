package monotonic

import (
	"context"
	"fmt"
	"math"
	"time"
)

// BackoffFn defines a function type for calculating backoff durations based on next attempt counter
// Will wait for the duration returned by Backoff before allowing the next retry attempt
// For the initial/0th attempt, the BackoffFn should likely return zero to allow an immediate first attempt
type BackoffFn func(nextAttemptCounter int) time.Duration

var ErrMaxAttemptsExceeded = fmt.Errorf("maximum retry attempts exceeded")

type Retry struct {
	MaxAttempts int
	Backoff     BackoffFn
	// OnAttempt, if non-nil, is called after each retried operation with the 0-indexed attempt counter and the attempt's error (nil on success).
	// Intended for observability (metrics, benchmarks counting retries-per-success); must be safe for concurrent use if the Retry is shared across goroutines.
	OnAttempt func(attempt int, err error)
}

func NewRetry(maxAttempts int, backoff BackoffFn) *Retry {
	return &Retry{
		MaxAttempts: maxAttempts,
		Backoff:     backoff,
	}
}

// NewSensibleDefaultRetry returns a Retry with a reasonable default configuration suitable for most use cases
func NewSensibleDefaultRetry() *Retry {
	return NewRetry(5, ExponentialBackoff(100*time.Millisecond))
}

// WaitForNextAttempt waits for the appropriate backoff duration before the next retry attempt.
// Returns nil if the next attempt should proceed, context.Err() if the context was cancelled,
// or ErrMaxAttemptsExceeded if max attempts have been reached.
func (r *Retry) WaitForNextAttempt(ctx context.Context, nextAttemptCounter int) error {
	if nextAttemptCounter > r.MaxAttempts {
		return ErrMaxAttemptsExceeded
	}

	return wait(ctx, r.Backoff(nextAttemptCounter))
}

// PlateauedBackoff returns the backoff duration for nextAttemptCounter with the counter held at MaxAttempts once it climbs past that point, so the wait stops growing instead of growing forever.
// A loop that retries indefinitely needs this: BackoffFn implementations like ExponentialBackoff are unbounded, and a counter left to climb eventually produces a duration that overflows the int64 a time.Duration is.
// The counter is also floored at 1, because a BackoffFn conventionally returns zero for the 0th attempt to allow an immediate first try, and a zero wait inside a retry loop is a busy spin.
//
// MaxAttempts is read here as "the attempt at which the wait stops growing" rather than "the last attempt allowed", which is the reading WaitForNextAttemptIndefinitely goes with.
func (r *Retry) PlateauedBackoff(nextAttemptCounter int) time.Duration {
	plateau := max(r.MaxAttempts, 1)
	return r.Backoff(min(max(nextAttemptCounter, 1), plateau))
}

// WaitForNextAttemptIndefinitely waits PlateauedBackoff before the next attempt and never gives up, so unlike WaitForNextAttempt it cannot return ErrMaxAttemptsExceeded.
// It is for callers that must outlive any individual failure, such as a long-running loop that logs and retries rather than exiting; MaxAttempts bounds how far the wait grows rather than how many attempts are allowed.
// Returns nil when the next attempt should proceed, or context.Err() if the context ended.
func (r *Retry) WaitForNextAttemptIndefinitely(ctx context.Context, nextAttemptCounter int) error {
	return wait(ctx, r.PlateauedBackoff(nextAttemptCounter))
}

// wait sleeps for d, returning early with context.Err() if ctx ends first.
// A non-positive d means proceed immediately, subject to the context still being live.
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func ConstantBackoff(delay time.Duration) BackoffFn {
	return func(nextAttemptCounter int) time.Duration {
		return delay
	}
}

func LinearBackoff(baseDelay time.Duration) BackoffFn {
	return func(nextAttemptCounter int) time.Duration {
		return baseDelay * time.Duration(nextAttemptCounter)
	}
}

func ExponentialBackoff(baseDelay time.Duration) BackoffFn {
	return func(nextAttemptCounter int) time.Duration {
		if nextAttemptCounter <= 0 {
			return 0
		}
		return baseDelay * time.Duration(math.Pow(2, float64(nextAttemptCounter-1)))
	}
}

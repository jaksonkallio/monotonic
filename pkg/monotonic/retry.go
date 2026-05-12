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

	wait := r.Backoff(nextAttemptCounter)
	if wait <= 0 {
		return ctx.Err()
	}

	timer := time.NewTimer(wait)
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

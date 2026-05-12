package monotonic

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConstantBackoff(t *testing.T) {
	backoff := ConstantBackoff(50 * time.Millisecond)

	for i := 0; i < 5; i++ {
		d := backoff(i)
		if d != 50*time.Millisecond {
			t.Errorf("attempt %d: expected 50ms, got %v", i, d)
		}
	}
}

func TestLinearBackoff(t *testing.T) {
	backoff := LinearBackoff(10 * time.Millisecond)

	cases := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 0},
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 30 * time.Millisecond},
	}

	for _, tc := range cases {
		d := backoff(tc.attempt)
		if d != tc.expected {
			t.Errorf("attempt %d: expected %v, got %v", tc.attempt, tc.expected, d)
		}
	}
}

func TestExponentialBackoff(t *testing.T) {
	backoff := ExponentialBackoff(10 * time.Millisecond)

	cases := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 0},                     // first attempt, no wait
		{1, 10 * time.Millisecond}, // 10 * 2^0
		{2, 20 * time.Millisecond}, // 10 * 2^1
		{3, 40 * time.Millisecond}, // 10 * 2^2
		{4, 80 * time.Millisecond}, // 10 * 2^3
	}

	for _, tc := range cases {
		d := backoff(tc.attempt)
		if d != tc.expected {
			t.Errorf("attempt %d: expected %v, got %v", tc.attempt, tc.expected, d)
		}
	}
}

func TestExponentialBackoffNegativeAttempt(t *testing.T) {
	backoff := ExponentialBackoff(10 * time.Millisecond)

	d := backoff(-1)
	if d != 0 {
		t.Errorf("negative attempt: expected 0, got %v", d)
	}
}

func TestRetryWaitForNextAttempt_MaxAttempts(t *testing.T) {
	ctx := context.Background()
	retry := NewRetry(3, ConstantBackoff(0))

	var attempts int
	for attempt := 0; retry.WaitForNextAttempt(ctx, attempt) == nil; attempt++ {
		attempts++
	}

	if attempts != 4 {
		t.Errorf("expected 4 attempts with MaxAttempts=3, got %d", attempts)
	}
}

func TestRetryWaitForNextAttempt_ZeroMaxAttempts(t *testing.T) {
	ctx := context.Background()
	retry := NewRetry(0, ConstantBackoff(0))

	var attempts int
	for attempt := 0; retry.WaitForNextAttempt(ctx, attempt) == nil; attempt++ {
		attempts++
	}

	if attempts != 1 {
		t.Errorf("expected 1 attempt with MaxAttempts=0, got %d", attempts)
	}
}

func TestRetryWaitForNextAttempt_OneMaxAttempt(t *testing.T) {
	ctx := context.Background()
	retry := NewRetry(1, ConstantBackoff(0))

	var attempts int
	for attempt := 0; retry.WaitForNextAttempt(ctx, attempt) == nil; attempt++ {
		attempts++
	}

	if attempts != 2 {
		t.Errorf("expected 2 attempts with MaxAttempts=1, got %d", attempts)
	}
}

func TestRetryWaitForNextAttempt_ReturnsErrMaxAttempts(t *testing.T) {
	ctx := context.Background()
	retry := NewRetry(2, ConstantBackoff(0))

	// Exhaust attempts
	for attempt := 0; retry.WaitForNextAttempt(ctx, attempt) == nil; attempt++ {
	}

	err := retry.WaitForNextAttempt(ctx, 3)
	if !errors.Is(err, ErrMaxAttemptsExceeded) {
		t.Errorf("expected ErrMaxAttemptsExceeded, got %v", err)
	}
}

func TestRetryWaitForNextAttempt_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	retry := NewRetry(5, ConstantBackoff(100*time.Millisecond))

	err := retry.WaitForNextAttempt(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRetryWaitForNextAttempt_ContextCancelledDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	retry := NewRetry(5, ConstantBackoff(500*time.Millisecond))

	// Cancel after a short delay
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := retry.WaitForNextAttempt(ctx, 1)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if elapsed >= 200*time.Millisecond {
		t.Errorf("expected early return on cancel, but waited %v", elapsed)
	}
}

func TestRetryWaitForNextAttempt_ZeroDelayChecksContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	retry := NewRetry(5, ConstantBackoff(0))

	// Even with zero delay, a cancelled context should return an error
	err := retry.WaitForNextAttempt(ctx, 0)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled for zero delay with cancelled ctx, got %v", err)
	}
}

func TestNewSensibleDefaultRetry(t *testing.T) {
	retry := NewSensibleDefaultRetry()

	if retry.MaxAttempts != 5 {
		t.Errorf("expected MaxAttempts=5, got %d", retry.MaxAttempts)
	}
	if retry.Backoff == nil {
		t.Error("expected Backoff to be set")
	}

	// First attempt should have zero delay
	d := retry.Backoff(0)
	if d != 0 {
		t.Errorf("expected 0 delay for attempt 0, got %v", d)
	}

	// Second attempt should be 100ms base
	d = retry.Backoff(1)
	if d != 100*time.Millisecond {
		t.Errorf("expected 100ms for attempt 1, got %v", d)
	}
}

func TestRetry_OnAttemptFiresOncePerAttempt(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()

	// Pre-seed counter=1 so a fresh aggregate's first append (counter=1) conflicts and forces a retry.
	if err := store.Append(ctx, AggregateEvent{
		AggregateType: "counter",
		AggregateID:   "retry-hook",
		Event:         AcceptedEvent{Event: NewEvent(eventIncremented, incrementedPayload{Amount: 1}), Counter: 1, AcceptedAt: time.Now()},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	c, err := loadCounter(ctx, store, "retry-hook")
	if err != nil {
		t.Fatalf("loadCounter: %v", err)
	}

	// Stale local view of the aggregate: c thinks counter=1, but store also has counter=1.
	// Force a stale state on a fresh aggregate by constructing a base bypassing Hydrate.
	staleBase := &AggregateBase{eventStream: eventStream{ID: c.ID, store: store}}
	stale := &counter{AggregateBase: staleBase}
	staleBase.self = stale

	var attempts []int
	var errsObserved []error
	retry := Retry{
		MaxAttempts: 5,
		Backoff:     ConstantBackoff(0),
		OnAttempt: func(attempt int, err error) {
			attempts = append(attempts, attempt)
			errsObserved = append(errsObserved, err)
		},
	}

	if err := stale.AcceptThenApplyRetryable(ctx, retry, NewEvent(eventIncremented, incrementedPayload{Amount: 1})); err != nil {
		t.Fatalf("AcceptThenApplyRetryable: %v", err)
	}

	if len(attempts) == 0 {
		t.Fatal("OnAttempt should fire at least once")
	}
	// Attempt indexes must be 0,1,2,... contiguously.
	for i, a := range attempts {
		if a != i {
			t.Errorf("attempts[%d] = %d, want %d (must be contiguous from 0)", i, a, i)
		}
	}
	// Last attempt observed must be a success (nil error); all preceding must be non-nil.
	if errsObserved[len(errsObserved)-1] != nil {
		t.Errorf("last OnAttempt err must be nil on success, got %v", errsObserved[len(errsObserved)-1])
	}
	for i := 0; i < len(errsObserved)-1; i++ {
		if errsObserved[i] == nil {
			t.Errorf("intermediate attempt %d should have a non-nil error, got nil", i)
		}
	}
}

func TestRetry_OnAttemptFiresOnceWhenNoRetryNeeded(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	c, _ := loadCounter(ctx, store, "first-try")

	var calls int
	retry := Retry{
		MaxAttempts: 5,
		Backoff:     ConstantBackoff(0),
		OnAttempt:   func(attempt int, err error) { calls++ },
	}

	if err := c.AcceptThenApplyRetryable(ctx, retry, NewEvent(eventIncremented, incrementedPayload{Amount: 1})); err != nil {
		t.Fatalf("AcceptThenApplyRetryable: %v", err)
	}
	if calls != 1 {
		t.Errorf("OnAttempt should fire exactly once on first-try success, got %d", calls)
	}
}

func TestErrMaxAttemptsExceeded(t *testing.T) {
	err := ErrMaxAttemptsExceeded

	expected := "maximum retry attempts exceeded"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
}

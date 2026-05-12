package monotonic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

const (
	eventIncremented = "incremented"
	eventDecremented = "decremented"
)

type incrementedPayload struct {
	Amount int `json:"amount"`
}

type counter struct {
	*AggregateBase
	Value int
}

func (c *counter) Apply(event AcceptedEvent) {
	switch event.Type {
	case eventIncremented:
		if p, err := ParsePayload[incrementedPayload](event); err == nil {
			c.Value += p.Amount
		}
	case eventDecremented:
		if p, err := ParsePayload[incrementedPayload](event); err == nil {
			c.Value -= p.Amount
		}
	}
}

func (c *counter) ShouldAccept(event Event) error {
	switch event.Type {
	case eventIncremented:
		return nil
	case eventDecremented:
		var p incrementedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		if c.Value-p.Amount < 0 {
			return errors.New("counter would go negative")
		}
		return nil
	}
	return errors.New("unknown event type")
}

func loadCounter(ctx context.Context, store Store, id string) (*counter, error) {
	return Hydrate(ctx, store, "counter", id, func(base *AggregateBase) *counter {
		return &counter{AggregateBase: base}
	})
}

func TestNewEvent(t *testing.T) {
	e := NewEvent("incremented", incrementedPayload{Amount: 5})
	if e.Type != "incremented" {
		t.Errorf("expected type 'incremented', got %q", e.Type)
	}
	var p incrementedPayload
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if p.Amount != 5 {
		t.Errorf("expected amount 5, got %d", p.Amount)
	}
}

func TestHydrate(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, err := loadCounter(ctx, store, "c1")
	if err != nil {
		t.Fatalf("loadCounter: %v", err)
	}

	// Apply some events
	if err := c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 3})); err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 7})); err != nil {
		t.Fatal(err)
	}
	if c.Value != 10 {
		t.Fatalf("expected 10, got %d", c.Value)
	}

	// Hydrate a fresh instance and verify state rebuilt
	c2, err := loadCounter(ctx, store, "c1")
	if err != nil {
		t.Fatalf("loadCounter c2: %v", err)
	}
	if c2.Value != 10 {
		t.Errorf("hydrated value: expected 10, got %d", c2.Value)
	}
	if c2.Counter() != c.Counter() {
		t.Errorf("counter mismatch: expected %d, got %d", c.Counter(), c2.Counter())
	}
}

func TestAcceptThenApply(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "c1")

	if err := c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 2})); err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 3})); err != nil {
		t.Fatal(err)
	}

	if c.Value != 5 {
		t.Errorf("expected 5, got %d", c.Value)
	}
	if c.Counter() != 2 {
		t.Errorf("expected counter 2, got %d", c.Counter())
	}
}

func TestAcceptThenApply_Rejection(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "c1")

	// Decrement on a zero counter should be rejected
	err := c.AcceptThenApply(ctx, NewEvent(eventDecremented, incrementedPayload{Amount: 1}))
	if err == nil {
		t.Fatal("expected rejection error, got nil")
	}
	if c.Value != 0 {
		t.Errorf("state should be unchanged, got value %d", c.Value)
	}
	if c.Counter() != 0 {
		t.Errorf("counter should be unchanged, got %d", c.Counter())
	}
}

func TestAcceptThenApply_NoEvents(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "c1")

	err := c.AcceptThenApply(ctx)
	if err != nil {
		t.Fatalf("no-op should not error: %v", err)
	}
	if c.Counter() != 0 {
		t.Errorf("counter should remain 0, got %d", c.Counter())
	}
}

func TestAccept(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "c1")

	accepted, err := c.Accept(ctx,
		NewEvent(eventIncremented, incrementedPayload{Amount: 1}),
		NewEvent(eventIncremented, incrementedPayload{Amount: 2}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 2 {
		t.Fatalf("expected 2 accepted events, got %d", len(accepted))
	}
	if accepted[0].Counter != 1 {
		t.Errorf("first event counter: expected 1, got %d", accepted[0].Counter)
	}
	if accepted[1].Counter != 2 {
		t.Errorf("second event counter: expected 2, got %d", accepted[1].Counter)
	}

	// State should NOT have changed (Accept doesn't apply)
	if c.Value != 0 {
		t.Errorf("value should be 0 after Accept, got %d", c.Value)
	}
}

func TestAcceptThenApplyRetryable(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "c1")

	// Apply an event directly via another instance to simulate a concurrent write
	c2, _ := loadCounter(ctx, store, "c1")
	if err := c2.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 10})); err != nil {
		t.Fatal(err)
	}

	// c is now stale (counter=0, store has counter=1).
	// AcceptThenApplyRetryable should catch up and succeed on retry.
	retry := NewRetry(3, ExponentialBackoff(0))
	err := c.AcceptThenApplyRetryable(ctx, *retry, NewEvent(eventIncremented, incrementedPayload{Amount: 5}))
	if err != nil {
		t.Fatalf("retryable apply failed: %v", err)
	}

	if c.Value != 15 {
		t.Errorf("expected 15, got %d", c.Value)
	}
}

func TestProjector(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c1, _ := loadCounter(ctx, store, "c1")
	c1.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
	c1.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 2}))

	c2, _ := loadCounter(ctx, store, "c2")
	c2.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 10}))

	persistence := NewInMemoryProjectionPersistence[incrementSummary]()

	projector, err := NewProjector(ctx, store, newIncrementLogic(), persistence, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	processed, err := projector.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if processed != 3 {
		t.Errorf("expected 3 events processed, got %d", processed)
	}
	summary, _ := persistence.Get(ctx, ProjectionKeySummary)
	if summary.Total != 13 {
		t.Errorf("expected total 13, got %d", summary.Total)
	}
	if projector.GlobalCounter() == 0 {
		t.Error("global counter should be > 0")
	}

	c1.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 100}))
	processed, err = projector.Update(ctx)
	if err != nil {
		t.Fatalf("second Update: %v", err)
	}
	if processed != 1 {
		t.Errorf("expected 1 new event, got %d", processed)
	}
	summary, _ = persistence.Get(ctx, ProjectionKeySummary)
	if summary.Total != 113 {
		t.Errorf("expected total 113, got %d", summary.Total)
	}

	// Resuming a fresh Projector against the same persistence should pick up only events past the stored counter.
	c2.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 50}))

	resumed, err := NewProjector(ctx, store, newIncrementLogic(), persistence, 0)
	if err != nil {
		t.Fatalf("NewProjector resume: %v", err)
	}
	processed, err = resumed.Update(ctx)
	if err != nil {
		t.Fatalf("Update from saved counter: %v", err)
	}
	if processed != 1 {
		t.Errorf("expected 1 event from saved counter, got %d", processed)
	}
	summary, _ = persistence.Get(ctx, ProjectionKeySummary)
	if summary.Total != 163 {
		t.Errorf("expected total 163, got %d", summary.Total)
	}
}

type incrementSummary struct {
	Total int
}

func newIncrementLogic() ProjectorLogic[incrementSummary] {
	return NewDispatch[incrementSummary]().
		On("counter", eventIncremented, applyIncrement)
}

func applyIncrement(ctx context.Context, reader ProjectionReader[incrementSummary], event AggregateEvent) ([]Projected[incrementSummary], error) {
	payload, err := ParsePayload[incrementedPayload](event.Event)
	if err != nil {
		return nil, err
	}
	return MutateByKey(ctx, reader, ProjectionKeySummary, func(s *incrementSummary) error {
		s.Total += payload.Amount
		return nil
	})
}

// High-Concurrency Tests

func TestConcurrentAppends_SameAggregate(t *testing.T) {
	// Test multiple goroutines trying to append to the same aggregate
	// Should see counter conflicts and successful retries
	store := NewInMemoryStore()
	ctx := context.Background()

	const numGoroutines = 50
	const eventsPerGoroutine = 10

	// Track results
	type result struct {
		goroutineID int
		succeeded   int
		failed      int
	}
	results := make(chan result, numGoroutines)

	// Launch concurrent writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			r := result{goroutineID: id}
			// Each goroutine needs its own instance to work with
			localCounter, _ := loadCounter(ctx, store, "concurrent")
			retry := NewRetry(50, ExponentialBackoff(0))

			for j := 0; j < eventsPerGoroutine; j++ {
				err := localCounter.AcceptThenApplyRetryable(ctx, *retry,
					NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				if err != nil {
					r.failed++
				} else {
					r.succeeded++
				}
			}
			results <- r
		}(i)
	}

	// Collect results
	totalSucceeded := 0
	totalFailed := 0
	for i := 0; i < numGoroutines; i++ {
		r := <-results
		totalSucceeded += r.succeeded
		totalFailed += r.failed
	}

	// Verify all events were written successfully
	expectedEvents := numGoroutines * eventsPerGoroutine
	if totalSucceeded != expectedEvents {
		t.Errorf("expected %d successful events, got %d (failed: %d)",
			expectedEvents, totalSucceeded, totalFailed)
	}

	// Verify final aggregate state
	finalCounter, _ := loadCounter(ctx, store, "concurrent")
	if finalCounter.Value != expectedEvents {
		t.Errorf("expected final value %d, got %d", expectedEvents, finalCounter.Value)
	}
	if finalCounter.Counter() != int64(expectedEvents) {
		t.Errorf("expected counter %d, got %d", expectedEvents, finalCounter.Counter())
	}
}

func TestConcurrentAppends_DifferentAggregates(t *testing.T) {
	// Test multiple goroutines appending to different aggregates
	// All should succeed without conflicts
	store := NewInMemoryStore()
	ctx := context.Background()

	const numAggregates = 100
	const eventsPerAggregate = 20

	type result struct {
		aggregateID string
		finalValue  int
		err         error
	}
	results := make(chan result, numAggregates)

	// Launch concurrent writers to different aggregates
	for i := 0; i < numAggregates; i++ {
		go func(id int) {
			aggregateID := fmt.Sprintf("counter-%d", id)
			c, _ := loadCounter(ctx, store, aggregateID)

			var lastErr error
			for j := 0; j < eventsPerAggregate; j++ {
				err := c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				if err != nil {
					lastErr = err
					break
				}
			}

			results <- result{
				aggregateID: aggregateID,
				finalValue:  c.Value,
				err:         lastErr,
			}
		}(i)
	}

	// Collect results
	for i := 0; i < numAggregates; i++ {
		r := <-results
		if r.err != nil {
			t.Errorf("aggregate %s failed: %v", r.aggregateID, r.err)
		}
		if r.finalValue != eventsPerAggregate {
			t.Errorf("aggregate %s: expected value %d, got %d",
				r.aggregateID, eventsPerAggregate, r.finalValue)
		}
	}

	// Verify total events in store
	events, err := store.LoadGlobalEvents(ctx, []EventFilter{{AggregateType: "counter"}}, 0, 0)
	if err != nil {
		t.Fatalf("LoadGlobalEvents: %v", err)
	}
	expectedTotal := numAggregates * eventsPerAggregate
	if len(events) != expectedTotal {
		t.Errorf("expected %d total events in store, got %d", expectedTotal, len(events))
	}
}

func TestConcurrentAppends_MixedReadWrite(t *testing.T) {
	// Test concurrent reads and writes
	// Verify global counter ordering remains consistent
	store := NewInMemoryStore()
	ctx := context.Background()

	const numWriters = 10
	const numReaders = 10
	const eventsPerWriter = 50

	done := make(chan bool)

	// Launch writers
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			aggregateID := fmt.Sprintf("writer-%d", id)
			c, _ := loadCounter(ctx, store, aggregateID)
			retry := NewRetry(5, ExponentialBackoff(0))

			for j := 0; j < eventsPerWriter; j++ {
				c.AcceptThenApplyRetryable(ctx, *retry,
					NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
			}
			done <- true
		}(i)
	}

	// Launch readers that continuously check global event ordering
	readerErrors := make(chan error, numReaders)
	stopReading := make(chan bool)

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			for {
				select {
				case <-stopReading:
					readerErrors <- nil
					return
				default:
					events, err := store.LoadGlobalEvents(ctx, []EventFilter{{AggregateType: "counter"}}, 0, 0)
					if err != nil {
						readerErrors <- err
						return
					}

					// Verify global counters are monotonically increasing within the returned slice
					var lastGlobalCounter int64 = 0
					for _, event := range events {
						if event.Event.GlobalCounter <= lastGlobalCounter {
							readerErrors <- errors.New("global counter not monotonically increasing")
							return
						}
						lastGlobalCounter = event.Event.GlobalCounter
					}
				}
			}
		}(i)
	}

	// Wait for all writers to complete
	for i := 0; i < numWriters; i++ {
		<-done
	}

	// Stop readers
	close(stopReading)
	for i := 0; i < numReaders; i++ {
		if err := <-readerErrors; err != nil {
			t.Errorf("reader error: %v", err)
		}
	}

	// Verify final event count
	events, _ := store.LoadGlobalEvents(ctx, []EventFilter{{AggregateType: "counter"}}, 0, 0)
	expectedTotal := numWriters * eventsPerWriter
	if len(events) != expectedTotal {
		t.Errorf("expected %d total events, got %d", expectedTotal, len(events))
	}

	// Verify global counter sequence has no gaps
	for i, event := range events {
		expectedGlobalCounter := i + 1
		if event.Event.GlobalCounter != int64(expectedGlobalCounter) {
			t.Errorf("event %d: expected global counter %d, got %d",
				i, expectedGlobalCounter, event.Event.GlobalCounter)
		}
	}
}

func TestConcurrentAppends_WithRejections(t *testing.T) {
	// Test concurrent appends with some events being rejected by business logic
	// Verifies that rejection doesn't break counter consistency
	store := NewInMemoryStore()
	ctx := context.Background()

	const numGoroutines = 20

	results := make(chan int, numGoroutines)

	// Pre-populate the counter with some value
	initial, _ := loadCounter(ctx, store, "rejection-test")
	initial.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 100}))

	// Each goroutine will try to decrement, some will be rejected
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			c, _ := loadCounter(ctx, store, "rejection-test")
			retry := NewRetry(10, ExponentialBackoff(0))

			// Try to decrement by a large amount (might be rejected if counter too low)
			err := c.AcceptThenApplyRetryable(ctx, *retry,
				NewEvent(eventDecremented, incrementedPayload{Amount: 10}))

			if err != nil {
				// Rejection is expected for some goroutines
				results <- 0
			} else {
				results <- 1
			}
		}(i)
	}

	// Collect results
	successCount := 0
	for i := 0; i < numGoroutines; i++ {
		if r := <-results; r > 0 {
			successCount++
		}
	}

	// Verify final state makes sense
	finalCounter, _ := loadCounter(ctx, store, "rejection-test")
	expectedValue := 100 - (successCount * 10)
	if finalCounter.Value != expectedValue {
		t.Errorf("expected final value %d, got %d (successCount: %d)", expectedValue, finalCounter.Value, successCount)
	}

	// Verify event count: initial increment + successful decrements
	expectedEventCount := 1 + successCount
	if finalCounter.Counter() != int64(expectedEventCount) {
		t.Errorf("expected %d events, got %d", expectedEventCount, finalCounter.Counter())
	}
}

func TestConcurrentAppends_HighContentionRetries(t *testing.T) {
	// Stress test the retry mechanism with very high contention
	store := NewInMemoryStore()
	ctx := context.Background()

	const numGoroutines = 100
	const eventsPerGoroutine = 5

	results := make(chan bool, numGoroutines)

	// All goroutines compete for the same aggregate
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			c, _ := loadCounter(ctx, store, "high-contention")
			// More retries for high contention scenario
			retry := NewRetry(20, ExponentialBackoff(0))

			success := true
			for j := 0; j < eventsPerGoroutine; j++ {
				err := c.AcceptThenApplyRetryable(ctx, *retry,
					NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				if err != nil {
					success = false
					break
				}
			}
			results <- success
		}(i)
	}

	// Collect results
	successCount := 0
	for i := 0; i < numGoroutines; i++ {
		if <-results {
			successCount++
		}
	}

	// With sufficient retries, all should eventually succeed
	if successCount != numGoroutines {
		t.Logf("warning: only %d/%d goroutines succeeded under high contention",
			successCount, numGoroutines)
	}

	// Verify final state matches successful writes
	finalCounter, _ := loadCounter(ctx, store, "high-contention")
	expectedValue := successCount * eventsPerGoroutine
	if finalCounter.Value != expectedValue {
		t.Errorf("expected final value %d, got %d", expectedValue, finalCounter.Value)
	}
}

func TestBatchAppend(t *testing.T) {
	// Test that batch appends work correctly (multiple events in single AcceptThenApply)
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "batch-test")

	// Append a batch of 5 events
	err := c.AcceptThenApply(ctx,
		NewEvent(eventIncremented, incrementedPayload{Amount: 1}),
		NewEvent(eventIncremented, incrementedPayload{Amount: 2}),
		NewEvent(eventIncremented, incrementedPayload{Amount: 3}),
		NewEvent(eventIncremented, incrementedPayload{Amount: 4}),
		NewEvent(eventIncremented, incrementedPayload{Amount: 5}),
	)
	if err != nil {
		t.Fatalf("batch append failed: %v", err)
	}

	// Verify all events were applied
	expectedValue := 1 + 2 + 3 + 4 + 5
	if c.Value != expectedValue {
		t.Errorf("expected value %d, got %d", expectedValue, c.Value)
	}
	if c.Counter() != 5 {
		t.Errorf("expected counter 5, got %d", c.Counter())
	}

	// Append another batch to verify it works after existing events
	err = c.AcceptThenApply(ctx,
		NewEvent(eventIncremented, incrementedPayload{Amount: 10}),
		NewEvent(eventIncremented, incrementedPayload{Amount: 20}),
	)
	if err != nil {
		t.Fatalf("second batch append failed: %v", err)
	}

	if c.Value != expectedValue+30 {
		t.Errorf("expected value %d, got %d", expectedValue+30, c.Value)
	}
	if c.Counter() != 7 {
		t.Errorf("expected counter 7, got %d", c.Counter())
	}
}

func TestConcurrentBatchAppends(t *testing.T) {
	// Test concurrent batch appends to the same aggregate
	store := NewInMemoryStore()
	ctx := context.Background()

	const numGoroutines = 20
	const batchSize = 5

	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			c, _ := loadCounter(ctx, store, "batch-concurrent")
			retry := NewRetry(15, ExponentialBackoff(0))

			// Create a batch of events
			events := make([]Event, batchSize)
			for j := 0; j < batchSize; j++ {
				events[j] = NewEvent(eventIncremented, incrementedPayload{Amount: 1})
			}

			err := c.AcceptThenApplyRetryable(ctx, *retry, events...)
			results <- err
		}(i)
	}

	// Collect results
	failCount := 0
	for i := 0; i < numGoroutines; i++ {
		if err := <-results; err != nil {
			failCount++
		}
	}

	if failCount > 0 {
		t.Errorf("%d goroutines failed to append batches", failCount)
	}

	// Verify final state
	finalCounter, _ := loadCounter(ctx, store, "batch-concurrent")
	expectedValue := (numGoroutines - failCount) * batchSize
	if finalCounter.Value != expectedValue {
		t.Errorf("expected final value %d, got %d", expectedValue, finalCounter.Value)
	}
	if finalCounter.Counter() != int64(expectedValue) {
		t.Errorf("expected counter %d, got %d", expectedValue, finalCounter.Counter())
	}
}

func TestConcurrentAppends_RapidFire(t *testing.T) {
	// Test rapid-fire appends from multiple goroutines
	// Each goroutine appends multiple single events in quick succession
	store := NewInMemoryStore()
	ctx := context.Background()

	const numGoroutines = 30
	const eventsPerGoroutine = 10

	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			c, _ := loadCounter(ctx, store, "rapid-fire")
			retry := NewRetry(50, ExponentialBackoff(0))

			var lastErr error
			for j := 0; j < eventsPerGoroutine; j++ {
				err := c.AcceptThenApplyRetryable(ctx, *retry,
					NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				if err != nil {
					lastErr = err
					break
				}
			}
			results <- lastErr
		}(i)
	}

	// Collect results
	failCount := 0
	for i := 0; i < numGoroutines; i++ {
		if err := <-results; err != nil {
			failCount++
		}
	}

	if failCount > 0 {
		t.Errorf("%d goroutines failed rapid-fire appends", failCount)
	}

	// Verify final state
	finalCounter, _ := loadCounter(ctx, store, "rapid-fire")
	expectedValue := (numGoroutines - failCount) * eventsPerGoroutine
	if finalCounter.Value != expectedValue {
		t.Errorf("expected final value %d, got %d", expectedValue, finalCounter.Value)
	}
}

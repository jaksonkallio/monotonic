// Package monotonic provides a lightweight framework for building event-sourced aggregates with monotonic state changes
package monotonic

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type AggregateID struct {
	Type string
	ID   string
}

func NewAggregateID(aggregateType, id string) AggregateID {
	return AggregateID{Type: aggregateType, ID: id}
}

// Logic defines business logic for an aggregate's state changes due to events
type Logic interface {
	// Apply applies an event to the aggregate's state
	// This cannot fail, the event had already been previously accepted
	Apply(event AcceptedEvent)

	// ShouldAccept checks if a new proposed event is valid given current state
	// Return an error to reject the event
	// Called internally by AggregateBase.Accept
	ShouldAccept(event Event) error
}

// AggregateBase provides the infrastructure for aggregates.
// Embed this in your aggregate structs to get Record() and hydration support.
type AggregateBase struct {
	eventStream
	self Logic

	// newInstance builds a fresh, zero-state instance of the concrete aggregate type.
	// Captured by Hydrate so Accept can replay a throwaway copy of the aggregate when validating a multi-event batch.
	newInstance func(*AggregateBase) Logic
}

// AcceptThenApply applies the event to the aggregate after it has been accepted, effectively recording the event and updating the state in one step.
func (b *AggregateBase) AcceptThenApply(ctx context.Context, events ...Event) error {
	if len(events) == 0 {
		// No-op
		return nil
	}

	// Accept all events atomically
	acceptedEvents, err := b.Accept(ctx, events...)
	if err != nil {
		return fmt.Errorf("accept: %w", err)
	}

	// Simple conversion from []AcceptedEvent to []AggregateEvent for appending to store
	aggregateEvents := make([]AggregateEvent, len(acceptedEvents))
	for i, acceptedEvent := range acceptedEvents {
		aggregateEvents[i] = AggregateEvent{
			Event:         acceptedEvent,
			AggregateType: b.ID.Type,
			AggregateID:   b.ID.ID,
		}
	}

	// Append all events atomically
	if err := b.append(ctx, aggregateEvents...); err != nil {
		return err
	}

	// Apply all accepted and appended events to local state
	for _, acceptedEvent := range acceptedEvents {
		b.self.Apply(acceptedEvent)
		b.applied(acceptedEvent)
	}

	return nil
}

func (b *AggregateBase) AcceptThenApplyRetryable(ctx context.Context, retry Retry, events ...Event) error {
	var lastAttemptErr error
	for attemptCounter := 0; ; attemptCounter++ {
		if err := retry.WaitForNextAttempt(ctx, attemptCounter); err != nil {
			if errors.Is(err, ErrMaxAttemptsExceeded) {
				return fmt.Errorf("%w, last attempt error: %w", err, lastAttemptErr)
			}
			return fmt.Errorf("wait for next attempt: %w", err)
		}

		lastAttemptErr = b.AcceptThenApply(ctx, events...)
		if retry.OnAttempt != nil {
			retry.OnAttempt(attemptCounter, lastAttemptErr)
		}
		if lastAttemptErr == nil {
			return nil
		}
	}
}

// Accept accepts events without applying yet; use when preparing events for multiple aggregates to be committed atomically.
func (b *AggregateBase) Accept(ctx context.Context, events ...Event) ([]AcceptedEvent, error) {
	if len(events) == 0 {
		// No-op
		return nil, nil
	}

	if err := b.catchUp(ctx, b.self.Apply); err != nil {
		return nil, err
	}

	// Capture the counter, all accepted events will be an incremental sequence from this value
	acceptMultiCounter := b.nextCounter()

	// When accepting a single event, there is no need to create a speculative copy.
	// The current aggregate can be used directly because there's nothing to "roll back" if the accept fails.
	speculative := len(events) > 1
	candidate := b.self
	if speculative {
		copied, err := b.copyState(ctx)
		if err != nil {
			return nil, fmt.Errorf("copy state for batch validation: %w", err)
		}
		candidate = copied
	}

	acceptedEvents := make([]AcceptedEvent, len(events))
	for i, event := range events {
		if err := candidate.ShouldAccept(event); err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		acceptedEvents[i] = AcceptedEvent{
			Event:      event,
			AcceptedAt: time.Now(),
			Counter:    acceptMultiCounter + int64(i),
		}
		if speculative {
			// Advance the copy so the next event's ShouldAccept sees this event's effect.
			candidate.Apply(acceptedEvents[i])
		}
	}

	return acceptedEvents, nil
}

// copyState builds a throwaway copy of the aggregate's current state by replaying its history into a fresh instance of the concrete aggregate type.
func (b *AggregateBase) copyState(ctx context.Context) (Logic, error) {
	if b.newInstance == nil {
		return nil, fmt.Errorf("aggregate was not built by Hydrate, so its state cannot be copied")
	}

	copyBase := &AggregateBase{
		eventStream: eventStream{
			ID:    b.ID,
			store: b.store,
		},
		newInstance: b.newInstance,
	}
	copied := b.newInstance(copyBase)
	copyBase.self = copied

	events, err := b.store.LoadAggregateEvents(ctx, b.ID.Type, b.ID.ID, 0)
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}

	// Replay only as far as the live aggregate has seen.
	// A concurrent writer may have appended past that point, and those events are not reflected in the counters this batch is being assigned.
	for _, event := range events {
		if event.Counter > b.counter {
			break
		}
		copied.Apply(event)
		copyBase.applied(event)
	}

	return copied, nil
}

// Hydrate loads an aggregate from the store by replaying all events.
// The init function should create a new aggregate instance with the base embedded.
func Hydrate[T Logic](ctx context.Context, store Store, aggType, id string, init func(*AggregateBase) T) (T, error) {
	events, err := store.LoadAggregateEvents(ctx, aggType, id, 0)
	if err != nil {
		var zero T
		return zero, err
	}

	base := &AggregateBase{
		eventStream: eventStream{
			ID:    NewAggregateID(aggType, id),
			store: store,
		},
		newInstance: func(b *AggregateBase) Logic { return init(b) },
	}
	agg := init(base)
	base.self = agg

	for _, e := range events {
		agg.Apply(e)
		base.applied(e)
	}

	return agg, nil
}

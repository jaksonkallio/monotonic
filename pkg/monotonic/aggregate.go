package monotonic

import (
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
}

// AcceptThenApply applies the event to the aggregate after it has been accepted, effectively recording the event and updating the state in one step.
func (b *AggregateBase) AcceptThenApply(events ...Event) error {
	if len(events) == 0 {
		// No-op
		return nil
	}

	// Accept all events atomically
	acceptedEvents, err := b.Accept(events...)
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
	if err := b.append(aggregateEvents...); err != nil {
		return err
	}

	// Apply all accepted and appended events to local state
	for _, acceptedEvent := range acceptedEvents {
		b.self.Apply(acceptedEvent)
		b.applied(acceptedEvent)
	}

	return nil
}

// Accept accepts events without applying yet
// Use when you need to prepare events for multiple aggregates to be committed atomically, such as in a saga
func (b *AggregateBase) Accept(events ...Event) ([]AcceptedEvent, error) {
	if len(events) == 0 {
		// No-op
		return nil, nil
	}

	if err := b.catchUp(b.self.Apply); err != nil {
		return nil, err
	}

	// Capture the counter, all accepted events will be an incremental sequence from this value
	acceptMultiCounter := b.nextCounter()

	acceptedEvents := make([]AcceptedEvent, len(events))
	for i, event := range events {
		if err := b.self.ShouldAccept(event); err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		acceptedEvents[i] = AcceptedEvent{
			Event:      event,
			AcceptedAt: time.Now(),
			Counter:    acceptMultiCounter + int64(i),
		}
	}

	return acceptedEvents, nil
}

// Hydrate loads an aggregate from the store by replaying all events.
// The init function should create a new aggregate instance with the base embedded.
func Hydrate[T Logic](store Store, aggType, id string, init func(*AggregateBase) T) (T, error) {
	events, err := store.Load(aggType, id)
	if err != nil {
		var zero T
		return zero, err
	}

	base := &AggregateBase{
		eventStream: eventStream{
			ID:    NewAggregateID(aggType, id),
			store: store,
		},
	}
	agg := init(base)
	base.self = agg

	for _, e := range events {
		agg.Apply(e)
		base.applied(e)
	}

	return agg, nil
}

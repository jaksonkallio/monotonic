package monotonic

import "time"

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
	Apply(event Event)

	// Accept checks if a new proposed event is valid given current state
	// Return an error to reject the event
	Accept(event Event) error
}

// AggregateBase provides the infrastructure for aggregates.
// Embed this in your aggregate structs to get Record() and hydration support.
type AggregateBase struct {
	eventStream
	self Logic
}

// Record validates and persists an event, then applies it to state
// Before validating, it catches up on any events that may have been appended by other processes since we last loaded
// This minimizes (but doesn't eliminate) optimistic concurrency conflicts
func (b *AggregateBase) Record(event Event) error {
	preparedEventAggregate, err := b.PrepareEvent(event)
	if err != nil {
		return err
	}

	if err := b.append(preparedEventAggregate); err != nil {
		return err
	}

	b.self.Apply(preparedEventAggregate.Event)
	b.applied(preparedEventAggregate.Event)

	return nil
}

// PrepareEvent accepts an event and prepares it for atomic commit
// This catches up on missed events, accepts, and sets the counter and timestamp
// The event is NOT appended or applied yet
//
// Use this when you need to prepare events for multiple aggregates to be committed atomically
// The returned AggregateEvent can be passed to Store.AppendMulti() or Store.Append
func (b *AggregateBase) PrepareEvent(event Event) (AggregateEvent, error) {
	// Catch up on any events we may have missed
	if err := b.catchUp(b.self.Apply); err != nil {
		return AggregateEvent{}, err
	}

	if err := b.self.Accept(event); err != nil {
		return AggregateEvent{}, err
	}

	event.Counter = b.nextCounter()
	event.AcceptedAt = time.Now()

	return AggregateEvent{
		AggregateType: b.ID.Type,
		AggregateID:   b.ID.ID,
		Event:         event,
	}, nil
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

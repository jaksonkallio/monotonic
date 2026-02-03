package monotonic

import "time"

type AggregateID struct {
	Type string
	ID   string
}

func NewAggregateID(aggregateType, id string) AggregateID {
	return AggregateID{Type: aggregateType, ID: id}
}

// Logic is the interface that user aggregates must implement
type Logic interface {
	// Apply applies an event to the aggregate's state
	// This cannot fail - the event had already been previously accepted
	Apply(event Event)

	// ValidateProposal checks if a proposed event is valid given current state
	// Return an error to reject the event
	ValidateProposal(event Event) error
}

// AggregateBase provides the infrastructure for aggregates.
// Embed this in your aggregate structs to get Propose() and hydration support.
type AggregateBase struct {
	eventStream
	self Logic
}

// Propose validates and persists an event, then applies it to state.
// This is the main entry point for making changes to an aggregate.
//
// Before validating, it catches up on any events that may have been
// appended by other processes since we last loaded. This minimizes
// (but doesn't eliminate) optimistic concurrency conflicts.
func (b *AggregateBase) Propose(event Event) error {
	// Catch up on any events we may have missed
	if err := b.catchUp(b.self.Apply); err != nil {
		return err
	}

	if err := b.self.ValidateProposal(event); err != nil {
		return err
	}

	event.Counter = b.nextCounter()
	event.AcceptedAt = time.Now()

	if err := b.append(event); err != nil {
		return err
	}

	b.self.Apply(event)
	b.applied(event)
	return nil
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

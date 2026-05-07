package monotonic

import (
	"context"
	"fmt"
)

// eventStream provides low-level event storage and replay functionality embedded by AggregateBase.
type eventStream struct {
	ID      AggregateID
	counter int64
	store   Store
}

// Counter returns the number of events that have been applied
func (e *eventStream) Counter() int64 {
	return e.counter
}

// catchUp replays any events from the store that haven't been seen yet.
// The apply function is called for each new event.
func (e *eventStream) catchUp(ctx context.Context, apply func(AcceptedEvent)) error {
	events, err := e.store.LoadAggregateEvents(ctx, e.ID.Type, e.ID.ID, e.counter)
	if err != nil {
		return fmt.Errorf("catch up: %w", err)
	}

	for _, event := range events {
		apply(event)
		e.counter = event.Counter
	}

	return nil
}

// append adds events to the store
func (e *eventStream) append(ctx context.Context, events ...AggregateEvent) error {
	return e.store.Append(ctx, events...)
}

// nextCounter returns the counter value for the next event
func (e *eventStream) nextCounter() int64 {
	return e.counter + 1
}

// applied marks an event as applied by updating the counter
func (e *eventStream) applied(event AcceptedEvent) {
	e.counter = event.Counter
}

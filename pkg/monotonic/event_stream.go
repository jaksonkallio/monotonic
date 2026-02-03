package monotonic

import "fmt"

// eventStream provides low-level event storage and replay functionality.
// Both AggregateBase and Saga embed this to share common event handling code.
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
func (e *eventStream) catchUp(apply func(Event)) error {
	events, err := e.store.LoadAfter(e.ID.Type, e.ID.ID, e.counter)
	if err != nil {
		return fmt.Errorf("catch up: %w", err)
	}

	for _, event := range events {
		apply(event)
		e.counter = event.Counter
	}

	return nil
}

// append adds a single event to the store
func (e *eventStream) append(event Event) error {
	return e.store.Append(e.ID.Type, e.ID.ID, event)
}

// appendMulti adds multiple events atomically to potentially multiple aggregates
func (e *eventStream) appendMulti(events []AggregateEvent) error {
	return e.store.AppendMulti(events)
}

// nextCounter returns the counter value for the next event
func (e *eventStream) nextCounter() int64 {
	return e.counter + 1
}

// applied marks an event as applied by updating the counter
func (e *eventStream) applied(event Event) {
	e.counter = event.Counter
}

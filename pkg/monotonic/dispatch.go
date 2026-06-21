package monotonic

import "context"

// Handler processes a single event with a store-specific environment ModificationTx
// (typically a transaction handle) supplied by the ProjectorBackend running the event.
type Handler[ModificationTx any] func(ctx context.Context, tx ModificationTx, event AggregateEvent) error

// dispatchKey identifies a registered handler by aggregate type and event type.
type dispatchKey struct {
	aggregateType string
	eventType     string
}

// Dispatch routes events to handlers registered per (aggregateType, eventType)
// and derives EventFilters from the registered set. Generic over ModificationTx so the same
// dispatch core works for any backend that supplies a per-event environment.
type Dispatch[ModificationTx any] struct {
	handlers map[dispatchKey]Handler[ModificationTx]
}

// NewDispatch creates an empty Dispatch parameterized over ModificationTx.
func NewDispatch[ModificationTx any]() *Dispatch[ModificationTx] {
	return &Dispatch[ModificationTx]{handlers: make(map[dispatchKey]Handler[ModificationTx])}
}

// On registers handler for the given (aggregateType, eventType); registering the same key twice overwrites.
func (d *Dispatch[ModificationTx]) On(aggregateType, eventType string, handler Handler[ModificationTx]) *Dispatch[ModificationTx] {
	d.handlers[dispatchKey{aggregateType, eventType}] = handler
	return d
}

// EventFilters returns one EventFilter per registered handler so the projector subscribes to exactly the events the dispatch can route.
func (d *Dispatch[ModificationTx]) EventFilters() []EventFilter {
	filters := make([]EventFilter, 0, len(d.handlers))
	for k := range d.handlers {
		filters = append(filters, EventFilter{AggregateType: k.aggregateType, EventType: k.eventType})
	}
	return filters
}

// Apply routes the event to its registered handler; events with no matching handler are a no-op.
func (d *Dispatch[ModificationTx]) Apply(ctx context.Context, tx ModificationTx, event AggregateEvent) error {
	h, ok := d.handlers[dispatchKey{event.AggregateType, event.Event.Type}]
	if !ok {
		return nil
	}
	return h(ctx, tx, event)
}

package monotonic

import "context"

// Handler processes a single event with a store-specific environment Sink
// (typically a transaction handle) supplied by the ProjectorBackend running the event.
type Handler[Sink any] func(ctx context.Context, sink Sink, event AggregateEvent) error

// dispatchKey identifies a registered handler by aggregate type and event type.
type dispatchKey struct {
	aggregateType string
	eventType     string
}

// Dispatch routes events to handlers registered per (aggregateType, eventType)
// and derives EventFilters from the registered set. Generic over Sink so the same
// dispatch core works for any backend that supplies a per-event environment.
type Dispatch[Sink any] struct {
	handlers map[dispatchKey]Handler[Sink]
}

// NewDispatch creates an empty Dispatch parameterized over Sink.
func NewDispatch[Sink any]() *Dispatch[Sink] {
	return &Dispatch[Sink]{handlers: make(map[dispatchKey]Handler[Sink])}
}

// On registers handler for the given (aggregateType, eventType); registering the same key twice overwrites.
func (d *Dispatch[Sink]) On(aggregateType, eventType string, handler Handler[Sink]) *Dispatch[Sink] {
	d.handlers[dispatchKey{aggregateType, eventType}] = handler
	return d
}

// EventFilters returns one EventFilter per registered handler so the projector subscribes to exactly the events the dispatch can route.
func (d *Dispatch[Sink]) EventFilters() []EventFilter {
	filters := make([]EventFilter, 0, len(d.handlers))
	for k := range d.handlers {
		filters = append(filters, EventFilter{AggregateType: k.aggregateType, EventType: k.eventType})
	}
	return filters
}

// Apply routes the event to its registered handler; events with no matching handler are a no-op.
func (d *Dispatch[Sink]) Apply(ctx context.Context, sink Sink, event AggregateEvent) error {
	h, ok := d.handlers[dispatchKey{event.AggregateType, event.Event.Type}]
	if !ok {
		return nil
	}
	return h(ctx, sink, event)
}

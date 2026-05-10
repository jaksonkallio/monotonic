package monotonic

import "context"

// DispatchHandler processes a single event for a Dispatch and returns the projection updates it produced.
type DispatchHandler[V any] func(ctx context.Context, reader ProjectionReader[V], event AggregateEvent) ([]Projected[V], error)

// dispatchKey identifies a registered handler by aggregate type and event type.
type dispatchKey struct {
	aggregateType string
	eventType     string
}

// Dispatch is a ProjectorLogic that routes events to handlers registered per (aggregateType, eventType) and derives EventFilters from the registered set.
type Dispatch[V any] struct {
	// handlers is the (aggregateType, eventType) → handler routing table; also the source of truth for EventFilters.
	handlers map[dispatchKey]DispatchHandler[V]
}

// NewDispatch creates an empty Dispatch.
func NewDispatch[V any]() *Dispatch[V] {
	return &Dispatch[V]{handlers: make(map[dispatchKey]DispatchHandler[V])}
}

// On registers handler for the given (aggregateType, eventType); registering the same key twice overwrites.
func (d *Dispatch[V]) On(aggregateType, eventType string, handler DispatchHandler[V]) *Dispatch[V] {
	d.handlers[dispatchKey{aggregateType, eventType}] = handler
	return d
}

// EventFilters returns one EventFilter per registered handler so the projector subscribes to exactly the events the dispatch can route.
func (d *Dispatch[V]) EventFilters() []EventFilter {
	filters := make([]EventFilter, 0, len(d.handlers))
	for k := range d.handlers {
		filters = append(filters, EventFilter{AggregateType: k.aggregateType, EventType: k.eventType})
	}
	return filters
}

// Apply routes the event to its registered handler; events with no matching handler are a no-op (which shouldn't occur if the projector subscribes via EventFilters).
func (d *Dispatch[V]) Apply(ctx context.Context, reader ProjectionReader[V], event AggregateEvent) ([]Projected[V], error) {
	h, ok := d.handlers[dispatchKey{event.AggregateType, event.Event.Type}]
	if !ok {
		return nil, nil
	}
	return h(ctx, reader, event)
}

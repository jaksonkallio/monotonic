package monotonic

import "context"

// Handler processes a single event. Store-specific resources (e.g. a transaction)
// are injected into ctx by the ProjectorBackend running the event.
type Handler func(ctx context.Context, event AggregateEvent) error

// dispatchKey identifies a registered handler by aggregate type and event type.
type dispatchKey struct {
	aggregateType string
	eventType     string
}

// Dispatch routes events to handlers registered per (aggregateType, eventType)
// and derives EventFilters from the registered set. Store-agnostic.
type Dispatch struct {
	handlers map[dispatchKey]Handler
}

// NewDispatch creates an empty Dispatch.
func NewDispatch() *Dispatch {
	return &Dispatch{handlers: make(map[dispatchKey]Handler)}
}

// On registers handler for the given (aggregateType, eventType); registering the same key twice overwrites.
func (d *Dispatch) On(aggregateType, eventType string, handler Handler) *Dispatch {
	d.handlers[dispatchKey{aggregateType, eventType}] = handler
	return d
}

// EventFilters returns one EventFilter per registered handler so the projector subscribes to exactly the events the dispatch can route.
func (d *Dispatch) EventFilters() []EventFilter {
	filters := make([]EventFilter, 0, len(d.handlers))
	for k := range d.handlers {
		filters = append(filters, EventFilter{AggregateType: k.aggregateType, EventType: k.eventType})
	}
	return filters
}

// Apply routes the event to its registered handler; events with no matching handler are a no-op.
func (d *Dispatch) Apply(ctx context.Context, event AggregateEvent) error {
	h, ok := d.handlers[dispatchKey{event.AggregateType, event.Event.Type}]
	if !ok {
		return nil
	}
	return h(ctx, event)
}

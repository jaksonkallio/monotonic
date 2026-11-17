package monotonic

import "context"

// Handler processes a single event with a store-specific environment ModificationTx
// (typically a transaction handle) supplied by the ProjectorBackend running the event.
type Handler[Tx ModificationTx] func(ctx context.Context, tx ModificationTx, event AggregateEvent) error

type TruncateHandler[Tx ModificationTx] func(ctx context.Context, tx Tx) error

// dispatchKey identifies a registered handler by aggregate type and event type.
type dispatchKey struct {
	aggregateType string
	eventType     string
}

// Dispatch routes events to handlers registered per (aggregateType, eventType)
// and derives EventFilters from the registered set. Generic over ModificationTx so the same
// dispatch core works for any backend that supplies a per-event environment.
type Dispatch[Tx ModificationTx] struct {
	applyHandlers   map[dispatchKey]Handler[ModificationTx]
	truncateHandler TruncateHandler[ModificationTx]
}

// NewDispatch creates an empty Dispatch parameterized over ModificationTx.
func NewDispatch[Tx ModificationTx]() *Dispatch[Tx] {
	return &Dispatch[Tx]{applyHandlers: make(map[dispatchKey]Handler[ModificationTx])}
}

// On registers handler for the given (aggregateType, eventType); registering the same key twice overwrites.
func (d *Dispatch[Tx]) On(aggregateType, eventType string, handler Handler[ModificationTx]) *Dispatch[Tx] {
	d.applyHandlers[dispatchKey{aggregateType, eventType}] = handler
	return d
}

func (d *Dispatch[Tx]) OnTruncate(handler TruncateHandler[ModificationTx]) *Dispatch[Tx] {
	d.truncateHandler = handler
	return d
}

// EventFilters returns one EventFilter per registered handler so the projector subscribes to exactly the events the dispatch can route.
func (d *Dispatch[Tx]) EventFilters() []EventFilter {
	filters := make([]EventFilter, 0, len(d.applyHandlers))
	for k := range d.applyHandlers {
		filters = append(filters, EventFilter{AggregateType: k.aggregateType, EventType: k.eventType})
	}
	return filters
}

// Apply routes the event to its registered handler; events with no matching handler are a no-op.
func (d *Dispatch[Tx]) Apply(ctx context.Context, tx ModificationTx, event AggregateEvent) error {
	applyHandler, ok := d.applyHandlers[dispatchKey{event.AggregateType, event.Event.Type}]
	if !ok {
		return nil
	}
	return applyHandler(ctx, tx, event)
}

func (d *Dispatch[Tx]) Truncate(ctx context.Context, tx ModificationTx) error {
	if d.truncateHandler == nil {
		return nil
	}
	return d.truncateHandler(ctx, tx)
}

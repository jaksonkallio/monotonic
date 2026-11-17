package monotonic

import "context"

// Handler processes a single event within a store-specific environment Tx (typically a transaction handle) supplied by the ProjectorBackend running the event.
type Handler[Tx ModificationTx] func(ctx context.Context, tx Tx, event AggregateEvent) error

// TruncateHandler clears whatever state the projector's handlers have written (e.g. truncating the implementer's own tables), so the projector can rebuild from the start of the event stream.
// It runs with the same Tx a Handler would get, inside the same modification transaction.
type TruncateHandler[Tx ModificationTx] func(ctx context.Context, tx Tx) error

// dispatchKey identifies a registered handler by aggregate type and event type.
type dispatchKey struct {
	aggregateType string
	eventType     string
}

// Dispatch routes events to handlers registered per (aggregateType, eventType) tuple.
type Dispatch[Tx ModificationTx] struct {
	applyHandlers   map[dispatchKey]Handler[Tx]
	truncateHandler TruncateHandler[Tx]
}

// NewDispatch creates an empty Dispatch parameterized over Tx.
func NewDispatch[Tx ModificationTx]() *Dispatch[Tx] {
	return &Dispatch[Tx]{applyHandlers: make(map[dispatchKey]Handler[Tx])}
}

// On registers handler for the given (aggregateType, eventType); registering the same key twice overwrites.
func (d *Dispatch[Tx]) On(aggregateType, eventType string, handler Handler[Tx]) *Dispatch[Tx] {
	d.applyHandlers[dispatchKey{aggregateType, eventType}] = handler
	return d
}

// OnTruncate registers the handler that clears the projection.
// Registering twice overwrites.
func (d *Dispatch[Tx]) OnTruncate(handler TruncateHandler[Tx]) *Dispatch[Tx] {
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

// Apply routes the event to its registered handler.
// Events with no matching handler are a no-op.
func (d *Dispatch[Tx]) Apply(ctx context.Context, tx Tx, event AggregateEvent) error {
	applyHandler, ok := d.applyHandlers[dispatchKey{event.AggregateType, event.Event.Type}]
	if !ok {
		return nil
	}
	return applyHandler(ctx, tx, event)
}

// Truncate runs the registered truncate handler, or nothing if none was registered.
func (d *Dispatch[Tx]) Truncate(ctx context.Context, tx Tx) error {
	if d.truncateHandler == nil {
		return nil
	}
	return d.truncateHandler(ctx, tx)
}

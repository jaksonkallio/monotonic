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
//
// A Dispatch also carries the reset function registered with OnReset, since clearing a projection
// means undoing exactly what this dispatch's handlers wrote, and the two belong together.
type Dispatch[ModificationTx any] struct {
	handlers map[dispatchKey]Handler[ModificationTx]
	reset    func(ctx context.Context, tx ModificationTx) error
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

// OnReset registers the function that clears whatever state this dispatch's handlers have written
// (e.g. truncating the implementer's own tables), which is what a projector replays onto when it is
// reset. It runs with the same ModificationTx a handler would get, in the same transaction that
// rewinds the resume counter to 0. Registering twice overwrites.
//
// A projector whose dispatch has no reset function cannot be reset: it fails loudly on a pending
// reset request rather than rewinding onto un-cleared state and letting every handler re-apply on
// top of rows that are already there. Register a no-op only if the handlers are genuinely
// idempotent over a full replay.
func (d *Dispatch[ModificationTx]) OnReset(reset func(ctx context.Context, tx ModificationTx) error) *Dispatch[ModificationTx] {
	d.reset = reset
	return d
}

// ResetFunc returns the function registered with OnReset, or nil if there is none.
func (d *Dispatch[ModificationTx]) ResetFunc() func(ctx context.Context, tx ModificationTx) error {
	return d.reset
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

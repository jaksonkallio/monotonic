package monotonic

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
)

// ErrProjectionStale is returned by ProjectionWriter.Set when the provided
// global counter is not greater than the current one for the key.
var ErrProjectionStale = fmt.Errorf("projection write is stale")

// EventFilter defines ways to filter events from the global event stream.
// Reduces the number of events processed by a projector.
type EventFilter struct {
	AggregateType string
	AggregateID   string
	EventType     string
}

type Projector[V ProjectionValue] struct {
	store Store

	// eventFilters controls which events are considered for processing by the projector.
	// Filters are OR'ed together, meaning an event is processed if it matches any of the filters.
	eventFilters []EventFilter

	// logic is the business logic for processing events and updating the projection value.
	logic ProjectorLogic[V]

	// persistence is the mechanism for reading and writing projection values.
	persistence ProjectionPersistence[V]

	// globalCounter is the resume position; the projector processes events with
	// global_counter > globalCounter. Initialized from persistence.LatestGlobalCounter.
	globalCounter uint64
}

// NewProjector creates a Projector and derives its resume position from persistence
// by querying the highest stored global counter. Subsequent reads from the event
// store will start strictly after that counter.
//
// Caveat: this assumes writes for a given event are atomic with respect to the
// persistence (e.g. a single Set per event, or a transaction wrapping multiple Sets).
// Otherwise a crash mid-event can leave MAX(global_counter) reflecting a partially
// written event, and resume will skip the remainder.
func NewProjector[V ProjectionValue](
	ctx context.Context,
	store Store,
	eventFilters []EventFilter,
	logic ProjectorLogic[V],
	persistence ProjectionPersistence[V],
) (*Projector[V], error) {
	counter, err := persistence.LatestGlobalCounter(ctx)
	if err != nil {
		return nil, fmt.Errorf("init projector: %w", err)
	}
	return &Projector[V]{
		store:         store,
		eventFilters:  eventFilters,
		logic:         logic,
		persistence:   persistence,
		globalCounter: counter,
	}, nil
}

type ProjectorLogic[V ProjectionValue] interface {
	// Apply produces zero or more projection updates for an event. The reader
	// can be used to fetch the current value for a key when computing aggregating
	// updates (e.g. running totals). Reads see committed state from prior events;
	// updates emitted from this Apply are not visible until they are persisted.
	Apply(ctx context.Context, reader ProjectionReader[V], event AggregateEvent) ([]Projected[V], error)
}

type Projected[V ProjectionValue] struct {
	Key   ProjectionKey
	Value V
}

type ProjectionKey string

type ProjectionValue interface {
	Fields() map[string]pgtype.Value
}

type ProjectionReader[V ProjectionValue] interface {
	// Get retrieves the projection value and its global counter for the given key.
	// If no row exists for the key, returns the zero value of V, counter 0, and nil error.
	// Counter 0 unambiguously means "not found" since real events start at global counter 1.
	Get(ctx context.Context, key ProjectionKey) (V, uint64, error)
}

type ProjectionWriter[V ProjectionValue] interface {
	// Set updates the projection value for the given key. globalCounter must be > 0
	// (zero is reserved as the not-found sentinel for ProjectionReader.Get) and must
	// be strictly greater than the currently stored counter for the key. Returns
	// ErrProjectionStale when the existing counter is greater than or equal.
	Set(ctx context.Context, key ProjectionKey, value V, globalCounter uint64) error
}

type ProjectionPersistence[V ProjectionValue] interface {
	ProjectionReader[V]
	ProjectionWriter[V]

	// LatestGlobalCounter returns the highest global counter recorded across all
	// rows in the projection, or 0 if the projection is empty. Used by NewProjector
	// to derive the resume position on startup.
	LatestGlobalCounter(ctx context.Context) (uint64, error)
}

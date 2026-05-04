package monotonic

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgtype"
)

// ErrProjectionStale is returned by ProjectionWriter.Set when a key's stored counter exceeds the provided globalCounter.
var ErrProjectionStale = fmt.Errorf("projection write is stale")

// EventFilter selects events from the global stream by aggregate type, aggregate ID, and/or event type; non-empty fields must match.
type EventFilter struct {
	AggregateType string
	AggregateID   string
	EventType     string
}

// Projector reads events from a Store and writes per-key updates to a ProjectionPersistence.
type Projector[V ProjectionValue] struct {
	// store is the event source the projector reads from.
	store Store
	// eventFilters select which events the projector processes; matched under OR semantics.
	eventFilters []EventFilter
	// logic produces per-key updates for each event.
	logic ProjectorLogic[V]
	// persistence reads and writes projection rows.
	persistence ProjectionPersistence[V]
	// mu serializes Update calls and protects globalCounter.
	mu sync.Mutex
	// globalCounter is the resume position; events with global_counter > this are pending.
	globalCounter uint64
}

// NewProjector creates a Projector and derives its resume position from persistence.LatestGlobalCounter.
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

// ProjectorLogic produces the per-key updates a projection emits in response to each event.
type ProjectorLogic[V ProjectionValue] interface {
	// Apply returns zero or more projection updates for an event; reader returns committed state from prior events.
	Apply(ctx context.Context, reader ProjectionReader[V], event AggregateEvent) ([]Projected[V], error)
}

// Projected is one key/value update emitted by ProjectorLogic.Apply.
type Projected[V ProjectionValue] struct {
	Key   ProjectionKey
	Value V
}

// ProjectionKey identifies a single row within a projection.
type ProjectionKey string

// ProjectionValue is a projection row whose Fields map exposes its column values for storage.
type ProjectionValue interface {
	Fields() map[string]pgtype.Value
}

// ProjectionReader fetches projection rows by key.
type ProjectionReader[V ProjectionValue] interface {
	// Get returns the value and global counter for key, or (zero V, 0, nil) when no row exists.
	Get(ctx context.Context, key ProjectionKey) (V, uint64, error)
}

// ProjectionWriter atomically persists batches of projection updates produced by a single event.
type ProjectionWriter[V ProjectionValue] interface {
	// Set atomically writes the batch at globalCounter; returns ErrProjectionStale if any key's stored counter exceeds globalCounter.
	Set(ctx context.Context, projecteds []Projected[V], globalCounter uint64) error
}

// ProjectionPersistence reads, writes, and reports progress for a projection's storage.
type ProjectionPersistence[V ProjectionValue] interface {
	ProjectionReader[V]
	ProjectionWriter[V]

	// LatestGlobalCounter returns the highest global counter stored across all rows, or 0 if empty.
	LatestGlobalCounter(ctx context.Context) (uint64, error)
}

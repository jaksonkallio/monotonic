package monotonic

import (
	"context"
	"fmt"
	"sync"
)

// ErrProjectionStale is returned by ProjectionWriter.Set when a key's stored counter exceeds the provided globalCounter.
var ErrProjectionStale = fmt.Errorf("projection write is stale")

// DefaultUpdateBatchSize is a sensible default maximum number of events loaded per Update call.
const DefaultUpdateBatchSize = 100

// Projector reads events from a Store and writes per-key updates to a ProjectionPersistence.
type Projector[V any] struct {
	// store is the event source the projector reads from.
	store Store
	// logic produces per-key updates for each event and supplies the projector's EventFilters.
	logic ProjectorLogic[V]
	// persistence reads and writes projection rows.
	persistence ProjectionPersistence[V]
	// mu serializes Update calls and protects globalCounter.
	mu sync.Mutex
	// globalCounter is the resume position; events with global_counter > this are pending.
	globalCounter uint64
	// updateBatchSize caps the number of events loaded per Update call.
	updateBatchSize int
}

// NewProjector creates a Projector and derives its resume position from persistence.LatestGlobalCounter.
func NewProjector[V any](
	ctx context.Context,
	store Store,
	logic ProjectorLogic[V],
	persistence ProjectionPersistence[V],
	updateBatchSize int,
) (*Projector[V], error) {
	counter, err := persistence.LatestGlobalCounter(ctx)
	if err != nil {
		return nil, fmt.Errorf("init projector: %w", err)
	}
	return &Projector[V]{
		store:           store,
		logic:           logic,
		persistence:     persistence,
		globalCounter:   counter,
		updateBatchSize: updateBatchSize,
	}, nil
}

// ProjectorLogic produces the per-key updates a projection emits in response to each event and declares the EventFilters it subscribes to.
type ProjectorLogic[V any] interface {
	// EventFilters returns the EventFilters the projector should subscribe to, typically derived from a Dispatch.
	EventFilters() []EventFilter
	// Apply returns zero or more projection updates for an event; reader returns committed state from prior events.
	Apply(ctx context.Context, reader ProjectionReader[V], event AggregateEvent) ([]Projected[V], error)
}

// Projected is one key/value update emitted by ProjectorLogic.Apply.
type Projected[V any] struct {
	Key   ProjectionKey
	Value V
}

// ProjectionKey identifies a single row within a projection.
type ProjectionKey string

// ProjectionKeySummary is the conventional ProjectionKey for summary/single-row projections.
const ProjectionKeySummary ProjectionKey = "summary"

// ProjectionReader fetches projection rows by key.
type ProjectionReader[V any] interface {
	// Get returns the value for key, or (zero V, nil) when no row exists.
	Get(ctx context.Context, key ProjectionKey) (V, error)
}

// ProjectionWriter atomically persists batches of projection updates produced by a single event.
type ProjectionWriter[V any] interface {
	// Set atomically writes the batch at globalCounter; returns ErrProjectionStale if any key's stored counter exceeds globalCounter.
	Set(ctx context.Context, projecteds []Projected[V], globalCounter uint64) error
}

// ProjectionPersistence reads, writes, and reports progress for a projection's storage.
type ProjectionPersistence[V any] interface {
	ProjectionReader[V]
	ProjectionWriter[V]

	// LatestGlobalCounter returns the highest global counter stored across all rows, or 0 if empty.
	LatestGlobalCounter(ctx context.Context) (uint64, error)

	// Truncate removes all rows from the projection, resetting it to an empty state.
	Truncate(ctx context.Context) error
}

// MutateByKey reads the row at key, applies mutate to it, and returns a single-element Projected slice ready to return from ProjectorLogic.Apply; if no row exists, mutate sees the zero value of V.
func MutateByKey[V any](ctx context.Context, reader ProjectionReader[V], key ProjectionKey, mutate func(v *V) error) ([]Projected[V], error) {
	current, err := reader.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if err := mutate(&current); err != nil {
		return nil, err
	}
	return []Projected[V]{{Key: key, Value: current}}, nil
}

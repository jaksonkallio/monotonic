package monotonic

import (
	"context"

	"github.com/jackc/pgtype"
)

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
}

type ProjectorLogic[V ProjectionValue] interface {
	Apply(event AggregateEvent) ([]Projected[V], error)
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
	// Get retrieves the projection value and its associated global counter for the given key.
	Get(ctx context.Context, key ProjectionKey) (V, uint64, error)
}

type ProjectionWriter[V ProjectionValue] interface {
	// Set updates the projection value and its associated global counter for the given key.
	// Will assert that the provided globalCounter is greater than the current global counter for the key to ensure monotonicity.
	Set(ctx context.Context, key ProjectionKey, value V, globalCounter uint64) error
}

type ProjectionPersistence[V ProjectionValue] interface {
	ProjectionReader[V]
	ProjectionWriter[V]
}

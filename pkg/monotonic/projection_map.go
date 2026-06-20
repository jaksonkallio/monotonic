package monotonic

import (
	"context"
	"fmt"
	"sync"
)

// ErrProjectionStale is returned by ProjectionWriter.Set when a key's stored counter exceeds the provided globalCounter.
var ErrProjectionStale = fmt.Errorf("projection write is stale")

// ErrProjectionModeMismatch is returned when an emission's ReconciliationMode does not match the persistence's configured mode, or when Get/GetSet is called on a persistence whose mode does not support that read shape.
var ErrProjectionModeMismatch = fmt.Errorf("projection reconciliation mode mismatch")

// DefaultUpdateBatchSize is a sensible default maximum number of events loaded per Update call.
const DefaultUpdateBatchSize = 100

// ReconciliationMode controls how a ProjectedSet is written to persistence.
type ReconciliationMode int

const (
	// ReconcileUpsert is the single-row mode: one row per projection_key, written via upsert. Values must contain exactly one element.
	ReconcileUpsert ReconciliationMode = iota
	// ReconcileReplace is the row-set mode: zero-or-more rows per projection_key, reconciled by deleting all rows for the key and inserting Values. Empty Values clears the key.
	ReconcileReplace
)

// String returns the canonical name for the mode, used in error messages.
func (m ReconciliationMode) String() string {
	switch m {
	case ReconcileUpsert:
		return "upsert"
	case ReconcileReplace:
		return "replace"
	default:
		return fmt.Sprintf("ReconciliationMode(%d)", int(m))
	}
}

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
	Apply(ctx context.Context, reader ProjectionReader[V], event AggregateEvent) ([]ProjectedSet[V], error)
}

// ProjectedSet is one key's reconciliation outcome emitted by ProjectorLogic.Apply: for ReconcileUpsert, Values has exactly one element and is upserted at Key; for ReconcileReplace, all existing rows at Key are removed and Values is inserted (empty Values clears the key).
type ProjectedSet[V any] struct {
	Key    ProjectionKey
	Mode   ReconciliationMode
	Values []V
}

// ProjectionKey identifies a slice of a projection — one row for upsert mode, zero-or-more rows for replace mode. It is framework plumbing and should not be exposed in domain queries; store domain identifiers in their own columns instead.
type ProjectionKey string

// ProjectionKeySummary is the conventional ProjectionKey for summary/single-row projections.
const ProjectionKeySummary ProjectionKey = "summary"

// ProjectionReader fetches projection rows by key. Both Get and GetSet are always callable; for replace-mode persistences, Get returns ErrProjectionModeMismatch and for upsert-mode persistences, GetSet returns a 0- or 1-element slice.
type ProjectionReader[V any] interface {
	// Get returns the value for key, or (zero V, nil) when no row exists. Returns ErrProjectionModeMismatch when called on a replace-mode persistence.
	Get(ctx context.Context, key ProjectionKey) (V, error)
	// GetSet returns the values for key, or an empty slice when no rows exist. For upsert-mode persistences, returns a slice with 0 or 1 elements.
	GetSet(ctx context.Context, key ProjectionKey) ([]V, error)
}

// ProjectionWriter atomically persists batches of projection updates produced by a single event.
type ProjectionWriter[V any] interface {
	// Set atomically writes the batch at globalCounter; returns ErrProjectionStale if any key's stored counter exceeds globalCounter, or ErrProjectionModeMismatch if an emission's Mode does not match the persistence's mode.
	Set(ctx context.Context, sets []ProjectedSet[V], globalCounter uint64) error
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

// MutateByKey reads the row at key, applies mutate to it, and returns a single-element ProjectedSet slice (ReconcileUpsert) ready to return from ProjectorLogic.Apply; if no row exists, mutate sees the zero value of V.
func MutateByKey[V any](ctx context.Context, reader ProjectionReader[V], key ProjectionKey, mutate func(v *V) error) ([]ProjectedSet[V], error) {
	current, err := reader.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if err := mutate(&current); err != nil {
		return nil, err
	}
	return []ProjectedSet[V]{{Key: key, Mode: ReconcileUpsert, Values: []V{current}}}, nil
}

// ReplaceSet builds a single-key ReconcileReplace emission with the given values; an empty or nil values slice clears the key.
func ReplaceSet[V any](key ProjectionKey, values []V) []ProjectedSet[V] {
	return []ProjectedSet[V]{{Key: key, Mode: ReconcileReplace, Values: values}}
}

// MutateSet reads the current values at key, applies mutate to them, and returns a single-element ProjectedSet slice (ReconcileReplace) ready to return from ProjectorLogic.Apply.
func MutateSet[V any](ctx context.Context, reader ProjectionReader[V], key ProjectionKey, mutate func(values []V) ([]V, error)) ([]ProjectedSet[V], error) {
	current, err := reader.GetSet(ctx, key)
	if err != nil {
		return nil, err
	}
	next, err := mutate(current)
	if err != nil {
		return nil, err
	}
	return ReplaceSet(key, next), nil
}

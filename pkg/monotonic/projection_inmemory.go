package monotonic

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryProjectionPersistence is a thread-safe in-memory ProjectionPersistence; useful for tests and ephemeral summary projections.
type InMemoryProjectionPersistence[V any] struct {
	// mu protects rows and maxCounter.
	mu sync.Mutex
	// rows holds the latest value per key and the counter at which it was written.
	rows map[ProjectionKey]inMemoryProjectionRow[V]
	// maxCounter is the highest globalCounter ever written, returned by LatestGlobalCounter.
	maxCounter uint64
}

// inMemoryProjectionRow pairs a stored value with the globalCounter at which it was written.
type inMemoryProjectionRow[V any] struct {
	value         V
	globalCounter uint64
}

// NewInMemoryProjectionPersistence creates an empty in-memory ProjectionPersistence for V.
func NewInMemoryProjectionPersistence[V any]() *InMemoryProjectionPersistence[V] {
	return &InMemoryProjectionPersistence[V]{
		rows: make(map[ProjectionKey]inMemoryProjectionRow[V]),
	}
}

// Get returns the projection value for key, or (zero V, nil) when no row exists.
func (p *InMemoryProjectionPersistence[V]) Get(ctx context.Context, key ProjectionKey) (V, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r, ok := p.rows[key]; ok {
		return r.value, nil
	}
	var zero V
	return zero, nil
}

// Set atomically writes the batch at globalCounter; returns ErrProjectionStale if any key's stored counter exceeds globalCounter.
func (p *InMemoryProjectionPersistence[V]) Set(ctx context.Context, projecteds []Projected[V], globalCounter uint64) error {
	if len(projecteds) == 0 {
		return nil
	}
	// globalCounter == 0 is reserved as the not-found sentinel in ProjectionReader.Get.
	if globalCounter == 0 {
		return fmt.Errorf("globalCounter must be > 0")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Validate the whole batch first so a stale row leaves nothing written; mirrors the Postgres impl's transaction.
	for _, pj := range projecteds {
		if existing, ok := p.rows[pj.Key]; ok && existing.globalCounter > globalCounter {
			return fmt.Errorf("%w: key=%q counter=%d", ErrProjectionStale, pj.Key, globalCounter)
		}
	}

	for _, pj := range projecteds {
		p.rows[pj.Key] = inMemoryProjectionRow[V]{value: pj.Value, globalCounter: globalCounter}
	}
	if globalCounter > p.maxCounter {
		p.maxCounter = globalCounter
	}
	return nil
}

// LatestGlobalCounter returns the highest globalCounter ever written, or 0 if empty.
func (p *InMemoryProjectionPersistence[V]) LatestGlobalCounter(ctx context.Context) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxCounter, nil
}

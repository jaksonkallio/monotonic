package monotonic

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryProjectionPersistence is a thread-safe in-memory ProjectionPersistence; useful for tests and ephemeral summary projections. It supports both reconciliation modes; the mode is fixed at construction.
type InMemoryProjectionPersistence[V any] struct {
	// mu protects rows and maxCounter.
	mu sync.Mutex
	// mode is the reconciliation mode this persistence accepts in Set calls.
	mode ReconciliationMode
	// rows holds the rows per key. Upsert mode has at most one row per key; replace mode has 0..N.
	rows map[ProjectionKey][]inMemoryProjectionRow[V]
	// maxCounter is the highest globalCounter ever written, returned by LatestGlobalCounter.
	maxCounter uint64
}

// inMemoryProjectionRow pairs a stored value with the globalCounter at which it was written.
type inMemoryProjectionRow[V any] struct {
	value         V
	globalCounter uint64
}

// NewInMemoryProjectionPersistence creates an empty in-memory ProjectionPersistence for V in the given reconciliation mode.
func NewInMemoryProjectionPersistence[V any](mode ReconciliationMode) *InMemoryProjectionPersistence[V] {
	return &InMemoryProjectionPersistence[V]{
		mode: mode,
		rows: make(map[ProjectionKey][]inMemoryProjectionRow[V]),
	}
}

// Get returns the projection value for key, or (zero V, nil) when no row exists. Returns ErrProjectionModeMismatch when the persistence is in replace mode.
func (p *InMemoryProjectionPersistence[V]) Get(ctx context.Context, key ProjectionKey) (V, error) {
	var zero V
	if p.mode != ReconcileUpsert {
		return zero, fmt.Errorf("%w: Get requires upsert mode, persistence is %s", ErrProjectionModeMismatch, p.mode)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if rs, ok := p.rows[key]; ok && len(rs) > 0 {
		return rs[0].value, nil
	}
	return zero, nil
}

// GetSet returns all values at key, or an empty slice when no rows exist. For upsert mode it returns 0 or 1 element.
func (p *InMemoryProjectionPersistence[V]) GetSet(ctx context.Context, key ProjectionKey) ([]V, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	rs, ok := p.rows[key]
	if !ok {
		return nil, nil
	}
	out := make([]V, len(rs))
	for i, r := range rs {
		out[i] = r.value
	}
	return out, nil
}

// Set atomically writes the batch at globalCounter; returns ErrProjectionStale if any key's stored counter exceeds globalCounter, or ErrProjectionModeMismatch if a set's Mode does not match the persistence's mode.
func (p *InMemoryProjectionPersistence[V]) Set(ctx context.Context, sets []ProjectedSet[V], globalCounter uint64) error {
	if len(sets) == 0 {
		return nil
	}
	// globalCounter == 0 is reserved as the not-found sentinel in ProjectionReader.Get.
	if globalCounter == 0 {
		return fmt.Errorf("globalCounter must be > 0")
	}

	for _, ps := range sets {
		if ps.Mode != p.mode {
			return fmt.Errorf("%w: set key=%q mode=%s, persistence mode=%s", ErrProjectionModeMismatch, ps.Key, ps.Mode, p.mode)
		}
		if ps.Mode == ReconcileUpsert && len(ps.Values) != 1 {
			return fmt.Errorf("upsert set for key %q must have exactly one value, got %d", ps.Key, len(ps.Values))
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Validate the whole batch first so a stale row leaves nothing written; mirrors the Postgres impl's transaction.
	for _, ps := range sets {
		for _, existing := range p.rows[ps.Key] {
			if existing.globalCounter > globalCounter {
				return fmt.Errorf("%w: key=%q counter=%d", ErrProjectionStale, ps.Key, globalCounter)
			}
		}
	}

	for _, ps := range sets {
		newRows := make([]inMemoryProjectionRow[V], len(ps.Values))
		for i, v := range ps.Values {
			newRows[i] = inMemoryProjectionRow[V]{value: v, globalCounter: globalCounter}
		}
		if p.mode == ReconcileReplace && len(newRows) == 0 {
			delete(p.rows, ps.Key)
		} else {
			p.rows[ps.Key] = newRows
		}
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

// Truncate removes all rows and resets the counter to zero.
func (p *InMemoryProjectionPersistence[V]) Truncate(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows = make(map[ProjectionKey][]inMemoryProjectionRow[V])
	p.maxCounter = 0
	return nil
}

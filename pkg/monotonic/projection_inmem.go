package monotonic

import (
	"context"
	"fmt"
	"maps"
	"sync"
)

// InMemoryProjection is an in-memory ProjectorBackend that stores projected rows of type T keyed by string, useful for tests and development.
type InMemoryProjection[T any] struct {
	// mu guards rows and state across concurrent RunEvent, ResetState, and reads.
	mu sync.RWMutex
	// rows holds the committed projection, keyed by aggregate ID (or whatever key the handlers write).
	rows map[string]T
	// state holds each projector's resume counter; an InMemoryProjection is intended to back a single projector.
	state map[string]uint64
}

// NewInMemoryProjection creates an empty InMemoryProjection.
func NewInMemoryProjection[T any]() *InMemoryProjection[T] {
	return &InMemoryProjection[T]{
		rows:  make(map[string]T),
		state: make(map[string]uint64),
	}
}

// InMemoryProjectionTx is the ModificationTx handlers receive; edits buffer here and commit only if the handler returns nil.
type InMemoryProjectionTx[T any] struct {
	rows map[string]T
}

// Get returns the row for key and whether it exists.
func (tx *InMemoryProjectionTx[T]) Get(key string) (T, bool) {
	v, ok := tx.rows[key]
	return v, ok
}

// Set inserts or replaces the row for key.
func (tx *InMemoryProjectionTx[T]) Set(key string, row T) {
	tx.rows[key] = row
}

// Delete removes the row for key if present.
func (tx *InMemoryProjectionTx[T]) Delete(key string) {
	delete(tx.rows, key)
}

// Clear removes every row, useful when a reset handler rebuilds the projection from scratch.
func (tx *InMemoryProjectionTx[T]) Clear() {
	clear(tx.rows)
}

// GetState returns the resume counter for projectorName, or 0 if none.
func (p *InMemoryProjection[T]) GetState(_ context.Context, projectorName string) (uint64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state[projectorName], nil
}

// RunEvent runs apply against a buffered copy of the rows and commits that copy plus the counter advance only if apply succeeds.
func (p *InMemoryProjection[T]) RunEvent(ctx context.Context, projectorName string, counter uint64, apply func(ctx context.Context, tx *InMemoryProjectionTx[T]) error) error {
	if counter == 0 {
		return fmt.Errorf("projection counter must be > 0")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Skip counters already applied, mirroring the exactly-once behavior of the Postgres backend.
	if p.state[projectorName] >= counter {
		return nil
	}

	tx := &InMemoryProjectionTx[T]{rows: maps.Clone(p.rows)}
	if err := apply(ctx, tx); err != nil {
		return err
	}

	p.rows = tx.rows
	p.state[projectorName] = counter
	return nil
}

// ResetState runs reset against a buffered copy and rewinds projectorName's counter to 0 only if reset succeeds.
func (p *InMemoryProjection[T]) ResetState(ctx context.Context, projectorName string, reset func(ctx context.Context, tx *InMemoryProjectionTx[T]) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tx := &InMemoryProjectionTx[T]{rows: maps.Clone(p.rows)}
	if err := reset(ctx, tx); err != nil {
		return err
	}

	p.rows = tx.rows
	p.state[projectorName] = 0
	return nil
}

// Get returns the committed row for key and whether it exists.
func (p *InMemoryProjection[T]) Get(key string) (T, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	v, ok := p.rows[key]
	return v, ok
}

// All returns a copy of every committed row keyed as the handlers wrote them.
func (p *InMemoryProjection[T]) All() map[string]T {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return maps.Clone(p.rows)
}

package monotonic

import (
	"context"
	"fmt"
	"maps"
	"sync"
)

// InMemoryProjection is an in-memory ProjectorBackend that stores projected rows of type T keyed by string, useful for tests and development.
//
// T must be a type whose copy is independent of the original, meaning a struct of scalars, strings, or other values that own their memory.
// Buffering works by cloning the row map, and a map clone copies values shallowly, so if T contains a pointer, slice, or map then the buffered copy and the committed row share that memory.
// A handler that reaches through such a field and mutates it in place edits committed state directly, and that edit survives even when the handler returns an error, which breaks the all-or-nothing guarantee ProjectorBackend documents.
// The rule that keeps this safe regardless of T is to treat anything InMemoryProjectionTx.Get returns as read-only: derive a new value from it and hand that to Set, rather than mutating what you were given.
// Enforcing this in the type system would mean requiring every T to supply a deep-copy method, which is a lot of ceremony to impose on the common case of a flat struct, so it is a documented contract instead.
type InMemoryProjection[T any] struct {
	// txMu is the in-memory stand-in for the Postgres backend's projector_state row lock. It is
	// taken by LockStateForModification and released only when that transaction commits or rolls
	// back, so a whole Update is serialized against any other Update on this projection.
	//
	// It is one lock for the projection rather than one per projector name, which is stricter than
	// Postgres: rows is a single map shared by every projector writing here, and buffering replaces
	// it wholesale, so two concurrent transactions would clone the same starting rows and the later
	// commit would discard the earlier one's writes.
	txMu sync.Mutex
	// mu guards rows and state. It is held only for the snapshot read and the commit swap, never
	// across a handler, so reads never block behind projector work and a handler calling back into
	// Get or All cannot deadlock.
	mu sync.RWMutex
	// rows holds the committed projection, keyed by aggregate ID (or whatever key the handlers write).
	rows map[string]T
	// state holds each projector's persisted state, keyed by projector name.
	state map[string]ProjectorStateSnapshot
}

var _ ProjectorBackend[*InMemoryProjectionTx[struct{}]] = (*InMemoryProjection[struct{}])(nil)

// NewInMemoryProjection creates an empty InMemoryProjection.
func NewInMemoryProjection[T any]() *InMemoryProjection[T] {
	return &InMemoryProjection[T]{
		rows:  make(map[string]T),
		state: make(map[string]ProjectorStateSnapshot),
	}
}

// InMemoryProjectionTx is the ModificationTx handlers receive; edits buffer here and commit only if the whole batch succeeds.
type InMemoryProjectionTx[T any] struct {
	// projection is the projection this transaction will commit into, used to reject a tx handed to the wrong backend.
	projection *InMemoryProjection[T]
	// rows is the buffered clone handlers read and write.
	rows map[string]T
	// finished records that the transaction has already committed or rolled back, so the
	// projector's deferred Rollback after a successful commit is a no-op rather than a double unlock.
	finished bool
}

// Get returns the row for key and whether it exists.
// Treat the result as read-only and pass any modification back through Set; see InMemoryProjection for why that matters when T holds a pointer, slice, or map.
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

// Clear removes every row, which is what a truncate handler registered via Dispatch.OnTruncate wants.
func (tx *InMemoryProjectionTx[T]) Clear() {
	clear(tx.rows)
}

// Rollback discards the buffered rows and releases the modification lock. It is safe to call on a
// transaction that already committed, which is what lets the projector defer it unconditionally.
func (tx *InMemoryProjectionTx[T]) Rollback(context.Context) error {
	if tx.finished {
		return nil
	}
	tx.finished = true
	tx.projection.txMu.Unlock()
	return nil
}

// LockStateForModification takes the projection's modification lock and returns a transaction
// buffering a clone of the committed rows, along with projectorName's state as of that moment.
// The lock is held until the returned transaction commits or rolls back.
func (p *InMemoryProjection[T]) LockStateForModification(_ context.Context, projectorName string) (*InMemoryProjectionTx[T], ProjectorStateSnapshot, error) {
	p.txMu.Lock()

	p.mu.RLock()
	defer p.mu.RUnlock()

	return &InMemoryProjectionTx[T]{projection: p, rows: maps.Clone(p.rows)}, p.state[projectorName], nil
}

// CommitAdvancedStateFromModification commits the buffered rows and advances projectorName's
// resume counter, then releases the modification lock.
func (p *InMemoryProjection[T]) CommitAdvancedStateFromModification(_ context.Context, tx *InMemoryProjectionTx[T], projectorName string, advancedToGlobalCounter uint64) error {
	// TruncateRequested is deliberately carried through rather than cleared: a request that
	// arrived while this batch was applying must still be serviced by the next Update.
	return p.commit(tx, projectorName, func(state ProjectorStateSnapshot) ProjectorStateSnapshot {
		state.GlobalCounter = advancedToGlobalCounter
		return state
	})
}

// CommitTruncatedStateFromModification commits the truncated rows, rewinds projectorName to the
// start of the stream and clears its truncate request, then releases the modification lock.
func (p *InMemoryProjection[T]) CommitTruncatedStateFromModification(_ context.Context, tx *InMemoryProjectionTx[T], projectorName string) error {
	return p.commit(tx, projectorName, func(ProjectorStateSnapshot) ProjectorStateSnapshot {
		return ProjectorStateSnapshot{}
	})
}

// commit swaps the buffered rows in as the committed projection, applies update to projectorName's
// state, and ends the transaction.
func (p *InMemoryProjection[T]) commit(tx *InMemoryProjectionTx[T], projectorName string, update func(ProjectorStateSnapshot) ProjectorStateSnapshot) error {
	if tx == nil || tx.projection != p {
		return fmt.Errorf("commit projection %q: transaction belongs to a different projection", projectorName)
	}
	if tx.finished {
		return fmt.Errorf("commit projection %q: transaction already finished", projectorName)
	}

	p.mu.Lock()
	p.rows = tx.rows
	p.state[projectorName] = update(p.state[projectorName])
	p.mu.Unlock()

	tx.finished = true
	p.txMu.Unlock()
	return nil
}

// EnqueueTruncate asks projectorName to rebuild from the start of the event stream on its next Update.
func (p *InMemoryProjection[T]) EnqueueTruncate(_ context.Context, projectorName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.state[projectorName]
	state.TruncateRequested = true
	p.state[projectorName] = state
	return nil
}

// State returns projectorName's committed state, or the zero snapshot if it has never run.
func (p *InMemoryProjection[T]) State(projectorName string) ProjectorStateSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state[projectorName]
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

package monotonic

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// DefaultUpdateBatchSize is a sensible default maximum number of events loaded per Update call.
const DefaultUpdateBatchSize = 100

// ProjectorBackend is the store-specific glue that runs each event atomically alongside
// the projector's resume-counter advance. ModificationTx is the per-event handle the backend
// hands to handlers (e.g. a Postgres pgx.Tx, a pipeline, a producer). Implementations open a transaction
// (or equivalent), construct a ModificationTx bound to it, invoke apply, and commit only if apply succeeds.
type ProjectorBackend[ModificationTx any] interface {
	// GetState returns the resume counter for projectorName, or 0 if none.
	GetState(ctx context.Context, projectorName string) (uint64, error)

	// RunEvent runs apply and records (projectorName, counter) atomically.
	// On apply error, neither the handler's writes nor the state advance are committed.
	RunEvent(ctx context.Context, projectorName string, counter uint64, apply func(ctx context.Context, tx ModificationTx) error) error

	// ResetState runs reset and sets projectorName's counter back to 0, atomically.
	// On reset error, neither reset's writes nor the counter reset are committed.
	ResetState(ctx context.Context, projectorName string, reset func(ctx context.Context, tx ModificationTx) error) error
}

// Projector reads events from a Store and dispatches each one through a Dispatch,
// using a ProjectorBackend to commit the handler's work atomically with the counter advance.
type Projector[ModificationTx any] struct {
	// name identifies this projector in the backend's state store.
	name string
	// store is the event source the projector reads from.
	store Store
	// dispatch routes events to handlers and supplies EventFilters.
	dispatch *Dispatch[ModificationTx]
	// backend runs each event atomically with the counter advance.
	backend ProjectorBackend[ModificationTx]
	// mu serializes Update calls and protects counter.
	mu sync.Mutex
	// counter is the resume position; events with global_counter > this are pending.
	counter uint64
	// updateBatchSize caps the number of events loaded per Update call.
	updateBatchSize int
}

// NewProjector creates a Projector and derives its resume position from backend.GetState.
func NewProjector[ModificationTx any](
	ctx context.Context,
	name string,
	store Store,
	dispatch *Dispatch[ModificationTx],
	backend ProjectorBackend[ModificationTx],
	updateBatchSize int,
) (*Projector[ModificationTx], error) {
	if name == "" {
		return nil, fmt.Errorf("NewProjector: name must be non-empty")
	}
	if updateBatchSize <= 0 {
		updateBatchSize = DefaultUpdateBatchSize
	}
	counter, err := backend.GetState(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("init projector %q: %w", name, err)
	}
	return &Projector[ModificationTx]{
		name:            name,
		store:           store,
		dispatch:        dispatch,
		backend:         backend,
		counter:         counter,
		updateBatchSize: updateBatchSize,
	}, nil
}

// Update processes a batch of pending events and returns the count, or 0 if caught up.
func (p *Projector[ModificationTx]) Update(ctx context.Context) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	events, err := p.store.LoadGlobalEvents(ctx, p.dispatch.EventFilters(), int64(p.counter), p.updateBatchSize)
	if err != nil {
		return 0, fmt.Errorf("load events: %w", err)
	}

	processed := 0
	for _, event := range events {
		counter := uint64(event.Event.GlobalCounter)
		err := p.backend.RunEvent(ctx, p.name, counter, func(ctx context.Context, tx ModificationTx) error {
			return p.dispatch.Apply(ctx, tx, event)
		})
		if err != nil {
			return processed, fmt.Errorf("apply event %d: %w", event.Event.GlobalCounter, err)
		}
		p.counter = counter
		processed++
	}

	return processed, nil
}

// Reset runs reset and rewinds the projector back to the beginning of the event stream, atomically.
// reset is responsible for clearing whatever state the projector's handlers have written (e.g. truncating
// the implementer's own tables); it runs with the same ModificationTx a handler would get. Callers must ensure no
// concurrent Run/Update loop is driving this projector, since Reset does not stop one.
func (p *Projector[ModificationTx]) Reset(ctx context.Context, reset func(ctx context.Context, tx ModificationTx) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.backend.ResetState(ctx, p.name, reset); err != nil {
		return fmt.Errorf("reset projector %q: %w", p.name, err)
	}
	p.counter = 0
	return nil
}

// Run drives Update in a loop, sleeping pollInterval between catch-up polls; returns nil on context cancellation.
func (p *Projector[ModificationTx]) Run(ctx context.Context, pollInterval time.Duration) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		n, err := p.Update(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		// Drain without sleeping while there is more work to process.
		if n > 0 {
			continue
		}

		select {
		case <-time.After(pollInterval):
		case <-ctx.Done():
			return nil
		}
	}
}

// GlobalCounter returns the highest global counter the projector has processed.
func (p *Projector[ModificationTx]) GlobalCounter() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counter
}

// Name returns the projector name used in the backend's state store.
func (p *Projector[ModificationTx]) Name() string {
	return p.name
}

// ProjectorRunner is implemented by any projector that can be driven by RunProjectors.
// *Projector[ModificationTx] satisfies this interface for any ModificationTx, so RunProjectors can drive
// projectors with different ModificationTx types together.
type ProjectorRunner interface {
	Run(ctx context.Context, pollInterval time.Duration) error
}

// RunProjectors runs each projector concurrently in its own goroutine and returns when all have stopped.
// If any projector returns an error, the shared context is cancelled and RunProjectors returns that error.
// On clean context cancellation all projectors stop and RunProjectors returns nil.
func RunProjectors(ctx context.Context, pollInterval time.Duration, projectors ...ProjectorRunner) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, p := range projectors {
		p := p
		g.Go(func() error {
			return p.Run(ctx, pollInterval)
		})
	}
	return g.Wait()
}

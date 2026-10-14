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

// ProjectorState is a projector's backend-held position: the resume counter plus whether a
// reset has been requested for it and not yet serviced.
type ProjectorState struct {
	// Counter is the highest global counter committed for the projector; 0 means nothing has been processed.
	Counter uint64
	// ResetRequested reports that someone has asked for this projector to be rebuilt from scratch.
	// It stays true until an instance claims the request and runs the reset, so every instance of
	// the projector observes it even if only one of them does the work.
	ResetRequested bool
}

// ProjectorBackend is the store-specific glue that runs each event atomically alongside
// the projector's resume-counter advance. ModificationTx is the per-event handle the backend
// hands to handlers (e.g. a Postgres pgx.Tx, a pipeline, a producer). Implementations open a transaction
// (or equivalent), construct a ModificationTx bound to it, invoke apply, and commit only if apply succeeds.
//
// The backend's stored state is authoritative. A Projector's in-memory counter is only a cache of it,
// because other instances of the same projector share the row and may move it in either direction.
type ProjectorBackend[ModificationTx any] interface {
	// GetState returns the stored state for projectorName, zero-valued if none.
	GetState(ctx context.Context, projectorName string) (ProjectorState, error)

	// RunEvent runs apply and records (projectorName, counter) atomically, returning the stored
	// counter as of the commit. from is the counter the caller believes is current, which lets the
	// backend tell "this event is next" apart from "the stored position moved since you last looked".
	// On apply error, neither the handler's writes nor the state advance are committed.
	//
	// Implementations must take the stored position under a lock and then:
	//   - skip apply when the stored counter is already >= counter (another instance got there first);
	//   - skip apply when the stored counter is < from (a reset rewound the projector underneath the caller);
	//   - skip apply when a reset is pending, since anything applied now is about to be discarded;
	//   - otherwise run apply, advance the stored counter to counter, and commit both together.
	// Any skip returns the stored counter unchanged and no error, so a caller detects all three
	// by comparing the returned counter against the one it asked for.
	RunEvent(ctx context.Context, projectorName string, from, counter uint64, apply func(ctx context.Context, tx ModificationTx) error) (uint64, error)

	// RequestReset marks projectorName for reset. It does no projection work itself: the next
	// instance to observe the request claims and performs it. Requesting a reset that is already
	// pending is a no-op rather than an error.
	RequestReset(ctx context.Context, projectorName string) error

	// ClaimReset services a pending reset exactly once across all instances of projectorName.
	// Implementations re-check the pending flag under the same lock RunEvent takes: if no reset is
	// pending (another instance claimed it first), ClaimReset reports claimed=false and does nothing.
	// Otherwise it runs reset, sets the counter back to 0, clears the pending flag, and commits all
	// three atomically. On reset error, none of it is committed.
	ClaimReset(ctx context.Context, projectorName string, reset func(ctx context.Context, tx ModificationTx) error) (claimed bool, err error)
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
	// counter caches the backend's resume position; events with global_counter > this are pending.
	// Only ever a cache: Update re-reads the backend's copy before each batch.
	counter uint64
	// updateBatchSize caps the number of events loaded per Update call.
	updateBatchSize int
	// reset clears whatever state this projector's handlers have written, and is nil unless
	// WithReset was supplied. Registered up front rather than passed to a reset call because a
	// reset can be requested from outside the process, and whichever instance claims it needs
	// the function already in hand.
	reset func(ctx context.Context, tx ModificationTx) error
}

// ProjectorOption configures a Projector at construction.
type ProjectorOption[ModificationTx any] func(*Projector[ModificationTx])

// WithReset registers the function that clears whatever state this projector's handlers have
// written (e.g. truncating the implementer's own tables). It runs with the same ModificationTx a
// handler would get, in the same transaction that rewinds the resume counter to 0.
//
// A projector without a reset function cannot be reset: Update fails loudly when it finds a pending
// request it cannot service, rather than rewinding the counter onto un-cleared projection state and
// letting every handler re-apply on top of rows that are already there.
func WithReset[ModificationTx any](reset func(ctx context.Context, tx ModificationTx) error) ProjectorOption[ModificationTx] {
	return func(p *Projector[ModificationTx]) {
		p.reset = reset
	}
}

// NewProjector creates a Projector and derives its resume position from backend.GetState.
func NewProjector[ModificationTx any](
	ctx context.Context,
	name string,
	store Store,
	dispatch *Dispatch[ModificationTx],
	backend ProjectorBackend[ModificationTx],
	updateBatchSize int,
	opts ...ProjectorOption[ModificationTx],
) (*Projector[ModificationTx], error) {
	if name == "" {
		return nil, fmt.Errorf("NewProjector: name must be non-empty")
	}
	if updateBatchSize <= 0 {
		updateBatchSize = DefaultUpdateBatchSize
	}
	state, err := backend.GetState(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("init projector %q: %w", name, err)
	}
	p := &Projector[ModificationTx]{
		name:            name,
		store:           store,
		dispatch:        dispatch,
		backend:         backend,
		counter:         state.Counter,
		updateBatchSize: updateBatchSize,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// Update services any pending reset, then processes a batch of pending events and returns the
// count, or 0 if caught up.
func (p *Projector[ModificationTx]) Update(ctx context.Context) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// The backend's stored position is authoritative and p.counter is only a cache of it. Other
	// instances of this projector share that position and may have moved it since the last batch,
	// forwards by processing events or backwards by servicing a reset, so re-read it before
	// loading anything. Adopting a position ahead of ours just skips work another instance already
	// did; adopting one behind ours is what makes a reset elsewhere visible here.
	state, err := p.backend.GetState(ctx, p.name)
	if err != nil {
		return 0, fmt.Errorf("load state for projector %q: %w", p.name, err)
	}
	p.counter = state.Counter

	if state.ResetRequested {
		if err := p.serviceReset(ctx); err != nil {
			return 0, err
		}
	}

	events, err := p.store.LoadGlobalEvents(ctx, p.dispatch.EventFilters(), int64(p.counter), p.updateBatchSize)
	if err != nil {
		return 0, fmt.Errorf("load events: %w", err)
	}

	processed := 0
	for _, event := range events {
		counter := uint64(event.Event.GlobalCounter)
		current, err := p.backend.RunEvent(ctx, p.name, p.counter, counter, func(ctx context.Context, tx ModificationTx) error {
			return p.dispatch.Apply(ctx, tx, event)
		})
		if err != nil {
			return processed, fmt.Errorf("apply event %d: %w", event.Event.GlobalCounter, err)
		}
		if current != counter {
			// The stored position did not end up where this event would have left it, so the
			// backend declined to apply: another instance raced ahead of us, reset the projector
			// underneath us, or a reset is now pending. Either way the rest of this batch was
			// loaded against a position that no longer holds. Adopt the stored one and let the
			// next Update reload from there.
			p.counter = current
			return processed, nil
		}
		p.counter = counter
		processed++
	}

	return processed, nil
}

// serviceReset claims a pending reset and rewinds this projector to the beginning of the stream.
// Callers must hold p.mu.
func (p *Projector[ModificationTx]) serviceReset(ctx context.Context) error {
	if p.reset == nil {
		return fmt.Errorf(
			"projector %q: reset requested but no reset function is registered; pass monotonic.WithReset to NewProjector so the projector can clear what its handlers wrote",
			p.name,
		)
	}

	// claimed is false when another instance got there first. Either way the projector is now
	// rewound, so our cached position is stale regardless of who did the work. Starting from 0 is
	// safe even if the winner has already replayed past it, since RunEvent skips what is applied.
	if _, err := p.backend.ClaimReset(ctx, p.name, p.reset); err != nil {
		return fmt.Errorf("claim reset for projector %q: %w", p.name, err)
	}
	p.counter = 0
	return nil
}

// RequestReset marks the projector for reset and returns without doing the work. The next instance
// to poll (this one or another) claims the request and performs the reset exactly once, truncating
// via the function registered with WithReset and rewinding the resume counter to 0. Every other
// instance picks up the rewound position on its next batch, so it is safe to call while projectors
// are running.
//
// Use this to trigger a rebuild across a fleet. Writing the backend's pending-reset flag directly
// (e.g. by hand in SQL) has the same effect; this is the in-process spelling of it.
func (p *Projector[ModificationTx]) RequestReset(ctx context.Context) error {
	if p.reset == nil {
		return fmt.Errorf(
			"projector %q: cannot request reset, no reset function is registered; pass monotonic.WithReset to NewProjector",
			p.name,
		)
	}
	if err := p.backend.RequestReset(ctx, p.name); err != nil {
		return fmt.Errorf("request reset for projector %q: %w", p.name, err)
	}
	return nil
}

// Reset requests a reset and services it before returning, so the projection is cleared and this
// instance is rewound by the time Reset returns. Other instances pick up the rewind on their next
// batch. Prefer RequestReset when the caller does not need to observe the reset synchronously.
//
// If another instance claims the request first, Reset still returns successfully: the reset
// happened exactly once, just not here.
func (p *Projector[ModificationTx]) Reset(ctx context.Context) error {
	if err := p.RequestReset(ctx); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	return p.serviceReset(ctx)
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

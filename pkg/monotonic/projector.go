package monotonic

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

// DefaultUpdateBatchSize is a sensible default maximum number of events loaded per Update call.
const DefaultUpdateBatchSize = 100

// ProjectorStateSnapshot is a projector's persisted state as of the moment its modification lock was taken.
type ProjectorStateSnapshot struct {
	GlobalCounter     uint64
	TruncateRequested bool
}

// ModificationTx constrains the store-specific handle a ProjectorBackend hands to handlers (e.g. a Postgres pgx.Tx, a pipeline, a producer).
type ModificationTx interface {
	Rollback(ctx context.Context) error
}

// ProjectorBackend is the store-specific backend for handling projector state and projection modifications.
type ProjectorBackend[Tx ModificationTx] interface {
	LockStateForModification(ctx context.Context, projectorName string) (Tx, ProjectorStateSnapshot, error)
	CommitTruncatedStateFromModification(ctx context.Context, tx Tx, name string) error
	CommitAdvancedStateFromModification(ctx context.Context, tx Tx, name string, advancedToGlobalCounter uint64) error
	EnqueueTruncate(ctx context.Context, name string) error
}

// Projector reads events from a store and applies them to the projected state via dispatch.
type Projector[Tx ModificationTx] struct {
	// name identifies this projector in the backend's state store.
	name string
	// store is the event source the projector reads from.
	store Store
	// dispatch routes events to handlers and supplies EventFilters.
	dispatch *Dispatch[Tx]
	// backend runs each batch atomically with the counter advance.
	backend ProjectorBackend[Tx]
	// updateBatchSize caps the number of events loaded per Update call.
	updateBatchSize int
}

func NewProjector[Tx ModificationTx](
	name string,
	store Store,
	dispatch *Dispatch[Tx],
	backend ProjectorBackend[Tx],
	updateBatchSize int,
) (*Projector[Tx], error) {
	if name == "" {
		return nil, fmt.Errorf("NewProjector: name must be non-empty")
	}
	if updateBatchSize <= 0 {
		updateBatchSize = DefaultUpdateBatchSize
	}
	return &Projector[Tx]{
		name:            name,
		store:           store,
		dispatch:        dispatch,
		backend:         backend,
		updateBatchSize: updateBatchSize,
	}, nil
}

// ProjectorUpdateResult reports what a single Update call did.
type ProjectorUpdateResult struct {
	// AppliedEvents is how many events this call dispatched to handlers.
	AppliedEvents uint64

	// Truncated reports whether this call serviced a truncate request instead of applying events.
	Truncated bool

	// AdvancedToGlobalCounter is the resume position after this call, 0 after a truncate.
	AdvancedToGlobalCounter uint64
}

// Updated reports whether this call did any meaningful work.
func (p ProjectorUpdateResult) Updated() bool {
	return p.AppliedEvents > 0 || p.Truncated
}

// Update runs one unit of projector work under a single modification transaction.
func (p *Projector[Tx]) Update(ctx context.Context) (ProjectorUpdateResult, error) {
	tx, snapshot, err := p.backend.LockStateForModification(ctx, p.name)
	if err != nil {
		return ProjectorUpdateResult{}, fmt.Errorf("lock state for modification: %w", err)
	}
	defer tx.Rollback(ctx)

	if snapshot.TruncateRequested {
		return p.truncate(ctx, tx)
	}
	return p.advance(ctx, tx, snapshot)
}

func (p *Projector[Tx]) advance(ctx context.Context, tx Tx, snapshot ProjectorStateSnapshot) (ProjectorUpdateResult, error) {
	// Get all events since lock snapshot global counter.
	events, err := p.store.LoadGlobalEvents(
		ctx,
		p.dispatch.EventFilters(),
		int64(snapshot.GlobalCounter),
		p.updateBatchSize,
	)
	if err != nil {
		return ProjectorUpdateResult{}, fmt.Errorf("load events: %w", err)
	}

	advancedToGlobalCounter := snapshot.GlobalCounter
	appliedEvents := 0
	for _, event := range events {
		if uint64(event.Event.GlobalCounter) <= advancedToGlobalCounter {
			// This should never happen if the store is implemented correctly.
			return ProjectorUpdateResult{}, fmt.Errorf("event global counter %d out of order, below advancement to global counter %d", event.Event.GlobalCounter, advancedToGlobalCounter)
		}

		// Apply this one event via dispatch.
		err := p.dispatch.Apply(ctx, tx, event)
		if err != nil {
			return ProjectorUpdateResult{}, fmt.Errorf("apply event %d: %w", event.Event.GlobalCounter, err)
		}

		// Bump counters.
		appliedEvents++
		advancedToGlobalCounter = uint64(event.Event.GlobalCounter)
	}

	if appliedEvents == 0 {
		// Nothing to commit.
		return ProjectorUpdateResult{AdvancedToGlobalCounter: advancedToGlobalCounter}, nil
	}

	if err := p.backend.CommitAdvancedStateFromModification(ctx, tx, p.name, advancedToGlobalCounter); err != nil {
		return ProjectorUpdateResult{}, fmt.Errorf("commit advanced state: %w", err)
	}

	return ProjectorUpdateResult{
		AppliedEvents:           uint64(appliedEvents),
		AdvancedToGlobalCounter: advancedToGlobalCounter,
	}, nil
}

// truncate services a pending truncate request: it clears the projection and rewinds the resume counter to the start of the stream, in one transaction.
//
// A projector with no truncate handler wedges here rather than rewinding, and that is deliberate.
// Rewinding without clearing would replay the whole stream over the existing projection, which is silent corruption for any handler that accumulates.
// The request is left set so the wedge survives a restart: the only ways out are to register a handler or to clear the flag by hand, both of which are deliberate acts.
func (p *Projector[Tx]) truncate(ctx context.Context, tx Tx) (ProjectorUpdateResult, error) {
	// Checked before dispatching so the error can name the projector and the fix; Dispatch.Truncate reports the same sentinel for anyone calling it directly.
	if !p.dispatch.HasTruncateHandler() {
		return ProjectorUpdateResult{}, fmt.Errorf("truncate requested but %w", ErrNoTruncateHandler)
	}

	// Call dispatch handler for executing the actual projection truncation.
	if err := p.dispatch.Truncate(ctx, tx); err != nil {
		return ProjectorUpdateResult{}, fmt.Errorf("truncate: %w", err)
	}

	// Commit the truncated projector state.
	if err := p.backend.CommitTruncatedStateFromModification(ctx, tx, p.name); err != nil {
		return ProjectorUpdateResult{}, fmt.Errorf("commit truncated state: %w", err)
	}

	return ProjectorUpdateResult{
		Truncated: true,
	}, nil
}

// Run drives Update in a loop, sleeping pollInterval between catch-up polls; returns nil on context cancellation.
func (p *Projector[Tx]) Run(ctx context.Context, pollInterval time.Duration) error {
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

		if n.Updated() {
			// Some meaningful work was performed, continue without sleeping because there's likely more work to do imminently.
			continue
		}

		select {
		case <-time.After(pollInterval):
		case <-ctx.Done():
			return nil
		}
	}
}

// RequestTruncate asks this projector to clear its projection and rebuild from the start of the event stream on its next Update.
// It rejects the request outright when no truncate handler is registered, so a wiring gap surfaces synchronously here instead of wedging a background Run loop later.
//
// Prefer this over calling ProjectorBackend.EnqueueTruncate directly: the backend takes a projector name as a string, and a name that does not match any running projector enqueues a truncate nothing will ever service.
func (p *Projector[Tx]) RequestTruncate(ctx context.Context) error {
	if !p.dispatch.HasTruncateHandler() {
		return fmt.Errorf("projector %q: cannot request truncate because %w; register one with Dispatch.OnTruncate", p.name, ErrNoTruncateHandler)
	}

	if err := p.backend.EnqueueTruncate(ctx, p.name); err != nil {
		return fmt.Errorf("request truncate for projector %q: %w", p.name, err)
	}

	return nil
}

// Name returns the projector name used in the backend's state store.
func (p *Projector[Tx]) Name() string {
	return p.name
}

// ProjectorRunner is implemented by any projector that can be driven by RunProjectors.
// *Projector[Tx] satisfies this interface for any Tx, so RunProjectors can drive projectors with different transaction types together.
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

package monotonic

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

// DefaultUpdateBatchSize is a sensible default maximum number of events loaded per Update call.
const DefaultUpdateBatchSize = 100

type ProjectorStateSnapshot interface {
	GlobalCounter() uint64
	TruncateRequested() bool
}

type ModificationTx interface {
	Rollback() error
}

// ProjectorBackend is the store-specific backend for handling projector state and projection modifications.
type ProjectorBackend interface {
	LockStateForModification(ctx context.Context, projectorName string) (ModificationTx, ProjectorStateSnapshot, error)
	CommitTruncatedStateFromModification(ctx context.Context, tx ModificationTx, name string) error
	CommitAdvancedStateFromModification(ctx context.Context, tx ModificationTx, name string, advancedToGlobalCounter uint64) error
	EnqueueTruncate(ctx context.Context, name string) error
}

// Projector reads events from a Store and dispatches each one through a Dispatch,
// using a ProjectorBackend to commit the handler's work atomically with the counter advance.
type Projector[Tx ModificationTx] struct {
	// name identifies this projector in the backend's state store.
	name string
	// store is the event source the projector reads from.
	store Store
	// dispatch routes events to handlers and supplies EventFilters.
	dispatch *Dispatch[Tx]
	// backend runs each event atomically with the counter advance.
	backend ProjectorBackend
	// updateBatchSize caps the number of events loaded per Update call.
	updateBatchSize int
}

// NewProjector creates a Projector and derives its resume position from backend.GetState.
func NewProjector[Tx ModificationTx](
	ctx context.Context,
	name string,
	store Store,
	dispatch *Dispatch[Tx],
	backend ProjectorBackend,
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

type ProjectorUpdateResult struct {
	AppliedEvents           uint64
	Truncated               bool
	AdvancedToGlobalCounter uint64
}

func (p ProjectorUpdateResult) Updated() bool {
	return p.AppliedEvents > 0 || p.Truncated
}

// Update processes a batch of pending events and returns the count, or 0 if caught up.
func (p *Projector[Tx]) Update(ctx context.Context) (ProjectorUpdateResult, error) {
	tx, snapshot, err := p.backend.LockStateForModification(ctx, p.name)
	if err != nil {
		return ProjectorUpdateResult{}, fmt.Errorf("lock state for modification: %w", err)
	}
	defer tx.Rollback()

	if snapshot.TruncateRequested() {
		return p.truncate(ctx, tx)
	} else {
		return p.advance(ctx, tx, snapshot)
	}
}

func (p *Projector[Tx]) advance(ctx context.Context, tx ModificationTx, snapshot ProjectorStateSnapshot) (ProjectorUpdateResult, error) {
	// Get all events since lock snapshot global counter.
	events, err := p.store.LoadGlobalEvents(
		ctx,
		p.dispatch.EventFilters(),
		int64(snapshot.GlobalCounter()),
		p.updateBatchSize,
	)
	if err != nil {
		return ProjectorUpdateResult{}, fmt.Errorf("load events: %w", err)
	}

	advancedToGlobalCounter := snapshot.GlobalCounter()
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

	if err := p.backend.CommitAdvancedStateFromModification(ctx, tx, p.name, advancedToGlobalCounter); err != nil {
		return ProjectorUpdateResult{}, fmt.Errorf("commit advanced state: %w", err)
	}

	return ProjectorUpdateResult{
		AppliedEvents:           uint64(appliedEvents),
		AdvancedToGlobalCounter: advancedToGlobalCounter,
	}, nil
}

func (p *Projector[Tx]) truncate(ctx context.Context, tx ModificationTx) (ProjectorUpdateResult, error) {
	// Call dispatch handler for executing the actual projection truncation.
	if err := p.dispatch.Truncate(ctx, tx); err != nil {
		return ProjectorUpdateResult{}, fmt.Errorf("truncate: %w", err)
	}

	// Commit the truncated projector state.
	if err := p.backend.CommitTruncatedStateFromModification(ctx, tx, p.name); err != nil {
		return ProjectorUpdateResult{}, fmt.Errorf("commit advanced state: %w", err)
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

// Name returns the projector name used in the backend's state store.
func (p *Projector[Tx]) Name() string {
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

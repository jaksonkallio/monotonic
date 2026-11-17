package monotonic

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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

// Drain calls Update until the projector is caught up, then returns nil.
// It never sleeps and never waits for new events, so it terminates: it is for tests, backfills, CLI tools, and priming a read model before serving traffic.
// Use Drive for a long-running process that should keep polling.
func (p *Projector[Tx]) Drain(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		result, err := p.Update(ctx)
		if err != nil {
			return fmt.Errorf("drain projector %q: %w", p.name, err)
		}

		if !result.Updated() {
			return nil
		}
	}
}

// DefaultDriveErrorBackoff is the error backoff Drive uses when the caller supplies none.
// It plateaus at roughly a minute between retries, which keeps a projector that is broken for a long time from dominating the logs while still recovering promptly once the cause clears.
func DefaultDriveErrorBackoff() *Retry {
	return NewRetry(10, ExponentialBackoff(100*time.Millisecond))
}

// Drive drives Update in a loop, sleeping pollInterval between catch-up polls, and keeps going until ctx ends.
// Unlike Drain it logs update errors rather than returning them, so one failing projector does not stop a long-running process; the only thing that stops it is ctx ending, whose error it returns.
//
// Consecutive failures back off using errorBackoff, and a single success resets that.
// Pass nil for DefaultDriveErrorBackoff.
// Waiting goes through Retry.WaitForNextAttemptIndefinitely, so the backoff's MaxAttempts bounds how far the wait grows rather than how many attempts are allowed; see Retry.PlateauedBackoff.
// A BackoffFn returning zero means retry immediately, matching the BackoffFn contract, so pairing one with Drive gives a busy loop on a persistent error.
func (p *Projector[Tx]) Drive(ctx context.Context, pollInterval time.Duration, errorBackoff *Retry) error {
	if errorBackoff == nil || errorBackoff.Backoff == nil {
		errorBackoff = DefaultDriveErrorBackoff()
	}
	consecutiveFailures := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		result, err := p.Update(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			consecutiveFailures++
			slog.Error(
				"projector update failed",
				"projector", p.name,
				"consecutive_failures", consecutiveFailures,
				"retry_in", errorBackoff.PlateauedBackoff(consecutiveFailures),
				"err", err,
			)

			// Indefinitely, so the only thing this reports is the context ending.
			if waitErr := errorBackoff.WaitForNextAttemptIndefinitely(ctx, consecutiveFailures); waitErr != nil {
				return fmt.Errorf("wait for next attempt: %w", waitErr)
			}

			continue
		}

		if consecutiveFailures > 0 {
			slog.Info("projector recovered", "projector", p.name, "after_failures", consecutiveFailures)
			consecutiveFailures = 0
		}

		if result.Updated() {
			// Some meaningful work was performed, continue without sleeping because there's likely more work to do imminently.
			continue
		}

		select {
		case <-time.After(pollInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// RequestTruncate asks this projector to clear its projection and rebuild from the start of the event stream on its next Update.
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

// ProjectorDrainer is implemented by any projector that can be caught up by DrainProjectors.
// *Projector[Tx] satisfies this interface for any Tx, so DrainProjectors can catch up projectors with different transaction types together.
type ProjectorDrainer interface {
	Drain(ctx context.Context) error
}

// ProjectorDriver is implemented by any projector that can be driven by DriveProjectors.
type ProjectorDriver interface {
	Drive(ctx context.Context, pollInterval time.Duration, errorBackoff *Retry) error
}

// DrainProjectors catches up each projector concurrently in its own goroutine and returns nil once every one is caught up.
// The first projector to fail cancels the shared context, so the others stop early rather than finishing work that is about to be discarded, and DrainProjectors returns that first error.
// Pair it with DriveProjectors rather than choosing between them: drain once at startup to prime the read models, then drive for the life of the process.
func DrainProjectors(ctx context.Context, projectors ...ProjectorDrainer) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, p := range projectors {
		g.Go(func() error {
			return p.Drain(ctx)
		})
	}
	return g.Wait()
}

// DriveProjectors drives each projector concurrently in its own goroutine and returns once every one has stopped, which only happens when ctx ends.
// Update errors are logged and retried by each projector rather than propagated, so unlike DrainProjectors one failing projector never stops the others.
// Pass nil for errorBackoff to use DefaultDriveErrorBackoff.
// Returns ctx.Err(), which is what stopped every projector.
func DriveProjectors(ctx context.Context, pollInterval time.Duration, errorBackoff *Retry, projectors ...ProjectorDriver) error {
	var projectorsDone sync.WaitGroup
	for _, p := range projectors {
		projectorsDone.Add(1)

		go func() {
			defer projectorsDone.Done()
			if err := p.Drive(ctx, pollInterval, errorBackoff); err != nil && ctx.Err() == nil {
				slog.Error("projector drive stopped", "err", err)
			}
		}()
	}

	projectorsDone.Wait()

	return ctx.Err()
}

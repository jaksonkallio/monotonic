package monotonic

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

// Update processes a batch of pending events and returns the count, or 0 if caught up.
func (p *Projector[V]) Update(ctx context.Context) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	events, err := p.store.LoadGlobalEvents(ctx, p.logic.EventFilters(), int64(p.globalCounter), p.updateBatchSize)
	if err != nil {
		return 0, fmt.Errorf("load events: %w", err)
	}

	processed := 0
	for _, event := range events {
		updates, err := p.logic.Apply(ctx, p.persistence, event)
		if err != nil {
			return processed, fmt.Errorf("apply event %d: %w", event.Event.GlobalCounter, err)
		}

		// One Set per event keeps the batch atomic; on failure globalCounter does not advance, so retry replays.
		if err := p.persistence.Set(ctx, updates, uint64(event.Event.GlobalCounter)); err != nil {
			return processed, fmt.Errorf("persist event %d: %w", event.Event.GlobalCounter, err)
		}

		p.globalCounter = uint64(event.Event.GlobalCounter)
		processed++
	}

	return processed, nil
}

// Rebuild truncates the projection and replays all events from the beginning.
func (p *Projector[V]) Rebuild(ctx context.Context) error {
	p.mu.Lock()
	if err := p.persistence.Truncate(ctx); err != nil {
		p.mu.Unlock()
		return fmt.Errorf("truncate projection: %w", err)
	}
	p.globalCounter = 0
	p.mu.Unlock()

	for {
		n, err := p.Update(ctx)
		if err != nil {
			return fmt.Errorf("rebuild replay: %w", err)
		}
		if n == 0 {
			return nil
		}
	}
}

// Run drives Update in a loop, sleeping pollInterval between catch-up polls; returns nil on context cancellation.
func (p *Projector[V]) Run(ctx context.Context, pollInterval time.Duration) error {
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
func (p *Projector[V]) GlobalCounter() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.globalCounter
}

// ProjectorRunner is implemented by any projector that can be driven by RunProjectors.
// *Projector[V] satisfies this interface automatically.
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

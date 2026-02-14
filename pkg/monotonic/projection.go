package monotonic

import (
	"context"
	"sync"
)

// ProjectionLogic defines how a projection processes events.
type ProjectionLogic interface {
	// Apply processes an event and updates the projection's state.
	Apply(event AggregateEvent)

	// AggregateTypes returns the aggregate types this projection subscribes to.
	AggregateTypes() []string
}

// Projection manages catching up on events for a ProjectionLogic implementation.
type Projection struct {
	mu            sync.Mutex
	store         Store
	logic         ProjectionLogic
	globalCounter int64
}

// NewProjection creates a new projection that will process events from the given store.
// The projection starts at global counter 0, meaning it will process all historical events on first CatchUp.
func NewProjection(store Store, logic ProjectionLogic) *Projection {
	return &Projection{
		store:         store,
		logic:         logic,
		globalCounter: 0,
	}
}

// NewProjectionFrom creates a projection starting from a specific global counter.
// Use this when resuming a projection that has persisted its progress.
func NewProjectionFrom(store Store, logic ProjectionLogic, fromGlobalCounter int64) *Projection {
	return &Projection{
		store:         store,
		logic:         logic,
		globalCounter: fromGlobalCounter,
	}
}

// Update loads and applies all events since the last processed global counter.
// Returns the number of events processed.
func (p *Projection) Update(ctx context.Context) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	events, err := p.store.LoadGlobalEvents(ctx, p.logic.AggregateTypes(), p.globalCounter)
	if err != nil {
		return 0, err
	}

	for _, event := range events {
		p.logic.Apply(event)
		p.globalCounter = event.Event.GlobalCounter
	}

	return len(events), nil
}

// GlobalCounter returns the last processed global counter.
// Useful for persisting projection progress.
func (p *Projection) GlobalCounter() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.globalCounter
}

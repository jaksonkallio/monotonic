package monotonic

import (
	"context"
	"fmt"
	"time"
)

// Update processes all pending events and returns the count, or 0 if caught up.
func (p *Projector[V]) Update(ctx context.Context) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	aggregateFilters := toAggregateFilters(p.eventFilters)
	events, err := p.store.LoadGlobalEvents(ctx, aggregateFilters, int64(p.globalCounter))
	if err != nil {
		return 0, fmt.Errorf("load events: %w", err)
	}

	processed := 0
	for _, event := range events {
		// Store filtering is per-aggregate only; apply EventType client-side here.
		if !matchesAnyFilter(event, p.eventFilters) {
			p.globalCounter = uint64(event.Event.GlobalCounter)
			processed++
			continue
		}

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

// toAggregateFilters extracts and dedupes the (Type, ID) filters from EventFilters for Store.LoadGlobalEvents.
func toAggregateFilters(filters []EventFilter) []AggregateID {
	seen := make(map[AggregateID]struct{}, len(filters))
	result := make([]AggregateID, 0, len(filters))
	for _, f := range filters {
		id := AggregateID{Type: f.AggregateType, ID: f.AggregateID}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

// matchesAnyFilter reports whether the event matches at least one filter under OR semantics.
func matchesAnyFilter(event AggregateEvent, filters []EventFilter) bool {
	for _, f := range filters {
		if f.AggregateType != "" && event.AggregateType != f.AggregateType {
			continue
		}
		if f.AggregateID != "" && event.AggregateID != f.AggregateID {
			continue
		}
		if f.EventType != "" && event.Event.Type != f.EventType {
			continue
		}
		return true
	}
	return false
}

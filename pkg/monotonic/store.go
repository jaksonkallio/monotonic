package monotonic

import (
	"context"
	"fmt"
	"sync"
)

// Store is the interface for event persistence.
type Store interface {
	// LoadAggregateEvents returns events for an aggregate in counter order; only events with counter > afterCounter are returned (pass 0 for all).
	LoadAggregateEvents(ctx context.Context, aggregateType, aggregateID string, afterCounter int64) ([]AcceptedEvent, error)

	// Append atomically adds events to one or more aggregates' histories; either all are appended or none.
	Append(ctx context.Context, events ...AggregateEvent) error

	// LoadGlobalEvents returns events matching any of the EventFilters with global counter > afterGlobalCounter, ordered by global counter; an empty filter list returns no events. Limit caps the number of returned events; 0 means no limit.
	LoadGlobalEvents(ctx context.Context, filters []EventFilter, afterGlobalCounter int64, limit int) ([]AggregateEvent, error)
}

// ErrCounterConflict is returned when an event's counter conflicts with an existing event.
var ErrCounterConflict = fmt.Errorf("event counter conflict")

// inMemoryStoredAggregate is the in-memory backing for one aggregate's event log.
type inMemoryStoredAggregate struct {
	events []AcceptedEvent
}

// InMemoryStore is an in-memory implementation of Store, useful for tests and development.
type InMemoryStore struct {
	// mu protects all maps and slices below.
	mu sync.Mutex
	// aggregates holds the per-aggregate event logs.
	aggregates map[AggregateID]*inMemoryStoredAggregate
	// globalEvents records every appended event in global order for projection reads.
	globalEvents []AggregateEvent
	// globalCounter is the next global counter to assign on append.
	globalCounter int64
}

// NewInMemoryStore creates an empty InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		aggregates:    make(map[AggregateID]*inMemoryStoredAggregate),
		globalEvents:  []AggregateEvent{},
		globalCounter: 1,
	}
}

func (s *InMemoryStore) LoadAggregateEvents(ctx context.Context, aggregateType, aggregateID string, afterCounter int64) ([]AcceptedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := NewAggregateID(aggregateType, aggregateID)
	agg, exists := s.aggregates[id]
	if !exists {
		return nil, nil
	}

	// Counter is 1-indexed, slice is 0-indexed.
	if afterCounter >= int64(len(agg.events)) {
		return nil, nil
	}
	return agg.events[afterCounter:], nil
}

func (s *InMemoryStore) Append(ctx context.Context, events ...AggregateEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Track per-aggregate counters within this batch so multiple events for the same aggregate validate sequentially.
	eventsInBatch := make(map[AggregateID]int64)

	for _, ae := range events {
		id := NewAggregateID(ae.AggregateType, ae.AggregateID)
		agg := s.aggregates[id]

		var expectedCounter int64 = 1
		if agg != nil {
			expectedCounter = int64(len(agg.events)) + 1
		}
		expectedCounter += eventsInBatch[id]

		if ae.Event.Counter != expectedCounter {
			return fmt.Errorf(
				"%w for %s/%s: expected %d, got %d",
				ErrCounterConflict, ae.AggregateType, ae.AggregateID, expectedCounter, ae.Event.Counter,
			)
		}

		eventsInBatch[id]++
	}

	for _, ae := range events {
		id := NewAggregateID(ae.AggregateType, ae.AggregateID)
		agg, exists := s.aggregates[id]
		if !exists {
			agg = &inMemoryStoredAggregate{}
			s.aggregates[id] = agg
		}
		agg.events = append(agg.events, ae.Event)

		// Mirror into the global log with an assigned global counter for projection reads.
		globalEvent := ae
		globalEvent.Event.GlobalCounter = s.globalCounter
		s.globalEvents = append(s.globalEvents, globalEvent)
		s.globalCounter++
	}

	return nil
}

func (s *InMemoryStore) LoadGlobalEvents(ctx context.Context, filters []EventFilter, afterGlobalCounter int64, limit int) ([]AggregateEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []AggregateEvent
	for _, ae := range s.globalEvents {
		if ae.Event.GlobalCounter <= afterGlobalCounter {
			continue
		}
		for _, f := range filters {
			if f.AggregateType != "" && ae.AggregateType != f.AggregateType {
				continue
			}
			if f.AggregateID != "" && ae.AggregateID != f.AggregateID {
				continue
			}
			if f.EventType != "" && ae.Event.Type != f.EventType {
				continue
			}
			result = append(result, ae)
			break
		}
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

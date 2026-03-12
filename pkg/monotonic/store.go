package monotonic

import (
	"context"
	"fmt"
	"sync"
)

// Store is the interface for event persistence
type Store interface {
	// LoadAggregateEvents returns events for an aggregate in order.
	// Only events with counter > afterCounter are returned. Pass 0 to load all events.
	LoadAggregateEvents(ctx context.Context, aggregateType, aggregateID string, afterCounter int64) ([]AcceptedEvent, error)

	// Append adds new event(s) to the aggregate's event history atomically
	// Either all events are appended at once or none are
	// Returns an error if any event could not be appended (e.g. counter mismatch, saga closed)
	Append(ctx context.Context, events ...AggregateEvent) error

	// LoadGlobalEvents returns all events matching the given filters with global counter > afterGlobalCounter, ordered by global counter.
	// Each filter is an AggregateID: if ID is empty, all aggregates of that type match; if ID is set, only that specific aggregate matches.
	// Filters are OR-ed together.
	LoadGlobalEvents(ctx context.Context, filters []AggregateID, afterGlobalCounter int64) ([]AggregateEvent, error)
}

// SagaStore extends Store with saga lifecycle operations
type SagaStore interface {
	Store

	// ListActiveSagas returns all saga IDs of a given type that have not been marked completed
	ListActiveSagas(ctx context.Context, sagaType string) ([]string, error)

	// MarkSagaCompleted marks a saga as completed
	// Completed sagas cannot have more events appended and are not returned by `ListActiveSagas`
	// Idempotent, marking an already completed saga is a no-op
	MarkSagaCompleted(ctx context.Context, sagaType, sagaID string) error
}

// ErrSagaCompleted is returned when attempting to append to a completed saga
var ErrSagaCompleted = fmt.Errorf("saga is completed")

type inMemoryStoredAggregate struct {
	events []AcceptedEvent
}

type inMemorySaga struct {
	completed bool
}

// InMemoryStore is an in-memory implementation of the Store interface
// Useful for testing and development
type InMemoryStore struct {
	mu            sync.Mutex
	aggregates    map[AggregateID]*inMemoryStoredAggregate
	sagas         map[AggregateID]*inMemorySaga
	globalEvents  []AggregateEvent // all events in global order
	globalCounter int64            // next global counter to assign
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		aggregates:    make(map[AggregateID]*inMemoryStoredAggregate),
		sagas:         make(map[AggregateID]*inMemorySaga),
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

	// Events are stored in order, so we can slice from afterCounter
	// (counter is 1-indexed, slice is 0-indexed)
	if afterCounter >= int64(len(agg.events)) {
		return nil, nil
	}
	return agg.events[afterCounter:], nil
}

func (s *InMemoryStore) Append(ctx context.Context, events ...AggregateEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Track how many events seen for each aggregate in this batch, necessary for expected counts
	eventsInBatch := make(map[AggregateID]int64)

	for _, ae := range events {
		id := NewAggregateID(ae.AggregateType, ae.AggregateID)
		agg := s.aggregates[id]

		if saga, ok := s.sagas[id]; ok && saga.completed {
			return fmt.Errorf("%w: %s/%s", ErrSagaCompleted, ae.AggregateType, ae.AggregateID)
		}

		var expectedCounter int64 = 1
		if agg != nil {
			expectedCounter = int64(len(agg.events)) + 1
		}

		expectedCounter += eventsInBatch[id]

		if ae.Event.Counter != expectedCounter {
			return fmt.Errorf(
				"event counter mismatch for %s/%s: expected %d, got %d",
				ae.AggregateType, ae.AggregateID, expectedCounter, ae.Event.Counter,
			)
		}

		eventsInBatch[id]++
	}

	// Commit
	for _, ae := range events {
		id := NewAggregateID(ae.AggregateType, ae.AggregateID)
		agg, exists := s.aggregates[id]
		if !exists {
			agg = &inMemoryStoredAggregate{}
			s.aggregates[id] = agg
		}
		agg.events = append(agg.events, ae.Event)

		// Register newly started sagas
		if ae.Event.Type == EventTypeStarted {
			if _, ok := s.sagas[id]; !ok {
				s.sagas[id] = &inMemorySaga{}
			}
		}

		// Mark sagas as completed when saga-completed event is appended
		if ae.Event.Type == EventTypeCompleted {
			if _, ok := s.sagas[id]; !ok {
				s.sagas[id] = &inMemorySaga{}
			}
			s.sagas[id].completed = true
		}

		// Track in global event log for projections with assigned global counter
		globalEvent := ae
		globalEvent.Event.GlobalCounter = s.globalCounter
		s.globalEvents = append(s.globalEvents, globalEvent)
		s.globalCounter++
	}

	return nil
}

func (s *InMemoryStore) LoadGlobalEvents(ctx context.Context, filters []AggregateID, afterGlobalCounter int64) ([]AggregateEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []AggregateEvent
	for _, ae := range s.globalEvents {
		if ae.Event.GlobalCounter <= afterGlobalCounter {
			continue
		}
		for _, f := range filters {
			if ae.AggregateType == f.Type && (f.ID == "" || ae.AggregateID == f.ID) {
				result = append(result, ae)
				break
			}
		}
	}
	return result, nil
}

func (s *InMemoryStore) ListActiveSagas(ctx context.Context, sagaType string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ids []string
	for id, saga := range s.sagas {
		if id.Type == sagaType && !saga.completed {
			ids = append(ids, id.ID)
		}
	}
	return ids, nil
}

func (s *InMemoryStore) MarkSagaCompleted(ctx context.Context, sagaType, sagaID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := NewAggregateID(sagaType, sagaID)
	saga, exists := s.sagas[id]
	if !exists {
		// Nothing to complete, but that's fine (idempotent)
		return nil
	}
	saga.completed = true
	return nil
}

func (s *InMemoryStore) IsSagaCompleted(sagaType, sagaID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := NewAggregateID(sagaType, sagaID)
	saga, exists := s.sagas[id]
	if !exists {
		return false, nil
	}
	return saga.completed, nil
}

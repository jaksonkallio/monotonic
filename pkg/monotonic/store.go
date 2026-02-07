package monotonic

import "fmt"

// Store is the interface for event persistence
type Store interface {
	// Load returns all events for an aggregate in order
	Load(aggregateType, aggregateID string) ([]AcceptedEvent, error)

	// LoadAfter returns events with counter > afterCounter
	LoadAfter(aggregateType, aggregateID string, afterCounter int64) ([]AcceptedEvent, error)

	// Append adds new event(s) to the aggregate's event history atomically
	// Either all events are appended at once or none are
	// Returns an error if any event could not be appended (e.g. counter mismatch, saga closed)
	Append(events ...AggregateEvent) error

	// LoadEventsSince returns all events for the specified aggregate types with global counter > afterGlobalCounter, ordered by global counter.
	// Used by projections to catch up on events across multiple aggregate types.
	LoadEventsSince(aggregateTypes []string, afterGlobalCounter int64) ([]AggregateEvent, error)
}

// SagaStore extends Store with saga lifecycle operations
type SagaStore interface {
	Store

	// ListActiveSagas returns all saga IDs of a given type that have not been marked completed
	ListActiveSagas(sagaType string) ([]string, error)

	// MarkSagaCompleted marks a saga as completed
	// Completed sagas cannot have more events appended and are not returned by `ListActiveSagas`
	// Idempotent, marking an already completed saga is a no-op
	MarkSagaCompleted(sagaType, sagaID string) error
}

// ErrSagaCompleted is returned when attempting to append to a completed saga
var ErrSagaCompleted = fmt.Errorf("saga is completed")

type inMemoryStoredAggregate struct {
	events    []AcceptedEvent
	completed bool // only used for sagas
}

// InMemoryStore is an in-memory implementation of the Store interface,
// useful for testing and development.
type InMemoryStore struct {
	aggregates    map[AggregateID]*inMemoryStoredAggregate
	globalEvents  []AggregateEvent // all events in global order
	globalCounter int64            // next global counter to assign
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		aggregates:    make(map[AggregateID]*inMemoryStoredAggregate),
		globalEvents:  []AggregateEvent{},
		globalCounter: 1,
	}
}

func (s *InMemoryStore) Load(aggregateType, aggregateID string) ([]AcceptedEvent, error) {
	id := NewAggregateID(aggregateType, aggregateID)
	agg, exists := s.aggregates[id]
	if !exists {
		return nil, nil // New aggregate, no events yet
	}
	return agg.events, nil
}

func (s *InMemoryStore) LoadAfter(aggregateType, aggregateID string, afterCounter int64) ([]AcceptedEvent, error) {
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

func (s *InMemoryStore) Append(events ...AggregateEvent) error {
	for _, ae := range events {
		id := NewAggregateID(ae.AggregateType, ae.AggregateID)
		agg := s.aggregates[id]

		if agg != nil && agg.completed {
			return fmt.Errorf("%w: %s/%s", ErrSagaCompleted, ae.AggregateType, ae.AggregateID)
		}

		var expectedCounter int64 = 1
		if agg != nil {
			expectedCounter = int64(len(agg.events)) + 1
		}

		if ae.Event.Counter != expectedCounter {
			return fmt.Errorf(
				"event counter mismatch for %s/%s: expected %d, got %d",
				ae.AggregateType, ae.AggregateID, expectedCounter, ae.Event.Counter,
			)
		}
	}

	// Phase 2: All counters valid, commit all events
	for _, ae := range events {
		id := NewAggregateID(ae.AggregateType, ae.AggregateID)
		agg, exists := s.aggregates[id]
		if !exists {
			agg = &inMemoryStoredAggregate{}
			s.aggregates[id] = agg
		}
		agg.events = append(agg.events, ae.Event)

		// Track in global event log for projections with assigned global counter
		globalEvent := ae
		globalEvent.Event.GlobalCounter = s.globalCounter
		s.globalEvents = append(s.globalEvents, globalEvent)
		s.globalCounter++
	}

	return nil
}

func (s *InMemoryStore) LoadEventsSince(aggregateTypes []string, afterGlobalCounter int64) ([]AggregateEvent, error) {
	// Build a set for O(1) lookup
	typeSet := make(map[string]bool, len(aggregateTypes))
	for _, t := range aggregateTypes {
		typeSet[t] = true
	}

	var result []AggregateEvent
	for _, ae := range s.globalEvents {
		if ae.Event.GlobalCounter > afterGlobalCounter && typeSet[ae.AggregateType] {
			result = append(result, ae)
		}
	}
	return result, nil
}

func (s *InMemoryStore) ListActiveSagas(sagaType string) ([]string, error) {
	var ids []string
	for aggID, agg := range s.aggregates {
		if aggID.Type == sagaType && !agg.completed {
			ids = append(ids, aggID.ID)
		}
	}
	return ids, nil
}

func (s *InMemoryStore) MarkSagaCompleted(sagaType, sagaID string) error {
	id := NewAggregateID(sagaType, sagaID)
	agg, exists := s.aggregates[id]
	if !exists {
		// Nothing to complete, but that's fine (idempotent)
		return nil
	}
	agg.completed = true
	return nil
}

func (s *InMemoryStore) IsSagaCompleted(sagaType, sagaID string) (bool, error) {
	id := NewAggregateID(sagaType, sagaID)
	agg, exists := s.aggregates[id]
	if !exists {
		return false, nil
	}
	return agg.completed, nil
}

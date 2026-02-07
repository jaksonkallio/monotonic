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
	// Returns an error if any event could not be appended (e.g. counter mismatch, aggregate closed)
	Append(events ...AggregateEvent) error

	// ListAggregates returns all non-closed aggregate IDs of a given type.
	// Used by saga drivers to discover sagas that need to be stepped.
	ListAggregates(aggregateType string) ([]string, error)

	// Close marks an aggregate as closed. Closed aggregates:
	// - Cannot have more events appended (Append/AppendMulti returns error)
	// - Are excluded from ListAggregates results
	// - Can still be loaded and read
	// This is idempotent - closing an already-closed aggregate is not an error.
	Close(aggregateType, aggregateID string) error

	// IsClosed returns whether an aggregate is closed.
	IsClosed(aggregateType, aggregateID string) (bool, error)
}

// ErrAggregateClosed is returned when attempting to append to a closed aggregate
var ErrAggregateClosed = fmt.Errorf("aggregate is closed")

type inMemoryStoredAggregate struct {
	events []AcceptedEvent
	closed bool
}

// InMemoryStore is an in-memory implementation of the Store interface,
// useful for testing and development.
type InMemoryStore struct {
	aggregates map[AggregateID]*inMemoryStoredAggregate
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		aggregates: make(map[AggregateID]*inMemoryStoredAggregate),
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

		if agg != nil && agg.closed {
			return fmt.Errorf("%w: %s/%s", ErrAggregateClosed, ae.AggregateType, ae.AggregateID)
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
	}

	return nil
}

func (s *InMemoryStore) ListAggregates(aggregateType string) ([]string, error) {
	var ids []string
	for aggID, agg := range s.aggregates {
		if aggID.Type == aggregateType && !agg.closed {
			ids = append(ids, aggID.ID)
		}
	}
	return ids, nil
}

func (s *InMemoryStore) Close(aggregateType, aggregateID string) error {
	id := NewAggregateID(aggregateType, aggregateID)
	agg, exists := s.aggregates[id]
	if !exists {
		// Nothing to close, but that's fine (idempotent)
		return nil
	}
	agg.closed = true
	return nil
}

func (s *InMemoryStore) IsClosed(aggregateType, aggregateID string) (bool, error) {
	id := NewAggregateID(aggregateType, aggregateID)
	agg, exists := s.aggregates[id]
	if !exists {
		return false, nil
	}
	return agg.closed, nil
}

package monotonic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ActionFunc executes work for a saga state.
// It receives the saga (for refs and current state) and store (to load aggregates).
//
// Return combinations:
//   - (*ActionResult, nil) with Close=false → normal transition, continue stepping
//   - (*ActionResult, nil) with Close=true  → close the saga (no other fields allowed)
//   - (nil, err) → transient error, retry later
//
// When Close=true, NewState, ExtraEvents, and Delay must be empty - the close
// operation is its own atomic step with no other side effects.
type ActionFunc func(ctx context.Context, saga *Saga, store Store) (*ActionResult, error)

// ActionMap maps state names to their action functions
type ActionMap map[string]ActionFunc

// ActionResult represents the outcome of a saga action
type ActionResult struct {
	// NewState is the state to transition to
	// Must be empty if `Close` is true
	NewState string

	// Events are events to append to other aggregates atomically
	// Must be empty if `Close` is true
	Events []AggregateEvent

	// Close indicates the saga should be closed
	// Typical pattern: transition to some "completed" state, then have "completed" action return `Close` as true
	// When true, other fields must be empty because closing implies that no other subsequent transitions or actions can occur
	Close bool

	// Delay before the next step can run
	// Must be zero if `Close` is true
	Delay time.Duration
}

// sagaStartedPayload is stored in the saga-started event
type sagaStartedPayload struct {
	InitialState string                 `json:"initial_state"`
	Refs         map[string]AggregateID `json:"refs"`
}

// stateTransitionPayload is stored in state-transitioned events
type stateTransitionPayload struct {
	ToState string        `json:"to_state"`
	ReadyAt time.Time     `json:"ready_at,omitempty"`
	Delay   time.Duration `json:"delay,omitempty"`
}

// Saga is a state machine that coordinates multiple aggregates.
// It tracks only its current state and references to aggregates -
// all actual data lives in the aggregates themselves.
type Saga struct {
	eventStream

	State   string
	Refs    map[string]AggregateID // named references to aggregates
	ReadyAt time.Time              // earliest time the next step can run
	Closed  bool                   // whether the saga is closed

	actions ActionMap
}

// ErrInvalidCloseResult is returned when ActionResult has Close=true with other fields set
var ErrInvalidCloseResult = fmt.Errorf("ActionResult with Close=true cannot have NewState, ExtraEvents, or Delay")

// NewSaga creates and persists a new saga.
func NewSaga(
	store Store,
	sagaType, id string,
	initialState string,
	refs map[string]AggregateID,
	actions ActionMap,
) (*Saga, error) {
	saga := &Saga{
		eventStream: eventStream{
			ID:    NewAggregateID(sagaType, id),
			store: store,
		},
		State:   initialState,
		Refs:    refs,
		ReadyAt: time.Now(),
		actions: actions,
	}

	// Persist the initial event
	payload, _ := json.Marshal(sagaStartedPayload{
		InitialState: initialState,
		Refs:         refs,
	})

	event := Event{
		Type:       "saga-started",
		Counter:    1,
		AcceptedAt: time.Now(),
		Payload:    payload,
	}

	if err := store.Append(sagaType, id, event); err != nil {
		return nil, fmt.Errorf("persist saga start: %w", err)
	}
	saga.counter = 1

	return saga, nil
}

// LoadSaga hydrates a saga from the store.
func LoadSaga(store Store, sagaType, id string, actions ActionMap) (*Saga, error) {
	events, err := store.Load(sagaType, id)
	if err != nil {
		return nil, err
	}

	if len(events) == 0 {
		return nil, fmt.Errorf("saga %s/%s not found", sagaType, id)
	}

	saga := &Saga{
		eventStream: eventStream{
			ID:    NewAggregateID(sagaType, id),
			store: store,
		},
		actions: actions,
	}

	for _, e := range events {
		saga.apply(e)
		saga.applied(e)
	}

	return saga, nil
}

func (s *Saga) apply(event Event) {
	switch event.Type {
	case "saga-started":
		var payload sagaStartedPayload
		json.Unmarshal(event.Payload, &payload)
		s.State = payload.InitialState
		s.Refs = payload.Refs
		s.ReadyAt = event.AcceptedAt

	case "state-transitioned":
		var payload stateTransitionPayload
		json.Unmarshal(event.Payload, &payload)
		s.State = payload.ToState
		if !payload.ReadyAt.IsZero() {
			s.ReadyAt = payload.ReadyAt
		} else {
			s.ReadyAt = event.AcceptedAt
		}

	case "saga-closed":
		s.Closed = true
	}
}

// IsReady returns true if the saga is ready to step (not delayed)
func (s *Saga) IsReady() bool {
	return time.Now().After(s.ReadyAt) || time.Now().Equal(s.ReadyAt)
}

// Step executes the action for the current state.
// Returns nil without doing anything if:
//   - The saga is closed
//   - The saga is delayed (ReadyAt is in the future)
//   - No action is defined for the current state
func (s *Saga) Step(ctx context.Context) error {
	// Check if already closed
	if s.Closed {
		return nil
	}

	// Check if delayed
	if !s.IsReady() {
		return nil
	}

	// Catch up on any missed events
	if err := s.catchUp(s.apply); err != nil {
		return err
	}

	// Check again after catch-up (might have been closed by another process)
	if s.Closed {
		return nil
	}

	// Get the action for current state
	action, exists := s.actions[s.State]
	if !exists {
		return nil // no action for this state, nothing to do
	}

	// Execute the action
	result, actionErr := action(ctx, s, s.store)

	// Handle transient errors
	if actionErr != nil {
		return actionErr
	}

	// If no result, nothing to do
	if result == nil {
		return nil
	}

	// Handle close request
	if result.Close {
		// Validate invariant: Close=true cannot have other fields set
		if result.NewState != "" || len(result.Events) > 0 || result.Delay > 0 {
			return ErrInvalidCloseResult
		}
		return s.closeSaga()
	}

	// Handle state transition
	return s.transition(result)
}

// closeSaga appends a saga-closed event and marks the saga as closed in the store
func (s *Saga) closeSaga() error {
	closeEvent := Event{
		Type:       "saga-closed",
		Counter:    s.nextCounter(),
		AcceptedAt: time.Now(),
	}

	if err := s.append(closeEvent); err != nil {
		return fmt.Errorf("saga close event: %w", err)
	}

	s.apply(closeEvent)
	s.applied(closeEvent)

	// Mark closed in store (for ListAggregates filtering)
	if err := s.store.Close(s.ID.Type, s.ID.ID); err != nil {
		return fmt.Errorf("saga store close: %w", err)
	}

	return nil
}

// transition appends a state transition event and any extra events atomically
func (s *Saga) transition(result *ActionResult) error {
	// Calculate ready time for delayed transitions
	readyAt := time.Now()
	if result.Delay > 0 {
		readyAt = readyAt.Add(result.Delay)
	}

	payload, _ := json.Marshal(stateTransitionPayload{
		ToState: result.NewState,
		ReadyAt: readyAt,
		Delay:   result.Delay,
	})

	sagaEvent := Event{
		Type:       "state-transitioned",
		Counter:    s.nextCounter(),
		AcceptedAt: time.Now(),
		Payload:    payload,
	}

	// Combine with extra events
	allEvents := []AggregateEvent{
		{AggregateType: s.ID.Type, AggregateID: s.ID.ID, Event: sagaEvent},
	}
	allEvents = append(allEvents, result.Events...)

	// Append atomically
	if err := s.appendMulti(allEvents); err != nil {
		return fmt.Errorf("saga transition: %w", err)
	}

	s.apply(sagaEvent)
	s.applied(sagaEvent)

	return nil
}

// Run drives the saga to completion by repeatedly calling Step.
// Stops when the saga is closed, delayed, or an error occurs.
// Note: This does not wait for delayed transitions - use a scheduler for that.
func (s *Saga) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Check if closed
		if s.Closed {
			return nil
		}

		// Check if delayed
		if !s.IsReady() {
			return nil // caller should wait and retry later
		}

		// Check if we have an action for current state
		if _, exists := s.actions[s.State]; !exists {
			return nil // no action, nothing to do
		}

		if err := s.Step(ctx); err != nil {
			return err
		}
	}
}

// PrepareEvent creates an AggregateEvent ready for AppendMulti
// The counter must be set to (current aggregate counter + 1)
func PrepareEvent(aggType, aggID string, counter int64, eventType string, payload any) (AggregateEvent, error) {
	var data []byte
	var err error
	if payload != nil {
		data, err = json.Marshal(payload)
		if err != nil {
			return AggregateEvent{}, err
		}
	}
	return AggregateEvent{
		AggregateType: aggType,
		AggregateID:   aggID,
		Event: Event{
			Type:       eventType,
			Counter:    counter,
			AcceptedAt: time.Now(),
			Payload:    data,
		},
	}, nil
}

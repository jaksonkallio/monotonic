package monotonic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ActionFunc executes work for a saga state
// It returns an ActionResult, describing the outcome of the action
// The outcome may be a state transition to a new state, or signalling to close the saga
// Events can be returned to be appended atomically (all-or-nothing) with the saga state transition
type ActionFunc func(ctx context.Context, saga *Saga, store Store) (ActionResult, error)

type ActionMap map[string]ActionFunc

// TypedActionFunc is an action function with a typed input parameter.
// Use with TypedAction to avoid manual input parsing in every action.
type TypedActionFunc[I any] func(ctx context.Context, saga *Saga, input I, store Store) (ActionResult, error)

// TypedAction wraps a TypedActionFunc to automatically parse the saga input.
// This eliminates boilerplate input parsing from every action.
func TypedAction[I any](fn TypedActionFunc[I]) ActionFunc {
	return func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
		var input I
		if err := saga.InputAs(&input); err != nil {
			return ActionResult{}, fmt.Errorf("parse saga input: %w", err)
		}
		return fn(ctx, saga, input, store)
	}
}

// ActionResult represents the outcome of a saga action
type ActionResult struct {
	// NewState is the state to transition to
	// Must be blank if `Complete` is true
	NewState string

	// Events are events to append to other aggregates atomically
	// Must be empty if `Complete` is true
	Events []AggregateEvent

	// Complete indicates the saga should be completed
	// Typical pattern: transition to some "completed" state, then have "completed" action return `Complete` as true
	// When true, other fields must be empty because completing implies that no other subsequent transitions or actions can occur
	Complete bool

	// Delay before the next step can run
	// Must be zero if `Complete` is true
	Delay time.Duration
}

// sagaStartedPayload is stored in the saga-started event
type sagaStartedPayload struct {
	InitialState string          `json:"initial_state"`
	Input        json.RawMessage `json:"input"`
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
	sagaStore SagaStore

	// State is the current state of the saga
	state string

	// Input is a blob of initial data provided when starting the saga
	// It does not change over the lifetime of the saga
	// Used to store any data needed to drive the saga that doesn't belong in aggregate state, such as the identifiers for relevant aggregates themselves
	input json.RawMessage

	// ReadyAt is the earliest time the next step may run
	readyAt time.Time

	// Completed indicates whether the saga is completed
	completed bool

	actions ActionMap
}

// ErrInvalidCloseResult is returned when ActionResult has Close=true with other fields set
var ErrInvalidCloseResult = fmt.Errorf("ActionResult with Close=true cannot have NewState, Events, or Delay")

// NewSaga creates and persists a new saga.
func NewSaga(
	store SagaStore,
	sagaType, id string,
	initialState string,
	input json.RawMessage,
	actions ActionMap,
) (*Saga, error) {
	ctx := context.Background()

	saga := &Saga{
		eventStream: eventStream{
			ID:    NewAggregateID(sagaType, id),
			store: store,
		},
		sagaStore: store,
		state:     initialState,
		input:     input,
		readyAt:   time.Now(),
		actions:   actions,
	}

	// Persist the initial event
	payload, _ := json.Marshal(sagaStartedPayload{
		InitialState: initialState,
		Input:        input,
	})

	event := AggregateEvent{
		AggregateType: sagaType,
		AggregateID:   id,
		Event: AcceptedEvent{
			Event: Event{
				Type:    "saga-started",
				Payload: payload,
			},
			Counter:    1,
			AcceptedAt: time.Now(),
		},
	}

	if err := store.Append(ctx, event); err != nil {
		return nil, fmt.Errorf("persist saga start: %w", err)
	}
	saga.counter = 1

	return saga, nil
}

// LoadSaga hydrates a saga from the store.
func LoadSaga(store SagaStore, sagaType, id string, actions ActionMap) (*Saga, error) {
	events, err := store.LoadAggregateEvents(context.Background(), sagaType, id, 0)
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
		sagaStore: store,
		actions:   actions,
	}

	for _, e := range events {
		saga.apply(e)
		saga.applied(e)
	}

	return saga, nil
}

// Input returns the saga input data
// Copied to prevent external modification
func (s *Saga) Input() json.RawMessage {
	var copyTo = make([]byte, len(s.input))
	copy(copyTo, s.input)
	return copyTo
}

func (s *Saga) InputAs(v any) error {
	return json.Unmarshal(s.input, v)
}

func (s *Saga) State() string {
	return s.state
}

func (s *Saga) Completed() bool {
	return s.completed
}

func (s *Saga) apply(event AcceptedEvent) {
	switch event.Type {
	case "saga-started":
		var payload sagaStartedPayload
		json.Unmarshal(event.Payload, &payload)
		s.state = payload.InitialState
		s.input = payload.Input
		s.readyAt = event.AcceptedAt

	case "state-transitioned":
		var payload stateTransitionPayload
		json.Unmarshal(event.Payload, &payload)
		s.state = payload.ToState
		if !payload.ReadyAt.IsZero() {
			s.readyAt = payload.ReadyAt
		} else {
			s.readyAt = event.AcceptedAt
		}

	case "saga-completed":
		s.completed = true
	}
}

// IsReady returns true if the saga is ready to step (not delayed)
func (s *Saga) IsReady() bool {
	return time.Now().After(s.readyAt) || time.Now().Equal(s.readyAt)
}

// Step executes an action for the current state, hopefully transitioning the saga into a new state
func (s *Saga) Step(ctx context.Context) error {
	// Catch up on any missed events
	if err := s.catchUp(ctx, s.apply); err != nil {
		return err
	}

	if s.completed {
		return nil
	}
	if !s.IsReady() {
		return nil
	}

	// Get the action for current state
	action, exists := s.actions[s.state]
	if !exists {
		return fmt.Errorf("no action defined for saga state %q", s.state)
	}

	// Execute the action
	result, actionErr := action(ctx, s, s.store)
	if actionErr != nil {
		return fmt.Errorf("action: %w", actionErr)
	}

	// Saga should be closed as a result of this action
	if result.Complete {
		if result.NewState != "" || len(result.Events) > 0 || result.Delay > 0 {
			return ErrInvalidCloseResult
		}
		return s.closeSaga(ctx)
	}

	if result.NewState == "" {
		return fmt.Errorf("action result new state is empty")
	}

	if result.Delay < 0 {
		return fmt.Errorf("action result delay cannot be negative")
	}

	if err := s.transition(ctx, result); err != nil {
		return fmt.Errorf("transition: %w", err)
	}

	return nil
}

// closeSaga appends a saga-completed event and marks the saga as closed in the store
func (s *Saga) closeSaga(ctx context.Context) error {
	closeEvent := AggregateEvent{
		AggregateType: s.ID.Type,
		AggregateID:   s.ID.ID,
		Event: AcceptedEvent{
			Event: Event{
				Type: "saga-completed",
			},
			Counter:    s.nextCounter(),
			AcceptedAt: time.Now(),
		},
	}

	if err := s.append(ctx, closeEvent); err != nil {
		return fmt.Errorf("saga close event: %w", err)
	}

	s.apply(closeEvent.Event)
	s.applied(closeEvent.Event)

	if err := s.sagaStore.MarkSagaCompleted(ctx, s.ID.Type, s.ID.ID); err != nil {
		return fmt.Errorf("mark saga completed: %w", err)
	}

	return nil
}

// transition appends a state transition event and any extra events atomically
func (s *Saga) transition(ctx context.Context, result ActionResult) error {
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

	sagaEvent := AcceptedEvent{
		Event: Event{
			Type:    "state-transitioned",
			Payload: payload,
		},
		Counter:    s.nextCounter(),
		AcceptedAt: time.Now(),
	}

	// Merge the saga state transition event with the other aggregate events
	allEvents := []AggregateEvent{
		{AggregateType: s.ID.Type, AggregateID: s.ID.ID, Event: sagaEvent},
	}
	allEvents = append(allEvents, result.Events...)
	if err := s.append(ctx, allEvents...); err != nil {
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
		if s.completed {
			return nil
		}

		// Check if delayed
		if !s.IsReady() {
			return nil // caller should wait and retry later
		}

		// Check if we have an action for current state
		if _, exists := s.actions[s.state]; !exists {
			return nil // no action, nothing to do
		}

		if err := s.Step(ctx); err != nil {
			return err
		}
	}
}

package monotonic

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// silentLogger discards all log output
type silentLogger struct{}

func (silentLogger) Printf(format string, v ...any) {}

func TestSagaDriver(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	// Track how many times each state's action is called
	var startedCalls, processedCalls, completedCalls int32

	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			atomic.AddInt32(&startedCalls, 1)
			return ActionResult{NewState: "processed"}, nil
		},
		"processed": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			atomic.AddInt32(&processedCalls, 1)
			return ActionResult{NewState: "completed"}, nil
		},
		"completed": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			atomic.AddInt32(&completedCalls, 1)
			return ActionResult{Complete: true}, nil
		},
	}

	// Create a few sagas
	NewSaga(store, "test-saga", "saga-1", "started", nil, actions)
	NewSaga(store, "test-saga", "saga-2", "started", nil, actions)
	NewSaga(store, "test-saga", "saga-3", "started", nil, actions)

	// Create driver
	driver := NewSagaDriver(SagaDriverConfig{
		Store:    store,
		SagaType: "test-saga",
		Actions:  actions,
		Interval: 50 * time.Millisecond,
	})

	// Step all once - should move all from "started" to "processed"
	err := driver.StepAll(ctx)
	if err != nil {
		t.Fatalf("StepAll failed: %v", err)
	}

	if startedCalls != 3 {
		t.Errorf("expected 3 started calls, got %d", startedCalls)
	}

	// Step again - should move all from "processed" to "completed"
	err = driver.StepAll(ctx)
	if err != nil {
		t.Fatalf("StepAll failed: %v", err)
	}

	if processedCalls != 3 {
		t.Errorf("expected 3 processed calls, got %d", processedCalls)
	}

	// Step again - should close all sagas
	err = driver.StepAll(ctx)
	if err != nil {
		t.Fatalf("StepAll failed: %v", err)
	}

	if completedCalls != 3 {
		t.Errorf("expected 3 completed calls, got %d", completedCalls)
	}

	// Step again - all are closed, nothing should happen
	prevStarted := startedCalls
	prevProcessed := processedCalls
	prevCompleted := completedCalls

	err = driver.StepAll(ctx)
	if err != nil {
		t.Fatalf("StepAll failed: %v", err)
	}

	if startedCalls != prevStarted || processedCalls != prevProcessed || completedCalls != prevCompleted {
		t.Error("expected no calls for closed sagas")
	}

	// Verify final states
	saga1, _ := LoadSaga(store, "test-saga", "saga-1", actions)
	saga2, _ := LoadSaga(store, "test-saga", "saga-2", actions)
	saga3, _ := LoadSaga(store, "test-saga", "saga-3", actions)

	for _, saga := range []*Saga{saga1, saga2, saga3} {
		if saga.state != "completed" {
			t.Errorf("expected state 'completed', got %s", saga.state)
		}
		if !saga.completed {
			t.Error("expected saga to be closed")
		}
	}
}

func TestSagaDriverRun(t *testing.T) {
	store := NewInMemoryStore()

	var calls int32
	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			atomic.AddInt32(&calls, 1)
			return ActionResult{NewState: "completed"}, nil
		},
		"completed": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{Complete: true}, nil
		},
	}

	NewSaga(store, "test-saga", "saga-1", "started", nil, actions)

	driver := NewSagaDriver(SagaDriverConfig{
		Store:    store,
		SagaType: "test-saga",
		Actions:  actions,
		Interval: 10 * time.Millisecond,
	})

	// Run driver in background
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		done <- driver.Run(ctx)
	}()

	// Wait for at least one step
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait for driver to stop
	<-done

	if calls < 1 {
		t.Errorf("expected at least 1 call, got %d", calls)
	}
}

func TestSagaDriverRespectsDelay(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	var calls int32
	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			atomic.AddInt32(&calls, 1)
			return ActionResult{
				NewState: "waiting",
				Delay:    100 * time.Millisecond,
			}, nil
		},
		"waiting": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			atomic.AddInt32(&calls, 1)
			return ActionResult{NewState: "completed"}, nil
		},
		"completed": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			atomic.AddInt32(&calls, 1)
			return ActionResult{Complete: true}, nil
		},
	}

	NewSaga(store, "test-saga", "saga-1", "started", nil, actions)

	driver := NewSagaDriver(SagaDriverConfig{
		Store:    store,
		SagaType: "test-saga",
		Actions:  actions,
	})

	// First step: started -> waiting (with delay)
	driver.StepAll(ctx)
	if calls != 1 {
		t.Errorf("expected 1 call after first step, got %d", calls)
	}

	// Second step: should be skipped due to delay
	driver.StepAll(ctx)
	if calls != 1 {
		t.Errorf("expected still 1 call (saga delayed), got %d", calls)
	}

	// Wait for delay to pass
	time.Sleep(150 * time.Millisecond)

	// Third step: should now proceed
	driver.StepAll(ctx)
	if calls != 2 {
		t.Errorf("expected 2 calls after delay, got %d", calls)
	}
}

func TestSagaClosesOnExplicitClose(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{NewState: "completed"}, nil
		},
		"completed": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{Complete: true}, nil // explicit close
		},
	}

	saga, _ := NewSaga(store, "test-saga", "saga-1", "started", nil, actions)

	// Verify saga is listed
	ids, _ := store.ListActiveSagas(ctx, "test-saga")
	if len(ids) != 1 {
		t.Errorf("expected 1 saga, got %d", len(ids))
	}

	// Step 1 - transition to completed
	err := saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 1 failed: %v", err)
	}

	if saga.state != "completed" {
		t.Errorf("expected state 'completed', got %s", saga.state)
	}

	if saga.completed {
		t.Error("saga should not be closed yet")
	}

	// Step 2 - close
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 2 failed: %v", err)
	}

	if !saga.completed {
		t.Error("expected saga.completed to be true")
	}

	// Saga should now be closed in store
	completed, _ := store.IsSagaCompleted("test-saga", "saga-1")
	if !completed {
		t.Error("expected saga to be completed in store")
	}

	// ListAggregates should not return completed sagas
	ids, _ = store.ListActiveSagas(ctx, "test-saga")
	if len(ids) != 0 {
		t.Errorf("expected 0 sagas after closing, got %d", len(ids))
	}

	// Saga can still be loaded and read
	loadedSaga, err := LoadSaga(store, "test-saga", "saga-1", actions)
	if err != nil {
		t.Fatalf("LoadSaga failed: %v", err)
	}
	if loadedSaga.state != "completed" {
		t.Errorf("expected loaded saga state 'completed', got %s", loadedSaga.state)
	}
	if !loadedSaga.completed {
		t.Error("expected loaded saga to be closed")
	}

	// Cannot append to closed saga
	err = store.Append(ctx, AggregateEvent{AggregateType: "test-saga", AggregateID: "saga-1", Event: AcceptedEvent{Event: Event{Type: "test"}, Counter: 4}})
	if err == nil {
		t.Error("expected error when appending to closed saga")
	}
}

func TestSagaFailureThenClose(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			// Detect failure, transition to failed state
			return ActionResult{NewState: "failed"}, nil
		},
		"failed": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			// Close the saga from failed state
			return ActionResult{Complete: true}, nil
		},
	}

	saga, _ := NewSaga(store, "test-saga", "saga-1", "started", nil, actions)

	// Step 1 - transition to failed
	err := saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 1 failed: %v", err)
	}

	if saga.state != "failed" {
		t.Errorf("expected state 'failed', got %s", saga.state)
	}

	// Step 2 - close
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 2 failed: %v", err)
	}

	if !saga.completed {
		t.Error("expected saga to be closed")
	}

	// Load and verify
	loadedSaga, _ := LoadSaga(store, "test-saga", "saga-1", actions)
	if loadedSaga.state != "failed" {
		t.Errorf("expected loaded saga state 'failed', got %s", loadedSaga.state)
	}
	if !loadedSaga.completed {
		t.Error("expected loaded saga to be closed")
	}
}

func TestSagaFailureReason(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	failureMsg := "payment gateway unreachable after 3 retries"

	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{
				Complete:          true,
				SagaFailureReason: failureMsg,
			}, nil
		},
	}

	saga, _ := NewSaga(store, "test-saga", "saga-1", "started", nil, actions)

	err := saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step failed: %v", err)
	}

	if !saga.Completed() {
		t.Error("expected saga to be completed")
	}
	if saga.SagaFailureReason() != failureMsg {
		t.Errorf("expected failure reason %q, got %q", failureMsg, saga.SagaFailureReason())
	}

	// Verify failure reason survives hydration
	loaded, err := LoadSaga(store, "test-saga", "saga-1", actions)
	if err != nil {
		t.Fatalf("LoadSaga failed: %v", err)
	}
	if !loaded.Completed() {
		t.Error("expected loaded saga to be completed")
	}
	if loaded.SagaFailureReason() != failureMsg {
		t.Errorf("expected loaded failure reason %q, got %q", failureMsg, loaded.SagaFailureReason())
	}
}

func TestSagaSuccessfulCompletionHasNoFailureReason(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{Complete: true}, nil
		},
	}

	saga, _ := NewSaga(store, "test-saga", "saga-1", "started", nil, actions)
	saga.Step(ctx)

	if saga.SagaFailureReason() != "" {
		t.Errorf("expected empty failure reason for successful completion, got %q", saga.SagaFailureReason())
	}

	// Verify after hydration
	loaded, _ := LoadSaga(store, "test-saga", "saga-1", actions)
	if loaded.SagaFailureReason() != "" {
		t.Errorf("expected empty failure reason after hydration, got %q", loaded.SagaFailureReason())
	}
}

func TestSagaFailureReasonRejectsWithoutComplete(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{
				NewState:          "next",
				SagaFailureReason: "should not be allowed",
			}, nil
		},
	}

	saga, _ := NewSaga(store, "test-saga", "saga-1", "started", nil, actions)

	err := saga.Step(ctx)
	if err == nil {
		t.Error("expected error when SagaFailureReason is set without Complete")
	}
}

func TestSagaTransientError(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	callCount := 0
	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			callCount++
			if callCount < 3 {
				// Transient error - retry later
				return ActionResult{}, fmt.Errorf("temporary network error")
			}
			// Success on third try
			return ActionResult{NewState: "completed"}, nil
		},
		"completed": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{Complete: true}, nil
		},
	}

	saga, _ := NewSaga(store, "test-saga", "saga-1", "started", nil, actions)

	// First attempt - transient error
	err := saga.Step(ctx)
	if err == nil {
		t.Error("expected transient error")
	}
	if saga.state != "started" {
		t.Errorf("expected state to remain 'started', got %s", saga.state)
	}

	// Second attempt - still transient error
	err = saga.Step(ctx)
	if err == nil {
		t.Error("expected transient error")
	}

	// Third attempt - success, transition to completed
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if saga.state != "completed" {
		t.Errorf("expected state 'completed', got %s", saga.state)
	}

	// Fourth attempt - close
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if !saga.completed {
		t.Error("expected saga to be closed")
	}
}

func TestDriverClosesOnRecovery(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	actions := ActionMap{
		"completed": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{Complete: true}, nil
		},
	}

	// Create saga and manually append saga-completed event WITHOUT calling store.Close
	// (simulating a crash after event append but before store.Close)
	saga, _ := NewSaga(store, "test-saga", "saga-1", "completed", nil, actions)

	// Manually append saga-completed event (bypassing saga.Step's store.Close call)
	closeEvent := AcceptedEvent{
		Event:   Event{Type: EventTypeCompleted},
		Counter: saga.Counter() + 1,
	}
	store.Append(ctx, AggregateEvent{AggregateType: "test-saga", AggregateID: "saga-1", Event: closeEvent})

	// Saga event says closed, but store doesn't know yet
	closed, _ := store.IsSagaCompleted("test-saga", "saga-1")
	if closed {
		t.Error("store should not know saga is closed yet (simulating crash)")
	}

	// ListAggregates still returns it
	ids, _ := store.ListActiveSagas(ctx, "test-saga")
	if len(ids) != 1 {
		t.Errorf("expected 1 saga before driver recovery, got %d", len(ids))
	}

	// Driver should detect closed flag and close it in store
	driver := NewSagaDriver(SagaDriverConfig{
		Store:    store,
		SagaType: "test-saga",
		Actions:  actions,
	})

	driver.StepAll(ctx)

	// Now saga should be closed in store
	closed, _ = store.IsSagaCompleted("test-saga", "saga-1")
	if !closed {
		t.Error("expected driver to close saga in store")
	}

	// ListAggregates should no longer return it
	ids, _ = store.ListActiveSagas(ctx, "test-saga")
	if len(ids) != 0 {
		t.Errorf("expected 0 sagas after driver recovery, got %d", len(ids))
	}
}

func TestStepRespectsDelayAfterCatchUp(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	var waitingCalls int32
	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{
				NewState: "waiting",
				Delay:    1 * time.Hour, // long delay
			}, nil
		},
		"waiting": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			atomic.AddInt32(&waitingCalls, 1)
			return ActionResult{NewState: "done"}, nil
		},
		"done": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{Complete: true}, nil
		},
	}

	// Process 1: create and step saga to "waiting" with delay
	saga1, _ := NewSaga(store, "test-saga", "saga-1", "started", nil, actions)
	saga1.Step(ctx)
	if saga1.State() != "waiting" {
		t.Fatalf("expected 'waiting', got %s", saga1.State())
	}

	// Process 2: load same saga (simulating a second process that hasn't caught up)
	saga2, _ := LoadSaga(store, "test-saga", "saga-1", actions)

	// saga2 is already caught up from LoadSaga, but let's simulate:
	// Create a saga that is behind and needs to catch up to the delayed state.
	// We do this by creating a saga at "started" state and having catchUp discover
	// the transition to "waiting" with a delay.
	saga3 := &Saga{
		eventStream: eventStream{
			ID:    NewAggregateID("test-saga", "saga-1"),
			store: store,
		},
		sagaStore: store,
		state:     "started",
		readyAt:   time.Now(),
		actions:   actions,
		// counter is 0, so catchUp will replay all events
	}

	// Step should catch up (discovering the transition to "waiting" with delay)
	// and then NOT execute the "waiting" action because it's delayed
	saga3.Step(ctx)

	if atomic.LoadInt32(&waitingCalls) != 0 {
		t.Error("expected 'waiting' action to NOT be called (delay not elapsed)")
	}
	if saga3.State() != "waiting" {
		t.Errorf("expected state 'waiting' after catch-up, got %s", saga3.State())
	}

	// Verify saga2 also respects delay
	saga2.Step(ctx)
	if atomic.LoadInt32(&waitingCalls) != 0 {
		t.Error("expected 'waiting' action to NOT be called on saga2 (delay not elapsed)")
	}
}

func TestMultipleDriversConcurrency(t *testing.T) {
	store := NewInMemoryStore()

	var calls int32
	actions := ActionMap{
		"started": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			atomic.AddInt32(&calls, 1)
			return ActionResult{NewState: "completed"}, nil
		},
		"completed": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			atomic.AddInt32(&calls, 1)
			return ActionResult{Complete: true}, nil
		},
	}

	// Create one saga
	NewSaga(store, "test-saga", "saga-1", "started", nil, actions)

	// Create two drivers (simulating multi-process)
	driver1 := NewSagaDriver(SagaDriverConfig{
		Store:    store,
		SagaType: "test-saga",
		Actions:  actions,
		Interval: 10 * time.Millisecond,
	})
	driver2 := NewSagaDriver(SagaDriverConfig{
		Store:    store,
		SagaType: "test-saga",
		Actions:  actions,
		Interval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Run both drivers concurrently
	go driver1.Run(ctx)
	go driver2.Run(ctx)

	// Let them run for a bit
	time.Sleep(100 * time.Millisecond)
	cancel()

	// The saga should only transition once (started -> completed)
	// Despite multiple drivers trying, optimistic concurrency ensures
	// only one succeeds per transition
	saga, _ := LoadSaga(store, "test-saga", "saga-1", actions)
	if saga.state != "completed" {
		t.Errorf("expected state 'completed', got %s", saga.state)
	}

	// The action may have been called multiple times (drivers racing),
	// but only one commit succeeded. This is expected behavior -
	// actions should be idempotent or handle failures gracefully.
	t.Logf("Action called %d times (expected: at least 1, possibly more due to races)", calls)
}

package monotonic_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// --- test helpers ---

// noopLogic satisfies ProjectorLogic but does nothing.
type noopLogic[V any] struct{}

func (noopLogic[V]) EventFilters() []monotonic.EventFilter {
	return []monotonic.EventFilter{{AggregateType: "test"}}
}
func (noopLogic[V]) Apply(_ context.Context, _ monotonic.ProjectionReader[V], _ monotonic.AggregateEvent) ([]monotonic.Projected[V], error) {
	return nil, nil
}

// countingLogic counts Apply calls and emits one update per event.
type countingLogic struct{ applied int }

func (l *countingLogic) EventFilters() []monotonic.EventFilter {
	return []monotonic.EventFilter{{AggregateType: "test"}}
}
func (l *countingLogic) Apply(_ context.Context, _ monotonic.ProjectionReader[int], _ monotonic.AggregateEvent) ([]monotonic.Projected[int], error) {
	l.applied++
	return []monotonic.Projected[int]{{Key: monotonic.ProjectionKeySummary, Value: l.applied}}, nil
}

// failingLogic always returns an error from Apply.
type failingLogic struct{ err error }

func (l *failingLogic) EventFilters() []monotonic.EventFilter {
	return []monotonic.EventFilter{{AggregateType: "test"}}
}
func (l *failingLogic) Apply(_ context.Context, _ monotonic.ProjectionReader[int], _ monotonic.AggregateEvent) ([]monotonic.Projected[int], error) {
	return nil, l.err
}

// emitEvent appends a single event to the store for aggregate "test"/"agg-1".
func emitEvent(ctx context.Context, t *testing.T, store monotonic.Store, counter int64) {
	t.Helper()
	err := store.Append(ctx, monotonic.AggregateEvent{
		AggregateType: "test",
		AggregateID:   "agg-1",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.NewEvent[any]("happened", nil),
			Counter:    counter,
			AcceptedAt: time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("emitEvent counter=%d: %v", counter, err)
	}
}

// --- Projector tests ---

func TestProjector_UpdateReturnsZeroWhenNoPendingEvents(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	persist := monotonic.NewInMemoryProjectionPersistence[int]()

	p, err := monotonic.NewProjector(ctx, store, &countingLogic{}, persist)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	n, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 processed events, got %d", n)
	}
}

func TestProjector_UpdateProcessesAllPendingEvents(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()

	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)
	emitEvent(ctx, t, store, 3)

	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	logic := &countingLogic{}
	p, err := monotonic.NewProjector(ctx, store, logic, persist)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	n, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if n != 3 {
		t.Errorf("expected 3 processed, got %d", n)
	}
	if logic.applied != 3 {
		t.Errorf("logic.Apply called %d times, want 3", logic.applied)
	}
}

func TestProjector_GlobalCounterIsZeroInitially(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	persist := monotonic.NewInMemoryProjectionPersistence[int]()

	p, err := monotonic.NewProjector(ctx, store, &countingLogic{}, persist)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if p.GlobalCounter() != 0 {
		t.Errorf("initial GlobalCounter should be 0, got %d", p.GlobalCounter())
	}
}

func TestProjector_GlobalCounterAdvancesAfterUpdate(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)

	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	p, _ := monotonic.NewProjector(ctx, store, &countingLogic{}, persist)

	p.Update(ctx)

	if p.GlobalCounter() == 0 {
		t.Error("GlobalCounter should have advanced after Update")
	}
}

func TestProjector_UpdateIsIdempotentWhenCaughtUp(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	logic := &countingLogic{}
	p, _ := monotonic.NewProjector(ctx, store, logic, persist)

	p.Update(ctx)

	n, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("second Update: %v", err)
	}
	if n != 0 {
		t.Errorf("second Update should return 0 when caught up, got %d", n)
	}
	if logic.applied != 1 {
		t.Errorf("Apply should have been called exactly once, got %d", logic.applied)
	}
}

func TestProjector_ResumesFromExistingPersistenceState(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()

	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)

	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	logic1 := &countingLogic{}
	p1, _ := monotonic.NewProjector(ctx, store, logic1, persist)
	p1.Update(ctx)

	// Append a new event AFTER p1 has caught up.
	emitEvent(ctx, t, store, 3)

	// A fresh projector against the same persistence should pick up only event 3.
	logic2 := &countingLogic{}
	p2, err := monotonic.NewProjector(ctx, store, logic2, persist)
	if err != nil {
		t.Fatalf("resume NewProjector: %v", err)
	}

	n, err := p2.Update(ctx)
	if err != nil {
		t.Fatalf("resumed Update: %v", err)
	}
	if n != 1 {
		t.Errorf("resumed projector should process exactly 1 new event, got %d", n)
	}
	if logic2.applied != 1 {
		t.Errorf("resumed logic applied %d events, want 1", logic2.applied)
	}
}

func TestProjector_NewProjectorReadsLatestGlobalCounterFromPersistence(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()

	// Pre-populate persistence as if events 1–5 were already processed.
	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	persist.Set(ctx, []monotonic.Projected[int]{{Key: monotonic.ProjectionKeySummary, Value: 0}}, 5)

	// Also emit 6 events into the store.
	for i := int64(1); i <= 6; i++ {
		emitEvent(ctx, t, store, i)
	}

	logic := &countingLogic{}
	p, err := monotonic.NewProjector(ctx, store, logic, persist)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if p.GlobalCounter() != 5 {
		t.Errorf("NewProjector should resume at counter 5, got %d", p.GlobalCounter())
	}

	// Only event 6 is pending.
	n, _ := p.Update(ctx)
	if n != 1 {
		t.Errorf("expected 1 pending event, got %d", n)
	}
}

func TestProjector_UpdatePropagatesApplyError(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	p, _ := monotonic.NewProjector(ctx, store, &failingLogic{err: fmt.Errorf("apply boom")}, persist)

	_, err := p.Update(ctx)
	if err == nil {
		t.Error("expected error from Update when Apply fails, got nil")
	}
}

func TestProjector_UpdateDoesNotAdvanceCounterAfterApplyError(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	p, _ := monotonic.NewProjector(ctx, store, &failingLogic{err: fmt.Errorf("boom")}, persist)

	p.Update(ctx)

	if p.GlobalCounter() != 0 {
		t.Errorf("GlobalCounter must not advance after Apply error: got %d", p.GlobalCounter())
	}
}

func TestProjector_RunStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := monotonic.NewInMemoryStore()
	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	p, _ := monotonic.NewProjector(ctx, store, noopLogic[int]{}, persist)

	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, time.Millisecond) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run should return nil on context cancel, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Run did not stop within 2s after context cancellation")
	}
}

func TestProjector_RunReturnsErrorFromUpdate(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	p, _ := monotonic.NewProjector(ctx, store, &failingLogic{err: fmt.Errorf("run boom")}, persist)

	err := p.Run(ctx, time.Millisecond)
	if err == nil {
		t.Error("expected Run to return error when Update fails")
	}
}

func TestProjector_RunDrainsWithoutSleepingWhileWorkPending(t *testing.T) {
	// Emit many events and verify they are all processed in a single Run that
	// does NOT use a long poll interval — if Run slept between each event the
	// test would time out.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := monotonic.NewInMemoryStore()
	const numEvents = 100
	for i := int64(1); i <= numEvents; i++ {
		emitEvent(ctx, t, store, i)
	}

	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	logic := &countingLogic{}
	p, _ := monotonic.NewProjector(ctx, store, logic, persist)

	// Poll every 10s — if Run sleeps between events it would time out.
	go func() {
		p.Run(ctx, 10*time.Second)
	}()

	// Wait until all events are processed or timeout.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p.GlobalCounter() == numEvents {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if p.GlobalCounter() != uint64(numEvents) {
		t.Errorf("Run processed %d events, want %d", p.GlobalCounter(), numEvents)
	}
}

// --- RunProjectors tests ---

func TestRunProjectors_AllStopOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := monotonic.NewInMemoryStore()

	makeProjector := func() monotonic.ProjectorRunner {
		persist := monotonic.NewInMemoryProjectionPersistence[int]()
		p, _ := monotonic.NewProjector(ctx, store, noopLogic[int]{}, persist)
		return p
	}

	done := make(chan error, 1)
	go func() {
		done <- monotonic.RunProjectors(ctx, time.Millisecond, makeProjector(), makeProjector(), makeProjector())
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("RunProjectors should return nil on context cancel, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("RunProjectors did not stop within 2s")
	}
}

func TestRunProjectors_ReturnsErrorWhenOneProjectorFails(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	failPersist := monotonic.NewInMemoryProjectionPersistence[int]()
	failP, _ := monotonic.NewProjector(ctx, store, &failingLogic{err: fmt.Errorf("fail")}, failPersist)

	okPersist := monotonic.NewInMemoryProjectionPersistence[int]()
	okP, _ := monotonic.NewProjector(ctx, store, noopLogic[int]{}, okPersist)

	err := monotonic.RunProjectors(ctx, time.Millisecond, failP, okP)
	if err == nil {
		t.Error("expected error when one projector fails")
	}
}

// --- MutateByKey tests ---

func TestMutateByKey_AppliesZeroValueWhenKeyMissing(t *testing.T) {
	ctx := context.Background()
	persist := monotonic.NewInMemoryProjectionPersistence[int]()

	updates, err := monotonic.MutateByKey(ctx, persist, "k", func(v *int) error {
		*v += 10
		return nil
	})
	if err != nil {
		t.Fatalf("MutateByKey: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(updates))
	}
	if updates[0].Key != "k" || updates[0].Value != 10 {
		t.Errorf("unexpected update: %+v", updates[0])
	}
}

func TestMutateByKey_ReadsExistingValueBeforeMutating(t *testing.T) {
	ctx := context.Background()
	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	persist.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: 5}}, 1)

	updates, err := monotonic.MutateByKey(ctx, persist, "k", func(v *int) error {
		*v += 10
		return nil
	})
	if err != nil {
		t.Fatalf("MutateByKey: %v", err)
	}
	if updates[0].Value != 15 {
		t.Errorf("expected 15 (5+10), got %d", updates[0].Value)
	}
}

func TestMutateByKey_PropagatesMutationError(t *testing.T) {
	ctx := context.Background()
	persist := monotonic.NewInMemoryProjectionPersistence[int]()

	_, err := monotonic.MutateByKey(ctx, persist, "k", func(v *int) error {
		return errors.New("mutation failed")
	})
	if err == nil {
		t.Error("expected error from mutate function, got nil")
	}
}

func TestMutateByKey_DoesNotMutateReaderOnError(t *testing.T) {
	ctx := context.Background()
	persist := monotonic.NewInMemoryProjectionPersistence[int]()
	persist.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: 42}}, 1)

	monotonic.MutateByKey(ctx, persist, "k", func(v *int) error {
		*v = 999
		return errors.New("mutation rejected")
	})

	// The returned updates are discarded (caller never calls Set). The reader
	// must still hold the original value.
	val, _ := persist.Get(ctx, "k")
	if val != 42 {
		t.Errorf("reader must not change when mutate errors: got %d", val)
	}
}

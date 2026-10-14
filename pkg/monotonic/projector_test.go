package monotonic_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// testModificationTx is the trivial ModificationTx type the unit tests parameterize over; handlers
// don't actually need any per-event environment, so a struct{} suffices.
type testModificationTx struct{}

// fakeBackend is an in-memory monotonic.ProjectorBackend[testModificationTx] used by projector unit tests.
// It records per-projector state and exposes hooks for failure injection. The skip rules in RunEvent and
// the claim rule in ClaimReset mirror the Postgres backend, so tests here exercise the same semantics.
type fakeBackend struct {
	mu          sync.Mutex
	state       map[string]monotonic.ProjectorState
	failOnEvent uint64 // if non-zero, RunEvent returns failErr for this counter
	failErr     error
	beforeRun   func(counter uint64) // if non-nil, runs at the top of RunEvent, before the skip checks
	runCalls    int
	resetErr    error // if non-nil, ClaimReset returns this error without resetting state
	resetCalls  int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{state: make(map[string]monotonic.ProjectorState)}
}

func (b *fakeBackend) GetState(_ context.Context, projectorName string) (monotonic.ProjectorState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state[projectorName], nil
}

func (b *fakeBackend) RunEvent(ctx context.Context, projectorName string, from, counter uint64, apply func(ctx context.Context, tx testModificationTx) error) (uint64, error) {
	if b.beforeRun != nil {
		b.beforeRun(counter)
	}

	b.mu.Lock()
	b.runCalls++
	shouldFail := b.failOnEvent != 0 && counter == b.failOnEvent
	state := b.state[projectorName]
	b.mu.Unlock()

	if shouldFail {
		return 0, b.failErr
	}
	if state.Counter >= counter || state.Counter < from || state.ResetRequested {
		return state.Counter, nil
	}
	if err := apply(ctx, testModificationTx{}); err != nil {
		return 0, err
	}
	b.mu.Lock()
	state = b.state[projectorName]
	state.Counter = counter
	b.state[projectorName] = state
	b.mu.Unlock()
	return counter, nil
}

func (b *fakeBackend) RequestReset(_ context.Context, projectorName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.state[projectorName]
	state.ResetRequested = true
	b.state[projectorName] = state
	return nil
}

func (b *fakeBackend) ClaimReset(ctx context.Context, projectorName string, reset func(ctx context.Context, tx testModificationTx) error) (bool, error) {
	b.mu.Lock()
	b.resetCalls++
	state := b.state[projectorName]
	b.mu.Unlock()

	if b.resetErr != nil {
		return false, b.resetErr
	}
	if !state.ResetRequested {
		return false, nil
	}
	if err := reset(ctx, testModificationTx{}); err != nil {
		return false, err
	}
	b.mu.Lock()
	b.state[projectorName] = monotonic.ProjectorState{}
	b.mu.Unlock()
	return true, nil
}

func (b *fakeBackend) seed(projectorName string, counter uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state[projectorName] = monotonic.ProjectorState{Counter: counter}
}

func (b *fakeBackend) counterOf(projectorName string) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state[projectorName].Counter
}

// countingDispatch returns a Dispatch that counts handler invocations.
type counterRef struct{ n int }

func countingDispatch(c *counterRef) *monotonic.Dispatch[testModificationTx] {
	return monotonic.NewDispatch[testModificationTx]().On("test", "happened", func(_ context.Context, _ testModificationTx, _ monotonic.AggregateEvent) error {
		c.n++
		return nil
	})
}

func failingDispatch(err error) *monotonic.Dispatch[testModificationTx] {
	return monotonic.NewDispatch[testModificationTx]().On("test", "happened", func(_ context.Context, _ testModificationTx, _ monotonic.AggregateEvent) error {
		return err
	})
}

func noopDispatch() *monotonic.Dispatch[testModificationTx] {
	return monotonic.NewDispatch[testModificationTx]().On("test", "happened", func(_ context.Context, _ testModificationTx, _ monotonic.AggregateEvent) error {
		return nil
	})
}

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

// --- Dispatch tests ---

func TestDispatch_EventFiltersDerivedFromHandlers(t *testing.T) {
	d := monotonic.NewDispatch[testModificationTx]().
		On("a", "x", func(context.Context, testModificationTx, monotonic.AggregateEvent) error { return nil }).
		On("b", "y", func(context.Context, testModificationTx, monotonic.AggregateEvent) error { return nil })

	filters := d.EventFilters()
	if len(filters) != 2 {
		t.Fatalf("expected 2 filters, got %d", len(filters))
	}
	seen := map[string]bool{}
	for _, f := range filters {
		seen[f.AggregateType+"/"+f.EventType] = true
	}
	if !seen["a/x"] || !seen["b/y"] {
		t.Errorf("missing expected filters: %v", seen)
	}
}

func TestDispatch_ApplyRoutesToRegisteredHandler(t *testing.T) {
	called := false
	d := monotonic.NewDispatch[testModificationTx]().On("a", "x", func(_ context.Context, _ testModificationTx, _ monotonic.AggregateEvent) error {
		called = true
		return nil
	})
	err := d.Apply(context.Background(), testModificationTx{}, monotonic.AggregateEvent{
		AggregateType: "a",
		Event:         monotonic.AcceptedEvent{Event: monotonic.Event{Type: "x"}},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !called {
		t.Error("handler was not invoked")
	}
}

func TestDispatch_ApplyUnregisteredIsNoop(t *testing.T) {
	d := monotonic.NewDispatch[testModificationTx]()
	err := d.Apply(context.Background(), testModificationTx{}, monotonic.AggregateEvent{
		AggregateType: "x",
		Event:         monotonic.AcceptedEvent{Event: monotonic.Event{Type: "y"}},
	})
	if err != nil {
		t.Errorf("Apply on unregistered should be no-op, got %v", err)
	}
}

// --- Projector tests ---

func TestProjector_UpdateReturnsZeroWhenNoPendingEvents(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	backend := newFakeBackend()
	cr := &counterRef{}

	p, err := monotonic.NewProjector(ctx, "p1", store, countingDispatch(cr), backend, 0)
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

	backend := newFakeBackend()
	cr := &counterRef{}
	p, err := monotonic.NewProjector(ctx, "p1", store, countingDispatch(cr), backend, 0)
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
	if cr.n != 3 {
		t.Errorf("handler called %d times, want 3", cr.n)
	}
}

func TestProjector_GlobalCounterAdvancesAfterUpdate(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)

	backend := newFakeBackend()
	p, _ := monotonic.NewProjector(ctx, "p1", store, countingDispatch(&counterRef{}), backend, 0)
	p.Update(ctx)

	if p.GlobalCounter() == 0 {
		t.Error("GlobalCounter should have advanced after Update")
	}
}

func TestProjector_ResumesFromBackendState(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	for i := int64(1); i <= 6; i++ {
		emitEvent(ctx, t, store, i)
	}

	backend := newFakeBackend()
	backend.seed("p1", 5)

	cr := &counterRef{}
	p, err := monotonic.NewProjector(ctx, "p1", store, countingDispatch(cr), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if p.GlobalCounter() != 5 {
		t.Errorf("expected resume at counter 5, got %d", p.GlobalCounter())
	}

	n, _ := p.Update(ctx)
	if n != 1 {
		t.Errorf("expected 1 pending event past counter 5, got %d", n)
	}
	if cr.n != 1 {
		t.Errorf("handler should have been called for 1 event, got %d", cr.n)
	}
}

func TestProjector_ResetRewindsCounterAndRunsResetFunc(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	for i := int64(1); i <= 3; i++ {
		emitEvent(ctx, t, store, i)
	}

	backend := newFakeBackend()
	cr := &counterRef{}
	resetFuncCalled := false
	p, err := monotonic.NewProjector(ctx, "p1", store, countingDispatch(cr), backend, 0,
		monotonic.WithReset(func(ctx context.Context, tx testModificationTx) error {
			resetFuncCalled = true
			return nil
		}))
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if p.GlobalCounter() != 3 {
		t.Fatalf("expected counter 3 before reset, got %d", p.GlobalCounter())
	}

	if err := p.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	if !resetFuncCalled {
		t.Error("reset func was not invoked")
	}
	if p.GlobalCounter() != 0 {
		t.Errorf("expected counter 0 after reset, got %d", p.GlobalCounter())
	}
	if got := backend.counterOf("p1"); got != 0 {
		t.Errorf("expected backend state 0 after reset, got %d", got)
	}

	n, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update after reset: %v", err)
	}
	if n != 3 {
		t.Errorf("expected reset projector to replay all 3 events, got %d", n)
	}
}

func TestProjector_ResetPropagatesResetFuncError(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	cr := &counterRef{}
	wantErr := errors.New("reset boom")
	p, err := monotonic.NewProjector(ctx, "p1", store, countingDispatch(cr), backend, 0,
		monotonic.WithReset(func(ctx context.Context, tx testModificationTx) error {
			return wantErr
		}))
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if err := p.Reset(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v from Reset, got %v", wantErr, err)
	}
	if p.GlobalCounter() != 1 {
		t.Errorf("counter must not change when reset func fails, got %d", p.GlobalCounter())
	}
}

// TestProjector_UpdateAdoptsResetCommittedByAnotherInstance is the regression test for the
// reason a projector cannot trust its own counter across a reset. Two instances of the same
// projector share one backend; one resets while the other still believes it is caught up. If
// the second instance kept its cached position it would resume from there onto a cleared
// projection, permanently skipping every event below it.
func TestProjector_UpdateAdoptsResetCommittedByAnotherInstance(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	for i := int64(1); i <= 3; i++ {
		emitEvent(ctx, t, store, i)
	}

	backend := newFakeBackend()
	noReset := func(ctx context.Context, tx testModificationTx) error { return nil }

	crA, crB := &counterRef{}, &counterRef{}
	a, err := monotonic.NewProjector(ctx, "shared", store, countingDispatch(crA), backend, 0, monotonic.WithReset(noReset))
	if err != nil {
		t.Fatalf("NewProjector a: %v", err)
	}
	b, err := monotonic.NewProjector(ctx, "shared", store, countingDispatch(crB), backend, 0, monotonic.WithReset(noReset))
	if err != nil {
		t.Fatalf("NewProjector b: %v", err)
	}

	// Both instances catch up to counter 3, then more events land.
	if _, err := a.Update(ctx); err != nil {
		t.Fatalf("a.Update: %v", err)
	}
	if _, err := b.Update(ctx); err != nil {
		t.Fatalf("b.Update: %v", err)
	}
	if b.GlobalCounter() != 3 {
		t.Fatalf("b should be caught up at 3, got %d", b.GlobalCounter())
	}
	for i := int64(4); i <= 5; i++ {
		emitEvent(ctx, t, store, i)
	}

	// a resets. b still has 3 cached and has no idea.
	if err := a.Reset(ctx); err != nil {
		t.Fatalf("a.Reset: %v", err)
	}

	crB.n = 0
	n, err := b.Update(ctx)
	if err != nil {
		t.Fatalf("b.Update after reset: %v", err)
	}
	if n != 5 {
		t.Errorf("b processed %d events after the reset, want 5 (a full replay, not a resume from its stale counter)", n)
	}
	if crB.n != 5 {
		t.Errorf("b dispatched %d events after the reset, want 5", crB.n)
	}
}

// TestProjector_ResetRequestIsClaimedExactlyOnce verifies that a reset requested once is
// performed once, no matter how many instances observe the request.
func TestProjector_ResetRequestIsClaimedExactlyOnce(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	resetRuns := 0
	countReset := func(ctx context.Context, tx testModificationTx) error {
		resetRuns++
		return nil
	}

	instances := make([]*monotonic.Projector[testModificationTx], 3)
	for i := range instances {
		p, err := monotonic.NewProjector(ctx, "shared", store, noopDispatch(), backend, 0, monotonic.WithReset(countReset))
		if err != nil {
			t.Fatalf("NewProjector %d: %v", i, err)
		}
		instances[i] = p
	}

	if err := instances[0].RequestReset(ctx); err != nil {
		t.Fatalf("RequestReset: %v", err)
	}

	// Every instance polls and sees the request; only the first one to claim it does the work.
	for i, p := range instances {
		if _, err := p.Update(ctx); err != nil {
			t.Fatalf("instance %d Update: %v", i, err)
		}
	}

	if resetRuns != 1 {
		t.Errorf("reset func ran %d times across 3 instances, want exactly 1", resetRuns)
	}
	if backend.resetCalls != 1 {
		t.Errorf("ClaimReset called %d times, want 1 (only instances seeing a pending request should claim)", backend.resetCalls)
	}
}

// TestProjector_RequestedResetWithoutResetFuncFailsLoudly pins that a projector which cannot
// clear its own state refuses to rewind rather than replaying every event on top of rows that
// are still there. The request can arrive from outside the process (an operator writing the
// backend's flag by hand), so Update has to defend against it too.
func TestProjector_RequestedResetWithoutResetFuncFailsLoudly(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	cr := &counterRef{}
	p, err := monotonic.NewProjector(ctx, "p1", store, countingDispatch(cr), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	if err := p.RequestReset(ctx); err == nil {
		t.Error("RequestReset should fail when no reset func is registered")
	}

	// Simulate the request arriving out of band instead.
	if err := backend.RequestReset(ctx, "p1"); err != nil {
		t.Fatalf("backend.RequestReset: %v", err)
	}
	if _, err := p.Update(ctx); err == nil {
		t.Error("Update should fail when a reset is pending and no reset func is registered")
	}
	if cr.n != 0 {
		t.Errorf("no events should have been dispatched, got %d", cr.n)
	}
}

// TestProjector_UpdateStopsBatchWhenAnotherInstanceRacesAhead verifies that a projector whose
// position has been overtaken abandons the rest of its batch instead of grinding through events
// the backend will only skip.
func TestProjector_UpdateStopsBatchWhenAnotherInstanceRacesAhead(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	for i := int64(1); i <= 5; i++ {
		emitEvent(ctx, t, store, i)
	}

	backend := newFakeBackend()
	cr := &counterRef{}
	p, err := monotonic.NewProjector(ctx, "shared", store, countingDispatch(cr), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	// Another instance drains the whole stream right after this one applies its first event,
	// leaving the remaining three events in the loaded batch already processed.
	backend.beforeRun = func(counter uint64) {
		if counter == 2 {
			backend.seed("shared", 5)
		}
	}

	n, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if n != 1 {
		t.Errorf("processed %d events, want 1 (only the event applied before being overtaken)", n)
	}
	if cr.n != 1 {
		t.Errorf("dispatched %d events, want 1", cr.n)
	}
	if backend.runCalls != 2 {
		t.Errorf("RunEvent called %d times, want 2 (batch abandoned at the first skip, not ground through)", backend.runCalls)
	}
	if p.GlobalCounter() != 5 {
		t.Errorf("counter = %d, want 5 (adopted from the backend)", p.GlobalCounter())
	}
}

func TestProjector_UpdatePropagatesHandlerError(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	p, _ := monotonic.NewProjector(ctx, "p1", store, failingDispatch(errors.New("apply boom")), backend, 0)

	_, err := p.Update(ctx)
	if err == nil {
		t.Error("expected error from Update when handler fails")
	}
}

func TestProjector_CounterDoesNotAdvanceOnHandlerError(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	p, _ := monotonic.NewProjector(ctx, "p1", store, failingDispatch(errors.New("boom")), backend, 0)
	p.Update(ctx)

	if p.GlobalCounter() != 0 {
		t.Errorf("GlobalCounter must not advance after handler error, got %d", p.GlobalCounter())
	}
	if got := backend.counterOf("p1"); got != 0 {
		t.Errorf("backend state must not advance after handler error, got %d", got)
	}
}

func TestProjector_BackendErrorStopsBatch(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)
	emitEvent(ctx, t, store, 3)

	backend := newFakeBackend()
	backend.failOnEvent = 2
	backend.failErr = errors.New("backend boom")
	cr := &counterRef{}

	p, _ := monotonic.NewProjector(ctx, "p1", store, countingDispatch(cr), backend, 0)
	processed, err := p.Update(ctx)
	if err == nil {
		t.Fatal("expected error from backend failure")
	}
	if processed != 1 {
		t.Errorf("expected 1 processed before failure, got %d", processed)
	}
	if p.GlobalCounter() != 1 {
		t.Errorf("counter should sit at last successful event (1), got %d", p.GlobalCounter())
	}
}

func TestProjector_RunStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := monotonic.NewInMemoryStore()
	backend := newFakeBackend()
	p, _ := monotonic.NewProjector(ctx, "p1", store, noopDispatch(), backend, 0)

	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, time.Millisecond) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run should return nil on context cancel, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Run did not stop within 2s after cancellation")
	}
}

func TestProjector_RunReturnsErrorFromUpdate(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	p, _ := monotonic.NewProjector(ctx, "p1", store, failingDispatch(fmt.Errorf("run boom")), backend, 0)

	err := p.Run(ctx, time.Millisecond)
	if err == nil {
		t.Error("expected Run to return error when Update fails")
	}
}

func TestProjector_RunDrainsWithoutSleepingWhileWorkPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := monotonic.NewInMemoryStore()
	const numEvents = 100
	for i := int64(1); i <= numEvents; i++ {
		emitEvent(ctx, t, store, i)
	}

	backend := newFakeBackend()
	cr := &counterRef{}
	p, _ := monotonic.NewProjector(ctx, "p1", store, countingDispatch(cr), backend, 0)

	go func() {
		p.Run(ctx, 10*time.Second)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p.GlobalCounter() == numEvents {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if p.GlobalCounter() != uint64(numEvents) {
		t.Errorf("processed counter=%d, want %d", p.GlobalCounter(), numEvents)
	}
}

func TestProjector_NameRequired(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	backend := newFakeBackend()
	_, err := monotonic.NewProjector(ctx, "", store, noopDispatch(), backend, 0)
	if err == nil {
		t.Error("expected error when name is empty")
	}
}

// --- RunProjectors tests ---

func TestRunProjectors_AllStopOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := monotonic.NewInMemoryStore()
	backend := newFakeBackend()

	makeProjector := func(name string) monotonic.ProjectorRunner {
		p, _ := monotonic.NewProjector(ctx, name, store, noopDispatch(), backend, 0)
		return p
	}

	done := make(chan error, 1)
	go func() {
		done <- monotonic.RunProjectors(ctx, time.Millisecond, makeProjector("a"), makeProjector("b"), makeProjector("c"))
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

	backend := newFakeBackend()
	failP, _ := monotonic.NewProjector(ctx, "fail", store, failingDispatch(errors.New("fail")), backend, 0)
	okP, _ := monotonic.NewProjector(ctx, "ok", store, noopDispatch(), backend, 0)

	err := monotonic.RunProjectors(ctx, time.Millisecond, failP, okP)
	if err == nil {
		t.Error("expected error when one projector fails")
	}
}

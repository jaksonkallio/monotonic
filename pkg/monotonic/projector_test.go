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

// testSink is the trivial Sink type the unit tests parameterize over; handlers
// don't actually need any per-event environment, so a struct{} suffices.
type testSink struct{}

// fakeBackend is an in-memory monotonic.ProjectorBackend[testSink] used by projector unit tests.
// It records per-projector counters and exposes hooks for failure injection.
type fakeBackend struct {
	mu          sync.Mutex
	state       map[string]uint64
	failOnEvent uint64 // if non-zero, RunEvent returns failErr for this counter
	failErr     error
	runCalls    int
	resetErr    error // if non-nil, ResetState returns this error without resetting state
	resetCalls  int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{state: make(map[string]uint64)}
}

func (b *fakeBackend) GetState(_ context.Context, projectorName string) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state[projectorName], nil
}

func (b *fakeBackend) RunEvent(ctx context.Context, projectorName string, counter uint64, apply func(ctx context.Context, sink testSink) error) error {
	b.mu.Lock()
	b.runCalls++
	shouldFail := b.failOnEvent != 0 && counter == b.failOnEvent
	b.mu.Unlock()

	if shouldFail {
		return b.failErr
	}
	if err := apply(ctx, testSink{}); err != nil {
		return err
	}
	b.mu.Lock()
	b.state[projectorName] = counter
	b.mu.Unlock()
	return nil
}

func (b *fakeBackend) ResetState(ctx context.Context, projectorName string, reset func(ctx context.Context, sink testSink) error) error {
	b.mu.Lock()
	b.resetCalls++
	b.mu.Unlock()

	if b.resetErr != nil {
		return b.resetErr
	}
	if err := reset(ctx, testSink{}); err != nil {
		return err
	}
	b.mu.Lock()
	b.state[projectorName] = 0
	b.mu.Unlock()
	return nil
}

func (b *fakeBackend) seed(projectorName string, counter uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state[projectorName] = counter
}

// countingDispatch returns a Dispatch that counts handler invocations.
type counterRef struct{ n int }

func countingDispatch(c *counterRef) *monotonic.Dispatch[testSink] {
	return monotonic.NewDispatch[testSink]().On("test", "happened", func(_ context.Context, _ testSink, _ monotonic.AggregateEvent) error {
		c.n++
		return nil
	})
}

func failingDispatch(err error) *monotonic.Dispatch[testSink] {
	return monotonic.NewDispatch[testSink]().On("test", "happened", func(_ context.Context, _ testSink, _ monotonic.AggregateEvent) error {
		return err
	})
}

func noopDispatch() *monotonic.Dispatch[testSink] {
	return monotonic.NewDispatch[testSink]().On("test", "happened", func(_ context.Context, _ testSink, _ monotonic.AggregateEvent) error {
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
	d := monotonic.NewDispatch[testSink]().
		On("a", "x", func(context.Context, testSink, monotonic.AggregateEvent) error { return nil }).
		On("b", "y", func(context.Context, testSink, monotonic.AggregateEvent) error { return nil })

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
	d := monotonic.NewDispatch[testSink]().On("a", "x", func(_ context.Context, _ testSink, _ monotonic.AggregateEvent) error {
		called = true
		return nil
	})
	err := d.Apply(context.Background(), testSink{}, monotonic.AggregateEvent{
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
	d := monotonic.NewDispatch[testSink]()
	err := d.Apply(context.Background(), testSink{}, monotonic.AggregateEvent{
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
	p, err := monotonic.NewProjector(ctx, "p1", store, countingDispatch(cr), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if p.GlobalCounter() != 3 {
		t.Fatalf("expected counter 3 before reset, got %d", p.GlobalCounter())
	}

	resetFuncCalled := false
	if err := p.Reset(ctx, func(ctx context.Context, sink testSink) error {
		resetFuncCalled = true
		return nil
	}); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	if !resetFuncCalled {
		t.Error("reset func was not invoked")
	}
	if p.GlobalCounter() != 0 {
		t.Errorf("expected counter 0 after reset, got %d", p.GlobalCounter())
	}
	if got, _ := backend.GetState(ctx, "p1"); got != 0 {
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
	p, err := monotonic.NewProjector(ctx, "p1", store, countingDispatch(cr), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}

	wantErr := errors.New("reset boom")
	err = p.Reset(ctx, func(ctx context.Context, sink testSink) error {
		return wantErr
	})
	if err == nil {
		t.Fatal("expected error from Reset when reset func fails")
	}
	if p.GlobalCounter() != 1 {
		t.Errorf("counter must not change when reset func fails, got %d", p.GlobalCounter())
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
	if got, _ := backend.GetState(ctx, "p1"); got != 0 {
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

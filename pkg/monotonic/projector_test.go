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

// testTx is the trivial ModificationTx the unit tests parameterize over. Handlers don't need any
// real per-event environment, so it only carries the bookkeeping the backend needs to verify that
// the projector ends every transaction exactly once.
type testTx struct {
	backend  *fakeBackend
	finished bool
}

// Rollback ends the transaction without committing, and is a no-op once the transaction has ended,
// since the projector defers it even on the committed path.
func (tx *testTx) Rollback(context.Context) error {
	if tx.finished {
		return nil
	}
	tx.finished = true
	tx.backend.unlock()
	return nil
}

// fakeBackend is an in-memory monotonic.ProjectorBackend[*testTx] used by projector unit tests.
// It records per-projector state and exposes hooks for failure injection.
type fakeBackend struct {
	mu    sync.Mutex
	state map[string]monotonic.ProjectorStateSnapshot
	// locked is held between LockStateForModification and the transaction ending, so the tests can
	// assert the projector never leaks or double-ends a transaction.
	locked bool

	lockErr   error // if non-nil, LockStateForModification fails with it
	commitErr error // if non-nil, both Commit methods fail with it
	// commitCalls counts Commit attempts, so a test can assert an idle Update writes nothing.
	commitCalls int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{state: make(map[string]monotonic.ProjectorStateSnapshot)}
}

func (b *fakeBackend) unlock() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.locked = false
}

func (b *fakeBackend) LockStateForModification(_ context.Context, projectorName string) (*testTx, monotonic.ProjectorStateSnapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.lockErr != nil {
		return nil, monotonic.ProjectorStateSnapshot{}, b.lockErr
	}
	if b.locked {
		return nil, monotonic.ProjectorStateSnapshot{}, errors.New("fakeBackend: state already locked")
	}
	b.locked = true

	return &testTx{backend: b}, b.state[projectorName], nil
}

func (b *fakeBackend) CommitAdvancedStateFromModification(_ context.Context, tx *testTx, projectorName string, advancedToGlobalCounter uint64) error {
	return b.commit(tx, projectorName, func(state monotonic.ProjectorStateSnapshot) monotonic.ProjectorStateSnapshot {
		state.GlobalCounter = advancedToGlobalCounter
		return state
	})
}

func (b *fakeBackend) CommitTruncatedStateFromModification(_ context.Context, tx *testTx, projectorName string) error {
	return b.commit(tx, projectorName, func(monotonic.ProjectorStateSnapshot) monotonic.ProjectorStateSnapshot {
		return monotonic.ProjectorStateSnapshot{}
	})
}

func (b *fakeBackend) commit(tx *testTx, projectorName string, update func(monotonic.ProjectorStateSnapshot) monotonic.ProjectorStateSnapshot) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.commitCalls++
	if tx.finished {
		return errors.New("fakeBackend: transaction already finished")
	}
	if b.commitErr != nil {
		// Leave the transaction open so the projector's deferred Rollback is what ends it.
		return b.commitErr
	}

	b.state[projectorName] = update(b.state[projectorName])
	tx.finished = true
	b.locked = false
	return nil
}

func (b *fakeBackend) EnqueueTruncate(_ context.Context, projectorName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	state := b.state[projectorName]
	state.TruncateRequested = true
	b.state[projectorName] = state
	return nil
}

func (b *fakeBackend) stateOf(projectorName string) monotonic.ProjectorStateSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state[projectorName]
}

func (b *fakeBackend) seed(projectorName string, counter uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state[projectorName] = monotonic.ProjectorStateSnapshot{GlobalCounter: counter}
}

// isLocked reports whether a modification transaction is still open, so tests can assert the
// projector released the lock on every path.
func (b *fakeBackend) isLocked() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.locked
}

// countingDispatch returns a Dispatch that counts handler invocations.
type counterRef struct{ n int }

func countingDispatch(c *counterRef) *monotonic.Dispatch[*testTx] {
	return monotonic.NewDispatch[*testTx]().On("test", "happened", func(_ context.Context, _ *testTx, _ monotonic.AggregateEvent) error {
		c.n++
		return nil
	})
}

func failingDispatch(err error) *monotonic.Dispatch[*testTx] {
	return monotonic.NewDispatch[*testTx]().On("test", "happened", func(_ context.Context, _ *testTx, _ monotonic.AggregateEvent) error {
		return err
	})
}

func failOnNthDispatch(c *counterRef, n int, err error) *monotonic.Dispatch[*testTx] {
	return monotonic.NewDispatch[*testTx]().On("test", "happened", func(_ context.Context, _ *testTx, _ monotonic.AggregateEvent) error {
		c.n++
		if c.n == n {
			return err
		}
		return nil
	})
}

func noopDispatch() *monotonic.Dispatch[*testTx] {
	return monotonic.NewDispatch[*testTx]().On("test", "happened", func(_ context.Context, _ *testTx, _ monotonic.AggregateEvent) error {
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
	d := monotonic.NewDispatch[*testTx]().
		On("a", "x", func(context.Context, *testTx, monotonic.AggregateEvent) error { return nil }).
		On("b", "y", func(context.Context, *testTx, monotonic.AggregateEvent) error { return nil })

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
	d := monotonic.NewDispatch[*testTx]().On("a", "x", func(_ context.Context, _ *testTx, _ monotonic.AggregateEvent) error {
		called = true
		return nil
	})
	err := d.Apply(context.Background(), &testTx{}, monotonic.AggregateEvent{
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

func TestDispatch_TruncateWithoutHandlerReturnsSentinel(t *testing.T) {
	d := monotonic.NewDispatch[*testTx]()
	if err := d.Truncate(context.Background(), &testTx{}); !errors.Is(err, monotonic.ErrNoTruncateHandler) {
		t.Errorf("Truncate err = %v, want ErrNoTruncateHandler", err)
	}
	if d.HasTruncateHandler() {
		t.Error("HasTruncateHandler() = true on an empty Dispatch")
	}

	d.OnTruncate(func(context.Context, *testTx) error { return nil })
	if !d.HasTruncateHandler() {
		t.Error("HasTruncateHandler() = false after OnTruncate")
	}
	if err := d.Truncate(context.Background(), &testTx{}); err != nil {
		t.Errorf("Truncate after OnTruncate: %v", err)
	}
}

func TestDispatch_ApplyUnregisteredIsNoop(t *testing.T) {
	d := monotonic.NewDispatch[*testTx]()
	err := d.Apply(context.Background(), &testTx{}, monotonic.AggregateEvent{
		AggregateType: "x",
		Event:         monotonic.AcceptedEvent{Event: monotonic.Event{Type: "y"}},
	})
	if err != nil {
		t.Errorf("Apply on unregistered should be no-op, got %v", err)
	}
}

// --- Projector tests ---

func TestProjector_UpdateReportsNoWorkWhenNoPendingEvents(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	backend := newFakeBackend()
	cr := &counterRef{}

	p, err := monotonic.NewProjector("p1", store, countingDispatch(cr), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if result.Updated() {
		t.Errorf("Updated() = true on a caught-up projector, want false")
	}
	if result.AppliedEvents != 0 {
		t.Errorf("AppliedEvents = %d, want 0", result.AppliedEvents)
	}
}

// TestProjector_UpdateDoesNotCommitWhenCaughtUp pins the short-circuit in advance: an idle
// projector polls on every tick, and rewriting the counter to the value it already holds would
// mean a write and a commit per tick per projector forever.
func TestProjector_UpdateDoesNotCommitWhenCaughtUp(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	backend := newFakeBackend()

	p, _ := monotonic.NewProjector("p1", store, countingDispatch(&counterRef{}), backend, 0)
	for i := 0; i < 3; i++ {
		if _, err := p.Update(ctx); err != nil {
			t.Fatalf("Update: %v", err)
		}
	}

	if backend.commitCalls != 0 {
		t.Errorf("caught-up Updates committed %d times, want 0", backend.commitCalls)
	}
	if backend.isLocked() {
		t.Error("modification lock still held after a caught-up Update")
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
	p, err := monotonic.NewProjector("p1", store, countingDispatch(cr), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if result.AppliedEvents != 3 {
		t.Errorf("AppliedEvents = %d, want 3", result.AppliedEvents)
	}
	if cr.n != 3 {
		t.Errorf("handler called %d times, want 3", cr.n)
	}
	// The whole batch is one transaction, so three events cost exactly one commit.
	if backend.commitCalls != 1 {
		t.Errorf("commitCalls = %d, want 1 for a 3-event batch", backend.commitCalls)
	}
}

func TestProjector_StateAdvancesAfterUpdate(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)

	backend := newFakeBackend()
	p, _ := monotonic.NewProjector("p1", store, countingDispatch(&counterRef{}), backend, 0)

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if result.AdvancedToGlobalCounter != 2 {
		t.Errorf("AdvancedToGlobalCounter = %d, want 2", result.AdvancedToGlobalCounter)
	}
	if got := backend.stateOf("p1").GlobalCounter; got != 2 {
		t.Errorf("backend counter = %d, want 2", got)
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
	p, err := monotonic.NewProjector("p1", store, countingDispatch(cr), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if result.AppliedEvents != 1 {
		t.Errorf("AppliedEvents = %d, want 1 pending event past counter 5", result.AppliedEvents)
	}
	if cr.n != 1 {
		t.Errorf("handler called %d times, want 1", cr.n)
	}
}

// TestProjector_ResumeStateIsReadPerUpdate covers the counter no longer being cached on the
// Projector: a second projector constructed over the same backend state picks up where the first
// left off without any construction-time read.
func TestProjector_ResumeStateIsReadPerUpdate(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	backend := newFakeBackend()

	p, _ := monotonic.NewProjector("p1", store, countingDispatch(&counterRef{}), backend, 0)

	// Constructed while the store is empty, then events arrive.
	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if result.AppliedEvents != 2 {
		t.Errorf("AppliedEvents = %d, want 2", result.AppliedEvents)
	}
}

func TestProjector_TruncateRunsHandlerAndRewindsState(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	for i := int64(1); i <= 3; i++ {
		emitEvent(ctx, t, store, i)
	}

	backend := newFakeBackend()
	cr := &counterRef{}
	truncated := 0
	dispatch := countingDispatch(cr).OnTruncate(func(context.Context, *testTx) error {
		truncated++
		return nil
	})

	p, err := monotonic.NewProjector("p1", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := backend.stateOf("p1").GlobalCounter; got != 3 {
		t.Fatalf("counter = %d before truncate, want 3", got)
	}

	if err := backend.EnqueueTruncate(ctx, "p1"); err != nil {
		t.Fatalf("EnqueueTruncate: %v", err)
	}

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update after EnqueueTruncate: %v", err)
	}
	if !result.Truncated {
		t.Error("Truncated = false, want true")
	}
	if !result.Updated() {
		t.Error("Updated() = false after a truncate, want true")
	}
	if truncated != 1 {
		t.Errorf("truncate handler ran %d times, want 1", truncated)
	}

	state := backend.stateOf("p1")
	if state.GlobalCounter != 0 {
		t.Errorf("counter = %d after truncate, want 0", state.GlobalCounter)
	}
	if state.TruncateRequested {
		t.Error("TruncateRequested still set after the truncate was serviced")
	}

	// A rewound projector replays the whole stream.
	cr.n = 0
	result, err = p.Update(ctx)
	if err != nil {
		t.Fatalf("Update after truncate: %v", err)
	}
	if result.AppliedEvents != 3 || cr.n != 3 {
		t.Errorf("replayed %d events (handler %d calls), want 3", result.AppliedEvents, cr.n)
	}
}

// TestProjector_TruncateWithoutHandlerWedgesProjector covers the strict reading of a missing
// truncate handler. Rewinding without clearing would replay the stream over a projection that was
// never emptied, so the projector refuses to make any progress at all until the gap is fixed.
func TestProjector_TruncateWithoutHandlerWedgesProjector(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	cr := &counterRef{}
	p, _ := monotonic.NewProjector("p1", store, countingDispatch(cr), backend, 0)
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := backend.EnqueueTruncate(ctx, "p1"); err != nil {
		t.Fatalf("EnqueueTruncate: %v", err)
	}

	emitEvent(ctx, t, store, 2)
	appliedBefore := cr.n

	// The wedge is permanent: nothing about retrying clears it, so every later Update fails the
	// same way rather than quietly resuming.
	for attempt := 0; attempt < 3; attempt++ {
		_, err := p.Update(ctx)
		if !errors.Is(err, monotonic.ErrNoTruncateHandler) {
			t.Fatalf("attempt %d: Update err = %v, want ErrNoTruncateHandler", attempt, err)
		}
	}

	if cr.n != appliedBefore {
		t.Errorf("handler ran %d more times while wedged, want 0; a pending truncate must block event application", cr.n-appliedBefore)
	}

	state := backend.stateOf("p1")
	if !state.TruncateRequested {
		t.Error("TruncateRequested cleared by a failed truncate; the request would be silently dropped")
	}
	if state.GlobalCounter != 1 {
		t.Errorf("counter = %d while wedged, want 1 (unchanged)", state.GlobalCounter)
	}
	if backend.commitCalls != 1 {
		t.Errorf("commitCalls = %d, want 1 (only the pre-truncate batch)", backend.commitCalls)
	}
	if backend.isLocked() {
		t.Error("modification lock still held after a wedged Update")
	}

	// Registering the handler is the way out, and the rebuild then proceeds normally.
	fixed, _ := monotonic.NewProjector("p1", store, countingDispatch(cr).OnTruncate(func(context.Context, *testTx) error {
		return nil
	}), backend, 0)
	result, err := fixed.Update(ctx)
	if err != nil {
		t.Fatalf("Update after registering a handler: %v", err)
	}
	if !result.Truncated {
		t.Error("Truncated = false after the handler was registered, want true")
	}
	if got := backend.stateOf("p1").GlobalCounter; got != 0 {
		t.Errorf("counter = %d after truncate, want 0", got)
	}
}

// TestProjector_RunSurfacesErrNoTruncateHandler covers the sentinel surviving the Run loop, which
// is where a supervisor actually observes it and has to decide whether restarting could help.
func TestProjector_RunSurfacesErrNoTruncateHandler(t *testing.T) {
	// Bounded so a regression that stops erroring fails this test instead of spinning forever:
	// Run only returns on an error or a dead context, and the lenient behavior produces neither.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	p, _ := monotonic.NewProjector("p1", store, countingDispatch(&counterRef{}), backend, 0)
	if err := backend.EnqueueTruncate(ctx, "p1"); err != nil {
		t.Fatalf("EnqueueTruncate: %v", err)
	}

	if err := p.Run(ctx, time.Millisecond); !errors.Is(err, monotonic.ErrNoTruncateHandler) {
		t.Errorf("Run err = %v, want ErrNoTruncateHandler", err)
	}
}

func TestProjector_RequestTruncateEnqueuesForItsOwnName(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	truncated := 0
	dispatch := countingDispatch(&counterRef{}).OnTruncate(func(context.Context, *testTx) error {
		truncated++
		return nil
	})
	p, _ := monotonic.NewProjector("p1", store, dispatch, backend, 0)
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if err := p.RequestTruncate(ctx); err != nil {
		t.Fatalf("RequestTruncate: %v", err)
	}
	if !backend.stateOf("p1").TruncateRequested {
		t.Fatal("RequestTruncate did not flag the projector's own name")
	}

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update after RequestTruncate: %v", err)
	}
	if !result.Truncated || truncated != 1 {
		t.Errorf("Truncated=%v handler ran %d times, want true and 1", result.Truncated, truncated)
	}
}

// TestProjector_RequestTruncateRejectsMissingHandler is the front door the wedge exists to make
// unnecessary: the gap is reported to whoever asked, and nothing is enqueued.
func TestProjector_RequestTruncateRejectsMissingHandler(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	backend := newFakeBackend()

	p, _ := monotonic.NewProjector("p1", store, countingDispatch(&counterRef{}), backend, 0)

	if err := p.RequestTruncate(ctx); !errors.Is(err, monotonic.ErrNoTruncateHandler) {
		t.Fatalf("RequestTruncate err = %v, want ErrNoTruncateHandler", err)
	}
	if backend.stateOf("p1").TruncateRequested {
		t.Error("a rejected RequestTruncate still enqueued the truncate")
	}

	// Nothing was enqueued, so the projector keeps working rather than wedging.
	emitEvent(ctx, t, store, 1)
	if _, err := p.Update(ctx); err != nil {
		t.Errorf("Update after a rejected RequestTruncate: %v", err)
	}
}

func TestProjector_TruncatePropagatesHandlerError(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	wantErr := errors.New("truncate boom")
	dispatch := countingDispatch(&counterRef{}).OnTruncate(func(context.Context, *testTx) error {
		return wantErr
	})

	p, _ := monotonic.NewProjector("p1", store, dispatch, backend, 0)
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := backend.EnqueueTruncate(ctx, "p1"); err != nil {
		t.Fatalf("EnqueueTruncate: %v", err)
	}

	if _, err := p.Update(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("Update err = %v, want %v", err, wantErr)
	}

	// The request survives a failed truncate, so the next Update tries again.
	state := backend.stateOf("p1")
	if !state.TruncateRequested {
		t.Error("TruncateRequested cleared by a failed truncate")
	}
	if state.GlobalCounter != 1 {
		t.Errorf("counter = %d after a failed truncate, want 1 (unchanged)", state.GlobalCounter)
	}
	if backend.isLocked() {
		t.Error("modification lock still held after a failed truncate")
	}
}

func TestProjector_UpdatePropagatesHandlerError(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	p, _ := monotonic.NewProjector("p1", store, failingDispatch(errors.New("apply boom")), backend, 0)

	if _, err := p.Update(ctx); err == nil {
		t.Error("expected error from Update when handler fails")
	}
}

// TestProjector_BatchIsAllOrNothingOnHandlerError pins the point of locking the whole update: the
// batch is one transaction, so a handler failing partway through discards the events that already
// applied rather than leaving the projection partially advanced.
func TestProjector_BatchIsAllOrNothingOnHandlerError(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)
	emitEvent(ctx, t, store, 3)

	backend := newFakeBackend()
	cr := &counterRef{}
	p, _ := monotonic.NewProjector("p1", store, failOnNthDispatch(cr, 2, errors.New("boom")), backend, 0)

	result, err := p.Update(ctx)
	if err == nil {
		t.Fatal("expected error from Update when the second handler fails")
	}
	if result.AppliedEvents != 0 {
		t.Errorf("AppliedEvents = %d on a failed batch, want 0", result.AppliedEvents)
	}
	if got := backend.stateOf("p1").GlobalCounter; got != 0 {
		t.Errorf("counter = %d after a failed batch, want 0 (nothing committed)", got)
	}
	if backend.commitCalls != 0 {
		t.Errorf("commitCalls = %d on a failed batch, want 0", backend.commitCalls)
	}
	if backend.isLocked() {
		t.Error("modification lock still held after a failed batch")
	}
}

func TestProjector_UpdatePropagatesLockError(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	backend.lockErr = errors.New("lock boom")

	p, _ := monotonic.NewProjector("p1", store, countingDispatch(&counterRef{}), backend, 0)
	if _, err := p.Update(ctx); !errors.Is(err, backend.lockErr) {
		t.Fatalf("Update err = %v, want %v", err, backend.lockErr)
	}
}

func TestProjector_UpdatePropagatesCommitError(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)

	backend := newFakeBackend()
	backend.commitErr = errors.New("commit boom")

	p, _ := monotonic.NewProjector("p1", store, countingDispatch(&counterRef{}), backend, 0)
	if _, err := p.Update(ctx); !errors.Is(err, backend.commitErr) {
		t.Fatalf("Update err = %v, want %v", err, backend.commitErr)
	}
	if got := backend.stateOf("p1").GlobalCounter; got != 0 {
		t.Errorf("counter = %d after a failed commit, want 0", got)
	}
	// The failed commit left the transaction open; the projector's deferred Rollback must end it.
	if backend.isLocked() {
		t.Error("modification lock still held after a failed commit")
	}
}

func TestProjector_RunStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := monotonic.NewInMemoryStore()
	backend := newFakeBackend()
	p, _ := monotonic.NewProjector("p1", store, noopDispatch(), backend, 0)

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
	p, _ := monotonic.NewProjector("p1", store, failingDispatch(fmt.Errorf("run boom")), backend, 0)

	err := p.Run(ctx, time.Millisecond)
	if err == nil {
		t.Error("expected Run to return error when Update fails")
	}
}

func TestProjector_RunDrainsWithoutSleepingWhileWorkPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := monotonic.NewInMemoryStore()
	const numEvents = 250
	for i := int64(1); i <= numEvents; i++ {
		emitEvent(ctx, t, store, i)
	}

	backend := newFakeBackend()
	// A batch size below numEvents forces Run to loop, which is the behavior under test.
	p, _ := monotonic.NewProjector("p1", store, countingDispatch(&counterRef{}), backend, 50)

	go func() {
		p.Run(ctx, 10*time.Second)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if backend.stateOf("p1").GlobalCounter == numEvents {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := backend.stateOf("p1").GlobalCounter; got != uint64(numEvents) {
		t.Errorf("counter = %d, want %d; Run slept instead of draining", got, numEvents)
	}
}

func TestProjector_NameRequired(t *testing.T) {
	store := monotonic.NewInMemoryStore()
	backend := newFakeBackend()
	_, err := monotonic.NewProjector("", store, noopDispatch(), backend, 0)
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
		p, _ := monotonic.NewProjector(name, store, noopDispatch(), backend, 0)
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
	failP, _ := monotonic.NewProjector("fail", store, failingDispatch(errors.New("fail")), backend, 0)
	okP, _ := monotonic.NewProjector("ok", store, noopDispatch(), backend, 0)

	err := monotonic.RunProjectors(ctx, time.Millisecond, failP, okP)
	if err == nil {
		t.Error("expected error when one projector fails")
	}
}

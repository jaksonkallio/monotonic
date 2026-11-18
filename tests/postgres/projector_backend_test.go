package postgres_integration_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
	pgstore "github.com/jaksonkallio/monotonic/pkg/store/postgres"
)

// resetProjectorState truncates events and projector_state so each test starts clean.
// Returns a fresh backend (already migrated via TestMain bootstrap below) for convenience.
func resetProjectorState(tb testing.TB) *pgstore.ProjectorBackend {
	tb.Helper()
	ctx := context.Background()

	if _, err := sharedPool.Exec(ctx, "TRUNCATE events RESTART IDENTITY"); err != nil {
		tb.Fatalf("truncate events: %v", err)
	}
	backend := pgstore.NewProjectorBackend(sharedPool)
	if err := backend.Migrate(ctx); err != nil {
		tb.Fatalf("migrate projector_state: %v", err)
	}
	if _, err := sharedPool.Exec(ctx, "TRUNCATE projector_state"); err != nil {
		tb.Fatalf("truncate projector_state: %v", err)
	}
	return backend
}

// resetProjectionTable drops and recreates a single-column projection table for tests.
func resetProjectionTable(tb testing.TB, tableName string) {
	tb.Helper()
	ctx := context.Background()
	if _, err := sharedPool.Exec(ctx, `DROP TABLE IF EXISTS `+tableName); err != nil {
		tb.Fatalf("drop %s: %v", tableName, err)
	}
	if _, err := sharedPool.Exec(ctx, `CREATE TABLE `+tableName+` (k TEXT PRIMARY KEY, n BIGINT NOT NULL)`); err != nil {
		tb.Fatalf("create %s: %v", tableName, err)
	}
	tb.Cleanup(func() {
		sharedPool.Exec(context.Background(), `DROP TABLE IF EXISTS `+tableName)
	})
}

// readProjectorState reads a projector's persisted state straight from the table, so assertions
// about what actually committed don't go through the code under test.
func readProjectorState(tb testing.TB, name string) monotonic.ProjectorStateSnapshot {
	tb.Helper()

	var counter int64
	var truncateRequested bool
	err := sharedPool.QueryRow(context.Background(),
		`SELECT global_counter, truncate_requested FROM projector_state WHERE projector_name = $1`,
		name,
	).Scan(&counter, &truncateRequested)
	if errors.Is(err, pgx.ErrNoRows) {
		return monotonic.ProjectorStateSnapshot{}
	}
	if err != nil {
		tb.Fatalf("read projector_state for %q: %v", name, err)
	}
	return monotonic.ProjectorStateSnapshot{GlobalCounter: uint64(counter), TruncateRequested: truncateRequested}
}

func appendTestEvent(tb testing.TB, store monotonic.Store, aggID string, counter int64, eventType string) {
	tb.Helper()
	err := store.Append(context.Background(), monotonic.AggregateEvent{
		AggregateType: "thing",
		AggregateID:   aggID,
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.NewEvent[any](eventType, nil),
			Counter:    counter,
			AcceptedAt: time.Now(),
		},
	})
	if err != nil {
		tb.Fatalf("append: %v", err)
	}
}

// countingDispatch builds a dispatch that bumps a row in tableName per event.
func countingDispatch(tableName string, onApply func()) *monotonic.Dispatch[pgx.Tx] {
	return monotonic.NewDispatch[pgx.Tx]().
		On("thing", "did", func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
			if onApply != nil {
				onApply()
			}
			_, err := tx.Exec(ctx,
				`INSERT INTO `+tableName+` (k, n) VALUES ($1, 1) ON CONFLICT (k) DO UPDATE SET n = `+tableName+`.n + 1`,
				event.AggregateID,
			)
			return err
		})
}

func projectedTotal(tb testing.TB, tableName string) int64 {
	tb.Helper()
	var total int64
	if err := sharedPool.QueryRow(context.Background(), `SELECT COALESCE(SUM(n), 0) FROM `+tableName).Scan(&total); err != nil {
		tb.Fatalf("read %s: %v", tableName, err)
	}
	return total
}

// TestProjectorBackend_AtomicCommitAcrossTables verifies that two implementer-owned
// tables and projector_state all advance together inside one commit.
func TestProjectorBackend_AtomicCommitAcrossTables(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_a")
	resetProjectionTable(t, "proj_b")

	dispatch := monotonic.NewDispatch[pgx.Tx]().
		On("thing", "did", func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
			if _, err := tx.Exec(ctx, `INSERT INTO proj_a (k, n) VALUES ($1, 1) ON CONFLICT (k) DO UPDATE SET n = proj_a.n + 1`, event.AggregateID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO proj_b (k, n) VALUES ($1, 1) ON CONFLICT (k) DO UPDATE SET n = proj_b.n + 1`, event.AggregateID); err != nil {
				return err
			}
			return nil
		})

	appendTestEvent(t, store, "x", 1, "did")
	appendTestEvent(t, store, "x", 2, "did")

	p, err := monotonic.NewProjector("two_tables", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	result, err := p.Update(ctx)
	if err != nil || result.AppliedEvents != 2 {
		t.Fatalf("Update: applied=%d err=%v, want 2", result.AppliedEvents, err)
	}

	var a, b int64
	if err := sharedPool.QueryRow(ctx, `SELECT n FROM proj_a WHERE k = 'x'`).Scan(&a); err != nil {
		t.Fatalf("query proj_a: %v", err)
	}
	if err := sharedPool.QueryRow(ctx, `SELECT n FROM proj_b WHERE k = 'x'`).Scan(&b); err != nil {
		t.Fatalf("query proj_b: %v", err)
	}
	if a != 2 || b != 2 {
		t.Errorf("expected both tables to show 2 events, got a=%d b=%d", a, b)
	}

	if got := readProjectorState(t, "two_tables").GlobalCounter; got != result.AdvancedToGlobalCounter {
		t.Errorf("committed state %d != reported counter %d", got, result.AdvancedToGlobalCounter)
	}
}

// TestProjectorBackend_RollsBackOnHandlerError verifies that if the handler writes
// then errors, neither the implementer table nor projector_state are committed.
func TestProjectorBackend_RollsBackOnHandlerError(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_rb")

	wantErr := errors.New("simulated handler failure")
	dispatch := monotonic.NewDispatch[pgx.Tx]().
		On("thing", "did", func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
			if _, err := tx.Exec(ctx, `INSERT INTO proj_rb (k, n) VALUES ($1, 1)`, event.AggregateID); err != nil {
				return err
			}
			return wantErr
		})

	appendTestEvent(t, store, "x", 1, "did")

	p, err := monotonic.NewProjector("rb", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	_, err = p.Update(ctx)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v, got %v", wantErr, err)
	}

	if got := projectedTotal(t, "proj_rb"); got != 0 {
		t.Errorf("proj_rb total: want 0 (rolled back), got %d", got)
	}
	if got := readProjectorState(t, "rb").GlobalCounter; got != 0 {
		t.Errorf("projector_state must not advance on handler error, got %d", got)
	}
}

// TestProjectorBackend_BatchRollsBackEntirelyOnMidBatchError pins the behavior that locking the
// whole update buys: a batch is one transaction, so a handler failing on the third of four events
// discards the first two as well rather than leaving the projection partially advanced.
func TestProjectorBackend_BatchRollsBackEntirelyOnMidBatchError(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_partial")

	for i := int64(1); i <= 4; i++ {
		appendTestEvent(t, store, "x", i, "did")
	}

	wantErr := errors.New("simulated mid-batch failure")
	calls := 0
	dispatch := monotonic.NewDispatch[pgx.Tx]().
		On("thing", "did", func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
			calls++
			if calls == 3 {
				return wantErr
			}
			_, err := tx.Exec(ctx, `INSERT INTO proj_partial (k, n) VALUES ($1, 1) ON CONFLICT (k) DO UPDATE SET n = proj_partial.n + 1`, event.AggregateID)
			return err
		})

	p, err := monotonic.NewProjector("partial", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := p.Update(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("Update err = %v, want %v", err, wantErr)
	}

	if got := projectedTotal(t, "proj_partial"); got != 0 {
		t.Errorf("proj_partial total = %d after a failed batch, want 0; the first two events leaked", got)
	}
	if got := readProjectorState(t, "partial").GlobalCounter; got != 0 {
		t.Errorf("projector_state = %d after a failed batch, want 0", got)
	}

	// The batch is retried whole on the next Update.
	calls = 0
	dispatch2 := countingDispatch("proj_partial", nil)
	p2, err := monotonic.NewProjector("partial", store, dispatch2, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	result, err := p2.Update(ctx)
	if err != nil || result.AppliedEvents != 4 {
		t.Fatalf("retry Update: applied=%d err=%v, want 4", result.AppliedEvents, err)
	}
	if got := projectedTotal(t, "proj_partial"); got != 4 {
		t.Errorf("proj_partial total = %d after retry, want 4", got)
	}
}

// TestProjectorBackend_ResumeFromStoredCounter verifies a fresh Projector against the
// same backend picks up where the last one left off.
func TestProjectorBackend_ResumeFromStoredCounter(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_resume")

	calls := 0
	dispatch := countingDispatch("proj_resume", func() { calls++ })

	appendTestEvent(t, store, "x", 1, "did")
	appendTestEvent(t, store, "x", 2, "did")

	p1, err := monotonic.NewProjector("resume", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	first, err := p1.Update(ctx)
	if err != nil || first.AppliedEvents != 2 {
		t.Fatalf("first Update: applied=%d err=%v, want 2", first.AppliedEvents, err)
	}

	appendTestEvent(t, store, "x", 3, "did")

	p2, err := monotonic.NewProjector("resume", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("resume NewProjector: %v", err)
	}
	second, err := p2.Update(ctx)
	if err != nil || second.AppliedEvents != 1 {
		t.Errorf("resumed Update: applied=%d err=%v, want exactly 1", second.AppliedEvents, err)
	}
	if second.AdvancedToGlobalCounter <= first.AdvancedToGlobalCounter {
		t.Errorf("resumed counter %d did not advance past %d", second.AdvancedToGlobalCounter, first.AdvancedToGlobalCounter)
	}
	if calls != 3 {
		t.Errorf("handler invoked %d times across both projectors, want 3", calls)
	}
}

// TestProjectorBackend_ConcurrentInstancesApplyEachEventExactlyOnce simulates two instances of the
// same projector (two processes, say) racing to drain the same stream. The projector_state row lock
// taken for the whole update must serialize them: each instance reads the resume counter only after
// winning the lock, so an event can never be applied twice or skipped.
func TestProjectorBackend_ConcurrentInstancesApplyEachEventExactlyOnce(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_race")

	const numEvents = 40
	for i := int64(1); i <= numEvents; i++ {
		appendTestEvent(t, store, "x", i, "did")
	}

	var applyCalls atomic.Int32
	// A small batch size forces many lock acquisitions, so the two instances interleave.
	const batchSize = 3

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			dispatch := countingDispatch("proj_race", func() { applyCalls.Add(1) })
			p, err := monotonic.NewProjector("race", store, dispatch, backend, batchSize)
			if err != nil {
				errs[i] = err
				return
			}
			for {
				result, err := p.Update(ctx)
				if err != nil {
					errs[i] = err
					return
				}
				if !result.Updated() {
					return
				}
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("instance %d: %v", i, err)
		}
	}

	if got := projectedTotal(t, "proj_race"); got != numEvents {
		t.Errorf("projection total = %d, want %d; events were duplicated or skipped", got, numEvents)
	}
	if got := applyCalls.Load(); got != numEvents {
		t.Errorf("handler ran %d times across both instances, want exactly %d", got, numEvents)
	}
	if got := readProjectorState(t, "race").GlobalCounter; got != numEvents {
		t.Errorf("projector_state = %d, want %d", got, numEvents)
	}
}

// TestProjectorBackend_TruncateClearsProjectionAndCounterAtomically verifies that a requested
// truncate clears the implementer's table and rewinds projector_state in one commit, and that the
// next Update replays the full event stream.
func TestProjectorBackend_TruncateClearsProjectionAndCounterAtomically(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_truncate")

	dispatch := countingDispatch("proj_truncate", nil).
		OnTruncate(func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `TRUNCATE proj_truncate`)
			return err
		})

	appendTestEvent(t, store, "x", 1, "did")
	appendTestEvent(t, store, "x", 2, "did")

	p, err := monotonic.NewProjector("truncate", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if result, err := p.Update(ctx); err != nil || result.AppliedEvents != 2 {
		t.Fatalf("Update: applied=%d err=%v, want 2", result.AppliedEvents, err)
	}

	if err := backend.EnqueueTruncate(ctx, "truncate"); err != nil {
		t.Fatalf("EnqueueTruncate: %v", err)
	}
	if !readProjectorState(t, "truncate").TruncateRequested {
		t.Fatal("EnqueueTruncate did not set truncate_requested")
	}

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update after EnqueueTruncate: %v", err)
	}
	if !result.Truncated {
		t.Error("Truncated = false, want true")
	}
	if result.AppliedEvents != 0 {
		t.Errorf("AppliedEvents = %d on a truncate, want 0", result.AppliedEvents)
	}

	state := readProjectorState(t, "truncate")
	if state.GlobalCounter != 0 {
		t.Errorf("projector_state = %d after truncate, want 0", state.GlobalCounter)
	}
	if state.TruncateRequested {
		t.Error("truncate_requested still set after the truncate was serviced")
	}
	if got := projectedTotal(t, "proj_truncate"); got != 0 {
		t.Errorf("proj_truncate total = %d after truncate, want 0", got)
	}

	// The rewound projector replays the whole stream.
	replay, err := p.Update(ctx)
	if err != nil || replay.AppliedEvents != 2 {
		t.Fatalf("Update after truncate: applied=%d err=%v, want 2 (full replay)", replay.AppliedEvents, err)
	}
	if got := projectedTotal(t, "proj_truncate"); got != 2 {
		t.Errorf("proj_truncate total = %d after replay, want 2", got)
	}
}

// TestProjectorBackend_TruncateRollsBackOnHandlerError verifies that if the truncate handler errors,
// neither its writes nor the projector_state rewind are committed, and the request survives so the
// next Update tries again.
func TestProjectorBackend_TruncateRollsBackOnHandlerError(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_truncate_rb")

	wantErr := errors.New("simulated truncate failure")
	dispatch := countingDispatch("proj_truncate_rb", nil).
		OnTruncate(func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `INSERT INTO proj_truncate_rb (k, n) VALUES ('leaked', 1)`); err != nil {
				return err
			}
			return wantErr
		})

	appendTestEvent(t, store, "x", 1, "did")

	p, err := monotonic.NewProjector("truncate_rb", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}
	before := readProjectorState(t, "truncate_rb")

	if err := backend.EnqueueTruncate(ctx, "truncate_rb"); err != nil {
		t.Fatalf("EnqueueTruncate: %v", err)
	}
	if _, err := p.Update(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("Update err = %v, want %v", err, wantErr)
	}

	var leaked int
	sharedPool.QueryRow(ctx, `SELECT COUNT(*) FROM proj_truncate_rb WHERE k = 'leaked'`).Scan(&leaked)
	if leaked != 0 {
		t.Errorf("truncate handler's write committed despite the error, got %d rows", leaked)
	}

	after := readProjectorState(t, "truncate_rb")
	if after.GlobalCounter != before.GlobalCounter {
		t.Errorf("projector_state = %d after a failed truncate, want %d (unchanged)", after.GlobalCounter, before.GlobalCounter)
	}
	if !after.TruncateRequested {
		t.Error("truncate_requested cleared by a failed truncate; the request would be lost")
	}
}

// TestProjectorBackend_LockStateCreatesMissingRow verifies a projector that has never run gets its
// projector_state row created rather than failing on a missing row.
func TestProjectorBackend_LockStateCreatesMissingRow(t *testing.T) {
	ctx := context.Background()
	backend := resetProjectorState(t)

	tx, state, err := backend.LockStateForModification(ctx, "never_ran", 0)
	if err != nil {
		t.Fatalf("LockStateForModification on an unknown projector: %v", err)
	}
	defer tx.Rollback(ctx)

	if state.GlobalCounter != 0 || state.TruncateRequested {
		t.Errorf("state = %+v for a projector that never ran, want zero", state)
	}
}

// TestProjectorBackend_EnqueueTruncateCreatesMissingRow verifies a truncate can be requested for a
// projector that has not run yet, rather than being silently dropped by an UPDATE matching no rows.
func TestProjectorBackend_EnqueueTruncateCreatesMissingRow(t *testing.T) {
	ctx := context.Background()
	backend := resetProjectorState(t)

	if err := backend.EnqueueTruncate(ctx, "never_ran"); err != nil {
		t.Fatalf("EnqueueTruncate: %v", err)
	}
	if state := readProjectorState(t, "never_ran"); !state.TruncateRequested {
		t.Errorf("state = %+v, want TruncateRequested", state)
	}
}

// TestProjectorBackend_CaughtUpUpdateWritesNothing pins the short-circuit for an idle projector:
// polling must not rewrite projector_state to the counter it already holds, or a fleet of idle
// projectors would write once per poll interval forever.
func TestProjectorBackend_CaughtUpUpdateWritesNothing(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_idle")

	appendTestEvent(t, store, "x", 1, "did")

	p, err := monotonic.NewProjector("idle", store, countingDispatch("proj_idle", nil), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}

	var before, after time.Time
	if err := sharedPool.QueryRow(ctx, `SELECT updated_at FROM projector_state WHERE projector_name = 'idle'`).Scan(&before); err != nil {
		t.Fatalf("read updated_at: %v", err)
	}

	for i := 0; i < 3; i++ {
		result, err := p.Update(ctx)
		if err != nil {
			t.Fatalf("idle Update: %v", err)
		}
		if result.Updated() {
			t.Fatalf("idle Update reported work: %+v", result)
		}
	}

	if err := sharedPool.QueryRow(ctx, `SELECT updated_at FROM projector_state WHERE projector_name = 'idle'`).Scan(&after); err != nil {
		t.Fatalf("read updated_at: %v", err)
	}
	if !after.Equal(before) {
		t.Errorf("updated_at moved from %v to %v; a caught-up Update wrote to projector_state", before, after)
	}
}

// TestProjectorBackend_TruncateWithoutHandlerWedgesProjector verifies the wedge is durable in the
// database rather than just in process memory: the request stays on the row, the counter does not
// move, and no further events are applied until a handler exists.
func TestProjectorBackend_TruncateWithoutHandlerWedgesProjector(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_wedge")

	appendTestEvent(t, store, "x", 1, "did")
	appendTestEvent(t, store, "x", 2, "did")

	// No OnTruncate registered.
	p, err := monotonic.NewProjector("wedge", store, countingDispatch("proj_wedge", nil), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if result, err := p.Update(ctx); err != nil || result.AppliedEvents != 2 {
		t.Fatalf("Update: applied=%d err=%v, want 2", result.AppliedEvents, err)
	}
	before := readProjectorState(t, "wedge")

	if err := backend.EnqueueTruncate(ctx, "wedge"); err != nil {
		t.Fatalf("EnqueueTruncate: %v", err)
	}

	// Events appended after the request must not be applied while the truncate is pending.
	appendTestEvent(t, store, "x", 3, "did")

	for attempt := 0; attempt < 3; attempt++ {
		_, err := p.Update(ctx)
		if !errors.Is(err, monotonic.ErrNoTruncateHandler) {
			t.Fatalf("attempt %d: Update err = %v, want ErrNoTruncateHandler", attempt, err)
		}
	}

	if got := projectedTotal(t, "proj_wedge"); got != 2 {
		t.Errorf("proj_wedge total = %d while wedged, want 2 (unchanged)", got)
	}
	after := readProjectorState(t, "wedge")
	if after.GlobalCounter != before.GlobalCounter {
		t.Errorf("projector_state = %d while wedged, want %d (unchanged)", after.GlobalCounter, before.GlobalCounter)
	}
	if !after.TruncateRequested {
		t.Error("truncate_requested cleared by a wedged Update; the request would be lost across a restart")
	}

	// Registering a handler is the way out, and the rebuild then covers all three events.
	fixed, err := monotonic.NewProjector("wedge", store, countingDispatch("proj_wedge", nil).
		OnTruncate(func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `TRUNCATE proj_wedge`)
			return err
		}), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if result, err := fixed.Update(ctx); err != nil || !result.Truncated {
		t.Fatalf("Update after registering a handler: truncated=%v err=%v", result.Truncated, err)
	}
	if result, err := fixed.Update(ctx); err != nil || result.AppliedEvents != 3 {
		t.Fatalf("replay: applied=%d err=%v, want 3", result.AppliedEvents, err)
	}
	if got := projectedTotal(t, "proj_wedge"); got != 3 {
		t.Errorf("proj_wedge total = %d after rebuild, want 3", got)
	}
}

// TestProjectorBackend_RequestTruncateRoundTrip verifies the front door: RequestTruncate flags the
// projector's own row, and rejects the request without writing anything when no handler exists.
func TestProjectorBackend_RequestTruncateRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_request")

	appendTestEvent(t, store, "x", 1, "did")

	unTruncatable, err := monotonic.NewProjector("request", store, countingDispatch("proj_request", nil), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if err := unTruncatable.RequestTruncate(ctx); !errors.Is(err, monotonic.ErrNoTruncateHandler) {
		t.Fatalf("RequestTruncate err = %v, want ErrNoTruncateHandler", err)
	}
	if readProjectorState(t, "request").TruncateRequested {
		t.Fatal("a rejected RequestTruncate still wrote truncate_requested")
	}

	truncatable, err := monotonic.NewProjector("request", store, countingDispatch("proj_request", nil).
		OnTruncate(func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `TRUNCATE proj_request`)
			return err
		}), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := truncatable.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := truncatable.RequestTruncate(ctx); err != nil {
		t.Fatalf("RequestTruncate: %v", err)
	}
	if !readProjectorState(t, "request").TruncateRequested {
		t.Fatal("RequestTruncate did not flag the projector's own row")
	}

	result, err := truncatable.Update(ctx)
	if err != nil || !result.Truncated {
		t.Fatalf("Update after RequestTruncate: truncated=%v err=%v", result.Truncated, err)
	}
	if got := projectedTotal(t, "proj_request"); got != 0 {
		t.Errorf("proj_request total = %d after truncate, want 0", got)
	}
}

// TestProjectorBackend_SlowBatchDoesNotFailAConcurrentInstance pins the absence of a lock_timeout
// in LockStateForModification. A second instance of the same projector must block until the holder
// finishes and then proceed, because waiting is the whole point of the lock. Erroring instead would
// be escalated by Projector.Drain into a returned error and by DrainProjectors into cancelling every
// sibling, so a batch that merely ran long would abort the rest of a startup catch-up.
//
// The hold deliberately exceeds the 5s timeout this code used to set, which is what makes the test
// able to detect a regression at all.
func TestProjectorBackend_SlowBatchDoesNotFailAConcurrentInstance(t *testing.T) {
	if testing.Short() {
		t.Skip("holds a lock for longer than the removed 5s timeout")
	}

	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_slow")

	appendTestEvent(t, store, "x", 1, "did")

	// Commit the projector_state row first, so the waiter below blocks on SELECT ... FOR UPDATE
	// rather than on an uncommitted insert of the same primary key.
	warmup, err := monotonic.NewProjector("slow", store, countingDispatch("proj_slow", nil), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := warmup.Update(ctx); err != nil {
		t.Fatalf("warmup Update: %v", err)
	}

	appendTestEvent(t, store, "x", 2, "did")

	// Stand in for a long-running batch holding the state lock.
	holder, _, err := backend.LockStateForModification(ctx, "slow", 0)
	if err != nil {
		t.Fatalf("holder LockStateForModification: %v", err)
	}

	const hold = 6 * time.Second
	waiterDone := make(chan error, 1)
	var waiterResult monotonic.ProjectorUpdateResult
	go func() {
		p, err := monotonic.NewProjector("slow", store, countingDispatch("proj_slow", nil), backend, 0)
		if err != nil {
			waiterDone <- err
			return
		}
		waiterResult, err = p.Update(ctx)
		waiterDone <- err
	}()

	// The waiter must still be blocked while the holder holds the lock.
	select {
	case err := <-waiterDone:
		holder.Rollback(ctx)
		t.Fatalf("concurrent instance returned while the lock was held (err=%v); it must block", err)
	case <-time.After(hold):
	}

	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("holder Rollback: %v", err)
	}

	select {
	case err := <-waiterDone:
		if err != nil {
			t.Fatalf("concurrent instance failed after waiting %v for the lock: %v", hold, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent instance never completed after the lock was released")
	}

	if waiterResult.AppliedEvents != 1 {
		t.Errorf("waiter applied %d events, want 1", waiterResult.AppliedEvents)
	}
	if got := projectedTotal(t, "proj_slow"); got != 2 {
		t.Errorf("proj_slow total = %d, want 2", got)
	}
}

// TestProjectorBackend_InitialCounterForwardSkipsHistory verifies Forward against a real stream:
// the projector_state row is created at the current MAX(global_counter), so events written before
// the projector existed are never replayed.
func TestProjectorBackend_InitialCounterForwardSkipsHistory(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_forward")

	for i := int64(1); i <= 4; i++ {
		appendTestEvent(t, store, "x", i, "did")
	}
	head, err := store.MaxGlobalCounter(ctx)
	if err != nil {
		t.Fatalf("MaxGlobalCounter: %v", err)
	}
	if head != 4 {
		t.Fatalf("stream head = %d, want 4", head)
	}

	p, err := monotonic.NewProjector("forward", store, countingDispatch("proj_forward", nil), backend, 0, monotonic.WithInitialCounterForward())
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if got := projectedTotal(t, "proj_forward"); got != 0 {
		t.Errorf("proj_forward total = %d, want 0; pre-existing history must be skipped", got)
	}
	if got := readProjectorState(t, "forward").GlobalCounter; got != uint64(head) {
		t.Errorf("projector_state = %d, want the stream head %d", got, head)
	}

	// Events after deployment are processed normally.
	appendTestEvent(t, store, "x", 5, "did")
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain after new event: %v", err)
	}
	if got := projectedTotal(t, "proj_forward"); got != 1 {
		t.Errorf("proj_forward total = %d, want 1", got)
	}
}

// TestProjectorBackend_InitialCounterOnlyAppliesOnRowCreation pins that the setting is consulted
// only by the INSERT that creates the row. A restart must resume from the committed counter, or a
// Forward projector would skip every event written while it was down.
func TestProjectorBackend_InitialCounterOnlyAppliesOnRowCreation(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_once")

	appendTestEvent(t, store, "x", 1, "did")

	first, err := monotonic.NewProjector("once", store, countingDispatch("proj_once", nil), backend, 0, monotonic.WithInitialCounterForward())
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if err := first.Drain(ctx); err != nil {
		t.Fatalf("first Drain: %v", err)
	}
	createdAt := readProjectorState(t, "once").GlobalCounter

	// Events arrive while the projector is down.
	appendTestEvent(t, store, "x", 2, "did")
	appendTestEvent(t, store, "x", 3, "did")

	restarted, err := monotonic.NewProjector("once", store, countingDispatch("proj_once", nil), backend, 0, monotonic.WithInitialCounterForward())
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if err := restarted.Drain(ctx); err != nil {
		t.Fatalf("restarted Drain: %v", err)
	}

	if got := projectedTotal(t, "proj_once"); got != 2 {
		t.Errorf("proj_once total = %d, want 2; a restart must resume from state, not re-skip to the head", got)
	}
	if got := readProjectorState(t, "once").GlobalCounter; got <= createdAt {
		t.Errorf("projector_state = %d, want it past the creation counter %d", got, createdAt)
	}
}

func TestProjectorBackend_InitialCounterExplicit(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_explicit")

	for i := int64(1); i <= 5; i++ {
		appendTestEvent(t, store, "x", i, "did")
	}

	p, err := monotonic.NewProjector("explicit", store, countingDispatch("proj_explicit", nil), backend, 0, monotonic.WithInitialCounterAt(3))
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if got := projectedTotal(t, "proj_explicit"); got != 2 {
		t.Errorf("proj_explicit total = %d, want 2 (the events above counter 3)", got)
	}
}

// TestProjectorBackend_TruncateRefusedForForward verifies the guard holds against a flag set
// directly on the backend, which is the path that bypasses RequestTruncate entirely.
func TestProjectorBackend_TruncateRefusedForForward(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_refuse")

	appendTestEvent(t, store, "x", 1, "did")

	dispatch := countingDispatch("proj_refuse", nil).
		OnTruncate(func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `TRUNCATE proj_refuse`)
			return err
		})
	p, err := monotonic.NewProjector("refuse", store, dispatch, backend, 0, monotonic.WithInitialCounterForward())
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if err := p.RequestTruncate(ctx); !errors.Is(err, monotonic.ErrInitialCounterNotReproducible) {
		t.Fatalf("RequestTruncate err = %v, want ErrInitialCounterNotReproducible", err)
	}
	if readProjectorState(t, "refuse").TruncateRequested {
		t.Fatal("a rejected RequestTruncate still wrote truncate_requested")
	}

	if err := backend.EnqueueTruncate(ctx, "refuse"); err != nil {
		t.Fatalf("EnqueueTruncate: %v", err)
	}
	if _, err := p.Update(ctx); !errors.Is(err, monotonic.ErrInitialCounterNotReproducible) {
		t.Fatalf("Update err = %v, want ErrInitialCounterNotReproducible", err)
	}
	if !readProjectorState(t, "refuse").TruncateRequested {
		t.Error("truncate_requested cleared by a refused truncate")
	}
}

// TestStore_MaxGlobalCounterIgnoresSequenceGaps covers the reason MaxGlobalCounter reads the table
// rather than the sequence: a rolled-back Append consumes sequence values permanently, so currval
// can sit above any counter an event will ever carry and a projector starting there would skip real events.
func TestStore_MaxGlobalCounterIgnoresSequenceGaps(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	resetProjectorState(t)

	appendTestEvent(t, store, "x", 1, "did")

	// Burn sequence values in a transaction that rolls back.
	tx, err := sharedPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := tx.Exec(ctx,
			`INSERT INTO events (aggregate_type, aggregate_id, counter, event_type, payload, accepted_at)
			 VALUES ('thing', 'doomed', $1, 'ignored', NULL, now())`, i+1,
		); err != nil {
			t.Fatalf("insert doomed event: %v", err)
		}
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	got, err := store.MaxGlobalCounter(ctx)
	if err != nil {
		t.Fatalf("MaxGlobalCounter: %v", err)
	}

	var realMax int64
	if err := sharedPool.QueryRow(ctx, `SELECT COALESCE(MAX(global_counter), 0) FROM events`).Scan(&realMax); err != nil {
		t.Fatalf("read real max: %v", err)
	}
	if got != realMax {
		t.Errorf("MaxGlobalCounter = %d, want %d; burned sequence values must not count", got, realMax)
	}
}

// TestProjectorBackend_TruncateRewindsToExplicitInitialCounter verifies against a real stream that
// truncation rewinds to the projector's configured start rather than to zero, so a projector that
// deliberately skips the beginning of the stream rebuilds the projection it actually had.
func TestProjectorBackend_TruncateRewindsToExplicitInitialCounter(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_rewind")

	for i := int64(1); i <= 6; i++ {
		appendTestEvent(t, store, "x", i, "did")
	}

	dispatch := countingDispatch("proj_rewind", nil).
		OnTruncate(func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `TRUNCATE proj_rewind`)
			return err
		})
	p, err := monotonic.NewProjector("rewind", store, dispatch, backend, 0, monotonic.WithInitialCounterAt(4))
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got := projectedTotal(t, "proj_rewind"); got != 2 {
		t.Fatalf("proj_rewind total = %d, want 2 (events 5 and 6)", got)
	}

	if err := p.RequestTruncate(ctx); err != nil {
		t.Fatalf("RequestTruncate: %v", err)
	}
	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update after RequestTruncate: %v", err)
	}
	if !result.Truncated {
		t.Error("Truncated = false, want true")
	}

	state := readProjectorState(t, "rewind")
	if state.GlobalCounter != 4 {
		t.Errorf("projector_state = %d after truncate, want 4 (the explicit start), not 0", state.GlobalCounter)
	}
	if state.TruncateRequested {
		t.Error("truncate_requested still set after the truncate was serviced")
	}
	if got := projectedTotal(t, "proj_rewind"); got != 0 {
		t.Errorf("proj_rewind total = %d after truncate, want 0", got)
	}

	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain after truncate: %v", err)
	}
	if got := projectedTotal(t, "proj_rewind"); got != 2 {
		t.Errorf("proj_rewind total = %d after rebuild, want 2; a rewind to zero would have replayed all 6", got)
	}
}

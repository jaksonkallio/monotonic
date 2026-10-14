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

	p, err := monotonic.NewProjector(ctx, "two_tables", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if n, err := p.Update(ctx); err != nil || n != 2 {
		t.Fatalf("Update: n=%d err=%v", n, err)
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

	state, err := backend.GetState(ctx, "two_tables")
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state.Counter != p.GlobalCounter() {
		t.Errorf("backend state %d != projector counter %d", state.Counter, p.GlobalCounter())
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

	p, err := monotonic.NewProjector(ctx, "rb", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	_, err = p.Update(ctx)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v, got %v", wantErr, err)
	}

	var count int
	sharedPool.QueryRow(ctx, `SELECT COUNT(*) FROM proj_rb`).Scan(&count)
	if count != 0 {
		t.Errorf("proj_rb row count: want 0 (rolled back), got %d", count)
	}

	state, _ := backend.GetState(ctx, "rb")
	if state.Counter != 0 {
		t.Errorf("projector_state must not advance on handler error, got %d", state.Counter)
	}
}

// TestProjectorBackend_ResumeFromStoredCounter verifies a fresh Projector against the
// same backend picks up where the last one left off.
func TestProjectorBackend_ResumeFromStoredCounter(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_resume")

	cr := 0
	dispatch := monotonic.NewDispatch[pgx.Tx]().
		On("thing", "did", func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
			cr++
			_, err := tx.Exec(ctx, `INSERT INTO proj_resume (k, n) VALUES ($1, 1) ON CONFLICT (k) DO UPDATE SET n = proj_resume.n + 1`, event.AggregateID)
			return err
		})

	appendTestEvent(t, store, "x", 1, "did")
	appendTestEvent(t, store, "x", 2, "did")

	p1, err := monotonic.NewProjector(ctx, "resume", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if n, err := p1.Update(ctx); err != nil || n != 2 {
		t.Fatalf("first Update n=%d, want 2", n)
	}

	appendTestEvent(t, store, "x", 3, "did")

	p2, err := monotonic.NewProjector(ctx, "resume", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("resume NewProjector: %v", err)
	}
	if p2.GlobalCounter() != p1.GlobalCounter() {
		t.Errorf("resumed counter %d, want %d", p2.GlobalCounter(), p1.GlobalCounter())
	}
	if n, err := p2.Update(ctx); err != nil || n != 1 {
		t.Errorf("resumed Update should process exactly 1 event, got n=%d err=%v", n, err)
	}
	if cr != 3 {
		t.Errorf("handler invoked %d times across both projectors, want 3", cr)
	}
}

// TestProjectorBackend_ReplayingSeenCounterIsIdempotent verifies that a backend
// asked to record a counter <= already-stored does not error and does not regress state.
func TestProjectorBackend_ReplayingSeenCounterIsIdempotent(t *testing.T) {
	ctx := context.Background()
	_ = testStore(t)
	backend := resetProjectorState(t)

	apply := func(_ context.Context, _ pgx.Tx) error { return nil }

	if _, err := backend.RunEvent(ctx, "idem", 0, 5, apply); err != nil {
		t.Fatalf("first RunEvent: %v", err)
	}
	// Replay an older counter — should not error and must not regress stored counter.
	if got, err := backend.RunEvent(ctx, "idem", 0, 3, apply); err != nil || got != 5 {
		t.Fatalf("replay older RunEvent: got %d err %v, want 5/nil", got, err)
	}
	state, _ := backend.GetState(ctx, "idem")
	if state.Counter != 5 {
		t.Errorf("counter regressed: got %d, want 5", state.Counter)
	}
	// Same counter again — idempotent rewrite.
	if got, err := backend.RunEvent(ctx, "idem", 5, 5, apply); err != nil || got != 5 {
		t.Fatalf("replay equal RunEvent: got %d err %v, want 5/nil", got, err)
	}
	state, _ = backend.GetState(ctx, "idem")
	if state.Counter != 5 {
		t.Errorf("equal-counter replay regressed: got %d, want 5", state.Counter)
	}
}

// TestProjectorBackend_RunEventSkipsAlreadyProcessedCounter verifies that apply is not
// re-invoked when RunEvent is called again for a counter already recorded.
func TestProjectorBackend_RunEventSkipsAlreadyProcessedCounter(t *testing.T) {
	ctx := context.Background()
	backend := resetProjectorState(t)

	applyCalls := 0
	apply := func(_ context.Context, _ pgx.Tx) error {
		applyCalls++
		return nil
	}

	if _, err := backend.RunEvent(ctx, "skip", 0, 5, apply); err != nil {
		t.Fatalf("first RunEvent: %v", err)
	}
	if applyCalls != 1 {
		t.Fatalf("expected apply called once, got %d", applyCalls)
	}

	// Replaying the same counter must not re-run apply.
	if _, err := backend.RunEvent(ctx, "skip", 5, 5, apply); err != nil {
		t.Fatalf("replay RunEvent: %v", err)
	}
	if applyCalls != 1 {
		t.Errorf("apply must not run again for an already-processed counter, calls=%d", applyCalls)
	}
}

// TestProjectorBackend_RunEventConcurrentInstancesApplyExactlyOnce simulates two instances of
// the same projector racing to process the same event concurrently. The row lock taken inside
// RunEvent must serialize them so apply runs exactly once, regardless of which instance wins.
func TestProjectorBackend_RunEventConcurrentInstancesApplyExactlyOnce(t *testing.T) {
	ctx := context.Background()
	backend := resetProjectorState(t)

	var applyCalls atomic.Int32
	apply := func(_ context.Context, _ pgx.Tx) error {
		applyCalls.Add(1)
		// Hold the lock briefly so both goroutines are guaranteed to overlap.
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = backend.RunEvent(ctx, "race", 0, 9, apply)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("instance %d RunEvent: %v", i, err)
		}
	}
	if got := applyCalls.Load(); got != 1 {
		t.Errorf("expected exactly 1 apply across both racing instances, got %d", got)
	}
	state, _ := backend.GetState(ctx, "race")
	if state.Counter != 9 {
		t.Errorf("expected counter 9 after race, got %d", state.Counter)
	}
}

// TestProjectorBackend_ResetClearsProjectionAndCounterAtomically verifies that Projector.Reset
// truncates the implementer's table and rewinds projector_state in the same commit, and that a
// subsequent Update replays the full event stream from the beginning.
func TestProjectorBackend_ResetClearsProjectionAndCounterAtomically(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_reset")

	dispatch := monotonic.NewDispatch[pgx.Tx]().
		On("thing", "did", func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
			_, err := tx.Exec(ctx, `INSERT INTO proj_reset (k, n) VALUES ($1, 1) ON CONFLICT (k) DO UPDATE SET n = proj_reset.n + 1`, event.AggregateID)
			return err
		}).
		OnReset(func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `TRUNCATE proj_reset`)
			return err
		})

	appendTestEvent(t, store, "x", 1, "did")
	appendTestEvent(t, store, "x", 2, "did")

	p, err := monotonic.NewProjector(ctx, "reset", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if n, err := p.Update(ctx); err != nil || n != 2 {
		t.Fatalf("Update: n=%d err=%v", n, err)
	}

	if err := p.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	if p.GlobalCounter() != 0 {
		t.Errorf("projector counter must be 0 after Reset, got %d", p.GlobalCounter())
	}
	state, _ := backend.GetState(ctx, "reset")
	if state.Counter != 0 {
		t.Errorf("projector_state must be 0 after Reset, got %d", state.Counter)
	}
	if state.ResetRequested {
		t.Error("reset request must be cleared once serviced")
	}
	var count int
	sharedPool.QueryRow(ctx, `SELECT COUNT(*) FROM proj_reset`).Scan(&count)
	if count != 0 {
		t.Errorf("proj_reset row count: want 0 after Reset, got %d", count)
	}

	if n, err := p.Update(ctx); err != nil || n != 2 {
		t.Fatalf("Update after Reset: n=%d err=%v, want 2 (full replay)", n, err)
	}
}

// TestProjectorBackend_ResetRollsBackOnResetFuncError verifies that if the reset func errors,
// neither its writes nor the projector_state rewind are committed.
func TestProjectorBackend_ResetRollsBackOnResetFuncError(t *testing.T) {
	ctx := context.Background()
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_reset_rb")

	if _, err := backend.RunEvent(ctx, "reset_rb", 0, 7, func(_ context.Context, _ pgx.Tx) error { return nil }); err != nil {
		t.Fatalf("seed RunEvent: %v", err)
	}
	if err := backend.RequestReset(ctx, "reset_rb"); err != nil {
		t.Fatalf("RequestReset: %v", err)
	}

	wantErr := errors.New("simulated reset failure")
	_, err := backend.ClaimReset(ctx, "reset_rb", func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO proj_reset_rb (k, n) VALUES ('x', 1)`); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v, got %v", wantErr, err)
	}

	var count int
	sharedPool.QueryRow(ctx, `SELECT COUNT(*) FROM proj_reset_rb`).Scan(&count)
	if count != 0 {
		t.Errorf("proj_reset_rb row count: want 0 (rolled back), got %d", count)
	}
	state, _ := backend.GetState(ctx, "reset_rb")
	if state.Counter != 7 {
		t.Errorf("projector_state must not be rewound on reset func error, got %d", state.Counter)
	}
	if !state.ResetRequested {
		t.Error("a failed claim must leave the request pending so it can be retried")
	}
}

// TestProjectorBackend_GetStateMissingProjectorReturnsZero verifies the empty-state default.
func TestProjectorBackend_GetStateMissingProjectorReturnsZero(t *testing.T) {
	ctx := context.Background()
	backend := resetProjectorState(t)

	state, err := backend.GetState(ctx, "never_ran")
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state != (monotonic.ProjectorState{}) {
		t.Errorf("missing projector should return zero state, got %+v", state)
	}
}

// TestProjectorBackend_RunEventDeclinesWhenRewoundUnderneath verifies the guard that keeps a
// stale instance from corrupting a freshly reset projection. An instance that was at counter 7
// asks to apply 8, but the stored counter has been rewound to 0 by a reset it has not noticed.
// Applying 8 there would strand events 1-7 forever, so RunEvent must decline and report the
// position the caller actually has to resume from.
func TestProjectorBackend_RunEventDeclinesWhenRewoundUnderneath(t *testing.T) {
	ctx := context.Background()
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_rewound")

	applied := false
	apply := func(ctx context.Context, tx pgx.Tx) error {
		applied = true
		_, err := tx.Exec(ctx, `INSERT INTO proj_rewound (k, n) VALUES ('x', 1)`)
		return err
	}

	if _, err := backend.RunEvent(ctx, "rewound", 0, 7, func(_ context.Context, _ pgx.Tx) error { return nil }); err != nil {
		t.Fatalf("seed RunEvent: %v", err)
	}
	// A reset lands: the stored counter goes back to 0 while our instance still believes it is at 7.
	if err := backend.RequestReset(ctx, "rewound"); err != nil {
		t.Fatalf("RequestReset: %v", err)
	}
	if claimed, err := backend.ClaimReset(ctx, "rewound", func(_ context.Context, _ pgx.Tx) error { return nil }); err != nil || !claimed {
		t.Fatalf("ClaimReset: claimed=%v err=%v", claimed, err)
	}

	got, err := backend.RunEvent(ctx, "rewound", 7, 8, apply)
	if err != nil {
		t.Fatalf("RunEvent after rewind: %v", err)
	}
	if applied {
		t.Error("apply must not run when the stored counter has been rewound below the caller's position")
	}
	if got != 0 {
		t.Errorf("RunEvent returned %d, want 0 (the rewound position the caller must resume from)", got)
	}

	var count int
	sharedPool.QueryRow(ctx, `SELECT COUNT(*) FROM proj_rewound`).Scan(&count)
	if count != 0 {
		t.Errorf("proj_rewound row count: want 0, got %d", count)
	}
}

// TestProjectorBackend_RunEventDeclinesWhileResetPending verifies that once a reset is requested,
// no instance keeps applying events that are about to be truncated.
func TestProjectorBackend_RunEventDeclinesWhileResetPending(t *testing.T) {
	ctx := context.Background()
	backend := resetProjectorState(t)

	if _, err := backend.RunEvent(ctx, "pending", 0, 3, func(_ context.Context, _ pgx.Tx) error { return nil }); err != nil {
		t.Fatalf("seed RunEvent: %v", err)
	}
	if err := backend.RequestReset(ctx, "pending"); err != nil {
		t.Fatalf("RequestReset: %v", err)
	}

	applied := false
	got, err := backend.RunEvent(ctx, "pending", 3, 4, func(_ context.Context, _ pgx.Tx) error {
		applied = true
		return nil
	})
	if err != nil {
		t.Fatalf("RunEvent with reset pending: %v", err)
	}
	if applied {
		t.Error("apply must not run while a reset is pending")
	}
	if got != 3 {
		t.Errorf("RunEvent returned %d, want 3 (counter unchanged)", got)
	}
}

// TestProjectorBackend_ConcurrentClaimResetRunsExactlyOnce simulates every instance of a projector
// noticing the same reset request at once. The projector_state row lock must let exactly one of
// them run the reset func, with the rest reporting claimed=false.
func TestProjectorBackend_ConcurrentClaimResetRunsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	backend := resetProjectorState(t)

	if _, err := backend.RunEvent(ctx, "claim_race", 0, 4, func(_ context.Context, _ pgx.Tx) error { return nil }); err != nil {
		t.Fatalf("seed RunEvent: %v", err)
	}
	if err := backend.RequestReset(ctx, "claim_race"); err != nil {
		t.Fatalf("RequestReset: %v", err)
	}

	var resetCalls atomic.Int32
	reset := func(_ context.Context, _ pgx.Tx) error {
		resetCalls.Add(1)
		// Hold the row lock briefly so every contender is guaranteed to overlap with the winner.
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	const instances = 4
	var wg sync.WaitGroup
	claims := make([]bool, instances)
	errs := make([]error, instances)
	for i := range claims {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			claims[i], errs[i] = backend.ClaimReset(ctx, "claim_race", reset)
		}(i)
	}
	wg.Wait()

	claimed := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("instance %d ClaimReset: %v", i, err)
		}
		if claims[i] {
			claimed++
		}
	}
	if claimed != 1 {
		t.Errorf("%d instances claimed the reset, want exactly 1", claimed)
	}
	if got := resetCalls.Load(); got != 1 {
		t.Errorf("reset func ran %d times across %d instances, want exactly 1", got, instances)
	}

	state, _ := backend.GetState(ctx, "claim_race")
	if state.Counter != 0 {
		t.Errorf("counter = %d, want 0 after reset", state.Counter)
	}
	if state.ResetRequested {
		t.Error("reset request must be cleared once claimed")
	}
}

// TestProjectorBackend_ResetRequestedOutOfBandIsPickedUpByProjector covers the operational path
// this feature exists for: stamping reset_requested_at directly in SQL, with no deploy and no
// in-process call, and having a running projector notice it, truncate, and replay.
func TestProjectorBackend_ResetRequestedOutOfBandIsPickedUpByProjector(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_oob")

	dispatch := monotonic.NewDispatch[pgx.Tx]().
		On("thing", "did", func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
			_, err := tx.Exec(ctx, `INSERT INTO proj_oob (k, n) VALUES ($1, 1) ON CONFLICT (k) DO UPDATE SET n = proj_oob.n + 1`, event.AggregateID)
			return err
		}).
		OnReset(func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `TRUNCATE proj_oob`)
			return err
		})

	appendTestEvent(t, store, "x", 1, "did")
	appendTestEvent(t, store, "x", 2, "did")

	p, err := monotonic.NewProjector(ctx, "oob", store, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if n, err := p.Update(ctx); err != nil || n != 2 {
		t.Fatalf("Update: n=%d err=%v", n, err)
	}

	// An operator asks for a rebuild by hand.
	if _, err := sharedPool.Exec(ctx,
		`UPDATE projector_state SET reset_requested_at = now() WHERE projector_name = 'oob'`,
	); err != nil {
		t.Fatalf("stamp reset_requested_at: %v", err)
	}

	// The projector's next poll services the request and replays in the same call.
	n, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update after out-of-band request: %v", err)
	}
	if n != 2 {
		t.Errorf("Update processed %d events, want 2 (reset then full replay)", n)
	}

	var count int64
	if err := sharedPool.QueryRow(ctx, `SELECT n FROM proj_oob WHERE k = 'x'`).Scan(&count); err != nil {
		t.Fatalf("query proj_oob: %v", err)
	}
	if count != 2 {
		t.Errorf("proj_oob n = %d, want 2 (rebuilt from scratch, not stacked on the old rows)", count)
	}

	state, _ := backend.GetState(ctx, "oob")
	if state.ResetRequested {
		t.Error("reset_requested_at must be cleared once serviced")
	}
}

// TestProjectorBackend_ResetWithConcurrentInstancesRebuildsExactlyOnce is the end-to-end version of
// the hazard this feature has to survive: one instance resets while a second is still running with a
// stale counter. The second instance must notice the rewind and replay from the beginning rather
// than resuming from where it thought it was, and the two of them together must apply every event
// exactly once.
func TestProjectorBackend_ResetWithConcurrentInstancesRebuildsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)
	resetProjectionTable(t, "proj_two_inst")

	newDispatch := func() *monotonic.Dispatch[pgx.Tx] {
		return monotonic.NewDispatch[pgx.Tx]().
			On("thing", "did", func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
				_, err := tx.Exec(ctx, `INSERT INTO proj_two_inst (k, n) VALUES ($1, 1) ON CONFLICT (k) DO UPDATE SET n = proj_two_inst.n + 1`, event.AggregateID)
				return err
			}).
			OnReset(func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `TRUNCATE proj_two_inst`)
				return err
			})
	}

	const totalEvents = 5
	for i := int64(1); i <= 3; i++ {
		appendTestEvent(t, store, "x", i, "did")
	}

	a, err := monotonic.NewProjector(ctx, "two_inst", store, newDispatch(), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector a: %v", err)
	}
	b, err := monotonic.NewProjector(ctx, "two_inst", store, newDispatch(), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector b: %v", err)
	}

	// Both instances catch up, then more events land.
	for _, p := range []*monotonic.Projector[pgx.Tx]{a, b} {
		if _, err := p.Update(ctx); err != nil {
			t.Fatalf("initial Update: %v", err)
		}
	}
	for i := int64(4); i <= totalEvents; i++ {
		appendTestEvent(t, store, "x", i, "did")
	}

	// a resets. b knows nothing about it and is still holding counter 3.
	if err := a.Reset(ctx); err != nil {
		t.Fatalf("a.Reset: %v", err)
	}

	// Drive both instances to quiescence, b first so it gets the chance to act on its stale counter.
	for round := 0; round < 10; round++ {
		nb, err := b.Update(ctx)
		if err != nil {
			t.Fatalf("b.Update: %v", err)
		}
		na, err := a.Update(ctx)
		if err != nil {
			t.Fatalf("a.Update: %v", err)
		}
		if na == 0 && nb == 0 {
			break
		}
	}

	var n int64
	if err := sharedPool.QueryRow(ctx, `SELECT n FROM proj_two_inst WHERE k = 'x'`).Scan(&n); err != nil {
		t.Fatalf("query proj_two_inst: %v", err)
	}
	if n != totalEvents {
		t.Errorf("proj_two_inst n = %d, want %d: every event must be applied exactly once across both instances after the reset", n, totalEvents)
	}

	state, _ := backend.GetState(ctx, "two_inst")
	if state.Counter != totalEvents {
		t.Errorf("projector_state counter = %d, want %d", state.Counter, totalEvents)
	}
}

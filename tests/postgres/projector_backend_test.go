package postgres_integration_test

import (
	"context"
	"errors"
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
	sharedPool.QueryRow(ctx, `SELECT n FROM proj_a WHERE k = 'x'`).Scan(&a)
	sharedPool.QueryRow(ctx, `SELECT n FROM proj_b WHERE k = 'x'`).Scan(&b)
	if a != 2 || b != 2 {
		t.Errorf("expected both tables to show 2 events, got a=%d b=%d", a, b)
	}

	got, err := backend.GetState(ctx, "two_tables")
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if got != p.GlobalCounter() {
		t.Errorf("backend state %d != projector counter %d", got, p.GlobalCounter())
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

	p, _ := monotonic.NewProjector(ctx, "rb", store, dispatch, backend, 0)
	_, err := p.Update(ctx)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v, got %v", wantErr, err)
	}

	var count int
	sharedPool.QueryRow(ctx, `SELECT COUNT(*) FROM proj_rb`).Scan(&count)
	if count != 0 {
		t.Errorf("proj_rb row count: want 0 (rolled back), got %d", count)
	}

	got, _ := backend.GetState(ctx, "rb")
	if got != 0 {
		t.Errorf("projector_state must not advance on handler error, got %d", got)
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

	p1, _ := monotonic.NewProjector(ctx, "resume", store, dispatch, backend, 0)
	if n, _ := p1.Update(ctx); n != 2 {
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

	if err := backend.RunEvent(ctx, "idem", 5, apply); err != nil {
		t.Fatalf("first RunEvent: %v", err)
	}
	// Replay an older counter — should not error and must not regress stored counter.
	if err := backend.RunEvent(ctx, "idem", 3, apply); err != nil {
		t.Fatalf("replay older RunEvent: %v", err)
	}
	got, _ := backend.GetState(ctx, "idem")
	if got != 5 {
		t.Errorf("counter regressed: got %d, want 5", got)
	}
	// Same counter again — idempotent rewrite.
	if err := backend.RunEvent(ctx, "idem", 5, apply); err != nil {
		t.Fatalf("replay equal RunEvent: %v", err)
	}
	got, _ = backend.GetState(ctx, "idem")
	if got != 5 {
		t.Errorf("equal-counter replay regressed: got %d, want 5", got)
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

	if err := p.Reset(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `TRUNCATE proj_reset`)
		return err
	}); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	if p.GlobalCounter() != 0 {
		t.Errorf("projector counter must be 0 after Reset, got %d", p.GlobalCounter())
	}
	got, _ := backend.GetState(ctx, "reset")
	if got != 0 {
		t.Errorf("projector_state must be 0 after Reset, got %d", got)
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

	if err := backend.RunEvent(ctx, "reset_rb", 7, func(_ context.Context, _ pgx.Tx) error { return nil }); err != nil {
		t.Fatalf("seed RunEvent: %v", err)
	}

	wantErr := errors.New("simulated reset failure")
	err := backend.ResetState(ctx, "reset_rb", func(ctx context.Context, tx pgx.Tx) error {
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
	got, _ := backend.GetState(ctx, "reset_rb")
	if got != 7 {
		t.Errorf("projector_state must not be rewound on reset func error, got %d", got)
	}
}

// TestProjectorBackend_GetStateMissingProjectorReturnsZero verifies the empty-state default.
func TestProjectorBackend_GetStateMissingProjectorReturnsZero(t *testing.T) {
	ctx := context.Background()
	backend := resetProjectorState(t)

	got, err := backend.GetState(ctx, "never_ran")
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if got != 0 {
		t.Errorf("missing projector should return 0, got %d", got)
	}
}

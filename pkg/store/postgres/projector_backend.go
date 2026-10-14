package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// ProjectorBackend implements monotonic.ProjectorBackend[pgx.Tx] on top of Postgres,
// opening a transaction per event and committing the handler's writes
// atomically with the projector_state advance.
type ProjectorBackend struct {
	pool *pgxpool.Pool
}

// NewProjectorBackend creates a Postgres-backed monotonic.ProjectorBackend[pgx.Tx].
func NewProjectorBackend(pool *pgxpool.Pool) *ProjectorBackend {
	return &ProjectorBackend{pool: pool}
}

// Migrate creates the projector_state table if it does not exist and adds any columns
// introduced since. Call once during application startup before constructing any Projector
// against this backend.
func (b *ProjectorBackend) Migrate(ctx context.Context) error {
	_, err := b.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS projector_state (
			projector_name TEXT NOT NULL PRIMARY KEY,
			global_counter BIGINT NOT NULL,
			updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	if err != nil {
		return fmt.Errorf("migrate projector_state: %w", err)
	}

	// reset_requested_at is the out-of-band reset trigger: non-NULL means a rebuild has been asked
	// for and not yet serviced. A nullable timestamp rather than a boolean so operators can see how
	// long a request has been outstanding. Setting it by hand in SQL is a supported way to trigger a
	// reset without a deploy.
	_, err = b.pool.Exec(ctx, `
		ALTER TABLE projector_state ADD COLUMN IF NOT EXISTS reset_requested_at TIMESTAMPTZ;
	`)
	if err != nil {
		return fmt.Errorf("migrate projector_state.reset_requested_at: %w", err)
	}
	return nil
}

// GetState returns the stored state for projectorName, zero-valued if no row exists.
func (b *ProjectorBackend) GetState(ctx context.Context, projectorName string) (monotonic.ProjectorState, error) {
	var counter int64
	var resetRequestedAt *time.Time
	err := b.pool.QueryRow(ctx,
		`SELECT global_counter, reset_requested_at FROM projector_state WHERE projector_name = $1`,
		projectorName,
	).Scan(&counter, &resetRequestedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return monotonic.ProjectorState{}, nil
		}
		return monotonic.ProjectorState{}, fmt.Errorf("read projector_state for %q: %w", projectorName, err)
	}
	return monotonic.ProjectorState{
		Counter:        uint64(counter),
		ResetRequested: resetRequestedAt != nil,
	}, nil
}

// lockState ensures a projector_state row exists for projectorName and returns its current
// state, having taken a row lock that is held until the caller's transaction ends. Concurrent
// callers (other instances of the same projector) block here until the lock holder commits or
// rolls back, then see its committed state rather than a stale snapshot.
func lockState(ctx context.Context, tx pgx.Tx, projectorName string) (monotonic.ProjectorState, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO projector_state (projector_name, global_counter)
		VALUES ($1, 0)
		ON CONFLICT (projector_name) DO NOTHING
	`, projectorName); err != nil {
		return monotonic.ProjectorState{}, fmt.Errorf("ensure projector_state row for %q: %w", projectorName, err)
	}

	var counter int64
	var resetRequestedAt *time.Time
	err := tx.QueryRow(ctx,
		`SELECT global_counter, reset_requested_at FROM projector_state WHERE projector_name = $1 FOR UPDATE`,
		projectorName,
	).Scan(&counter, &resetRequestedAt)
	if err != nil {
		return monotonic.ProjectorState{}, fmt.Errorf("lock projector_state for %q: %w", projectorName, err)
	}
	return monotonic.ProjectorState{
		Counter:        uint64(counter),
		ResetRequested: resetRequestedAt != nil,
	}, nil
}

// RunEvent opens a tx, locks projector_state for projectorName, and decides under that lock whether
// apply should run at all. It skips when counter has already been recorded (by this or another
// instance of the same projector racing concurrently), when the stored counter has fallen behind
// from (a reset rewound the projector since the caller last looked), and when a reset is pending
// (whatever apply wrote would be truncated moments later). Otherwise it runs apply, advances
// projector_state to counter, and commits both atomically.
//
// The returned counter is projector_state as of the commit, so a caller that gets back something
// other than the counter it asked for knows apply did not run and that its own position is stale.
// The row lock serializes concurrent RunEvent/ClaimReset calls for the same projectorName, so two
// instances racing on the same event can never both run apply.
func (b *ProjectorBackend) RunEvent(
	ctx context.Context,
	projectorName string,
	from, counter uint64,
	apply func(ctx context.Context, tx pgx.Tx) error,
) (uint64, error) {
	if counter == 0 {
		return 0, fmt.Errorf("projector_state counter must be > 0")
	}

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	state, err := lockState(ctx, tx, projectorName)
	if err != nil {
		return 0, err
	}

	// Nothing to do: already applied, rewound underneath the caller, or about to be truncated.
	if state.Counter >= counter || state.Counter < from || state.ResetRequested {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit projector_state for %q: %w", projectorName, err)
		}
		return state.Counter, nil
	}

	if err := apply(ctx, tx); err != nil {
		return 0, err
	}

	if _, err := tx.Exec(ctx,
		`UPDATE projector_state SET global_counter = $2, updated_at = now() WHERE projector_name = $1`,
		projectorName, int64(counter),
	); err != nil {
		return 0, fmt.Errorf("advance projector_state for %q: %w", projectorName, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit projector_state for %q: %w", projectorName, err)
	}
	return counter, nil
}

// RequestReset stamps reset_requested_at for projectorName, creating the row if the projector has
// never run. An already-pending request keeps its original timestamp so the stamp always reads as
// "when a rebuild was first asked for" rather than being pushed forward by repeated calls.
func (b *ProjectorBackend) RequestReset(ctx context.Context, projectorName string) error {
	_, err := b.pool.Exec(ctx, `
		INSERT INTO projector_state (projector_name, global_counter, reset_requested_at)
		VALUES ($1, 0, now())
		ON CONFLICT (projector_name) DO UPDATE SET
			reset_requested_at = COALESCE(projector_state.reset_requested_at, now()),
			updated_at = now()
	`, projectorName)
	if err != nil {
		return fmt.Errorf("request reset for %q: %w", projectorName, err)
	}
	return nil
}

// ClaimReset opens a tx and locks projector_state for projectorName, which is what makes the reset
// happen exactly once: every instance that notices the request races here, and the losers find
// reset_requested_at already cleared by the winner's commit and report claimed=false. The winner
// hands the tx to reset, rewinds projector_state to 0, clears the request, and commits all three
// together. The row lock also serializes against concurrent RunEvent calls for the same
// projectorName, so reset cannot race with an in-flight apply.
func (b *ProjectorBackend) ClaimReset(
	ctx context.Context,
	projectorName string,
	reset func(ctx context.Context, tx pgx.Tx) error,
) (bool, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	state, err := lockState(ctx, tx, projectorName)
	if err != nil {
		return false, err
	}
	if !state.ResetRequested {
		// Another instance serviced the request while we were waiting for the lock.
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit projector_state for %q: %w", projectorName, err)
		}
		return false, nil
	}

	if err := reset(ctx, tx); err != nil {
		return false, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE projector_state
		SET global_counter = 0, reset_requested_at = NULL, updated_at = now()
		WHERE projector_name = $1
	`, projectorName); err != nil {
		return false, fmt.Errorf("reset projector_state for %q: %w", projectorName, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit projector_state reset for %q: %w", projectorName, err)
	}
	return true, nil
}

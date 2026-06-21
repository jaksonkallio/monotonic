package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// Migrate creates the projector_state table if it does not exist.
// Call once during application startup before constructing any Projector against this backend.
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
	return nil
}

// GetState returns the resume counter for projectorName, or 0 if no row exists.
func (b *ProjectorBackend) GetState(ctx context.Context, projectorName string) (uint64, error) {
	var counter int64
	err := b.pool.QueryRow(ctx,
		`SELECT global_counter FROM projector_state WHERE projector_name = $1`,
		projectorName,
	).Scan(&counter)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("read projector_state for %q: %w", projectorName, err)
	}
	return uint64(counter), nil
}

// lockState ensures a projector_state row exists for projectorName and returns its current
// counter, having taken a row lock that is held until the caller's transaction ends. Concurrent
// callers (other instances of the same projector) block here until the lock holder commits or
// rolls back, then see its committed counter rather than a stale snapshot.
func lockState(ctx context.Context, tx pgx.Tx, projectorName string) (uint64, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO projector_state (projector_name, global_counter)
		VALUES ($1, 0)
		ON CONFLICT (projector_name) DO NOTHING
	`, projectorName); err != nil {
		return 0, fmt.Errorf("ensure projector_state row for %q: %w", projectorName, err)
	}

	var counter int64
	err := tx.QueryRow(ctx,
		`SELECT global_counter FROM projector_state WHERE projector_name = $1 FOR UPDATE`,
		projectorName,
	).Scan(&counter)
	if err != nil {
		return 0, fmt.Errorf("lock projector_state for %q: %w", projectorName, err)
	}
	return uint64(counter), nil
}

// RunEvent opens a tx, locks projector_state for projectorName, and skips apply entirely if
// counter has already been recorded (by this or another instance of the same projector racing
// concurrently). Otherwise it runs apply, advances projector_state to counter, and commits both
// atomically. The row lock serializes concurrent RunEvent/ResetState calls for the same
// projectorName, so two instances racing on the same event can never both run apply.
func (b *ProjectorBackend) RunEvent(
	ctx context.Context,
	projectorName string,
	counter uint64,
	apply func(ctx context.Context, tx pgx.Tx) error,
) error {
	if counter == 0 {
		return fmt.Errorf("projector_state counter must be > 0")
	}

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	current, err := lockState(ctx, tx, projectorName)
	if err != nil {
		return err
	}
	if current >= counter {
		// Already applied by this or another instance; nothing left to do.
		return tx.Commit(ctx)
	}

	if err := apply(ctx, tx); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx,
		`UPDATE projector_state SET global_counter = $2, updated_at = now() WHERE projector_name = $1`,
		projectorName, int64(counter),
	); err != nil {
		return fmt.Errorf("advance projector_state for %q: %w", projectorName, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit projector_state for %q: %w", projectorName, err)
	}
	return nil
}

// ResetState opens a tx, hands it to reset, sets projector_state back to 0, and commits.
func (b *ProjectorBackend) ResetState(
	ctx context.Context,
	projectorName string,
	reset func(ctx context.Context, tx pgx.Tx) error,
) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := reset(ctx, tx); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO projector_state (projector_name, global_counter, updated_at)
		VALUES ($1, 0, now())
		ON CONFLICT (projector_name) DO UPDATE
		SET global_counter = 0, updated_at = now()
	`, projectorName)
	if err != nil {
		return fmt.Errorf("reset projector_state for %q: %w", projectorName, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit projector_state reset for %q: %w", projectorName, err)
	}
	return nil
}

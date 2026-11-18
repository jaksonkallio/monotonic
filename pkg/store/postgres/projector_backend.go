package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// ProjectorBackend implements monotonic.ProjectorBackend[pgx.Tx] on top of Postgres,
// opening one transaction per Update and committing the handlers' writes
// atomically with the projector_state advance.
type ProjectorBackend struct {
	pool *pgxpool.Pool
}

var _ monotonic.ProjectorBackend[pgx.Tx] = (*ProjectorBackend)(nil)

// NewProjectorBackend creates a Postgres-backed monotonic.ProjectorBackend[pgx.Tx].
func NewProjectorBackend(pool *pgxpool.Pool) *ProjectorBackend {
	return &ProjectorBackend{pool: pool}
}

// Migrate creates the projector_state table if it does not exist.
// Call once during application startup before constructing any Projector against this backend.
func (b *ProjectorBackend) Migrate(ctx context.Context) error {
	_, err := b.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS projector_state (
			projector_name     TEXT NOT NULL PRIMARY KEY,
			global_counter     BIGINT NOT NULL,
			truncate_requested BOOLEAN NOT NULL DEFAULT FALSE,
			updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		ALTER TABLE projector_state ADD COLUMN IF NOT EXISTS truncate_requested BOOLEAN NOT NULL DEFAULT FALSE;
	`)
	if err != nil {
		return fmt.Errorf("migrate projector_state: %w", err)
	}
	return nil
}

// LockStateForModification opens a transaction, ensures a projector_state row exists for name, and locks it FOR UPDATE.
// The caller owns the returned transaction and must end it.
func (b *ProjectorBackend) LockStateForModification(ctx context.Context, name string, initialGlobalCounter uint64) (pgx.Tx, monotonic.ProjectorStateSnapshot, error) {
	// Make sure there's a projector state row for this projector, inserting with initialGlobalCounter if missing.
	if err := b.ensureState(ctx, name, initialGlobalCounter); err != nil {
		return nil, monotonic.ProjectorStateSnapshot{}, err
	}

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, monotonic.ProjectorStateSnapshot{}, fmt.Errorf("begin tx: %w", err)
	}

	snapshot, err := lockExistingState(ctx, tx, name)
	if err != nil {
		// The caller never saw this transaction, so returning without ending it would leak the connection.
		_ = tx.Rollback(ctx)
		return nil, monotonic.ProjectorStateSnapshot{}, err
	}

	return tx, snapshot, nil
}

// ensureState creates name's state row if it does not exist yet, which is the one moment initialGlobalCounter applies.
func (b *ProjectorBackend) ensureState(ctx context.Context, name string, initialGlobalCounter uint64) error {
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO projector_state (projector_name, global_counter)
		VALUES ($1, $2)
		ON CONFLICT (projector_name) DO NOTHING
	`, name, int64(initialGlobalCounter)); err != nil {
		return fmt.Errorf("ensure projector_state row for %q: %w", name, err)
	}
	return nil
}

// lockExistingState locks and reads name's state row inside an already-open transaction, so the
// error paths have a single place to roll back from.
func lockExistingState(ctx context.Context, tx pgx.Tx, name string) (monotonic.ProjectorStateSnapshot, error) {
	var globalCounter int64
	var truncateRequested bool
	if err := tx.QueryRow(ctx, `
		SELECT global_counter, truncate_requested
		FROM projector_state
		WHERE projector_name = $1
		FOR UPDATE
	`, name).Scan(&globalCounter, &truncateRequested); err != nil {
		return monotonic.ProjectorStateSnapshot{}, fmt.Errorf("lock projector_state for %q: %w", name, err)
	}

	return monotonic.ProjectorStateSnapshot{
		GlobalCounter:     uint64(globalCounter),
		TruncateRequested: truncateRequested,
	}, nil
}

// CommitTruncatedStateFromModification rewinds name to the start of the event stream, clears its
// truncate request, and commits the transaction along with the truncate handler's writes.
func (b *ProjectorBackend) CommitTruncatedStateFromModification(ctx context.Context, tx pgx.Tx, name string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE projector_state
		SET global_counter = 0, truncate_requested = false, updated_at = now()
		WHERE projector_name = $1
	`, name); err != nil {
		return fmt.Errorf("reset projector_state for %q: %w", name, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit truncated projector_state for %q: %w", name, err)
	}

	return nil
}

// CommitAdvancedStateFromModification advances name's resume counter and commits the transaction
// along with the handlers' writes. truncate_requested is left alone so a request that arrived
// mid-batch is still serviced by the next Update.
func (b *ProjectorBackend) CommitAdvancedStateFromModification(ctx context.Context, tx pgx.Tx, name string, advancedToGlobalCounter uint64) error {
	if _, err := tx.Exec(ctx, `
		UPDATE projector_state
		SET global_counter = $2, updated_at = now()
		WHERE projector_name = $1
	`, name, int64(advancedToGlobalCounter)); err != nil {
		return fmt.Errorf("advance projector_state for %q: %w", name, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit advanced projector_state for %q: %w", name, err)
	}

	return nil
}

// EnqueueTruncate asks name to rebuild from the start of the event stream on its next Update.
// It does not take the modification lock, so it returns as soon as the flag is set rather than
// blocking behind an in-flight batch.
func (b *ProjectorBackend) EnqueueTruncate(ctx context.Context, name string) error {
	// A projector that has never run has no row yet, and a bare UPDATE would silently do nothing.
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO projector_state (projector_name, global_counter, truncate_requested)
		VALUES ($1, 0, true)
		ON CONFLICT (projector_name) DO UPDATE SET truncate_requested = true, updated_at = now()
	`, name); err != nil {
		return fmt.Errorf("enqueue truncate for %q: %w", name, err)
	}
	return nil
}

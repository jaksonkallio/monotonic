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
			projector_name     TEXT NOT NULL PRIMARY KEY,
			global_counter     BIGINT NOT NULL,
			truncate_requested BOOLEAN NOT NULL DEFAULT FALSE,
			updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		ALTER TABLE projector_state ADD COLUMN IF NOT EXISTS queued_truncate BOOLEAN NOT NULL DEFAULT FALSE;
	`)
	if err != nil {
		return fmt.Errorf("migrate projector_state: %w", err)
	}
	return nil
}

type ProjectorStateSnapshot struct {
	globalCounter     int64
	truncateRequested bool
}

func (s ProjectorStateSnapshot) GlobalCounter() uint64 {
	return uint64(s.globalCounter)
}

func (s ProjectorStateSnapshot) TruncateRequested() bool {
	return s.truncateRequested
}

// LockStateForModification starts a transaction and locks the projector state row for the given projector name for update modification.
// It returns the transaction, which must be committed or rolled back by the caller.
func (b *ProjectorBackend) LockStateForModification(ctx context.Context, name string) (pgx.Tx, ProjectorStateSnapshot, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, ProjectorStateSnapshot{}, fmt.Errorf("begin tx: %w", err)
	}

	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s';`); err != nil {
		return nil, ProjectorStateSnapshot{}, fmt.Errorf("set lock timeout: %w", err)
	}

	var globalCounter int64
	var truncateRequested bool
	if err := tx.QueryRow(ctx,
		`SELECT global_counter, truncate_requested
		FROM projector_state
		WHERE projector_name = $1
		FOR UPDATE;`,
		name,
	).Scan(&globalCounter, &truncateRequested); err != nil {
		return nil, ProjectorStateSnapshot{}, fmt.Errorf("query projector state: %w", err)
	}

	return tx, ProjectorStateSnapshot{
		globalCounter:     globalCounter,
		truncateRequested: truncateRequested,
	}, nil
}

// CommitTruncatedStateFromModification resets the truncate flag and global counter for the given projector state, and commits the transaction.
func (b *ProjectorBackend) CommitTruncatedStateFromModification(ctx context.Context, tx pgx.Tx, name string) error {
	if _, err := tx.Exec(ctx,
		`UPDATE projector_state
		SET truncate_requested = false, global_counter = 0
		WHERE projector_name = $1`,
		name,
	); err != nil {
		return fmt.Errorf("reset truncate flag for projector state: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit truncated state for projector state: %w", err)
	}

	return nil
}

// CommitAdvancedStateFromModification advances the global counter for the given projector state, and commits the transaction.
func (b *ProjectorBackend) CommitAdvancedStateFromModification(ctx context.Context, tx pgx.Tx, name string, advancedToGlobalCounter uint64) error {
	if _, err := tx.Exec(ctx,
		`UPDATE projector_state
		SET global_counter = $2
		WHERE projector_name = $1`,
		name,
		advancedToGlobalCounter,
	); err != nil {
		return fmt.Errorf("advance global counter for projector state: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit advanced state for projector state: %w", err)
	}

	return nil
}

func (b *ProjectorBackend) EnqueueTruncate(ctx context.Context, name string) error {
	if _, err := b.pool.Exec(ctx,
		`UPDATE projector_state
		SET truncate_requested = true
		WHERE projector_name = $1`,
		name,
	); err != nil {
		return fmt.Errorf("enqueue truncate for projector state: %w", err)
	}
	return nil
}

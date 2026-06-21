package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// txCtxKey is the unexported context key used to carry a pgx.Tx through to a Handler.
type txCtxKey struct{}

// withTx returns a new ctx carrying tx; TxFromContext retrieves it.
func withTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txCtxKey{}, tx)
}

// TxFromContext returns the pgx.Tx the Postgres ProjectorBackend injected, if any.
// Handlers fetch it to do their writes inside the framework's atomic per-event transaction.
func TxFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txCtxKey{}).(pgx.Tx)
	return tx, ok
}

// TxHandler adapts a tx-aware function into a monotonic.Handler.
// Use this at handler registration so handler bodies can take a typed pgx.Tx
// without the dispatch core having to know about transactions.
func TxHandler(h func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error) monotonic.Handler {
	return func(ctx context.Context, event monotonic.AggregateEvent) error {
		tx, ok := TxFromContext(ctx)
		if !ok {
			return fmt.Errorf("postgres: handler invoked without tx in context")
		}
		return h(ctx, tx, event)
	}
}

// ProjectorBackend implements monotonic.ProjectorBackend on top of Postgres,
// opening a transaction per event and committing the handler's writes
// atomically with the projector_state advance.
type ProjectorBackend struct {
	pool *pgxpool.Pool
}

// NewProjectorBackend creates a Postgres-backed monotonic.ProjectorBackend.
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

// RunEvent opens a tx, injects it into ctx, runs apply, upserts projector_state, and commits.
// The upsert's WHERE clause keeps replays of an already-seen counter idempotent.
func (b *ProjectorBackend) RunEvent(
	ctx context.Context,
	projectorName string,
	counter uint64,
	apply func(ctx context.Context) error,
) error {
	if counter == 0 {
		return fmt.Errorf("projector_state counter must be > 0")
	}

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := apply(withTx(ctx, tx)); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO projector_state (projector_name, global_counter, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (projector_name) DO UPDATE
		SET global_counter = EXCLUDED.global_counter, updated_at = now()
		WHERE projector_state.global_counter <= EXCLUDED.global_counter
	`, projectorName, int64(counter))
	if err != nil {
		return fmt.Errorf("upsert projector_state for %q: %w", projectorName, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit projector_state for %q: %w", projectorName, err)
	}
	return nil
}

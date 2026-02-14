// Package postgres provides a Postgres-backed implementation of the monotonic.Store and monotonic.SagaStore interfaces.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// Store is a Postgres-backed implementation of the monotonic.SagaStore interface.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a new Postgres store using the provided connection pool.
// The caller is responsible for pool lifecycle.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Migrate creates the required tables and indexes if they do not already exist.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS events (
			aggregate_type TEXT NOT NULL,
			aggregate_id   TEXT NOT NULL,
			counter        BIGINT NOT NULL,
			global_counter BIGSERIAL PRIMARY KEY,
			event_type     TEXT NOT NULL,
			payload        JSONB,
			accepted_at    TIMESTAMPTZ NOT NULL,
			UNIQUE (aggregate_type, aggregate_id, counter)
		);

		CREATE INDEX IF NOT EXISTS idx_events_aggregate
			ON events (aggregate_type, aggregate_id, counter);

		CREATE INDEX IF NOT EXISTS idx_events_global
			ON events (aggregate_type, global_counter);

		CREATE TABLE IF NOT EXISTS sagas (
			saga_type TEXT NOT NULL,
			saga_id   TEXT NOT NULL,
			completed BOOLEAN NOT NULL DEFAULT FALSE,
			PRIMARY KEY (saga_type, saga_id)
		);
	`)
	return err
}

func (s *Store) LoadAggregateEvents(ctx context.Context, aggregateType, aggregateID string, afterCounter int64) ([]monotonic.AcceptedEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT counter, global_counter, event_type, payload, accepted_at
		 FROM events
		 WHERE aggregate_type = $1 AND aggregate_id = $2 AND counter > $3
		 ORDER BY counter`,
		aggregateType, aggregateID, afterCounter,
	)
	if err != nil {
		return nil, fmt.Errorf("load aggregate events: %w", err)
	}
	defer rows.Close()

	var events []monotonic.AcceptedEvent
	for rows.Next() {
		var e monotonic.AcceptedEvent
		var payload []byte
		if err := rows.Scan(&e.Counter, &e.GlobalCounter, &e.Type, &payload, &e.AcceptedAt); err != nil {
			return nil, fmt.Errorf("scan aggregate event: %w", err)
		}
		if payload != nil {
			e.Payload = json.RawMessage(payload)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *Store) Append(ctx context.Context, events ...monotonic.AggregateEvent) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Collect distinct aggregates to check saga completion
	type aggKey struct{ typ, id string }
	checked := make(map[aggKey]bool)

	for _, ae := range events {
		key := aggKey{ae.AggregateType, ae.AggregateID}
		if checked[key] {
			continue
		}
		checked[key] = true

		// Check saga completion with row-level lock
		var completed bool
		err := tx.QueryRow(ctx,
			`SELECT completed FROM sagas WHERE saga_type = $1 AND saga_id = $2 FOR UPDATE`,
			ae.AggregateType, ae.AggregateID,
		).Scan(&completed)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("check saga completion: %w", err)
		}
		if err == nil && completed {
			return fmt.Errorf("%w: %s/%s", monotonic.ErrSagaCompleted, ae.AggregateType, ae.AggregateID)
		}
	}

	// Insert all events; rely on UNIQUE constraint for counter conflicts
	for _, ae := range events {
		_, err := tx.Exec(ctx,
			`INSERT INTO events (aggregate_type, aggregate_id, counter, event_type, payload, accepted_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			ae.AggregateType, ae.AggregateID,
			ae.Event.Counter, ae.Event.Type,
			payloadBytes(ae.Event.Payload), ae.Event.AcceptedAt,
		)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				// Unique violation on (aggregate_type, aggregate_id, counter)
				expected, _ := currentMaxCounter(ctx, tx, ae.AggregateType, ae.AggregateID)
				return fmt.Errorf(
					"event counter mismatch for %s/%s: expected %d, got %d",
					ae.AggregateType, ae.AggregateID, expected+1, ae.Event.Counter,
				)
			}
			return fmt.Errorf("insert event: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) LoadGlobalEvents(ctx context.Context, aggregateTypes []string, afterGlobalCounter int64) ([]monotonic.AggregateEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT aggregate_type, aggregate_id, counter, global_counter, event_type, payload, accepted_at
		 FROM events
		 WHERE aggregate_type = ANY($1) AND global_counter > $2
		 ORDER BY global_counter`,
		aggregateTypes, afterGlobalCounter,
	)
	if err != nil {
		return nil, fmt.Errorf("load global events: %w", err)
	}
	defer rows.Close()

	var result []monotonic.AggregateEvent
	for rows.Next() {
		var ae monotonic.AggregateEvent
		var payload []byte
		if err := rows.Scan(
			&ae.AggregateType, &ae.AggregateID,
			&ae.Event.Counter, &ae.Event.GlobalCounter,
			&ae.Event.Type, &payload, &ae.Event.AcceptedAt,
		); err != nil {
			return nil, fmt.Errorf("scan global event: %w", err)
		}
		if payload != nil {
			ae.Event.Payload = json.RawMessage(payload)
		}
		result = append(result, ae)
	}
	return result, rows.Err()
}

func (s *Store) ListActiveSagas(ctx context.Context, sagaType string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT saga_id FROM sagas WHERE saga_type = $1 AND completed = FALSE`,
		sagaType,
	)
	if err != nil {
		return nil, fmt.Errorf("list active sagas: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan saga id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) MarkSagaCompleted(ctx context.Context, sagaType, sagaID string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO sagas (saga_type, saga_id, completed) VALUES ($1, $2, TRUE)
		 ON CONFLICT (saga_type, saga_id) DO UPDATE SET completed = TRUE`,
		sagaType, sagaID,
	)
	if err != nil {
		return fmt.Errorf("mark saga completed: %w", err)
	}
	return nil
}

// currentMaxCounter returns the current maximum counter for an aggregate, or 0 if none exist.
func currentMaxCounter(ctx context.Context, tx pgx.Tx, aggregateType, aggregateID string) (int64, error) {
	var maxCounter int64
	err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(counter), 0) FROM events WHERE aggregate_type = $1 AND aggregate_id = $2`,
		aggregateType, aggregateID,
	).Scan(&maxCounter)
	return maxCounter, err
}

// payloadBytes returns nil for empty/null payloads, otherwise the raw bytes.
func payloadBytes(p json.RawMessage) []byte {
	if len(p) == 0 {
		return nil
	}
	return []byte(p)
}

// Compile-time interface checks
var _ monotonic.Store = (*Store)(nil)
var _ monotonic.SagaStore = (*Store)(nil)

// IsSagaCompleted checks if a saga has been marked as completed.
func (s *Store) IsSagaCompleted(sagaType, sagaID string) (bool, error) {
	var completed bool
	err := s.pool.QueryRow(context.Background(),
		`SELECT completed FROM sagas WHERE saga_type = $1 AND saga_id = $2`,
		sagaType, sagaID,
	).Scan(&completed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check saga completed: %w", err)
	}
	return completed, nil
}

// Package postgres provides a Postgres-backed implementation of the monotonic.Store and monotonic.SagaStore interfaces.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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

		CREATE INDEX IF NOT EXISTS idx_events_saga_lifecycle
			ON events (aggregate_type, aggregate_id, event_type);

		CREATE OR REPLACE VIEW sagas AS
		SELECT
			e.aggregate_type AS saga_type,
			e.aggregate_id AS saga_id,
			EXISTS(
				SELECT 1 FROM events e2
				WHERE e2.aggregate_type = e.aggregate_type
				AND e2.aggregate_id = e.aggregate_id
				AND e2.event_type = 'saga-completed'
			) AS completed
		FROM events e
		WHERE e.event_type = 'saga-started';
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

	// Collect distinct aggregates to check saga completion and load current counters
	type aggKey struct{ typ, id string }
	checked := make(map[aggKey]bool)
	currentCounters := make(map[aggKey]int64)

	for _, ae := range events {
		key := aggKey{ae.AggregateType, ae.AggregateID}
		if checked[key] {
			continue
		}
		checked[key] = true

		// Check if saga is completed by looking for a saga-completed event
		var exists bool
		err := tx.QueryRow(ctx,
			`SELECT EXISTS(
				SELECT 1 FROM events
				WHERE aggregate_type = $1 AND aggregate_id = $2 AND event_type = 'saga-completed'
			)`,
			ae.AggregateType, ae.AggregateID,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("check saga completion: %w", err)
		}
		if exists {
			return fmt.Errorf("%w: %s/%s", monotonic.ErrSagaCompleted, ae.AggregateType, ae.AggregateID)
		}

		// Load current max counter for validation
		maxCounter, err := currentMaxCounter(ctx, tx, ae.AggregateType, ae.AggregateID)
		if err != nil {
			return fmt.Errorf("get current counter: %w", err)
		}
		currentCounters[key] = maxCounter
	}

	// Validate all event counters are sequential before inserting
	eventsInBatch := make(map[aggKey]int64)

	for _, ae := range events {
		key := aggKey{ae.AggregateType, ae.AggregateID}

		expectedCounter := currentCounters[key] + 1 + eventsInBatch[key]

		if ae.Event.Counter != expectedCounter {
			return fmt.Errorf(
				"event counter mismatch for %s/%s: expected %d, got %d",
				ae.AggregateType, ae.AggregateID, expectedCounter, ae.Event.Counter,
			)
		}

		eventsInBatch[key]++
	}

	// Insert all events (counters already validated above)
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
				// UNIQUE constraint violation (should be rare now that we pre-validate)
				// This can still happen due to concurrent transactions
				return fmt.Errorf(
					"event counter conflict for %s/%s counter %d (concurrent write detected)",
					ae.AggregateType, ae.AggregateID, ae.Event.Counter,
				)
			}
			return fmt.Errorf("insert event: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) LoadGlobalEvents(ctx context.Context, filters []monotonic.AggregateID, afterGlobalCounter int64) ([]monotonic.AggregateEvent, error) {
	// Build dynamic WHERE clause: each filter becomes a condition OR-ed together
	// $1 is always afterGlobalCounter
	args := []any{afterGlobalCounter}
	var conditions []string
	for _, f := range filters {
		if f.ID == "" {
			args = append(args, f.Type)
			conditions = append(conditions, fmt.Sprintf("aggregate_type = $%d", len(args)))
		} else {
			args = append(args, f.Type, f.ID)
			conditions = append(conditions, fmt.Sprintf("(aggregate_type = $%d AND aggregate_id = $%d)", len(args)-1, len(args)))
		}
	}

	filterClause := "FALSE"
	if len(conditions) > 0 {
		filterClause = strings.Join(conditions, " OR ")
	}

	query := fmt.Sprintf(
		`SELECT aggregate_type, aggregate_id, counter, global_counter, event_type, payload, accepted_at
		 FROM events
		 WHERE (%s) AND global_counter > $1
		 ORDER BY global_counter`, filterClause,
	)

	rows, err := s.pool.Query(ctx, query, args...)
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
	// No-op: saga completion is derived from the saga-completed event in the events table.
	// The sagas view automatically reflects this.
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

// IsSagaCompleted checks if a saga has been completed.
func (s *Store) IsSagaCompleted(sagaType, sagaID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(context.Background(),
		`SELECT EXISTS(
			SELECT 1 FROM events
			WHERE aggregate_type = $1 AND aggregate_id = $2 AND event_type = 'saga-completed'
		)`,
		sagaType, sagaID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check saga completed: %w", err)
	}
	return exists, nil
}

// Package postgres provides a Postgres-backed implementation of the monotonic.Store interface.
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

// Store is a Postgres-backed implementation of monotonic.Store.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a new Postgres store using the provided connection pool; the caller owns the pool's lifecycle.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Migrate creates the events table and supporting indexes if they do not already exist.
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

	type aggKey struct{ typ, id string }
	currentCounters := make(map[aggKey]int64)

	// Load each distinct aggregate's current max counter once.
	for _, ae := range events {
		key := aggKey{ae.AggregateType, ae.AggregateID}
		if _, ok := currentCounters[key]; ok {
			continue
		}
		maxCounter, err := currentMaxCounter(ctx, tx, ae.AggregateType, ae.AggregateID)
		if err != nil {
			return fmt.Errorf("get current counter: %w", err)
		}
		currentCounters[key] = maxCounter
	}

	// Validate all event counters are sequential before inserting.
	eventsInBatch := make(map[aggKey]int64)
	for _, ae := range events {
		key := aggKey{ae.AggregateType, ae.AggregateID}
		expectedCounter := currentCounters[key] + 1 + eventsInBatch[key]
		if ae.Event.Counter != expectedCounter {
			return fmt.Errorf(
				"%w for %s/%s: expected %d, got %d",
				monotonic.ErrCounterConflict, ae.AggregateType, ae.AggregateID, expectedCounter, ae.Event.Counter,
			)
		}
		eventsInBatch[key]++
	}

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
			// 23505 is UNIQUE constraint violation; rare given pre-validation, but possible under concurrent transactions.
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return fmt.Errorf(
					"%w for %s/%s counter %d (concurrent write detected)",
					monotonic.ErrCounterConflict, ae.AggregateType, ae.AggregateID, ae.Event.Counter,
				)
			}
			return fmt.Errorf("insert event: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) LoadGlobalEvents(ctx context.Context, filters []monotonic.EventFilter, afterGlobalCounter int64, limit int) ([]monotonic.AggregateEvent, error) {
	// $1 is always afterGlobalCounter; each filter becomes one OR-ed clause built from its non-empty fields AND-ed together.
	args := []any{afterGlobalCounter}
	var conditions []string
	for _, f := range filters {
		var parts []string
		if f.AggregateType != "" {
			args = append(args, f.AggregateType)
			parts = append(parts, fmt.Sprintf("aggregate_type = $%d", len(args)))
		}
		if f.AggregateID != "" {
			args = append(args, f.AggregateID)
			parts = append(parts, fmt.Sprintf("aggregate_id = $%d", len(args)))
		}
		if f.EventType != "" {
			args = append(args, f.EventType)
			parts = append(parts, fmt.Sprintf("event_type = $%d", len(args)))
		}
		if len(parts) == 0 {
			// An empty EventFilter has no constraints, so it matches every event.
			conditions = append(conditions, "TRUE")
			continue
		}
		conditions = append(conditions, "("+strings.Join(parts, " AND ")+")")
	}

	filterClause := "FALSE"
	if len(conditions) > 0 {
		filterClause = strings.Join(conditions, " OR ")
	}

	limitClause := ""
	if limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", limit)
	}

	query := fmt.Sprintf(
		`SELECT aggregate_type, aggregate_id, counter, global_counter, event_type, payload, accepted_at
		 FROM events
		 WHERE (%s) AND global_counter > $1
		 ORDER BY global_counter%s`, filterClause, limitClause,
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

var _ monotonic.Store = (*Store)(nil)

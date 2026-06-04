package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type eventRow struct {
	GlobalCounter int64
	AcceptedAt    time.Time
	AggregateType string
	AggregateID   string
	Counter       int64
	EventType     string
	Payload       json.RawMessage
}

type aggTypeRow struct {
	AggregateType  string
	EventCount     int64
	AggregateCount int64
}

type aggIDRow struct {
	AggregateID string
	EventCount  int64
	LastEventAt time.Time
	MaxCounter  int64
}

type projectionInfo struct {
	TableName string
	RowCount  int64
	MaxGC     int64
	Lag       int64
}

type projectionColumn struct {
	Name     string
	DataType string
}

type projectionRow struct {
	Values map[string]string
}

type db struct {
	pool *pgxpool.Pool
}

func (d *db) globalMaxCounter(ctx context.Context) (int64, error) {
	var n int64
	err := d.pool.QueryRow(ctx, `SELECT COALESCE(MAX(global_counter), 0) FROM events`).Scan(&n)
	return n, err
}

func (d *db) listEvents(ctx context.Context, aggType, aggID, eventType string, beforeGC int64, limit int) ([]eventRow, error) {
	q := `
		SELECT global_counter, accepted_at, aggregate_type, aggregate_id, counter, event_type, payload
		FROM events
		WHERE ($1 = '' OR aggregate_type = $1)
		  AND ($2 = '' OR aggregate_id = $2)
		  AND ($3 = '' OR event_type = $3)
		  AND ($4 = 0 OR global_counter < $4)
		ORDER BY global_counter DESC
		LIMIT $5`

	rows, err := d.pool.Query(ctx, q, aggType, aggID, eventType, beforeGC, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []eventRow
	for rows.Next() {
		var e eventRow
		if err := rows.Scan(&e.GlobalCounter, &e.AcceptedAt, &e.AggregateType, &e.AggregateID, &e.Counter, &e.EventType, &e.Payload); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (d *db) listEventsAfter(ctx context.Context, afterGC int64) ([]eventRow, error) {
	q := `
		SELECT global_counter, accepted_at, aggregate_type, aggregate_id, counter, event_type, payload
		FROM events
		WHERE global_counter > $1
		ORDER BY global_counter ASC
		LIMIT 100`

	rows, err := d.pool.Query(ctx, q, afterGC)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []eventRow
	for rows.Next() {
		var e eventRow
		if err := rows.Scan(&e.GlobalCounter, &e.AcceptedAt, &e.AggregateType, &e.AggregateID, &e.Counter, &e.EventType, &e.Payload); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (d *db) listAggregateTypes(ctx context.Context) ([]aggTypeRow, error) {
	q := `
		SELECT aggregate_type, COUNT(*) as event_count, COUNT(DISTINCT aggregate_id) as agg_count
		FROM events
		GROUP BY aggregate_type
		ORDER BY aggregate_type`

	rows, err := d.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var types []aggTypeRow
	for rows.Next() {
		var t aggTypeRow
		if err := rows.Scan(&t.AggregateType, &t.EventCount, &t.AggregateCount); err != nil {
			return nil, err
		}
		types = append(types, t)
	}
	return types, rows.Err()
}

func (d *db) listAggregateIDs(ctx context.Context, aggType string) ([]aggIDRow, error) {
	q := `
		SELECT aggregate_id, COUNT(*) as event_count, MAX(accepted_at) as last_event_at, MAX(counter) as max_counter
		FROM events
		WHERE aggregate_type = $1
		GROUP BY aggregate_id
		ORDER BY last_event_at DESC
		LIMIT 500`

	rows, err := d.pool.Query(ctx, q, aggType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []aggIDRow
	for rows.Next() {
		var r aggIDRow
		if err := rows.Scan(&r.AggregateID, &r.EventCount, &r.LastEventAt, &r.MaxCounter); err != nil {
			return nil, err
		}
		ids = append(ids, r)
	}
	return ids, rows.Err()
}

func (d *db) listAggregateEvents(ctx context.Context, aggType, aggID string) ([]eventRow, error) {
	q := `
		SELECT global_counter, accepted_at, aggregate_type, aggregate_id, counter, event_type, payload
		FROM events
		WHERE aggregate_type = $1 AND aggregate_id = $2
		ORDER BY counter ASC`

	rows, err := d.pool.Query(ctx, q, aggType, aggID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []eventRow
	for rows.Next() {
		var e eventRow
		if err := rows.Scan(&e.GlobalCounter, &e.AcceptedAt, &e.AggregateType, &e.AggregateID, &e.Counter, &e.EventType, &e.Payload); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (d *db) discoverProjections(ctx context.Context) ([]projectionInfo, error) {
	q := `
		SELECT t.table_name
		FROM information_schema.tables t
		WHERE t.table_schema = 'public'
		  AND t.table_type = 'BASE TABLE'
		  AND EXISTS (
		    SELECT 1 FROM information_schema.columns c
		    WHERE c.table_schema = 'public' AND c.table_name = t.table_name AND c.column_name = 'projection_key'
		  )
		  AND EXISTS (
		    SELECT 1 FROM information_schema.columns c
		    WHERE c.table_schema = 'public' AND c.table_name = t.table_name AND c.column_name = 'global_counter'
		  )
		ORDER BY t.table_name`

	rows, err := d.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	globalMax, err := d.globalMaxCounter(ctx)
	if err != nil {
		return nil, err
	}

	var projections []projectionInfo
	for _, name := range tableNames {
		var rowCount, maxGC int64
		// table name comes from information_schema (not user input), safe to interpolate
		err := d.pool.QueryRow(ctx, fmt.Sprintf(
			`SELECT COUNT(*), COALESCE(MAX(global_counter), 0) FROM %q`, name,
		)).Scan(&rowCount, &maxGC)
		if err != nil {
			continue
		}
		projections = append(projections, projectionInfo{
			TableName: name,
			RowCount:  rowCount,
			MaxGC:     maxGC,
			Lag:       globalMax - maxGC,
		})
	}
	return projections, nil
}

func (d *db) getProjectionColumns(ctx context.Context, tableName string) ([]projectionColumn, error) {
	q := `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position`

	rows, err := d.pool.Query(ctx, q, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []projectionColumn
	for rows.Next() {
		var c projectionColumn
		if err := rows.Scan(&c.Name, &c.DataType); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func (d *db) listProjectionRows(ctx context.Context, tableName string, cols []projectionColumn) ([]projectionRow, error) {
	// table name from information_schema, not user input
	q := fmt.Sprintf(`SELECT * FROM %q ORDER BY global_counter DESC LIMIT 500`, tableName)

	rows, err := d.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []projectionRow
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		pr := projectionRow{Values: make(map[string]string, len(cols))}
		for i, col := range cols {
			if vals[i] == nil {
				pr.Values[col.Name] = "null"
			} else {
				pr.Values[col.Name] = fmt.Sprintf("%v", vals[i])
			}
		}
		result = append(result, pr)
	}
	return result, rows.Err()
}

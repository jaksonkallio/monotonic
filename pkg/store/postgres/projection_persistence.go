package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// ProjectionPersistence is a Postgres-backed implementation of monotonic.ProjectionPersistence.
//
// Each instance manages a single table with this shape:
//
//	CREATE TABLE <table> (
//	    projection_key TEXT PRIMARY KEY,
//	    global_counter BIGINT NOT NULL,
//	    -- one column per key returned by V.Fields()
//	);
//
// The caller is responsible for creating the table — column types depend on the
// pgtype.Value implementations the value type uses, which this package does not
// attempt to infer.
type ProjectionPersistence[V monotonic.ProjectionValue] struct {
	pool      *pgxpool.Pool
	tableName string
	newValue  func() V
	columns   []string
}

// NewProjectionPersistence creates a Postgres-backed projection persistence for values of type V.
// newValue is invoked during Get to produce a value whose Fields() map is populated from
// the row, and once at construction time to learn the column set.
func NewProjectionPersistence[V monotonic.ProjectionValue](
	pool *pgxpool.Pool,
	tableName string,
	newValue func() V,
) *ProjectionPersistence[V] {
	sample := newValue()
	fields := sample.Fields()
	cols := make([]string, 0, len(fields))
	for col := range fields {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	return &ProjectionPersistence[V]{
		pool:      pool,
		tableName: tableName,
		newValue:  newValue,
		columns:   cols,
	}
}

// Get returns the projection value and its global counter for the given key.
// If no row exists, returns the zero value of V, counter 0, and nil error.
func (p *ProjectionPersistence[V]) Get(ctx context.Context, key monotonic.ProjectionKey) (V, uint64, error) {
	var zero V

	selectCols := append([]string{"global_counter"}, p.columns...)
	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE projection_key = $1`,
		strings.Join(quoteIdents(selectCols), ", "),
		quoteIdent(p.tableName),
	)

	rows, err := p.pool.Query(ctx, query, string(key))
	if err != nil {
		return zero, 0, fmt.Errorf("query projection %q: %w", key, err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return zero, 0, fmt.Errorf("query projection %q: %w", key, err)
		}
		return zero, 0, nil
	}

	rawValues, err := rows.Values()
	if err != nil {
		return zero, 0, fmt.Errorf("read projection row %q: %w", key, err)
	}

	globalCounter, ok := rawValues[0].(int64)
	if !ok {
		return zero, 0, fmt.Errorf("global_counter has unexpected type %T", rawValues[0])
	}

	value := p.newValue()
	fields := value.Fields()
	for i, col := range p.columns {
		pgVal, ok := fields[col]
		if !ok {
			return zero, 0, fmt.Errorf("projection value missing field %q", col)
		}
		if err := pgVal.Set(rawValues[i+1]); err != nil {
			return zero, 0, fmt.Errorf("decode field %q: %w", col, err)
		}
	}

	return value, uint64(globalCounter), nil
}

// Set upserts the projection value for the given key, but only if the provided
// globalCounter is strictly greater than the existing one. If a row exists with
// a counter >= globalCounter, returns monotonic.ErrProjectionStale.
func (p *ProjectionPersistence[V]) Set(ctx context.Context, key monotonic.ProjectionKey, value V, globalCounter uint64) error {
	if globalCounter == 0 {
		return fmt.Errorf("globalCounter must be > 0")
	}

	fields := value.Fields()

	insertCols := append([]string{"projection_key", "global_counter"}, p.columns...)
	args := make([]any, 0, len(insertCols))
	args = append(args, string(key), int64(globalCounter))

	for _, col := range p.columns {
		pgVal, ok := fields[col]
		if !ok {
			return fmt.Errorf("projection value missing field %q", col)
		}
		raw := pgVal.Get()
		if raw == pgtype.Undefined {
			return fmt.Errorf("projection field %q is undefined", col)
		}
		args = append(args, raw)
	}

	placeholders := make([]string, len(insertCols))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	setClauses := make([]string, 0, len(p.columns)+1)
	setClauses = append(setClauses, "global_counter = EXCLUDED.global_counter")
	for _, col := range p.columns {
		qc := quoteIdent(col)
		setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", qc, qc))
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
		 ON CONFLICT (projection_key) DO UPDATE SET %s
		 WHERE %s.global_counter < EXCLUDED.global_counter`,
		quoteIdent(p.tableName),
		strings.Join(quoteIdents(insertCols), ", "),
		strings.Join(placeholders, ", "),
		strings.Join(setClauses, ", "),
		quoteIdent(p.tableName),
	)

	tag, err := p.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("upsert projection %q: %w", key, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: key=%q counter=%d", monotonic.ErrProjectionStale, key, globalCounter)
	}

	return nil
}

// LatestGlobalCounter returns the highest global_counter recorded across all rows
// in the projection table, or 0 if the table is empty. For this to be cheap on
// large tables, the caller should ensure an index exists on global_counter.
func (p *ProjectionPersistence[V]) LatestGlobalCounter(ctx context.Context) (uint64, error) {
	var counter int64
	query := fmt.Sprintf(
		`SELECT COALESCE(MAX(global_counter), 0) FROM %s`,
		quoteIdent(p.tableName),
	)
	if err := p.pool.QueryRow(ctx, query).Scan(&counter); err != nil {
		return 0, fmt.Errorf("read latest global counter: %w", err)
	}
	return uint64(counter), nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteIdents(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = quoteIdent(n)
	}
	return out
}

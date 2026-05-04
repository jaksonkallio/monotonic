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

// ProjectionPersistence stores projection rows in a Postgres table; callers must create the table with columns: projection_key TEXT PRIMARY KEY, global_counter BIGINT NOT NULL, plus one column per V.Fields() key.
type ProjectionPersistence[V monotonic.ProjectionValue] struct {
	// pool is the connection pool; lifecycle is the caller's responsibility.
	pool *pgxpool.Pool
	// tableName is the Postgres table backing this projection.
	tableName string
	// newValue produces an empty V; called once at construction and again on each Get to scan into.
	newValue func() V
	// columns are the V.Fields() keys in deterministic (sorted) order for stable SQL.
	columns []string
}

// NewProjectionPersistence creates a Postgres-backed projection persistence for values of type V.
func NewProjectionPersistence[V monotonic.ProjectionValue](
	pool *pgxpool.Pool,
	tableName string,
	newValue func() V,
) *ProjectionPersistence[V] {
	// Sample once to learn the column set; sorted for stable SQL ordering.
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

// Get returns the projection value and counter for key, or (zero V, 0, nil) when no row exists.
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

	// rows.Values() returns pgx-decoded native Go values; we then forward each to its pgtype.Value via Set.
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

// Set atomically upserts the batch in one transaction; returns ErrProjectionStale if any key's stored counter exceeds globalCounter.
func (p *ProjectionPersistence[V]) Set(ctx context.Context, projecteds []monotonic.Projected[V], globalCounter uint64) error {
	if len(projecteds) == 0 {
		return nil
	}
	// globalCounter == 0 is reserved as the not-found sentinel in ProjectionReader.Get.
	if globalCounter == 0 {
		return fmt.Errorf("globalCounter must be > 0")
	}

	insertCols := append([]string{"projection_key", "global_counter"}, p.columns...)
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

	// `<=` keeps equal-counter writes idempotent (redundant rewrite) so retries and direct replays converge.
	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
		 ON CONFLICT (projection_key) DO UPDATE SET %s
		 WHERE %s.global_counter <= EXCLUDED.global_counter`,
		quoteIdent(p.tableName),
		strings.Join(quoteIdents(insertCols), ", "),
		strings.Join(placeholders, ", "),
		strings.Join(setClauses, ", "),
		quoteIdent(p.tableName),
	)

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, pj := range projecteds {
		args, err := p.argsForRow(pj.Key, pj.Value, globalCounter)
		if err != nil {
			return err
		}

		tag, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("upsert projection %q: %w", pj.Key, err)
		}
		// RowsAffected == 0 means the WHERE rejected the update, i.e. existing counter > globalCounter.
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: key=%q counter=%d", monotonic.ErrProjectionStale, pj.Key, globalCounter)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit projection batch: %w", err)
	}
	return nil
}

func (p *ProjectionPersistence[V]) argsForRow(key monotonic.ProjectionKey, value V, globalCounter uint64) ([]any, error) {
	fields := value.Fields()
	args := make([]any, 0, 2+len(p.columns))
	args = append(args, string(key), int64(globalCounter))
	for _, col := range p.columns {
		pgVal, ok := fields[col]
		if !ok {
			return nil, fmt.Errorf("projection value for %q missing field %q", key, col)
		}
		raw := pgVal.Get()
		if raw == pgtype.Undefined {
			return nil, fmt.Errorf("projection value for %q has undefined field %q", key, col)
		}
		args = append(args, raw)
	}
	return args, nil
}

// LatestGlobalCounter returns MAX(global_counter) across the projection table, or 0 if empty; callers should index global_counter for this to be cheap at scale.
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

package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// fieldInfo describes one tagged struct field used as a projection column.
type fieldInfo struct {
	// column is the Postgres column name from the field's `proj` tag.
	column string
	// index is the field's position in the struct, used by reflect.Value.Field.
	index int
}

// ProjectionPersistence stores projection rows in a Postgres table; V must be a struct whose exported fields are tagged `proj:"column_name"` to describe the projected columns.
type ProjectionPersistence[V any] struct {
	// pool is the connection pool; lifecycle is the caller's responsibility.
	pool *pgxpool.Pool
	// tableName is the Postgres table backing this projection.
	tableName string
	// fields are the tagged columns of V, sorted by column name for stable SQL.
	fields []fieldInfo
}

// NewProjectionPersistence creates a Postgres-backed projection persistence for V; returns an error if V is not a struct or has no `proj`-tagged exported fields.
func NewProjectionPersistence[V any](pool *pgxpool.Pool, tableName string) (*ProjectionPersistence[V], error) {
	var zero V
	t := reflect.TypeOf(zero)
	// nil reflect.Type means V was an interface or untyped nil; either way, not a struct.
	if t == nil || t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("ProjectionPersistence: V must be a struct, got %T", zero)
	}

	var fields []fieldInfo
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("proj")
		if tag == "" || tag == "-" {
			continue
		}
		if !f.IsExported() {
			return nil, fmt.Errorf("ProjectionPersistence: field %q has proj tag but is unexported", f.Name)
		}
		fields = append(fields, fieldInfo{column: tag, index: i})
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("ProjectionPersistence: %s has no proj-tagged fields", t.Name())
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].column < fields[j].column })

	return &ProjectionPersistence[V]{
		pool:      pool,
		tableName: tableName,
		fields:    fields,
	}, nil
}

// Get returns the projection value for key, or (zero V, nil) when no row exists.
func (p *ProjectionPersistence[V]) Get(ctx context.Context, key monotonic.ProjectionKey) (V, error) {
	var value V

	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE projection_key = $1`,
		strings.Join(quoteIdents(p.columnNames()), ", "),
		quoteIdent(p.tableName),
	)

	// Address &value so reflect.Value.Field returns addressable Values that pgx can scan into.
	valueElem := reflect.ValueOf(&value).Elem()

	dests := make([]any, 0, len(p.fields))
	for _, fi := range p.fields {
		dests = append(dests, valueElem.Field(fi.index).Addr().Interface())
	}

	err := p.pool.QueryRow(ctx, query, string(key)).Scan(dests...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return value, nil
		}
		return value, fmt.Errorf("query projection %q: %w", key, err)
	}

	return value, nil
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

	insertCols := append([]string{"projection_key", "global_counter"}, p.columnNames()...)
	placeholders := make([]string, len(insertCols))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	setClauses := make([]string, 0, len(p.fields)+1)
	setClauses = append(setClauses, "global_counter = EXCLUDED.global_counter")
	for _, fi := range p.fields {
		qc := quoteIdent(fi.column)
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
		args := p.argsForRow(pj.Key, pj.Value, globalCounter)
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

func (p *ProjectionPersistence[V]) argsForRow(key monotonic.ProjectionKey, value V, globalCounter uint64) []any {
	valueRef := reflect.ValueOf(value)
	args := make([]any, 0, 2+len(p.fields))
	args = append(args, string(key), int64(globalCounter))
	for _, fi := range p.fields {
		args = append(args, valueRef.Field(fi.index).Interface())
	}
	return args
}

// Migrate creates the projection table and a global_counter index if they do not already exist.
// Call once during application startup before using Get, Set, or LatestGlobalCounter.
func (p *ProjectionPersistence[V]) Migrate(ctx context.Context) error {
	var zero V
	t := reflect.TypeOf(zero)

	colDefs := make([]string, 0, 2+len(p.fields))
	colDefs = append(colDefs,
		`"projection_key" TEXT NOT NULL PRIMARY KEY`,
		`"global_counter" BIGINT NOT NULL`,
	)
	for _, fi := range p.fields {
		f := t.Field(fi.index)
		pgType, err := goTypeToPostgres(f.Type)
		if err != nil {
			return fmt.Errorf("migrate: field %q: %w", f.Name, err)
		}
		colDefs = append(colDefs, fmt.Sprintf("%s %s NOT NULL", quoteIdent(fi.column), pgType))
	}

	ddl := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s);\n"+
			"CREATE INDEX IF NOT EXISTS %s ON %s (global_counter)",
		quoteIdent(p.tableName),
		strings.Join(colDefs, ", "),
		quoteIdent("idx_"+p.tableName+"_gc"),
		quoteIdent(p.tableName),
	)
	if _, err := p.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("migrate projection table %q: %w", p.tableName, err)
	}
	return nil
}

// goTypeToPostgres maps a Go reflect.Type to the matching Postgres column type.
// Only the primitive types that pgx natively encodes are supported; everything else is an error.
func goTypeToPostgres(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.String:
		return "TEXT", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "BIGINT", nil
	case reflect.Float32:
		return "REAL", nil
	case reflect.Float64:
		return "DOUBLE PRECISION", nil
	case reflect.Bool:
		return "BOOLEAN", nil
	default:
		return "", fmt.Errorf("unsupported Go type %s; use string, integer, float, or bool fields", t.Kind())
	}
}

func (p *ProjectionPersistence[V]) columnNames() []string {
	out := make([]string, len(p.fields))
	for i, f := range p.fields {
		out[i] = f.column
	}
	return out
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

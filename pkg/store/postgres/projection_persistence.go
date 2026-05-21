package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

var (
	timeType           = reflect.TypeFor[time.Time]()
	jsonRawMessageType = reflect.TypeFor[json.RawMessage]()
)

// fieldInfo describes one tagged struct field used as a projection column.
type fieldInfo struct {
	// column is the Postgres column name from the field's `proj` tag.
	column string
	// index is the field's position in the struct, used by reflect.Value.Field.
	index int
	// isJSON is true for json.RawMessage fields, which are stored as JSONB.
	isJSON bool
}

// ProjectionPersistence stores projection rows in a Postgres table; V must be a struct whose exported fields are tagged `proj:"column_name"` to describe the projected columns.
type ProjectionPersistence[V any] struct {
	// pool is the connection pool; lifecycle is the caller's responsibility.
	pool *pgxpool.Pool
	// tableName is the Postgres table backing this projection.
	tableName string
	// fields are the tagged columns of V, sorted by column name for stable SQL.
	fields []fieldInfo
	// allowMigrationRebuild, when true, causes Migrate to drop and recreate the table if the existing schema does not match the expected columns and types.
	allowMigrationRebuild bool
}

// NewProjectionPersistence creates a Postgres-backed projection persistence for V; returns an error if V is not a struct or has no `proj`-tagged exported fields.
// When allowMigrationRebuild is true, Migrate will drop and recreate the table if the existing schema does not match the expected columns and types.
func NewProjectionPersistence[V any](pool *pgxpool.Pool, tableName string, allowMigrationRebuild bool) (*ProjectionPersistence[V], error) {
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
		if _, err := goTypeToPostgres(f.Type); err != nil {
			return nil, fmt.Errorf("ProjectionPersistence: field %q: %w", f.Name, err)
		}
		fields = append(fields, fieldInfo{column: tag, index: i, isJSON: f.Type == jsonRawMessageType})
	}
	seen := make(map[string]string, len(fields)) // column → first field name
	for _, fi := range fields {
		first := t.Field(fi.index).Name
		if prev, ok := seen[fi.column]; ok {
			return nil, fmt.Errorf("ProjectionPersistence: duplicate proj tag %q on fields %q and %q", fi.column, prev, first)
		}
		seen[fi.column] = first
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("ProjectionPersistence: %s has no proj-tagged fields", t.Name())
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].column < fields[j].column })

	return &ProjectionPersistence[V]{
		pool:                  pool,
		tableName:             tableName,
		fields:                fields,
		allowMigrationRebuild: allowMigrationRebuild,
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
	// First two placeholders (projection_key, global_counter) are always untyped.
	placeholders := []string{"$1", "$2"}
	for i, fi := range p.fields {
		ph := fmt.Sprintf("$%d", i+3)
		if fi.isJSON {
			ph += "::jsonb"
		}
		placeholders = append(placeholders, ph)
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

// Truncate removes all rows from the projection table.
func (p *ProjectionPersistence[V]) Truncate(ctx context.Context) error {
	query := fmt.Sprintf(`TRUNCATE %s`, quoteIdent(p.tableName))
	if _, err := p.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("truncate projection table %q: %w", p.tableName, err)
	}
	return nil
}

func (p *ProjectionPersistence[V]) argsForRow(key monotonic.ProjectionKey, value V, globalCounter uint64) []any {
	valueRef := reflect.ValueOf(value)
	args := make([]any, 0, 2+len(p.fields))
	args = append(args, string(key), int64(globalCounter))
	for _, fi := range p.fields {
		fv := valueRef.Field(fi.index)
		if fi.isJSON {
			// Pass as string so the ::jsonb placeholder cast is applied correctly.
			// Default nil/empty to "{}" so the NOT NULL JSONB column always receives valid JSON.
			rm := fv.Interface().(json.RawMessage)
			s := string(rm)
			if s == "" {
				s = "{}"
			}
			args = append(args, s)
		} else {
			args = append(args, fv.Interface())
		}
	}
	return args
}

// Migrate creates the projection table and a global_counter index if they do not already exist.
// When allowMigrationRebuild is true, the existing table is dropped and recreated if its columns
// or types do not match the expected schema derived from V.
// Call once during application startup before using Get, Set, or LatestGlobalCounter.
func (p *ProjectionPersistence[V]) Migrate(ctx context.Context) error {
	var zero V
	t := reflect.TypeOf(zero)

	// Build the expected column type map (includes the two fixed columns).
	expectedCols := map[string]string{
		"projection_key": "TEXT",
		"global_counter": "BIGINT",
	}
	for _, fi := range p.fields {
		f := t.Field(fi.index)
		pgType, err := goTypeToPostgres(f.Type)
		if err != nil {
			return fmt.Errorf("migrate: field %q: %w", f.Name, err)
		}
		expectedCols[fi.column] = pgType
	}

	schemaChanged, err := p.schemaChanged(ctx, expectedCols)
	if err != nil {
		return err
	}
	if schemaChanged {
		if !p.allowMigrationRebuild {
			return fmt.Errorf("projection table %q schema does not match expected columns/types; set allowMigrationRebuild to automatically drop and recreate", p.tableName)
		}
		if err := p.dropTable(ctx); err != nil {
			return err
		}
	}

	colDefs := make([]string, 0, 2+len(p.fields))
	colDefs = append(colDefs,
		`"projection_key" TEXT NOT NULL PRIMARY KEY`,
		`"global_counter" BIGINT NOT NULL`,
	)
	for _, fi := range p.fields {
		colDefs = append(colDefs, fmt.Sprintf("%s %s NOT NULL", quoteIdent(fi.column), expectedCols[fi.column]))
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

// schemaChanged returns true when the existing table's columns or types do not match expectedCols.
// Returns false if the table does not exist yet (no columns found).
func (p *ProjectionPersistence[V]) schemaChanged(ctx context.Context, expectedCols map[string]string) (bool, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT column_name, UPPER(data_type) FROM information_schema.columns
		 WHERE table_name = $1 AND table_schema = 'public'`,
		p.tableName,
	)
	if err != nil {
		return false, fmt.Errorf("query schema for rebuild check on %q: %w", p.tableName, err)
	}
	defer rows.Close()

	actualCols := make(map[string]string)
	for rows.Next() {
		var colName, dataType string
		if err := rows.Scan(&colName, &dataType); err != nil {
			return false, fmt.Errorf("scan schema column for %q: %w", p.tableName, err)
		}
		actualCols[colName] = normalizeDataType(dataType)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate schema columns for %q: %w", p.tableName, err)
	}

	// Table does not exist yet.
	if len(actualCols) == 0 {
		return false, nil
	}

	if len(actualCols) != len(expectedCols) {
		return true, nil
	}
	for col, expectedType := range expectedCols {
		if actualType, ok := actualCols[col]; !ok || actualType != normalizeDataType(expectedType) {
			return true, nil
		}
	}
	return false, nil
}

// dropTable drops the projection table and its global_counter index.
func (p *ProjectionPersistence[V]) dropTable(ctx context.Context) error {
	drop := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s; DROP INDEX IF EXISTS %s",
		quoteIdent(p.tableName),
		quoteIdent("idx_"+p.tableName+"_gc"),
	)
	if _, err := p.pool.Exec(ctx, drop); err != nil {
		return fmt.Errorf("drop projection table %q for rebuild: %w", p.tableName, err)
	}
	return nil
}

// normalizeDataType maps information_schema data_type strings to the type names used in our DDL
// so comparisons are consistent.
func normalizeDataType(dt string) string {
	switch dt {
	case "CHARACTER VARYING":
		return "TEXT"
	case "DOUBLE PRECISION":
		return "DOUBLE PRECISION"
	case "REAL":
		return "REAL"
	case "BIGINT", "INTEGER", "SMALLINT":
		return "BIGINT"
	case "BOOLEAN":
		return "BOOLEAN"
	case "TEXT":
		return "TEXT"
	case "BYTEA":
		return "BYTEA"
	case "JSONB":
		return "JSONB"
	case "TIMESTAMP WITH TIME ZONE":
		return "TIMESTAMPTZ"
	default:
		return dt
	}
}

// goTypeToPostgres maps a Go reflect.Type to the matching Postgres column type.
// Supported types: string, int*, uint*, float32, float64, bool, time.Time, []byte, json.RawMessage.
func goTypeToPostgres(t reflect.Type) (string, error) {
	if t == timeType {
		return "TIMESTAMPTZ", nil
	}
	if t == jsonRawMessageType {
		return "JSONB", nil
	}
	// []byte after json.RawMessage so the named type is caught above.
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return "BYTEA", nil
	}
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
		return "", fmt.Errorf("unsupported Go type %s (%s); supported types: string, int*, uint*, float32, float64, bool, time.Time, []byte, json.RawMessage", t, t.Kind())
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

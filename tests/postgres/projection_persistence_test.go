package postgres_integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
	pgstore "github.com/jaksonkallio/monotonic/pkg/store/postgres"
)

// projRow is a simple two-column projection struct used across these tests.
type projRow struct {
	Name  string `proj:"name"`
	Score int64  `proj:"score"`
}

// safeTableName converts a test name into a valid Postgres identifier (≤ 63 bytes).
// The table name is "proj_" + safe (≤ 56 bytes total), which keeps the derived
// index name "idx_" + tableName + "_gc" within the 63-byte limit.
// When the safe portion exceeds 51 bytes it is truncated to 42 bytes and a
// "_xxxxxxxx" fnv32 hash suffix is appended, preventing collisions between
// test names that share a long common prefix.
var nonAlphaNum = regexp.MustCompile(`[^a-zA-Z0-9]`)

func safeTableName(testName string) string {
	safe := strings.ToLower(nonAlphaNum.ReplaceAllString(testName, "_"))
	// "proj_" prefix (5) + safe must fit in 63 bytes; index adds "idx_"+"_gc" (7 more),
	// so safe is capped at 51. Hash suffix uses 9 of those 51 bytes (1 + 8 hex digits).
	const maxSafe = 51
	if len(safe) > maxSafe {
		h := fnv.New32a()
		h.Write([]byte(safe))
		safe = fmt.Sprintf("%s_%08x", safe[:maxSafe-9], h.Sum32())
	}
	return "proj_" + safe
}

// testProjPersistence creates a fresh, migrated ProjectionPersistence for type V and
// registers a cleanup to drop the table when the test finishes.
func testProjPersistence[V any](t *testing.T) *pgstore.ProjectionPersistence[V] {
	t.Helper()
	ctx := context.Background()
	tableName := safeTableName(t.Name())

	p, err := pgstore.NewProjectionPersistence[V](sharedPool, tableName)
	if err != nil {
		t.Fatalf("NewProjectionPersistence: %v", err)
	}
	if err := p.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		if _, err := sharedPool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName)); err != nil {
			t.Errorf("cleanup: drop table %q: %v", tableName, err)
		}
		indexName := "idx_" + tableName + "_gc"
		if _, err := sharedPool.Exec(ctx, fmt.Sprintf(`DROP INDEX IF EXISTS "%s"`, indexName)); err != nil {
			t.Errorf("cleanup: drop index %q: %v", indexName, err)
		}
	})
	return p
}

// --- Migrate ---

func TestProjectionPersistence_MigrateCreatesTable(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	// A successful Get against the newly created table is proof the table exists.
	_, err := p.Get(ctx, "probe")
	if err != nil {
		t.Fatalf("Get after Migrate failed (table may not exist): %v", err)
	}
}

func TestProjectionPersistence_MigrateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	// Calling Migrate a second time on the same table must not error.
	if err := p.Migrate(ctx); err != nil {
		t.Errorf("second Migrate should be a no-op, got: %v", err)
	}
}

// --- Get ---

func TestProjectionPersistence_GetMissingKeyReturnsZero(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	row, err := p.Get(ctx, "missing")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if row.Name != "" || row.Score != 0 {
		t.Errorf("expected zero value, got %+v", row)
	}
}

// --- Set + Get roundtrip ---

func TestProjectionPersistence_SetThenGet(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	want := projRow{Name: "alice", Score: 42}
	if err := p.Set(ctx, []monotonic.Projected[projRow]{{Key: "alice", Value: want}}, 1); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := p.Get(ctx, "alice")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != want {
		t.Errorf("roundtrip mismatch: got %+v, want %+v", got, want)
	}
}

func TestProjectionPersistence_SetUpdatesExistingRow(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	if err := p.Set(ctx, []monotonic.Projected[projRow]{{Key: "k", Value: projRow{Score: 1}}}, 1); err != nil {
		t.Fatalf("first Set: %v", err)
	}
	if err := p.Set(ctx, []monotonic.Projected[projRow]{{Key: "k", Value: projRow{Score: 2}}}, 2); err != nil {
		t.Fatalf("second Set: %v", err)
	}

	got, err := p.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Score != 2 {
		t.Errorf("expected Score=2 after update, got %d", got.Score)
	}
}

func TestProjectionPersistence_MultipleKeysInBatch(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	batch := []monotonic.Projected[projRow]{
		{Key: "a", Value: projRow{Name: "alpha", Score: 1}},
		{Key: "b", Value: projRow{Name: "beta", Score: 2}},
		{Key: "c", Value: projRow{Name: "gamma", Score: 3}},
	}
	if err := p.Set(ctx, batch, 1); err != nil {
		t.Fatalf("Set batch: %v", err)
	}

	for _, item := range batch {
		got, err := p.Get(ctx, item.Key)
		if err != nil {
			t.Fatalf("Get %q: %v", item.Key, err)
		}
		if got != item.Value {
			t.Errorf("key %q: got %+v, want %+v", item.Key, got, item.Value)
		}
	}
}

// --- LatestGlobalCounter ---

func TestProjectionPersistence_LatestGlobalCounterIsZeroOnEmptyTable(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	c, err := p.LatestGlobalCounter(ctx)
	if err != nil {
		t.Fatalf("LatestGlobalCounter: %v", err)
	}
	if c != 0 {
		t.Errorf("expected 0 on empty table, got %d", c)
	}
}

func TestProjectionPersistence_LatestGlobalCounterAdvancesWithSet(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	for _, counter := range []uint64{1, 5, 9} {
		if err := p.Set(ctx, []monotonic.Projected[projRow]{{Key: "k", Value: projRow{}}}, counter); err != nil {
			t.Fatalf("Set at counter %d: %v", counter, err)
		}
		got, err := p.LatestGlobalCounter(ctx)
		if err != nil {
			t.Fatalf("LatestGlobalCounter: %v", err)
		}
		if got != counter {
			t.Errorf("after Set(%d): LatestGlobalCounter=%d", counter, got)
		}
	}
}

func TestProjectionPersistence_LatestGlobalCounterReflectsMaxAcrossAllKeys(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	p.Set(ctx, []monotonic.Projected[projRow]{{Key: "a", Value: projRow{}}}, 10)
	p.Set(ctx, []monotonic.Projected[projRow]{{Key: "b", Value: projRow{}}}, 7) // lower counter for different key

	c, _ := p.LatestGlobalCounter(ctx)
	if c != 10 {
		t.Errorf("expected max=10, got %d", c)
	}
}

// --- Stale write detection ---

func TestProjectionPersistence_StaleWriteReturnsErrProjectionStale(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	p.Set(ctx, []monotonic.Projected[projRow]{{Key: "k", Value: projRow{Score: 1}}}, 5)

	err := p.Set(ctx, []monotonic.Projected[projRow]{{Key: "k", Value: projRow{Score: 99}}}, 3)
	if !errors.Is(err, monotonic.ErrProjectionStale) {
		t.Errorf("expected ErrProjectionStale, got %v", err)
	}

	// Value must not have changed.
	got, _ := p.Get(ctx, "k")
	if got.Score != 1 {
		t.Errorf("value should be unchanged after stale write: got %d", got.Score)
	}
}

func TestProjectionPersistence_EqualCounterWriteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	row := projRow{Name: "alice", Score: 10}
	p.Set(ctx, []monotonic.Projected[projRow]{{Key: "k", Value: row}}, 5)

	if err := p.Set(ctx, []monotonic.Projected[projRow]{{Key: "k", Value: row}}, 5); err != nil {
		t.Errorf("equal-counter write should succeed (idempotent), got %v", err)
	}
}

func TestProjectionPersistence_BatchRolledBackWhenOneKeyIsStale(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	// Establish k1 at counter=5.
	p.Set(ctx, []monotonic.Projected[projRow]{{Key: "k1", Value: projRow{Score: 1}}}, 5)

	// Batch: k2 (new, would succeed) + k1 (stale). Whole batch must fail.
	err := p.Set(ctx, []monotonic.Projected[projRow]{
		{Key: "k2", Value: projRow{Score: 20}},
		{Key: "k1", Value: projRow{Score: 99}},
	}, 3)
	if !errors.Is(err, monotonic.ErrProjectionStale) {
		t.Errorf("expected ErrProjectionStale for batch, got %v", err)
	}

	// k2 must NOT have been written (transaction rollback).
	row, _ := p.Get(ctx, "k2")
	if row.Score != 0 {
		t.Errorf("k2 must not be written after failed batch: got %+v", row)
	}
}

// --- Edge cases ---

func TestProjectionPersistence_EmptyBatchIsNoOp(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	if err := p.Set(ctx, nil, 1); err != nil {
		t.Errorf("nil batch must not error, got %v", err)
	}
	if err := p.Set(ctx, []monotonic.Projected[projRow]{}, 1); err != nil {
		t.Errorf("empty batch must not error, got %v", err)
	}

	c, _ := p.LatestGlobalCounter(ctx)
	if c != 0 {
		t.Errorf("empty batch must not advance counter, got %d", c)
	}
}

func TestProjectionPersistence_ZeroGlobalCounterRejected(t *testing.T) {
	ctx := context.Background()
	p := testProjPersistence[projRow](t)

	err := p.Set(ctx, []monotonic.Projected[projRow]{{Key: "k", Value: projRow{}}}, 0)
	if err == nil {
		t.Error("expected error for globalCounter=0")
	}
}

// --- Projector integration: full round-trip using ProjectionPersistence ---

func TestProjector_ResumeWithPostgresPersistence(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	p := testProjPersistence[projRow](t)

	// Emit two events via the store directly.
	for i := int64(1); i <= 2; i++ {
		err := store.Append(ctx, monotonic.AggregateEvent{
			AggregateType: "account",
			AggregateID:   "alice",
			Event: monotonic.AcceptedEvent{
				Event:      monotonic.NewEvent("scored", map[string]int{"pts": 10}),
				Counter:    i,
				AcceptedAt: time.Now(),
			},
		})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Logic: on each "scored" event increment Score for the aggregate's key.
	logic := monotonic.NewDispatch[projRow]().
		On("account", "scored", func(ctx context.Context, reader monotonic.ProjectionReader[projRow], event monotonic.AggregateEvent) ([]monotonic.Projected[projRow], error) {
			return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKey(event.AggregateID), func(v *projRow) error {
				v.Score += 10
				return nil
			})
		})

	proj1, err := monotonic.NewProjector(ctx, store, logic, p)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if n, err := proj1.Update(ctx); err != nil || n != 2 {
		t.Fatalf("first Update: n=%d err=%v", n, err)
	}

	got, _ := p.Get(ctx, "alice")
	if got.Score != 20 {
		t.Errorf("alice Score after 2 events: want 20, got %d", got.Score)
	}

	// Emit a third event and resume via a NEW projector against the same persistence.
	store.Append(ctx, monotonic.AggregateEvent{
		AggregateType: "account",
		AggregateID:   "alice",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.NewEvent("scored", map[string]int{"pts": 10}),
			Counter:    3,
			AcceptedAt: time.Now(),
		},
	})

	proj2, err := monotonic.NewProjector(ctx, store, logic, p)
	if err != nil {
		t.Fatalf("resume NewProjector: %v", err)
	}
	if n, err := proj2.Update(ctx); err != nil || n != 1 {
		t.Fatalf("resumed Update: n=%d err=%v", n, err)
	}

	got, _ = p.Get(ctx, "alice")
	if got.Score != 30 {
		t.Errorf("alice Score after resume: want 30, got %d", got.Score)
	}
}

func TestProjector_MultipleProjectionsPersistCorrectlyToPostgres(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)

	type stats struct {
		Total int64 `proj:"total"`
		Count int64 `proj:"count"`
	}
	statsPersist := testProjPersistence[stats](t)

	// Emit 5 events for two aggregates.
	for _, aggID := range []string{"a", "b"} {
		for i := int64(1); i <= 5; i++ {
			store.Append(ctx, monotonic.AggregateEvent{
				AggregateType: "item",
				AggregateID:   aggID,
				Event: monotonic.AcceptedEvent{
					Event:      monotonic.NewEvent("added", map[string]int{"v": 10}),
					Counter:    i,
					AcceptedAt: time.Now(),
				},
			})
		}
	}

	logic := monotonic.NewDispatch[stats]().
		On("item", "added", func(ctx context.Context, reader monotonic.ProjectionReader[stats], event monotonic.AggregateEvent) ([]monotonic.Projected[stats], error) {
			return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *stats) error {
				s.Total += 10
				s.Count++
				return nil
			})
		})

	proj, err := monotonic.NewProjector(ctx, store, logic, statsPersist)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := proj.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}

	summary, err := statsPersist.Get(ctx, monotonic.ProjectionKeySummary)
	if err != nil {
		t.Fatalf("Get summary: %v", err)
	}
	if summary.Count != 10 {
		t.Errorf("expected Count=10, got %d", summary.Count)
	}
	if summary.Total != 100 {
		t.Errorf("expected Total=100, got %d", summary.Total)
	}
}

// --- Field type round-trip tests ---

func TestProjectionPersistence_TimeFieldRoundtrip(t *testing.T) {
	type row struct {
		OccurredAt time.Time `proj:"occurred_at"`
	}
	ctx := context.Background()
	p := testProjPersistence[row](t)

	// Truncate to microseconds: Postgres TIMESTAMPTZ stores microsecond precision,
	// so a nanosecond-precision Go time.Time will not survive the round-trip unchanged.
	want := time.Now().UTC().Truncate(time.Microsecond)
	if err := p.Set(ctx, []monotonic.Projected[row]{{Key: "k", Value: row{OccurredAt: want}}}, 1); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := p.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.OccurredAt.Equal(want) {
		t.Errorf("time mismatch: got %v, want %v", got.OccurredAt, want)
	}
}

func TestProjectionPersistence_BytesFieldRoundtrip(t *testing.T) {
	type row struct {
		Data []byte `proj:"data"`
	}
	ctx := context.Background()
	p := testProjPersistence[row](t)

	want := []byte{0x01, 0x02, 0x03, 0xFF}
	if err := p.Set(ctx, []monotonic.Projected[row]{{Key: "k", Value: row{Data: want}}}, 1); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := p.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.Data) != string(want) {
		t.Errorf("bytes mismatch: got %v, want %v", got.Data, want)
	}
}

func TestProjectionPersistence_JSONRawMessageFieldRoundtrip(t *testing.T) {
	type row struct {
		Meta json.RawMessage `proj:"meta"`
	}
	ctx := context.Background()
	p := testProjPersistence[row](t)

	want := json.RawMessage(`{"count":3,"tags":["a","b"]}`)
	if err := p.Set(ctx, []monotonic.Projected[row]{{Key: "k", Value: row{Meta: want}}}, 1); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := p.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	// Postgres normalizes JSONB (key order, spacing), so compare parsed values.
	var gotVal, wantVal any
	if err := json.Unmarshal(got.Meta, &gotVal); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal(want, &wantVal); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if !reflect.DeepEqual(gotVal, wantVal) {
		t.Errorf("JSON mismatch: got %s, want %s", got.Meta, want)
	}
}

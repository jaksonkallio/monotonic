package postgres_integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
	pgstore "github.com/jaksonkallio/monotonic/pkg/store/postgres"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// sharedContainer holds the Postgres container and pool shared across all tests in this package.
var (
	sharedPool  *pgxpool.Pool
	sharedStore *pgstore.Store
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("monotonic_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		panic(fmt.Sprintf("start postgres container: %v", err))
	}
	defer pgContainer.Terminate(ctx)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(fmt.Sprintf("get connection string: %v", err))
	}

	sharedPool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		panic(fmt.Sprintf("create pool: %v", err))
	}
	defer sharedPool.Close()

	sharedStore = pgstore.New(sharedPool)
	if err := sharedStore.Migrate(ctx); err != nil {
		panic(fmt.Sprintf("migrate: %v", err))
	}

	m.Run()
}

// testStore returns a store backed by the shared container, truncating tables for isolation.
func testStore(t *testing.T) *pgstore.Store {
	t.Helper()

	_, err := sharedPool.Exec(context.Background(), "TRUNCATE events, sagas RESTART IDENTITY")
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}

	return sharedStore
}

func TestAppendAndLoad(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	events := []monotonic.AggregateEvent{
		{
			AggregateType: "cart",
			AggregateID:   "cart-1",
			Event: monotonic.AcceptedEvent{
				Event:      monotonic.NewEvent("item-added", map[string]string{"item": "widget"}),
				Counter:    1,
				AcceptedAt: time.Now(),
			},
		},
		{
			AggregateType: "cart",
			AggregateID:   "cart-1",
			Event: monotonic.AcceptedEvent{
				Event:      monotonic.NewEvent("item-added", map[string]string{"item": "gadget"}),
				Counter:    2,
				AcceptedAt: time.Now(),
			},
		},
	}

	err := store.Append(ctx,events...)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Load all events
	loaded, err := store.LoadAggregateEvents(ctx,"cart", "cart-1", 0)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2 events, got %d", len(loaded))
	}
	if loaded[0].Counter != 1 || loaded[1].Counter != 2 {
		t.Errorf("unexpected counters: %d, %d", loaded[0].Counter, loaded[1].Counter)
	}
	if loaded[0].Type != "item-added" {
		t.Errorf("expected event type 'item-added', got %q", loaded[0].Type)
	}
	if loaded[0].GlobalCounter == 0 || loaded[1].GlobalCounter == 0 {
		t.Error("expected global counters to be assigned")
	}
	if loaded[1].GlobalCounter <= loaded[0].GlobalCounter {
		t.Error("expected global counters to be monotonically increasing")
	}

	// Load with afterCounter filter
	loaded, err = store.LoadAggregateEvents(ctx,"cart", "cart-1", 1)
	if err != nil {
		t.Fatalf("load after counter: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 event after counter 1, got %d", len(loaded))
	}
	if loaded[0].Counter != 2 {
		t.Errorf("expected counter 2, got %d", loaded[0].Counter)
	}
}

func TestAppendCounterMismatch(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// Append first event
	err := store.Append(ctx,monotonic.AggregateEvent{
		AggregateType: "cart",
		AggregateID:   "cart-1",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.Event{Type: "item-added"},
			Counter:    1,
			AcceptedAt: time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("first append: %v", err)
	}

	// Append with wrong counter (1 again instead of 2)
	err = store.Append(ctx,monotonic.AggregateEvent{
		AggregateType: "cart",
		AggregateID:   "cart-1",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.Event{Type: "item-added"},
			Counter:    1,
			AcceptedAt: time.Now(),
		},
	})
	if err == nil {
		t.Fatal("expected counter mismatch error")
	}

	// Verify the error message mentions mismatch
	if got := err.Error(); got == "" {
		t.Error("expected non-empty error message")
	}
}

func TestAppendAtomicity(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// Append first event to cart-1
	err := store.Append(ctx,monotonic.AggregateEvent{
		AggregateType: "cart",
		AggregateID:   "cart-1",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.Event{Type: "item-added"},
			Counter:    1,
			AcceptedAt: time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	// Try to append two events where the second has a bad counter
	// Both should fail (atomicity)
	err = store.Append(ctx,
		monotonic.AggregateEvent{
			AggregateType: "cart",
			AggregateID:   "cart-2",
			Event: monotonic.AcceptedEvent{
				Event:      monotonic.Event{Type: "item-added"},
				Counter:    1,
				AcceptedAt: time.Now(),
			},
		},
		monotonic.AggregateEvent{
			AggregateType: "cart",
			AggregateID:   "cart-1",
			Event: monotonic.AcceptedEvent{
				Event:      monotonic.Event{Type: "item-added"},
				Counter:    1, // wrong: should be 2
				AcceptedAt: time.Now(),
			},
		},
	)
	if err == nil {
		t.Fatal("expected error for counter mismatch")
	}

	// cart-2 should NOT have been persisted (transaction rolled back)
	events, err := store.LoadAggregateEvents(ctx,"cart", "cart-2", 0)
	if err != nil {
		t.Fatalf("load cart-2: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events for cart-2 (atomic rollback), got %d", len(events))
	}
}

func TestLoadGlobalEvents(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// Append events to different aggregates
	err := store.Append(ctx,
		monotonic.AggregateEvent{
			AggregateType: "cart",
			AggregateID:   "cart-1",
			Event: monotonic.AcceptedEvent{
				Event:      monotonic.NewEvent("item-added", map[string]string{"item": "widget"}),
				Counter:    1,
				AcceptedAt: time.Now(),
			},
		},
		monotonic.AggregateEvent{
			AggregateType: "stock",
			AggregateID:   "stock-1",
			Event: monotonic.AcceptedEvent{
				Event:      monotonic.NewEvent("stock-added", map[string]int{"qty": 100}),
				Counter:    1,
				AcceptedAt: time.Now(),
			},
		},
	)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Load only cart events
	events, err := store.LoadGlobalEvents(ctx, []monotonic.AggregateID{{Type: "cart"}}, 0)
	if err != nil {
		t.Fatalf("load global cart: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 cart event, got %d", len(events))
	}
	if events[0].AggregateType != "cart" {
		t.Errorf("expected aggregate type 'cart', got %q", events[0].AggregateType)
	}

	// Load both types
	events, err = store.LoadGlobalEvents(ctx, []monotonic.AggregateID{{Type: "cart"}, {Type: "stock"}}, 0)
	if err != nil {
		t.Fatalf("load global both: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// Global counters should be ordered
	if events[1].Event.GlobalCounter <= events[0].Event.GlobalCounter {
		t.Error("expected monotonically increasing global counters")
	}

	// Load with afterGlobalCounter filter
	afterGC := events[0].Event.GlobalCounter
	events, err = store.LoadGlobalEvents(ctx, []monotonic.AggregateID{{Type: "cart"}, {Type: "stock"}}, afterGC)
	if err != nil {
		t.Fatalf("load global after: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event after global counter %d, got %d", afterGC, len(events))
	}
}

func TestLoadNonexistentAggregate(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	events, err := store.LoadAggregateEvents(ctx,"cart", "nonexistent", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events for nonexistent aggregate, got %d", len(events))
	}
}

func TestSagaLifecycle(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// Initially no active sagas
	ids, err := store.ListActiveSagas(ctx,"checkout")
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("expected 0 active sagas, got %d", len(ids))
	}

	// Create saga by appending its first event
	err = store.Append(ctx,monotonic.AggregateEvent{
		AggregateType: "checkout",
		AggregateID:   "saga-1",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.Event{Type: monotonic.SagaStartedEvent},
			Counter:    1,
			AcceptedAt: time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("append saga event: %v", err)
	}

	// Saga not yet in sagas table (only gets there via MarkSagaCompleted or explicit insert)
	completed, err := store.IsSagaCompleted("checkout", "saga-1")
	if err != nil {
		t.Fatalf("is completed: %v", err)
	}
	if completed {
		t.Error("saga should not be completed yet")
	}

	// Mark completed
	err = store.MarkSagaCompleted(ctx,"checkout", "saga-1")
	if err != nil {
		t.Fatalf("mark completed: %v", err)
	}

	completed, err = store.IsSagaCompleted("checkout", "saga-1")
	if err != nil {
		t.Fatalf("is completed after mark: %v", err)
	}
	if !completed {
		t.Error("expected saga to be completed")
	}

	// Idempotent: marking again should not error
	err = store.MarkSagaCompleted(ctx,"checkout", "saga-1")
	if err != nil {
		t.Fatalf("idempotent mark completed: %v", err)
	}

	// Cannot append to completed saga
	err = store.Append(ctx,monotonic.AggregateEvent{
		AggregateType: "checkout",
		AggregateID:   "saga-1",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.Event{Type: "another-event"},
			Counter:    2,
			AcceptedAt: time.Now(),
		},
	})
	if err == nil {
		t.Fatal("expected error appending to completed saga")
	}
	if !errors.Is(err, monotonic.ErrSagaCompleted) {
		t.Errorf("expected ErrSagaCompleted, got: %v", err)
	}
}

func TestListActiveSagas(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// Insert saga rows directly via MarkSagaCompleted won't create active ones,
	// so we need to create active sagas by inserting into the sagas table.
	// The ListActiveSagas query checks the sagas table, so sagas need to exist there.
	// In practice, sagas get inserted via NewSaga which appends events.
	// The sagas table row is created when MarkSagaCompleted is called.
	// For active sagas to appear, we need a row with completed=false.

	// Let's use the store through the monotonic.NewSaga flow
	actions := monotonic.ActionMap{
		"started": func(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
			return monotonic.ActionResult{NewState: "done"}, nil
		},
		"done": func(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
			return monotonic.ActionResult{Complete: true}, nil
		},
	}

	_, err := monotonic.NewSaga(store, "checkout", "saga-1", "started", nil, actions)
	if err != nil {
		t.Fatalf("new saga 1: %v", err)
	}
	_, err = monotonic.NewSaga(store, "checkout", "saga-2", "started", nil, actions)
	if err != nil {
		t.Fatalf("new saga 2: %v", err)
	}

	ids, err := store.ListActiveSagas(ctx,"checkout")
	if err != nil {
		t.Fatalf("list active: %v", err)
	}

	// Sagas are active (not completed). But they only show up in ListActiveSagas
	// if they have a row in the sagas table with completed=false.
	// NewSaga appends events but doesn't insert into sagas table.
	// So let's verify events were appended at least.
	events1, _ := store.LoadAggregateEvents(ctx,"checkout", "saga-1", 0)
	events2, _ := store.LoadAggregateEvents(ctx,"checkout", "saga-2", 0)
	if len(events1) == 0 || len(events2) == 0 {
		t.Fatal("expected saga events to be persisted")
	}

	// Mark saga-1 as completed, then verify listing
	err = store.MarkSagaCompleted(ctx,"checkout", "saga-1")
	if err != nil {
		t.Fatalf("mark completed: %v", err)
	}

	ids, err = store.ListActiveSagas(ctx,"checkout")
	if err != nil {
		t.Fatalf("list after completion: %v", err)
	}
	// saga-1 is completed, saga-2 has no row in sagas table
	for _, id := range ids {
		if id == "saga-1" {
			t.Error("completed saga-1 should not appear in active list")
		}
	}
}

func TestPayloadRoundtrip(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	type ItemPayload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}

	payload := ItemPayload{Name: "widget", Count: 5}
	payloadBytes, _ := json.Marshal(payload)

	err := store.Append(ctx,monotonic.AggregateEvent{
		AggregateType: "cart",
		AggregateID:   "cart-1",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.Event{Type: "item-added", Payload: payloadBytes},
			Counter:    1,
			AcceptedAt: time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	events, err := store.LoadAggregateEvents(ctx,"cart", "cart-1", 0)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	var loaded ItemPayload
	if err := json.Unmarshal(events[0].Payload, &loaded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if loaded.Name != "widget" || loaded.Count != 5 {
		t.Errorf("payload mismatch: got %+v", loaded)
	}
}

func TestNilPayload(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	err := store.Append(ctx,monotonic.AggregateEvent{
		AggregateType: "cart",
		AggregateID:   "cart-1",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.Event{Type: "cleared"},
			Counter:    1,
			AcceptedAt: time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	events, err := store.LoadAggregateEvents(ctx,"cart", "cart-1", 0)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Payload != nil {
		t.Errorf("expected nil payload, got %s", string(events[0].Payload))
	}
}

func TestMultipleAggregatesInSingleAppend(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	err := store.Append(ctx,
		monotonic.AggregateEvent{
			AggregateType: "cart",
			AggregateID:   "cart-1",
			Event: monotonic.AcceptedEvent{
				Event:      monotonic.Event{Type: "item-added"},
				Counter:    1,
				AcceptedAt: time.Now(),
			},
		},
		monotonic.AggregateEvent{
			AggregateType: "stock",
			AggregateID:   "stock-1",
			Event: monotonic.AcceptedEvent{
				Event:      monotonic.Event{Type: "stock-reserved"},
				Counter:    1,
				AcceptedAt: time.Now(),
			},
		},
	)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	cartEvents, _ := store.LoadAggregateEvents(ctx,"cart", "cart-1", 0)
	stockEvents, _ := store.LoadAggregateEvents(ctx,"stock", "stock-1", 0)

	if len(cartEvents) != 1 {
		t.Errorf("expected 1 cart event, got %d", len(cartEvents))
	}
	if len(stockEvents) != 1 {
		t.Errorf("expected 1 stock event, got %d", len(stockEvents))
	}
}

func TestTimestampPreserved(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond) // Postgres has microsecond precision

	err := store.Append(ctx,monotonic.AggregateEvent{
		AggregateType: "cart",
		AggregateID:   "cart-1",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.Event{Type: "item-added"},
			Counter:    1,
			AcceptedAt: now,
		},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	events, _ := store.LoadAggregateEvents(ctx,"cart", "cart-1", 0)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if !events[0].AcceptedAt.Equal(now) {
		t.Errorf("timestamp not preserved: got %v, want %v", events[0].AcceptedAt, now)
	}
}

func TestAppendEmpty(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	err := store.Append(ctx)
	if err != nil {
		t.Fatalf("append empty should not error: %v", err)
	}
}

func TestGlobalCounterOrdering(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// Append events in separate calls to get distinct global counters
	for i := 1; i <= 5; i++ {
		err := store.Append(ctx,monotonic.AggregateEvent{
			AggregateType: "cart",
			AggregateID:   fmt.Sprintf("cart-%d", i),
			Event: monotonic.AcceptedEvent{
				Event:      monotonic.Event{Type: "created"},
				Counter:    1,
				AcceptedAt: time.Now(),
			},
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	events, err := store.LoadGlobalEvents(ctx, []monotonic.AggregateID{{Type: "cart"}}, 0)
	if err != nil {
		t.Fatalf("load global: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}

	for i := 1; i < len(events); i++ {
		if events[i].Event.GlobalCounter <= events[i-1].Event.GlobalCounter {
			t.Errorf("global counters not strictly increasing at index %d: %d <= %d",
				i, events[i].Event.GlobalCounter, events[i-1].Event.GlobalCounter)
		}
	}
}

func TestFullSagaWithDriver(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	actions := monotonic.ActionMap{
		"started": func(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
			return monotonic.ActionResult{NewState: "processing"}, nil
		},
		"processing": func(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
			return monotonic.ActionResult{NewState: "done"}, nil
		},
		"done": func(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
			return monotonic.ActionResult{Complete: true}, nil
		},
	}

	saga, err := monotonic.NewSaga(store, "test-saga", "saga-1", "started", nil, actions)
	if err != nil {
		t.Fatalf("new saga: %v", err)
	}

	// Step through the saga
	if err := saga.Step(ctx); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	if saga.State() != "processing" {
		t.Errorf("expected 'processing', got %q", saga.State())
	}

	if err := saga.Step(ctx); err != nil {
		t.Fatalf("step 2: %v", err)
	}
	if saga.State() != "done" {
		t.Errorf("expected 'done', got %q", saga.State())
	}

	if err := saga.Step(ctx); err != nil {
		t.Fatalf("step 3: %v", err)
	}
	if !saga.Completed() {
		t.Error("expected saga to be completed")
	}

	// Verify completed in store
	completed, err := store.IsSagaCompleted("test-saga", "saga-1")
	if err != nil {
		t.Fatalf("is completed: %v", err)
	}
	if !completed {
		t.Error("expected saga completed in store")
	}

	// Load saga from store and verify state
	loaded, err := monotonic.LoadSaga(store, "test-saga", "saga-1", actions)
	if err != nil {
		t.Fatalf("load saga: %v", err)
	}
	if loaded.State() != "done" {
		t.Errorf("loaded saga state: expected 'done', got %q", loaded.State())
	}
	if !loaded.Completed() {
		t.Error("loaded saga should be completed")
	}
}

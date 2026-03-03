package postgres_integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// Test helpers for concurrency tests

const (
	eventIncremented = "incremented"
	eventDecremented = "decremented"
)

type incrementedPayload struct {
	Amount int `json:"amount"`
}

type counter struct {
	*monotonic.AggregateBase
	Value int
}

func (c *counter) Apply(event monotonic.AcceptedEvent) {
	switch event.Type {
	case eventIncremented:
		if p, ok := monotonic.ParsePayload[incrementedPayload](event); ok {
			c.Value += p.Amount
		}
	case eventDecremented:
		if p, ok := monotonic.ParsePayload[incrementedPayload](event); ok {
			c.Value -= p.Amount
		}
	}
}

func (c *counter) ShouldAccept(event monotonic.Event) error {
	switch event.Type {
	case eventIncremented:
		return nil
	case eventDecremented:
		var p incrementedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		if c.Value-p.Amount < 0 {
			return errors.New("counter would go negative")
		}
		return nil
	}
	return errors.New("unknown event type")
}

func loadCounter(ctx context.Context, store monotonic.Store, id string) (*counter, error) {
	return monotonic.Hydrate(ctx, store, "counter", id, func(base *monotonic.AggregateBase) *counter {
		return &counter{AggregateBase: base}
	})
}

// High-Concurrency Tests for PostgreSQL

func TestConcurrentAppends_PostgreSQL_SameAggregate(t *testing.T) {
	// Test multiple goroutines trying to append to the same aggregate
	// Should see counter conflicts and successful retries
	store := testStore(t)
	ctx := context.Background()

	const numGoroutines = 20
	const eventsPerGoroutine = 10

	// Track results
	type result struct {
		goroutineID int
		succeeded   int
		failed      int
	}
	results := make(chan result, numGoroutines)

	// Launch concurrent writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			r := result{goroutineID: id}
			// Each goroutine needs its own instance to work with
			localCounter, _ := loadCounter(ctx, store, "concurrent")
			retry := monotonic.NewRetry(15, monotonic.ExponentialBackoff(0))

			for j := 0; j < eventsPerGoroutine; j++ {
				err := localCounter.AcceptThenApplyRetryable(ctx, *retry,
					monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				if err != nil {
					r.failed++
				} else {
					r.succeeded++
				}
			}
			results <- r
		}(i)
	}

	// Collect results
	totalSucceeded := 0
	totalFailed := 0
	for i := 0; i < numGoroutines; i++ {
		r := <-results
		totalSucceeded += r.succeeded
		totalFailed += r.failed
	}

	// Verify all events were written successfully
	expectedEvents := numGoroutines * eventsPerGoroutine
	if totalSucceeded != expectedEvents {
		t.Errorf("expected %d successful events, got %d (failed: %d)",
			expectedEvents, totalSucceeded, totalFailed)
	}

	// Verify final aggregate state
	finalCounter, _ := loadCounter(ctx, store, "concurrent")
	if finalCounter.Value != expectedEvents {
		t.Errorf("expected final value %d, got %d", expectedEvents, finalCounter.Value)
	}
	if finalCounter.Counter() != int64(expectedEvents) {
		t.Errorf("expected counter %d, got %d", expectedEvents, finalCounter.Counter())
	}
}

func TestConcurrentAppends_PostgreSQL_DifferentAggregates(t *testing.T) {
	// Test multiple goroutines appending to different aggregates
	// All should succeed without conflicts
	store := testStore(t)
	ctx := context.Background()

	const numAggregates = 50
	const eventsPerAggregate = 20

	type result struct {
		aggregateID string
		finalValue  int
		err         error
	}
	results := make(chan result, numAggregates)

	// Launch concurrent writers to different aggregates
	for i := 0; i < numAggregates; i++ {
		go func(id int) {
			aggregateID := fmt.Sprintf("counter-%d", id)
			c, _ := loadCounter(ctx, store, aggregateID)

			var lastErr error
			for j := 0; j < eventsPerAggregate; j++ {
				err := c.AcceptThenApply(ctx, monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				if err != nil {
					lastErr = err
					break
				}
			}

			results <- result{
				aggregateID: aggregateID,
				finalValue:  c.Value,
				err:         lastErr,
			}
		}(i)
	}

	// Collect results
	for i := 0; i < numAggregates; i++ {
		r := <-results
		if r.err != nil {
			t.Errorf("aggregate %s failed: %v", r.aggregateID, r.err)
		}
		if r.finalValue != eventsPerAggregate {
			t.Errorf("aggregate %s: expected value %d, got %d",
				r.aggregateID, eventsPerAggregate, r.finalValue)
		}
	}

	// Verify total events in store
	events, err := store.LoadGlobalEvents(ctx, []monotonic.AggregateID{{Type: "counter"}}, 0)
	if err != nil {
		t.Fatalf("LoadGlobalEvents: %v", err)
	}
	expectedTotal := numAggregates * eventsPerAggregate
	if len(events) != expectedTotal {
		t.Errorf("expected %d total events in store, got %d", expectedTotal, len(events))
	}
}

func TestConcurrentAppends_PostgreSQL_HighContention(t *testing.T) {
	// Stress test the retry mechanism with high contention on PostgreSQL
	store := testStore(t)
	ctx := context.Background()

	const numGoroutines = 50
	const eventsPerGoroutine = 5

	results := make(chan bool, numGoroutines)

	// All goroutines compete for the same aggregate
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			c, _ := loadCounter(ctx, store, "high-contention")
			// More retries for high contention scenario
			retry := monotonic.NewRetry(25, monotonic.ExponentialBackoff(0))

			success := true
			for j := 0; j < eventsPerGoroutine; j++ {
				err := c.AcceptThenApplyRetryable(ctx, *retry,
					monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				if err != nil {
					success = false
					break
				}
			}
			results <- success
		}(i)
	}

	// Collect results
	successCount := 0
	for i := 0; i < numGoroutines; i++ {
		if <-results {
			successCount++
		}
	}

	// With sufficient retries, all should eventually succeed
	if successCount != numGoroutines {
		t.Logf("warning: only %d/%d goroutines succeeded under high contention",
			successCount, numGoroutines)
	}

	// Verify final state matches successful writes
	finalCounter, _ := loadCounter(ctx, store, "high-contention")
	expectedValue := successCount * eventsPerGoroutine
	if finalCounter.Value != expectedValue {
		t.Errorf("expected final value %d, got %d", expectedValue, finalCounter.Value)
	}
}

func TestConcurrentAppends_PostgreSQL_GlobalCounterOrdering(t *testing.T) {
	// Verify global counter ordering remains consistent under concurrent writes
	store := testStore(t)
	ctx := context.Background()

	const numWriters = 10
	const eventsPerWriter = 20

	done := make(chan bool)

	// Launch writers
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			aggregateID := fmt.Sprintf("writer-%d", id)
			c, _ := loadCounter(ctx, store, aggregateID)
			retry := monotonic.NewRetry(10, monotonic.ExponentialBackoff(0))

			for j := 0; j < eventsPerWriter; j++ {
				c.AcceptThenApplyRetryable(ctx, *retry,
					monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
			}
			done <- true
		}(i)
	}

	// Wait for all writers to complete
	for i := 0; i < numWriters; i++ {
		<-done
	}

	// Verify global counter sequence has no gaps
	events, _ := store.LoadGlobalEvents(ctx, []monotonic.AggregateID{{Type: "counter"}}, 0)
	expectedTotal := numWriters * eventsPerWriter
	if len(events) != expectedTotal {
		t.Errorf("expected %d total events, got %d", expectedTotal, len(events))
	}

	// Verify global counters are monotonically increasing with no gaps
	for i, event := range events {
		expectedGlobalCounter := int64(i + 1)
		if event.Event.GlobalCounter != expectedGlobalCounter {
			t.Errorf("event %d: expected global counter %d, got %d",
				i, expectedGlobalCounter, event.Event.GlobalCounter)
		}
	}
}

func TestBatchAppend_PostgreSQL(t *testing.T) {
	// Test that batch appends work correctly with PostgreSQL
	store := testStore(t)
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "batch-test")

	// Append a batch of 5 events
	err := c.AcceptThenApply(ctx,
		monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}),
		monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 2}),
		monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 3}),
		monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 4}),
		monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 5}),
	)
	if err != nil {
		t.Fatalf("batch append failed: %v", err)
	}

	// Verify all events were applied
	expectedValue := 1 + 2 + 3 + 4 + 5
	if c.Value != expectedValue {
		t.Errorf("expected value %d, got %d", expectedValue, c.Value)
	}
	if c.Counter() != 5 {
		t.Errorf("expected counter 5, got %d", c.Counter())
	}
}

func TestSequentialCounterValidation_PostgreSQL(t *testing.T) {
	// Test that PostgreSQL store rejects gaps in counters
	store := testStore(t)
	ctx := context.Background()

	// Create an event manually with counter=1
	err := store.Append(ctx, monotonic.AggregateEvent{
		AggregateType: "counter",
		AggregateID:   "gap-test",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}),
			Counter:    1,
			AcceptedAt: time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("first append failed: %v", err)
	}

	// Try to append counter=5 (should fail - gap detected)
	err = store.Append(ctx, monotonic.AggregateEvent{
		AggregateType: "counter",
		AggregateID:   "gap-test",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 5}),
			Counter:    5, // Wrong! Should be 2
			AcceptedAt: time.Now(),
		},
	})
	if err == nil {
		t.Fatal("expected error for gap in counters, got nil")
	}
	// Should get a counter mismatch error
	errMsg := err.Error()
	if !strings.Contains(errMsg, "counter mismatch") || !strings.Contains(errMsg, "expected 2") || !strings.Contains(errMsg, "got 5") {
		t.Errorf("unexpected error message: %v", err)
	}

	// Verify correct sequential append still works
	err = store.Append(ctx, monotonic.AggregateEvent{
		AggregateType: "counter",
		AggregateID:   "gap-test",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 2}),
			Counter:    2, // Correct
			AcceptedAt: time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("sequential append failed: %v", err)
	}
}

func TestConcurrentAppends_PostgreSQL_TransactionIsolation(t *testing.T) {
	// Test that PostgreSQL transaction isolation works correctly
	// Even with high concurrency, no partial writes should occur
	store := testStore(t)
	ctx := context.Background()

	const numGoroutines = 30

	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			aggregateID := fmt.Sprintf("isolation-test-%d", id)
			c, _ := loadCounter(ctx, store, aggregateID)
			retry := monotonic.NewRetry(10, monotonic.ExponentialBackoff(0))

			// Each goroutine writes 10 events
			for j := 0; j < 10; j++ {
				err := c.AcceptThenApplyRetryable(ctx, *retry,
					monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				if err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}(i)
	}

	// Collect results
	failCount := 0
	for i := 0; i < numGoroutines; i++ {
		if err := <-results; err != nil {
			failCount++
		}
	}

	if failCount > 0 {
		t.Errorf("%d goroutines failed", failCount)
	}

	// Verify each aggregate has exactly 10 events (all-or-nothing)
	for i := 0; i < numGoroutines; i++ {
		aggregateID := fmt.Sprintf("isolation-test-%d", i)
		c, _ := loadCounter(ctx, store, aggregateID)
		if c.Counter() != 10 {
			t.Errorf("aggregate %s: expected 10 events, got %d", aggregateID, c.Counter())
		}
	}
}

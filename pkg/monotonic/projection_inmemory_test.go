package monotonic_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

func upsertSet[V any](key monotonic.ProjectionKey, value V) []monotonic.ProjectedSet[V] {
	return []monotonic.ProjectedSet[V]{{Key: key, Mode: monotonic.ReconcileUpsert, Values: []V{value}}}
}

func TestInMemoryProjectionPersistence_GetMissingReturnsZero(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[string](monotonic.ReconcileUpsert)

	val, err := p.Get(ctx, "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "" {
		t.Errorf("expected zero value, got %q", val)
	}
}

func TestInMemoryProjectionPersistence_SetThenGet(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[string](monotonic.ReconcileUpsert)

	if err := p.Set(ctx, upsertSet[string]("k", "hello"), 1); err != nil {
		t.Fatalf("Set: %v", err)
	}

	val, err := p.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "hello" {
		t.Errorf("expected 'hello', got %q", val)
	}
}

func TestInMemoryProjectionPersistence_OverwriteWithHigherCounter(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	p.Set(ctx, upsertSet[int]("k", 1), 1)
	p.Set(ctx, upsertSet[int]("k", 2), 2)

	val, _ := p.Get(ctx, "k")
	if val != 2 {
		t.Errorf("expected 2 after overwrite, got %d", val)
	}
}

func TestInMemoryProjectionPersistence_LatestGlobalCounterInitiallyZero(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	c, err := p.LatestGlobalCounter(ctx)
	if err != nil {
		t.Fatalf("LatestGlobalCounter: %v", err)
	}
	if c != 0 {
		t.Errorf("expected 0, got %d", c)
	}
}

func TestInMemoryProjectionPersistence_LatestGlobalCounterAdvances(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	for _, counter := range []uint64{1, 3, 7} {
		if err := p.Set(ctx, upsertSet[int]("k", 0), counter); err != nil {
			t.Fatalf("Set at counter %d: %v", counter, err)
		}
		got, _ := p.LatestGlobalCounter(ctx)
		if got != counter {
			t.Errorf("after Set(%d): LatestGlobalCounter=%d, want %d", counter, got, counter)
		}
	}
}

func TestInMemoryProjectionPersistence_LatestGlobalCounterDoesNotDecreaseForMultipleKeys(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	p.Set(ctx, upsertSet[int]("a", 0), 10)
	p.Set(ctx, upsertSet[int]("b", 0), 5)

	c, _ := p.LatestGlobalCounter(ctx)
	if c != 10 {
		t.Errorf("LatestGlobalCounter should not decrease: got %d, want 10", c)
	}
}

func TestInMemoryProjectionPersistence_StaleWriteRejected(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	p.Set(ctx, upsertSet[int]("k", 1), 5)

	err := p.Set(ctx, upsertSet[int]("k", 2), 3)
	if !errors.Is(err, monotonic.ErrProjectionStale) {
		t.Errorf("expected ErrProjectionStale, got %v", err)
	}

	val, _ := p.Get(ctx, "k")
	if val != 1 {
		t.Errorf("value should be unchanged after stale write: got %d", val)
	}
}

func TestInMemoryProjectionPersistence_EqualCounterIsIdempotent(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	p.Set(ctx, upsertSet[int]("k", 1), 5)

	if err := p.Set(ctx, upsertSet[int]("k", 1), 5); err != nil {
		t.Errorf("same-counter write should succeed (idempotent), got %v", err)
	}
}

func TestInMemoryProjectionPersistence_EmptyBatchIsNoOp(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	if err := p.Set(ctx, nil, 1); err != nil {
		t.Errorf("empty batch should not error, got %v", err)
	}
	if err := p.Set(ctx, []monotonic.ProjectedSet[int]{}, 1); err != nil {
		t.Errorf("empty slice batch should not error, got %v", err)
	}

	c, _ := p.LatestGlobalCounter(ctx)
	if c != 0 {
		t.Errorf("empty batch must not advance counter: got %d", c)
	}
}

func TestInMemoryProjectionPersistence_ZeroCounterRejected(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	err := p.Set(ctx, upsertSet[int]("k", 1), 0)
	if err == nil {
		t.Error("expected error for globalCounter=0")
	}
}

func TestInMemoryProjectionPersistence_BatchAtomicOnStale(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	p.Set(ctx, upsertSet[int]("k1", 10), 5)

	err := p.Set(ctx, []monotonic.ProjectedSet[int]{
		{Key: "k2", Mode: monotonic.ReconcileUpsert, Values: []int{20}},
		{Key: "k1", Mode: monotonic.ReconcileUpsert, Values: []int{99}},
	}, 3)
	if !errors.Is(err, monotonic.ErrProjectionStale) {
		t.Errorf("expected ErrProjectionStale, got %v", err)
	}

	val, _ := p.Get(ctx, "k2")
	if val != 0 {
		t.Errorf("k2 must not be written when batch is stale: got %d", val)
	}
	val, _ = p.Get(ctx, "k1")
	if val != 10 {
		t.Errorf("k1 must be unchanged: got %d", val)
	}
}

func TestInMemoryProjectionPersistence_MultipleKeysInBatch(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	err := p.Set(ctx, []monotonic.ProjectedSet[int]{
		{Key: "a", Mode: monotonic.ReconcileUpsert, Values: []int{1}},
		{Key: "b", Mode: monotonic.ReconcileUpsert, Values: []int{2}},
		{Key: "c", Mode: monotonic.ReconcileUpsert, Values: []int{3}},
	}, 1)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	for key, want := range map[string]int{"a": 1, "b": 2, "c": 3} {
		got, _ := p.Get(ctx, monotonic.ProjectionKey(key))
		if got != want {
			t.Errorf("key %q: expected %d, got %d", key, want, got)
		}
	}
}

func TestInMemoryProjectionPersistence_GetDoesNotMutateInternalState(t *testing.T) {
	ctx := context.Background()
	type mutable struct{ V int }
	p := monotonic.NewInMemoryProjectionPersistence[mutable](monotonic.ReconcileUpsert)

	p.Set(ctx, upsertSet[mutable]("k", mutable{V: 42}), 1)

	got, _ := p.Get(ctx, "k")
	got.V = 999

	got2, _ := p.Get(ctx, "k")
	if got2.V != 42 {
		t.Errorf("internal state was mutated: got %d, want 42", got2.V)
	}
}

func TestInMemoryProjectionPersistence_ConcurrentReadWrite(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int](monotonic.ReconcileUpsert)

	var wg sync.WaitGroup
	const goroutines = 50
	for i := 1; i <= goroutines; i++ {
		wg.Add(1)
		go func(counter uint64) {
			defer wg.Done()
			p.Set(ctx, upsertSet[int]("k", int(counter)), counter)
			p.Get(ctx, "k")
			p.LatestGlobalCounter(ctx)
		}(uint64(i))
	}
	wg.Wait()

	c, err := p.LatestGlobalCounter(ctx)
	if err != nil {
		t.Fatalf("LatestGlobalCounter: %v", err)
	}
	if c < 1 || c > goroutines {
		t.Errorf("LatestGlobalCounter out of range: %d", c)
	}
}

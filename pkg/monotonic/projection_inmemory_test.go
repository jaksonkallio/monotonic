package monotonic_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

func TestInMemoryProjectionPersistence_GetMissingReturnsZero(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[string]()

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
	p := monotonic.NewInMemoryProjectionPersistence[string]()

	if err := p.Set(ctx, []monotonic.Projected[string]{{Key: "k", Value: "hello"}}, 1); err != nil {
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
	p := monotonic.NewInMemoryProjectionPersistence[int]()

	p.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: 1}}, 1)
	p.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: 2}}, 2)

	val, _ := p.Get(ctx, "k")
	if val != 2 {
		t.Errorf("expected 2 after overwrite, got %d", val)
	}
}

func TestInMemoryProjectionPersistence_LatestGlobalCounterInitiallyZero(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int]()

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
	p := monotonic.NewInMemoryProjectionPersistence[int]()

	for _, counter := range []uint64{1, 3, 7} {
		if err := p.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: 0}}, counter); err != nil {
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
	p := monotonic.NewInMemoryProjectionPersistence[int]()

	p.Set(ctx, []monotonic.Projected[int]{{Key: "a", Value: 0}}, 10)
	p.Set(ctx, []monotonic.Projected[int]{{Key: "b", Value: 0}}, 5) // stale for b, but b doesn't exist yet

	// "a" was at counter 10, "b" write at counter 5 should fail (b doesn't exist yet,
	// but counter 5 < maxCounter of 10... actually no. ErrProjectionStale is per-key,
	// not global. "b" has no stored counter so 5 > 0, which succeeds.)
	// Let's verify: the stale check is "existing.globalCounter > globalCounter".
	// For b (new key), existing.globalCounter=0, so 0 > 5 is false → write succeeds.
	c, _ := p.LatestGlobalCounter(ctx)
	// maxCounter should still be 10 (not replaced by 5)
	if c != 10 {
		t.Errorf("LatestGlobalCounter should not decrease: got %d, want 10", c)
	}
}

func TestInMemoryProjectionPersistence_StaleWriteRejected(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int]()

	p.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: 1}}, 5)

	err := p.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: 2}}, 3)
	if !errors.Is(err, monotonic.ErrProjectionStale) {
		t.Errorf("expected ErrProjectionStale, got %v", err)
	}

	// Value must be unchanged.
	val, _ := p.Get(ctx, "k")
	if val != 1 {
		t.Errorf("value should be unchanged after stale write: got %d", val)
	}
}

func TestInMemoryProjectionPersistence_EqualCounterIsIdempotent(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int]()

	p.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: 1}}, 5)

	if err := p.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: 1}}, 5); err != nil {
		t.Errorf("same-counter write should succeed (idempotent), got %v", err)
	}
}

func TestInMemoryProjectionPersistence_EmptyBatchIsNoOp(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int]()

	if err := p.Set(ctx, nil, 1); err != nil {
		t.Errorf("empty batch should not error, got %v", err)
	}
	if err := p.Set(ctx, []monotonic.Projected[int]{}, 1); err != nil {
		t.Errorf("empty slice batch should not error, got %v", err)
	}

	c, _ := p.LatestGlobalCounter(ctx)
	if c != 0 {
		t.Errorf("empty batch must not advance counter: got %d", c)
	}
}

func TestInMemoryProjectionPersistence_ZeroCounterRejected(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int]()

	err := p.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: 1}}, 0)
	if err == nil {
		t.Error("expected error for globalCounter=0")
	}
}

func TestInMemoryProjectionPersistence_BatchAtomicOnStale(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int]()

	// k1 written at counter=5.
	p.Set(ctx, []monotonic.Projected[int]{{Key: "k1", Value: 10}}, 5)

	// Batch: k2 (new) + k1 (stale at counter=3 < 5). Must fail atomically.
	err := p.Set(ctx, []monotonic.Projected[int]{
		{Key: "k2", Value: 20},
		{Key: "k1", Value: 99},
	}, 3)
	if !errors.Is(err, monotonic.ErrProjectionStale) {
		t.Errorf("expected ErrProjectionStale, got %v", err)
	}

	// k2 must NOT have been written (atomic rollback).
	val, _ := p.Get(ctx, "k2")
	if val != 0 {
		t.Errorf("k2 must not be written when batch is stale: got %d", val)
	}
	// k1 must still hold its original value.
	val, _ = p.Get(ctx, "k1")
	if val != 10 {
		t.Errorf("k1 must be unchanged: got %d", val)
	}
}

func TestInMemoryProjectionPersistence_MultipleKeysInBatch(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int]()

	err := p.Set(ctx, []monotonic.Projected[int]{
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
		{Key: "c", Value: 3},
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
	p := monotonic.NewInMemoryProjectionPersistence[mutable]()

	p.Set(ctx, []monotonic.Projected[mutable]{{Key: "k", Value: mutable{V: 42}}}, 1)

	got, _ := p.Get(ctx, "k")
	got.V = 999 // mutate the returned copy

	// Internal state must not change.
	got2, _ := p.Get(ctx, "k")
	if got2.V != 42 {
		t.Errorf("internal state was mutated: got %d, want 42", got2.V)
	}
}

func TestInMemoryProjectionPersistence_ConcurrentReadWrite(t *testing.T) {
	ctx := context.Background()
	p := monotonic.NewInMemoryProjectionPersistence[int]()

	var wg sync.WaitGroup
	const goroutines = 50
	for i := 1; i <= goroutines; i++ {
		wg.Add(1)
		go func(counter uint64) {
			defer wg.Done()
			p.Set(ctx, []monotonic.Projected[int]{{Key: "k", Value: int(counter)}}, counter)
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

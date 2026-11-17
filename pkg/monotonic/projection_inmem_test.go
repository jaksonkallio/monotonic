package monotonic_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// summaryRow is a minimal projection row used to exercise InMemoryProjection.
type summaryRow struct {
	Balance int64
}

// appendEvent appends one event for the given aggregate to the store.
func appendEvent(ctx context.Context, t *testing.T, store monotonic.Store, aggType, aggID, eventType string, counter int64, payload any) {
	t.Helper()
	err := store.Append(ctx, monotonic.AggregateEvent{
		AggregateType: aggType,
		AggregateID:   aggID,
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.NewEvent(eventType, payload),
			Counter:    counter,
			AcceptedAt: time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("append %s counter=%d: %v", eventType, counter, err)
	}
}

func balanceDispatch() *monotonic.Dispatch[*monotonic.InMemoryProjectionTx[summaryRow]] {
	return monotonic.NewDispatch[*monotonic.InMemoryProjectionTx[summaryRow]]().
		On("account", "opened", func(_ context.Context, tx *monotonic.InMemoryProjectionTx[summaryRow], event monotonic.AggregateEvent) error {
			tx.Set(event.AggregateID, summaryRow{})
			return nil
		}).
		On("account", "deposited", func(_ context.Context, tx *monotonic.InMemoryProjectionTx[summaryRow], event monotonic.AggregateEvent) error {
			p, err := monotonic.ParsePayload[int64](event.Event)
			if err != nil {
				return err
			}
			row, _ := tx.Get(event.AggregateID)
			row.Balance += p
			tx.Set(event.AggregateID, row)
			return nil
		})
}

func TestInMemoryProjection_ProjectsThroughProjector(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	appendEvent(ctx, t, store, "account", "alice", "opened", 1, nil)
	appendEvent(ctx, t, store, "account", "alice", "deposited", 2, int64(100))
	appendEvent(ctx, t, store, "account", "bob", "opened", 1, nil)
	appendEvent(ctx, t, store, "account", "bob", "deposited", 2, int64(40))

	proj := monotonic.NewInMemoryProjection[summaryRow]()
	p, err := monotonic.NewProjector("balances", store, balanceDispatch(), proj, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if row, ok := proj.Get("alice"); !ok || row.Balance != 100 {
		t.Errorf("alice = %+v ok=%v, want balance 100", row, ok)
	}
	if row, ok := proj.Get("bob"); !ok || row.Balance != 40 {
		t.Errorf("bob = %+v ok=%v, want balance 40", row, ok)
	}
	if got := len(proj.All()); got != 2 {
		t.Errorf("All() len = %d, want 2", got)
	}
}

// runTx drives one modification transaction by hand, the way Projector.Update does, so the tests
// below can exercise the backend without going through a Projector.
func runTx(
	t *testing.T,
	proj *monotonic.InMemoryProjection[summaryRow],
	projectorName string,
	apply func(tx *monotonic.InMemoryProjectionTx[summaryRow]) error,
	commit func(ctx context.Context, tx *monotonic.InMemoryProjectionTx[summaryRow]) error,
) error {
	t.Helper()
	ctx := context.Background()

	tx, _, err := proj.LockStateForModification(ctx, projectorName)
	if err != nil {
		t.Fatalf("LockStateForModification: %v", err)
	}
	defer tx.Rollback(ctx)

	if err := apply(tx); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// advance commits a transaction as an event batch would.
func advance(proj *monotonic.InMemoryProjection[summaryRow], projectorName string, counter uint64) func(context.Context, *monotonic.InMemoryProjectionTx[summaryRow]) error {
	return func(ctx context.Context, tx *monotonic.InMemoryProjectionTx[summaryRow]) error {
		return proj.CommitAdvancedStateFromModification(ctx, tx, projectorName, counter)
	}
}

func TestInMemoryProjection_RollsBackOnHandlerError(t *testing.T) {
	proj := monotonic.NewInMemoryProjection[summaryRow]()

	// A successful batch establishes committed state.
	if err := runTx(t, proj, "p", func(tx *monotonic.InMemoryProjectionTx[summaryRow]) error {
		tx.Set("alice", summaryRow{Balance: 10})
		return nil
	}, advance(proj, "p", 1)); err != nil {
		t.Fatalf("first batch: %v", err)
	}

	// A failing batch mutates the buffer then errors; neither the writes nor the counter commit.
	wantErr := errors.New("boom")
	err := runTx(t, proj, "p", func(tx *monotonic.InMemoryProjectionTx[summaryRow]) error {
		tx.Set("alice", summaryRow{Balance: 999})
		tx.Set("mallory", summaryRow{Balance: 1})
		return wantErr
	}, advance(proj, "p", 2))
	if !errors.Is(err, wantErr) {
		t.Fatalf("second batch err = %v, want %v", err, wantErr)
	}

	if row, _ := proj.Get("alice"); row.Balance != 10 {
		t.Errorf("alice balance = %d, want 10 (rolled back)", row.Balance)
	}
	if _, ok := proj.Get("mallory"); ok {
		t.Error("mallory row should not have been committed")
	}
	if got := proj.State("p").GlobalCounter; got != 1 {
		t.Errorf("counter = %d, want 1 (not advanced)", got)
	}
}

// TestInMemoryProjection_RollbackReleasesTheLock pins that an abandoned transaction frees the
// modification lock; if it did not, the next LockStateForModification would block forever.
func TestInMemoryProjection_RollbackReleasesTheLock(t *testing.T) {
	ctx := context.Background()
	proj := monotonic.NewInMemoryProjection[summaryRow]()

	tx, _, err := proj.LockStateForModification(ctx, "p")
	if err != nil {
		t.Fatalf("LockStateForModification: %v", err)
	}
	tx.Set("alice", summaryRow{Balance: 10})
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	locked := make(chan struct{})
	go func() {
		next, _, err := proj.LockStateForModification(ctx, "p")
		if err == nil {
			next.Rollback(ctx)
		}
		close(locked)
	}()

	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		t.Fatal("LockStateForModification blocked after a rollback; the lock was not released")
	}

	if _, ok := proj.Get("alice"); ok {
		t.Error("rolled-back write was committed")
	}
}

// TestInMemoryProjection_RollbackAfterCommitIsNoop covers the projector deferring Rollback even on
// the committed path: the second end-of-transaction must not unlock the mutex a second time.
func TestInMemoryProjection_RollbackAfterCommitIsNoop(t *testing.T) {
	ctx := context.Background()
	proj := monotonic.NewInMemoryProjection[summaryRow]()

	tx, _, err := proj.LockStateForModification(ctx, "p")
	if err != nil {
		t.Fatalf("LockStateForModification: %v", err)
	}
	tx.Set("alice", summaryRow{Balance: 10})
	if err := proj.CommitAdvancedStateFromModification(ctx, tx, "p", 1); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback after commit: %v", err)
	}

	if row, _ := proj.Get("alice"); row.Balance != 10 {
		t.Errorf("alice balance = %d, want 10; the deferred Rollback undid a commit", row.Balance)
	}
	// A second commit on a finished transaction is a caller bug, not a silent no-op.
	if err := proj.CommitAdvancedStateFromModification(ctx, tx, "p", 2); err == nil {
		t.Error("committing a finished transaction should fail")
	}
}

func TestInMemoryProjection_TruncateClearsRowsAndCounter(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	appendEvent(ctx, t, store, "account", "alice", "opened", 1, nil)
	appendEvent(ctx, t, store, "account", "alice", "deposited", 2, int64(100))

	proj := monotonic.NewInMemoryProjection[summaryRow]()
	dispatch := balanceDispatch().OnTruncate(func(_ context.Context, tx *monotonic.InMemoryProjectionTx[summaryRow]) error {
		tx.Clear()
		return nil
	})

	p, err := monotonic.NewProjector("balances", store, dispatch, proj, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if row, _ := proj.Get("alice"); row.Balance != 100 {
		t.Fatalf("alice balance = %d before truncate, want 100", row.Balance)
	}

	if err := proj.EnqueueTruncate(ctx, "balances"); err != nil {
		t.Fatalf("EnqueueTruncate: %v", err)
	}

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update after EnqueueTruncate: %v", err)
	}
	if !result.Truncated {
		t.Error("Truncated = false, want true")
	}
	if got := len(proj.All()); got != 0 {
		t.Errorf("All() len = %d, want 0 after truncate", got)
	}
	if state := proj.State("balances"); state.GlobalCounter != 0 || state.TruncateRequested {
		t.Errorf("state = %+v after truncate, want zero", state)
	}

	// The rewound projector rebuilds from the start of the stream.
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update after truncate: %v", err)
	}
	if row, _ := proj.Get("alice"); row.Balance != 100 {
		t.Errorf("alice balance = %d after rebuild, want 100", row.Balance)
	}
}

// TestInMemoryProjection_ConcurrentTransactionsDoNotLoseWrites pins the reason the modification
// lock spans the whole transaction. Buffering clones the rows and then swaps them in wholesale, so
// clone-and-commit has to be indivisible; if two transactions overlapped they would clone the same
// starting rows and the later commit would discard the earlier one's writes. Each goroutine below
// writes a distinct key, so any lost commit shows up as a missing key.
//
// One InMemoryProjection backing several projectors is the case that makes this reachable:
// Projector.Update serializes a single projector against itself, but RunProjectors drives multiple
// projectors concurrently.
func TestInMemoryProjection_ConcurrentTransactionsDoNotLoseWrites(t *testing.T) {
	proj := monotonic.NewInMemoryProjection[summaryRow]()

	const writers = 8
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("account-%d", i)
			err := runTx(t, proj, key, func(tx *monotonic.InMemoryProjectionTx[summaryRow]) error {
				// Widen the window a concurrent commit could slip into, so an unlocked apply loses
				// writes reliably rather than occasionally.
				time.Sleep(2 * time.Millisecond)
				tx.Set(key, summaryRow{Balance: int64(i)})
				return nil
			}, advance(proj, key, 1))
			if err != nil {
				t.Errorf("transaction for %s: %v", key, err)
			}
		}(i)
	}
	wg.Wait()

	rows := proj.All()
	if len(rows) != writers {
		t.Fatalf("projection has %d rows, want %d; concurrent commits clobbered each other", len(rows), writers)
	}
	for i := 0; i < writers; i++ {
		key := fmt.Sprintf("account-%d", i)
		row, ok := rows[key]
		if !ok {
			t.Errorf("row %s missing", key)
			continue
		}
		if row.Balance != int64(i) {
			t.Errorf("row %s has balance %d, want %d", key, row.Balance, i)
		}
	}
}

// TestInMemoryProjection_ReadsDoNotBlockBehindAHandler covers mu being separate from the
// modification lock: an open transaction must not stop readers, and a handler calling back into
// Get must not deadlock.
func TestInMemoryProjection_ReadsDoNotBlockBehindAHandler(t *testing.T) {
	ctx := context.Background()
	proj := monotonic.NewInMemoryProjection[summaryRow]()

	tx, _, err := proj.LockStateForModification(ctx, "p")
	if err != nil {
		t.Fatalf("LockStateForModification: %v", err)
	}
	defer tx.Rollback(ctx)
	tx.Set("alice", summaryRow{Balance: 10})

	read := make(chan int, 1)
	go func() {
		read <- len(proj.All())
	}()

	select {
	case n := <-read:
		if n != 0 {
			t.Errorf("All() saw %d rows mid-transaction, want 0 uncommitted", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("All() blocked behind an open modification transaction")
	}
}

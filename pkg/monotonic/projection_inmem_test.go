package monotonic_test

import (
	"context"
	"errors"
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
	p, err := monotonic.NewProjector(ctx, "balances", store, balanceDispatch(), proj, 0)
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

func TestInMemoryProjection_RollsBackOnHandlerError(t *testing.T) {
	ctx := context.Background()
	proj := monotonic.NewInMemoryProjection[summaryRow]()

	// A successful event establishes committed state.
	if err := proj.RunEvent(ctx, "p", 1, func(_ context.Context, tx *monotonic.InMemoryProjectionTx[summaryRow]) error {
		tx.Set("alice", summaryRow{Balance: 10})
		return nil
	}); err != nil {
		t.Fatalf("RunEvent 1: %v", err)
	}

	// A failing event mutates the buffer then errors; neither the write nor the counter should commit.
	wantErr := errors.New("boom")
	err := proj.RunEvent(ctx, "p", 2, func(_ context.Context, tx *monotonic.InMemoryProjectionTx[summaryRow]) error {
		tx.Set("alice", summaryRow{Balance: 999})
		tx.Set("mallory", summaryRow{Balance: 1})
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("RunEvent 2 err = %v, want %v", err, wantErr)
	}

	if row, _ := proj.Get("alice"); row.Balance != 10 {
		t.Errorf("alice balance = %d, want 10 (rolled back)", row.Balance)
	}
	if _, ok := proj.Get("mallory"); ok {
		t.Error("mallory row should not have been committed")
	}
	if c, _ := proj.GetState(ctx, "p"); c != 1 {
		t.Errorf("counter = %d, want 1 (not advanced)", c)
	}
}

func TestInMemoryProjection_SkipsAlreadyAppliedCounter(t *testing.T) {
	ctx := context.Background()
	proj := monotonic.NewInMemoryProjection[summaryRow]()

	apply := func(bal int64) func(context.Context, *monotonic.InMemoryProjectionTx[summaryRow]) error {
		return func(_ context.Context, tx *monotonic.InMemoryProjectionTx[summaryRow]) error {
			tx.Set("alice", summaryRow{Balance: bal})
			return nil
		}
	}

	if err := proj.RunEvent(ctx, "p", 5, apply(10)); err != nil {
		t.Fatalf("RunEvent 5: %v", err)
	}
	// Replaying counter 5 (or lower) must be a no-op, leaving the committed row untouched.
	if err := proj.RunEvent(ctx, "p", 5, apply(999)); err != nil {
		t.Fatalf("RunEvent replay: %v", err)
	}
	if row, _ := proj.Get("alice"); row.Balance != 10 {
		t.Errorf("alice balance = %d, want 10 (replay skipped)", row.Balance)
	}
}

func TestInMemoryProjection_ResetClearsRowsAndCounter(t *testing.T) {
	ctx := context.Background()
	proj := monotonic.NewInMemoryProjection[summaryRow]()
	if err := proj.RunEvent(ctx, "p", 1, func(_ context.Context, tx *monotonic.InMemoryProjectionTx[summaryRow]) error {
		tx.Set("alice", summaryRow{Balance: 10})
		return nil
	}); err != nil {
		t.Fatalf("RunEvent: %v", err)
	}

	if err := proj.ResetState(ctx, "p", func(_ context.Context, tx *monotonic.InMemoryProjectionTx[summaryRow]) error {
		tx.Clear()
		return nil
	}); err != nil {
		t.Fatalf("ResetState: %v", err)
	}

	if got := len(proj.All()); got != 0 {
		t.Errorf("All() len = %d, want 0 after reset", got)
	}
	if c, _ := proj.GetState(ctx, "p"); c != 0 {
		t.Errorf("counter = %d, want 0 after reset", c)
	}
}

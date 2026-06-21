package pgtest_test

import (
	"context"
	"testing"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
	"github.com/jaksonkallio/monotonic/pkg/store/postgres/pgtest"
)

// TestHarnessSmoke verifies that the harness returns components wired against a working
// Postgres, that the event store accepts an append, and that the projector backend has
// projector_state already migrated.
func TestHarnessSmoke(t *testing.T) {
	c := pgtest.Harness(t)
	ctx := context.Background()

	// Clean slate for this test.
	if _, err := c.Pool.Exec(ctx, "TRUNCATE events RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate events: %v", err)
	}
	if _, err := c.Pool.Exec(ctx, "TRUNCATE projector_state"); err != nil {
		t.Fatalf("truncate projector_state: %v", err)
	}

	if err := c.Store.Append(ctx, monotonic.AggregateEvent{
		AggregateType: "smoke",
		AggregateID:   "x",
		Event: monotonic.AcceptedEvent{
			Event:   monotonic.NewEvent[any]("did", nil),
			Counter: 1,
		},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	got, err := c.Backend.GetState(ctx, "never-ran")
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if got != 0 {
		t.Errorf("fresh projector state must be 0, got %d", got)
	}
}

// TestHarnessReuse verifies that two calls to Harness in the same test binary return
// the same components (i.e. the container is genuinely shared, not re-booted).
func TestHarnessReuse(t *testing.T) {
	a := pgtest.Harness(t)
	b := pgtest.Harness(t)
	if a.Pool != b.Pool || a.Store != b.Store || a.Backend != b.Backend {
		t.Errorf("expected Harness to return identical components across calls")
	}
}

package postgres_integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// These tests cover the invariant that a projector's resume rule depends on: for any two events, the
// one with the lower global_counter became visible first. global_counter is a BIGSERIAL and nextval is
// non-transactional, so without the advisory lock in Store.Append that invariant does not hold and a
// projector reading `global_counter > resume` silently drops events forever.
//
// TestGlobalCounterMatchesCommitOrder is the authoritative regression guard: it asserts the invariant
// directly, and with the lock removed it reports dozens of violations on every run.
//
// TestProjectorLosesNoEventsUnderConcurrentAppends demonstrates the consequence end to end, but it is
// only a supplementary guard. Detecting the loss requires a poll to land inside a window that is
// sub-millisecond wide, so with the lock removed it fails on roughly two runs in three rather than
// every run. Do not treat a green result from it as evidence the invariant holds; that is what
// TestGlobalCounterMatchesCommitOrder is for.
//
// TestProjectorToleratesSequenceGap guards against anyone "fixing" this by waiting for a contiguous
// run of counters, which would deadlock on the permanent gaps that rolled-back appends leave behind.

// countCommitOrderInversions returns the number of adjacent event pairs, ordered by global_counter,
// whose commit timestamps run backwards. Any inversion means some transaction committed after a
// transaction that was assigned a higher global_counter, which is exactly the window in which a
// polling projector can skip an event permanently.
//
// Requires track_commit_timestamp=on, set on the test container in TestMain.
func countCommitOrderInversions(tb testing.TB) int {
	tb.Helper()

	var inversions int
	err := sharedPool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM (
			SELECT
				pg_xact_commit_timestamp(xmin) AS commit_ts,
				LAG(pg_xact_commit_timestamp(xmin)) OVER (ORDER BY global_counter) AS prev_commit_ts
			FROM events
		) ordered
		WHERE commit_ts < prev_commit_ts
	`).Scan(&inversions)
	if err != nil {
		tb.Fatalf("count commit order inversions: %v", err)
	}
	return inversions
}

func TestGlobalCounterMatchesCommitOrder(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// Confirm the container actually has commit timestamps on, otherwise every comparison below is
	// against NULL and the test passes without asserting anything.
	var trackCommitTimestamp string
	if err := sharedPool.QueryRow(ctx, `SHOW track_commit_timestamp`).Scan(&trackCommitTimestamp); err != nil {
		t.Fatalf("read track_commit_timestamp: %v", err)
	}
	if trackCommitTimestamp != "on" {
		t.Fatalf("track_commit_timestamp must be on for this test, got %q", trackCommitTimestamp)
	}

	const numWriters = 16
	const eventsPerWriter = 25

	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := loadCounter(ctx, store, fmt.Sprintf("commit-order-%d", id))
			if err != nil {
				t.Errorf("hydrate writer %d: %v", id, err)
				return
			}
			for j := 0; j < eventsPerWriter; j++ {
				if err := c.AcceptThenApply(ctx, monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1})); err != nil {
					// Each writer owns its own aggregate, so a counter conflict here would be a real bug.
					t.Errorf("writer %d append %d: %v", id, j, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	var total int
	if err := sharedPool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&total); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if want := numWriters * eventsPerWriter; total != want {
		t.Fatalf("expected %d events, got %d", want, total)
	}

	// Guard against the whole run having serialized by accident, which would make a green result
	// meaningless. With this much concurrency the appends must have overlapped in time.
	var distinctCommits int
	if err := sharedPool.QueryRow(ctx,
		`SELECT count(DISTINCT pg_xact_commit_timestamp(xmin)) FROM events`,
	).Scan(&distinctCommits); err != nil {
		t.Fatalf("count distinct commits: %v", err)
	}
	if distinctCommits < 2 {
		t.Fatalf("expected many distinct commits, got %d; the test did not exercise concurrency", distinctCommits)
	}

	if inversions := countCommitOrderInversions(t); inversions != 0 {
		t.Errorf("global_counter order disagrees with commit order in %d place(s); a polling projector can permanently skip events", inversions)
	}
}

func TestProjectorLosesNoEventsUnderConcurrentAppends(t *testing.T) {
	backend := resetProjectorState(t)
	resetProjectionTable(t, "global_order_projection")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Each append writes a batch, which holds several counters uncommitted for the duration of all
	// its inserts. That widens the window in which a poll can observe a higher counter while a lower
	// one is still invisible, which is what makes this test able to detect a regression at all.
	const numWriters = 24
	const batchesPerWriter = 6
	const eventsPerBatch = 5
	const eventsPerWriter = batchesPerWriter * eventsPerBatch

	dispatch := monotonic.NewDispatch[pgx.Tx]().
		On("counter", eventIncremented, func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
			_, err := tx.Exec(ctx,
				`INSERT INTO global_order_projection (k, n) VALUES ($1, 1)
				 ON CONFLICT (k) DO UPDATE SET n = global_order_projection.n + 1`,
				event.AggregateID,
			)
			return err
		})

	projector, err := monotonic.NewProjector(ctx, "global_order", sharedStore, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("new projector: %v", err)
	}

	// Run the projector concurrently with the writers so its polls land inside the
	// insert-to-commit windows that make the skip possible. The poll interval is deliberately tiny:
	// every extra poll is another chance to observe the inconsistent window.
	projectorDone := make(chan error, 1)
	go func() { projectorDone <- projector.Run(ctx, 100*time.Microsecond) }()

	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := loadCounter(ctx, sharedStore, fmt.Sprintf("no-loss-%d", id))
			if err != nil {
				t.Errorf("hydrate writer %d: %v", id, err)
				return
			}
			batch := make([]monotonic.Event, eventsPerBatch)
			for j := range batch {
				batch[j] = monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1})
			}
			for j := 0; j < batchesPerWriter; j++ {
				if err := c.AcceptThenApply(ctx, batch...); err != nil {
					t.Errorf("writer %d batch %d: %v", id, j, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	// Drain whatever the background loop has not yet reached, then stop it.
	for {
		n, err := projector.Update(ctx)
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		if n == 0 {
			break
		}
	}
	cancel()
	if err := <-projectorDone; err != nil {
		t.Fatalf("projector run: %v", err)
	}

	var projected int64
	if err := sharedPool.QueryRow(context.Background(),
		`SELECT COALESCE(SUM(n), 0) FROM global_order_projection`,
	).Scan(&projected); err != nil {
		t.Fatalf("read projection: %v", err)
	}

	if want := int64(numWriters * eventsPerWriter); projected != want {
		t.Errorf("projection applied %d events, want %d; %d were silently skipped", projected, want, want-projected)
	}
}

func TestProjectorToleratesSequenceGap(t *testing.T) {
	backend := resetProjectorState(t)
	resetProjectionTable(t, "gap_projection")
	ctx := context.Background()

	// Burn a global_counter inside a transaction that rolls back. nextval is non-transactional, so the
	// value is consumed permanently and no event will ever carry it.
	tx, err := sharedPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO events (aggregate_type, aggregate_id, counter, event_type, payload, accepted_at)
		 VALUES ('thing', 'rolled-back', 1, 'ignored', NULL, now())`,
	); err != nil {
		t.Fatalf("insert doomed event: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	appendTestEvent(t, sharedStore, "after-gap", 1, "thing-happened")

	var counters []int64
	rows, err := sharedPool.Query(ctx, `SELECT global_counter FROM events ORDER BY global_counter`)
	if err != nil {
		t.Fatalf("read counters: %v", err)
	}
	for rows.Next() {
		var c int64
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan counter: %v", err)
		}
		counters = append(counters, c)
	}
	rows.Close()
	if len(counters) != 1 {
		t.Fatalf("expected 1 surviving event, got %d", len(counters))
	}
	if counters[0] == 1 {
		t.Skip("rolled-back insert did not consume a sequence value, so there is no gap to tolerate")
	}

	dispatch := monotonic.NewDispatch[pgx.Tx]().
		On("thing", "thing-happened", func(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
			_, err := tx.Exec(ctx, `INSERT INTO gap_projection (k, n) VALUES ($1, $2)`, event.AggregateID, event.Event.GlobalCounter)
			return err
		})

	projector, err := monotonic.NewProjector(ctx, "gap", sharedStore, dispatch, backend, 0)
	if err != nil {
		t.Fatalf("new projector: %v", err)
	}

	// The projector must process the event sitting above the gap rather than waiting for the
	// counter that will never arrive.
	n, err := projector.Update(ctx)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if n != 1 {
		t.Fatalf("projector processed %d events across a permanent sequence gap, want 1", n)
	}

	var got int64
	if err := sharedPool.QueryRow(ctx, `SELECT n FROM gap_projection WHERE k = 'after-gap'`).Scan(&got); err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if got != counters[0] {
		t.Errorf("projected global_counter %d, want %d", got, counters[0])
	}
}

// TestAppendCounterConflictStillDetectedWithLockHeld covers the one behavioral consequence of taking
// the advisory lock after the max-counter reads rather than before them: two concurrent appends can
// both read the same max counter, and the loser is rejected by the unique constraint instead of by
// pre-validation. Either way the caller must see ErrCounterConflict.
func TestAppendCounterConflictStillDetectedWithLockHeld(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	appendTestEvent(t, store, "conflict", 1, "thing-happened")

	err := store.Append(ctx, monotonic.AggregateEvent{
		AggregateType: "thing",
		AggregateID:   "conflict",
		Event: monotonic.AcceptedEvent{
			Event:      monotonic.NewEvent[any]("thing-happened", nil),
			Counter:    1,
			AcceptedAt: time.Now(),
		},
	})
	if !errors.Is(err, monotonic.ErrCounterConflict) {
		t.Fatalf("expected ErrCounterConflict, got %v", err)
	}
}

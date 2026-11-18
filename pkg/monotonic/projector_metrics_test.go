package monotonic_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricUpdates      = "monotonic_projector_updates_total"
	metricAppliedEvent = "monotonic_projector_applied_events_total"
	metricLastSuccess  = "monotonic_projector_last_successful_update_timestamp_seconds"
)

// The projector metrics live on prometheus.DefaultRegisterer, so every test in this package shares them.
// Each test below uses a projector name of its own, which is also the label the series are keyed by, so what one
// test records stays invisible to the others.

// gatheredValue reads one series out of the default gatherer, reporting whether it is there at all.
//
// Reading through WithLabelValues would not do: that creates the child on demand, so a series that was never
// recorded and a series sitting at zero would be indistinguishable, and "never recorded" is exactly what several of
// these tests assert.
func gatheredValue(t *testing.T, name string, labels map[string]string) (float64, bool) {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	for _, family := range families {
		if family.GetName() != name {
			continue
		}

		for _, metric := range family.GetMetric() {
			found := make(map[string]string, len(metric.GetLabel()))
			for _, pair := range metric.GetLabel() {
				found[pair.GetName()] = pair.GetValue()
			}

			matches := true
			for key, want := range labels {
				if found[key] != want {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}

			if counter := metric.GetCounter(); counter != nil {
				return counter.GetValue(), true
			}
			if gauge := metric.GetGauge(); gauge != nil {
				return gauge.GetValue(), true
			}
		}
	}

	return 0, false
}

// wantValue asserts a series exists and holds want.
func wantValue(t *testing.T, name string, labels map[string]string, want float64) {
	t.Helper()
	got, ok := gatheredValue(t, name, labels)
	if !ok {
		t.Fatalf("%s%v: series missing, want %v", name, labels, want)
	}
	if got != want {
		t.Errorf("%s%v = %v, want %v", name, labels, got, want)
	}
}

func TestProjectorMetrics_NewProjectorSeedsSeriesAtZero(t *testing.T) {
	// Seeded so a rate() over the error series matches something from startup rather than only after the first
	// failure, which is when an alert on a projector that has never failed would otherwise be matching nothing.
	// The freshness gauge is seeded too, so a projector that never manages a clean Update reads as stale rather than
	// as no series at all.
	const name = "metrics-seeded"
	if _, err := monotonic.NewProjector(name, monotonic.NewInMemoryStore(), noopDispatch(), newFakeBackend(), 0); err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	wantValue(t, metricUpdates, map[string]string{"projector": name, "status": "success"}, 0)
	wantValue(t, metricUpdates, map[string]string{"projector": name, "status": "error"}, 0)
	wantValue(t, metricAppliedEvent, map[string]string{"projector": name}, 0)
	wantValue(t, metricLastSuccess, map[string]string{"projector": name}, 0)
}

func TestProjectorMetrics_SuccessfulUpdateRecordsEventsAndFreshness(t *testing.T) {
	const name = "metrics-success"
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)
	emitEvent(ctx, t, store, 3)

	p, err := monotonic.NewProjector(name, store, noopDispatch(), newFakeBackend(), 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	before := time.Now()
	if _, err := p.Update(ctx); err != nil {
		t.Fatalf("Update: %v", err)
	}

	wantValue(t, metricUpdates, map[string]string{"projector": name, "status": "success"}, 1)
	wantValue(t, metricUpdates, map[string]string{"projector": name, "status": "error"}, 0)
	// Counted once per event rather than once per Update, so a batch of three reports three.
	wantValue(t, metricAppliedEvent, map[string]string{"projector": name}, 3)

	stamp, ok := gatheredValue(t, metricLastSuccess, map[string]string{"projector": name})
	if !ok {
		t.Fatal("last successful update gauge missing after a clean Update")
	}
	if stamp < float64(before.Unix()) || stamp > float64(time.Now().Add(time.Second).Unix()) {
		t.Errorf("last successful update = %v, want a timestamp within this test's run", stamp)
	}
}

// TestProjectorMetrics_IdleUpdateIsStillFresh pins the reason the freshness gauge is alertable: a projector that is
// caught up and finding nothing is healthy, so it must keep the gauge moving even though it applies no events.
func TestProjectorMetrics_IdleUpdateIsStillFresh(t *testing.T) {
	const name = "metrics-idle"
	ctx := context.Background()

	p, err := monotonic.NewProjector(name, monotonic.NewInMemoryStore(), noopDispatch(), newFakeBackend(), 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if result.Updated() {
		t.Fatalf("expected an idle Update, got %+v", result)
	}

	wantValue(t, metricUpdates, map[string]string{"projector": name, "status": "success"}, 1)
	wantValue(t, metricAppliedEvent, map[string]string{"projector": name}, 0)

	if stamp, ok := gatheredValue(t, metricLastSuccess, map[string]string{"projector": name}); !ok || stamp <= 0 {
		t.Errorf("last successful update = %v (present=%v) after an idle Update, want a real timestamp", stamp, ok)
	}
}

// TestProjectorMetrics_FailedUpdateRecordsOnlyTheFailure covers the all-or-nothing accounting: the batch rolled
// back, so its events must not be counted as applied, and the projector must not be reported as fresh.
func TestProjectorMetrics_FailedUpdateRecordsOnlyTheFailure(t *testing.T) {
	const name = "metrics-error"
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)

	p, err := monotonic.NewProjector(name, store, failingDispatch(errors.New("apply boom")), newFakeBackend(), 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	if _, err := p.Update(ctx); err == nil {
		t.Fatal("expected Update to fail")
	}

	wantValue(t, metricUpdates, map[string]string{"projector": name, "status": "error"}, 1)
	wantValue(t, metricUpdates, map[string]string{"projector": name, "status": "success"}, 0)
	wantValue(t, metricAppliedEvent, map[string]string{"projector": name}, 0)

	// Still at its seeded zero: a failed Update must not make the projector look fresh.
	wantValue(t, metricLastSuccess, map[string]string{"projector": name}, 0)
}

// TestProjectorMetrics_RetriedBatchIsCountedOnce guards against counting events per attempt rather than per commit:
// the first Update dispatches both events and fails on the second, and only the retry's committed events count.
func TestProjectorMetrics_RetriedBatchIsCountedOnce(t *testing.T) {
	const name = "metrics-retry"
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)
	emitEvent(ctx, t, store, 2)

	cr := &counterRef{}
	p, err := monotonic.NewProjector(name, store, failOnNthDispatch(cr, 2, errors.New("apply boom")), newFakeBackend(), 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	if _, err := p.Update(ctx); err == nil {
		t.Fatal("expected the first Update to fail")
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// The handler ran four times: both events on the attempt that failed on the second, then both again on the retry.
	// Only the two the retry committed are counted.
	if cr.n != 4 {
		t.Fatalf("handler called %d times, want 4 (a failed two-event attempt plus the retry)", cr.n)
	}
	wantValue(t, metricAppliedEvent, map[string]string{"projector": name}, 2)
	wantValue(t, metricUpdates, map[string]string{"projector": name, "status": "error"}, 1)
}

// TestProjectorMetrics_TruncateCountsAsASuccessfulUpdate pins that servicing a truncate keeps the projector looking
// alive without inflating the applied-events count, since a truncate clears the projection rather than applying to it.
func TestProjectorMetrics_TruncateCountsAsASuccessfulUpdate(t *testing.T) {
	const name = "metrics-truncate"
	ctx := context.Background()

	truncated := 0
	dispatch := monotonic.NewDispatch[*testTx]().
		On("test", "happened", func(context.Context, *testTx, monotonic.AggregateEvent) error { return nil }).
		OnTruncate(func(context.Context, *testTx) error {
			truncated++
			return nil
		})

	p, err := monotonic.NewProjector(name, monotonic.NewInMemoryStore(), dispatch, newFakeBackend(), 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if err := p.RequestTruncate(ctx); err != nil {
		t.Fatalf("RequestTruncate: %v", err)
	}

	result, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !result.Truncated || truncated != 1 {
		t.Fatalf("expected the truncate to be serviced, got %+v with %d handler calls", result, truncated)
	}

	wantValue(t, metricUpdates, map[string]string{"projector": name, "status": "success"}, 1)
	wantValue(t, metricAppliedEvent, map[string]string{"projector": name}, 0)
}

// TestProjectorMetrics_SeriesAreKeyedPerProjector pins the label: two projectors sharing a process must not share a
// count, which is the whole point of the metrics being labelled rather than global.
func TestProjectorMetrics_SeriesAreKeyedPerProjector(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()
	emitEvent(ctx, t, store, 1)
	backend := newFakeBackend()

	busy, err := monotonic.NewProjector("metrics-keyed-busy", store, noopDispatch(), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	idle, err := monotonic.NewProjector("metrics-keyed-idle", store, noopDispatch(), backend, 0)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	// Seeded past the only event so this one has nothing to do.
	backend.seed("metrics-keyed-idle", math.MaxInt64)

	if _, err := busy.Update(ctx); err != nil {
		t.Fatalf("busy Update: %v", err)
	}
	if _, err := idle.Update(ctx); err != nil {
		t.Fatalf("idle Update: %v", err)
	}

	wantValue(t, metricAppliedEvent, map[string]string{"projector": "metrics-keyed-busy"}, 1)
	wantValue(t, metricAppliedEvent, map[string]string{"projector": "metrics-keyed-idle"}, 0)
}

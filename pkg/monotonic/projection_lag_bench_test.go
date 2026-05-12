package monotonic_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
	"github.com/jaksonkallio/monotonic/pkg/mtest"
)

// lagLogic records wall-time lag (now - AcceptedAt) for each event the projector applies, into a Latencies the bench reads at end.
type lagLogic struct {
	walltime *mtest.Latencies
}

func (l *lagLogic) EventFilters() []monotonic.EventFilter {
	return []monotonic.EventFilter{{AggregateType: benchAggregateType}}
}

func (l *lagLogic) Apply(_ context.Context, _ monotonic.ProjectionReader[int], event monotonic.AggregateEvent) ([]monotonic.Projected[int], error) {
	l.walltime.Record(time.Since(event.Event.AcceptedAt))
	// Emit one update so InMemoryProjectionPersistence.Set runs the full write path on every event instead of being a no-op.
	return []monotonic.Projected[int]{{Key: monotonic.ProjectionKeySummary, Value: 1}}, nil
}

// BenchmarkProjection_LagSteadyState runs a rate-limited writer alongside a Projector and samples projection lag two ways:
//
//	wall_us:       per applied event, (now - AcceptedAt) — what an end user observing the projection would experience.
//	events_behind: at 5ms cadence, (events written - events processed) — backlog size.
//
// Reports p50/p95/p99 for both plus the achieved write rate. This is the headline benchmark for the "how stale can projections get under load?" question.
func BenchmarkProjection_LagSteadyState(b *testing.B) {
	type config struct {
		name         string
		rateHz       int           // target writer rate, events/sec
		pollInterval time.Duration // projector poll interval
	}
	configs := []config{
		{"rate=1000_poll=10ms", 1000, 10 * time.Millisecond},
		{"rate=5000_poll=10ms", 5000, 10 * time.Millisecond},
		{"rate=5000_poll=1ms", 5000, 1 * time.Millisecond},
		{"rate=10000_poll=1ms", 10000, 1 * time.Millisecond},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			store := monotonic.NewInMemoryStore()
			persist := monotonic.NewInMemoryProjectionPersistence[int]()
			walltimeLag := mtest.NewLatencies()
			// Each event-lag sample is stored in a Latencies as time.Duration(N) — N is the count of events behind. Reusing Latencies' percentile machinery; units are interpreted as "events" when reading back.
			eventsLag := mtest.NewLatencies()

			projector, err := monotonic.NewProjector(ctx, store, &lagLogic{walltime: walltimeLag}, persist)
			if err != nil {
				b.Fatalf("NewProjector: %v", err)
			}
			c, err := loadBenchCounter(ctx, store, "lag-test")
			if err != nil {
				b.Fatalf("loadBenchCounter: %v", err)
			}

			var writerCount atomic.Int64

			samplerStop := make(chan struct{})
			samplerDone := make(chan struct{})
			go func() {
				defer close(samplerDone)
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-samplerStop:
						return
					case <-ticker.C:
						lag := max(writerCount.Load()-int64(projector.GlobalCounter()), 0)
						eventsLag.Record(time.Duration(lag))
					}
				}
			}()

			projDone := make(chan error, 1)
			go func() {
				projDone <- projector.Run(ctx, cfg.pollInterval)
			}()

			period := time.Second / time.Duration(cfg.rateHz)
			next := time.Now()
			start := time.Now()
			b.ResetTimer()
			for range b.N {
				now := time.Now()
				if next.After(now) {
					time.Sleep(next.Sub(now))
				}
				next = next.Add(period)
				if err := c.AcceptThenApply(ctx, monotonic.NewEvent(benchEventIncrement, benchIncrementPayload{Amount: 1})); err != nil {
					b.Fatalf("AcceptThenApply: %v", err)
				}
				writerCount.Add(1)
			}
			b.StopTimer()
			elapsed := time.Since(start)

			// Stop sampler before cancelling the projector so post-writer drain doesn't pollute the steady-state distribution with zero-lag samples.
			close(samplerStop)
			<-samplerDone
			cancel()
			<-projDone

			if elapsed > 0 {
				b.ReportMetric(float64(b.N)/elapsed.Seconds(), "achieved_rate")
			}
			if walltimeLag.Count() > 0 {
				b.ReportMetric(float64(walltimeLag.Percentile(0.50).Microseconds()), "p50_wall_us")
				b.ReportMetric(float64(walltimeLag.Percentile(0.95).Microseconds()), "p95_wall_us")
				b.ReportMetric(float64(walltimeLag.Percentile(0.99).Microseconds()), "p99_wall_us")
				b.ReportMetric(float64(walltimeLag.Percentile(1.00).Microseconds()), "max_wall_us")
			}
			if eventsLag.Count() > 0 {
				b.ReportMetric(float64(eventsLag.Percentile(0.50).Nanoseconds()), "p50_events_behind")
				b.ReportMetric(float64(eventsLag.Percentile(0.95).Nanoseconds()), "p95_events_behind")
				b.ReportMetric(float64(eventsLag.Percentile(0.99).Nanoseconds()), "p99_events_behind")
				b.ReportMetric(float64(eventsLag.Percentile(1.00).Nanoseconds()), "max_events_behind")
			}
		})
	}
}

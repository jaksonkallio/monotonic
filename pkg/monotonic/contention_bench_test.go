package monotonic_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
	"github.com/jaksonkallio/monotonic/pkg/mtest"
)

// benchCounter is the minimal aggregate used by contention benchmarks: a single-event-type incrementing counter with no invariants beyond event-type validity.
type benchCounter struct {
	*monotonic.AggregateBase
	Value int64
}

type benchIncrementPayload struct{ Amount int64 }

const benchEventIncrement = "incremented"
const benchAggregateType = "counter"

func (c *benchCounter) Apply(event monotonic.AcceptedEvent) {
	if event.Type == benchEventIncrement {
		if p, err := monotonic.ParsePayload[benchIncrementPayload](event); err == nil {
			c.Value += p.Amount
		}
	}
}

func (c *benchCounter) ShouldAccept(event monotonic.Event) error {
	if event.Type == benchEventIncrement {
		return nil
	}
	return errors.New("unknown event type")
}

func loadBenchCounter(ctx context.Context, store monotonic.Store, id string) (*benchCounter, error) {
	return monotonic.Hydrate(ctx, store, benchAggregateType, id, func(b *monotonic.AggregateBase) *benchCounter {
		return &benchCounter{AggregateBase: b}
	})
}

// BenchmarkContention_Zipfian distributes writes across K aggregates with Zipfian skew, mirroring real workloads where a few keys are hot and the long tail is uncontended.
// Reports attempts/op, retries/op, and p50/p95/p99 latency so the operating cost of optimistic concurrency is visible — not just throughput.
func BenchmarkContention_Zipfian(b *testing.B) {
	type config struct {
		name          string
		numAggregates int
		skew          float64 // Zipf s parameter; must be > 1 (math/rand/v2 requirement). Higher = more skewed (hotter keys hotter).
		concurrency   int
	}
	configs := []config{
		// Hold concurrency at 8 and sweep skew at K=100 to show how hotness affects retries.
		{"K=100_skew=1.05_c=8", 100, 1.05, 8},
		{"K=100_skew=1.2_c=8", 100, 1.2, 8},
		{"K=100_skew=1.5_c=8", 100, 1.5, 8},
		// Larger key set with the same skew: fewer collisions.
		{"K=1000_skew=1.2_c=8", 1000, 1.2, 8},
		// Hold K and skew, sweep concurrency to find the contention cliff.
		{"K=100_skew=1.2_c=16", 100, 1.2, 16},
		{"K=100_skew=1.2_c=32", 100, 1.2, 32},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			store := monotonic.NewInMemoryStore()
			ctx := context.Background()
			latencies := mtest.NewLatencies()
			var attempts atomic.Int64

			retry := monotonic.Retry{
				MaxAttempts: 200,
				Backoff:     monotonic.ConstantBackoff(0),
				OnAttempt: func(_ int, _ error) {
					attempts.Add(1)
				},
			}

			opsPerGoroutine := max(b.N/cfg.concurrency, 1)

			var wg sync.WaitGroup
			b.ResetTimer()
			for g := range cfg.concurrency {
				wg.Add(1)
				go func(gID int) {
					defer wg.Done()
					rng := rand.New(rand.NewPCG(uint64(gID+1), 42))
					// Zipf yields k in [0, imax] with P(k) ~ (k+v)^-s; s must be > 1, v >= 1.
					zipf := rand.NewZipf(rng, cfg.skew, 1.0, uint64(cfg.numAggregates-1))
					if zipf == nil {
						b.Errorf("NewZipf returned nil for s=%v v=1 imax=%d", cfg.skew, cfg.numAggregates-1)
						return
					}
					// Per-goroutine cache of hydrated aggregates so we pay hydration once per (goroutine, key), like a real app holding aggregate instances behind a cache.
					cache := make(map[uint64]*benchCounter)
					for range opsPerGoroutine {
						idx := zipf.Uint64()
						c, ok := cache[idx]
						if !ok {
							var err error
							c, err = loadBenchCounter(ctx, store, fmt.Sprintf("counter-%d", idx))
							if err != nil {
								b.Errorf("loadBenchCounter: %v", err)
								return
							}
							cache[idx] = c
						}
						start := time.Now()
						if err := c.AcceptThenApplyRetryable(ctx, retry, monotonic.NewEvent(benchEventIncrement, benchIncrementPayload{Amount: 1})); err != nil {
							b.Errorf("AcceptThenApplyRetryable: %v", err)
							return
						}
						latencies.Record(time.Since(start))
					}
				}(g)
			}
			wg.Wait()
			b.StopTimer()

			totalOps := int64(opsPerGoroutine * cfg.concurrency)
			if totalOps > 0 {
				a := attempts.Load()
				b.ReportMetric(float64(a)/float64(totalOps), "attempts/op")
				// retries/op = (total attempts - successes) / successes; successes = totalOps when every op eventually succeeded.
				b.ReportMetric(float64(a-totalOps)/float64(totalOps), "retries/op")
			}
			latencies.ReportTo(b)
		})
	}
}

package postgres_integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// Benchmark single event appends to PostgreSQL
func BenchmarkPostgres_Append_SingleEvent(b *testing.B) {
	store := testStore(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregateID := fmt.Sprintf("counter-%d", i)
		c, _ := loadCounter(ctx, store, aggregateID)
		c.AcceptThenApply(ctx, monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
	}
}

// Benchmark appends to the same aggregate (sequential)
func BenchmarkPostgres_Append_SameAggregateSequential(b *testing.B) {
	store := testStore(b)
	ctx := context.Background()
	c, _ := loadCounter(ctx, store, "counter")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.AcceptThenApply(ctx, monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
	}
}

// Benchmark appends to different aggregates
func BenchmarkPostgres_Append_DifferentAggregates(b *testing.B) {
	store := testStore(b)
	ctx := context.Background()

	// Pre-create aggregates
	counters := make([]*counter, 100)
	for i := 0; i < 100; i++ {
		counters[i], _ = loadCounter(ctx, store, fmt.Sprintf("counter-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % 100
		counters[idx].AcceptThenApply(ctx, monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
	}
}

// Benchmark hydration (event replay) from PostgreSQL
func BenchmarkPostgres_Hydrate(b *testing.B) {
	store := testStore(b)
	ctx := context.Background()

	// Pre-populate with events
	for _, numEvents := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("events-%d", numEvents), func(b *testing.B) {
			aggregateID := fmt.Sprintf("counter-hydrate-%d", numEvents)
			c, _ := loadCounter(ctx, store, aggregateID)
			for j := 0; j < numEvents; j++ {
				c.AcceptThenApply(ctx, monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				loadCounter(ctx, store, aggregateID)
			}
		})
	}
}

// Benchmark concurrent appends to different aggregates (low contention)
func BenchmarkPostgres_Append_ConcurrentDifferentAggregates(b *testing.B) {
	for _, concurrency := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("concurrency-%d", concurrency), func(b *testing.B) {
			store := testStore(b)
			ctx := context.Background()

			b.ResetTimer()

			var wg sync.WaitGroup
			opsPerGoroutine := b.N / concurrency

			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					aggregateID := fmt.Sprintf("counter-%d", id)
					c, _ := loadCounter(ctx, store, aggregateID)

					for j := 0; j < opsPerGoroutine; j++ {
						c.AcceptThenApply(ctx, monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
					}
				}(i)
			}
			wg.Wait()
		})
	}
}

// Benchmark concurrent appends to the same aggregate (high contention)
func BenchmarkPostgres_Append_HighContention(b *testing.B) {
	for _, concurrency := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("concurrency-%d", concurrency), func(b *testing.B) {
			store := testStore(b)
			ctx := context.Background()
			retry := monotonic.NewRetry(20, monotonic.ExponentialBackoff(0))

			b.ResetTimer()

			var wg sync.WaitGroup
			opsPerGoroutine := b.N / concurrency

			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					c, _ := loadCounter(ctx, store, "shared-counter")
					for j := 0; j < opsPerGoroutine; j++ {
						c.AcceptThenApplyRetryable(ctx, *retry,
							monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
					}
				}()
			}
			wg.Wait()
		})
	}
}

// Benchmark global event loading from PostgreSQL
func BenchmarkPostgres_LoadGlobalEvents(b *testing.B) {
	ctx := context.Background()

	// Pre-populate with events across multiple aggregates
	for _, totalEvents := range []int{100, 1000} {
		b.Run(fmt.Sprintf("total-events-%d", totalEvents), func(b *testing.B) {
			store := testStore(b)
			numAggregates := 10
			eventsPerAggregate := totalEvents / numAggregates

			for i := 0; i < numAggregates; i++ {
				c, _ := loadCounter(ctx, store, fmt.Sprintf("counter-%d", i))
				for j := 0; j < eventsPerAggregate; j++ {
					c.AcceptThenApply(ctx, monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				}
			}

			filters := []monotonic.EventFilter{{AggregateType: "counter"}}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				store.LoadGlobalEvents(ctx, filters, 0, 0)
			}
		})
	}
}

// Benchmark transaction overhead
func BenchmarkPostgres_TransactionOverhead(b *testing.B) {
	store := testStore(b)
	ctx := context.Background()

	b.Run("single-event", func(b *testing.B) {
		c, _ := loadCounter(ctx, store, "tx-overhead-1")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.AcceptThenApply(ctx, monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
		}
	})
}

// Benchmark retry mechanism under contention
func BenchmarkPostgres_Retry_UnderContention(b *testing.B) {
	for _, maxRetries := range []int{5, 10, 20} {
		b.Run(fmt.Sprintf("max-retries-%d", maxRetries), func(b *testing.B) {
			store := testStore(b)
			ctx := context.Background()

			b.ResetTimer()

			var wg sync.WaitGroup
			concurrency := 8
			opsPerGoroutine := b.N / concurrency

			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					c, _ := loadCounter(ctx, store, "contention-test")
					retry := monotonic.NewRetry(maxRetries, monotonic.ExponentialBackoff(0))

					for j := 0; j < opsPerGoroutine; j++ {
						c.AcceptThenApplyRetryable(ctx, *retry,
							monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
					}
				}()
			}
			wg.Wait()
		})
	}
}

// Benchmark throughput: events per second
func BenchmarkPostgres_Throughput(b *testing.B) {
	for _, concurrency := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("concurrency-%d", concurrency), func(b *testing.B) {
			store := testStore(b)
			ctx := context.Background()

			b.ResetTimer()

			var wg sync.WaitGroup
			opsPerGoroutine := b.N / concurrency

			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					aggregateID := fmt.Sprintf("throughput-%d", id)
					c, _ := loadCounter(ctx, store, aggregateID)

					for j := 0; j < opsPerGoroutine; j++ {
						c.AcceptThenApply(ctx, monotonic.NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
					}
				}(i)
			}
			wg.Wait()
		})
	}
}

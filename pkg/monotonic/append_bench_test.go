package monotonic

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// Benchmark single event appends
func BenchmarkAppend_SingleEvent(b *testing.B) {
	store := NewInMemoryStore()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregateID := fmt.Sprintf("counter-%d", i)
		c, _ := loadCounter(ctx, store, aggregateID)
		c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
	}
}

// Benchmark appends to the same aggregate (sequential)
func BenchmarkAppend_SameAggregateSequential(b *testing.B) {
	store := NewInMemoryStore()
	ctx := context.Background()
	c, _ := loadCounter(ctx, store, "counter")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
	}
}

// Benchmark appends to different aggregates
func BenchmarkAppend_DifferentAggregates(b *testing.B) {
	store := NewInMemoryStore()
	ctx := context.Background()

	// Pre-create aggregates
	counters := make([]*counter, 1000)
	for i := 0; i < 1000; i++ {
		counters[i], _ = loadCounter(ctx, store, fmt.Sprintf("counter-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % 1000
		counters[idx].AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
	}
}

// Benchmark hydration (event replay)
func BenchmarkHydrate(b *testing.B) {
	store := NewInMemoryStore()
	ctx := context.Background()

	// Pre-populate with events
	for _, numEvents := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("events-%d", numEvents), func(b *testing.B) {
			aggregateID := fmt.Sprintf("counter-hydrate-%d", numEvents)
			c, _ := loadCounter(ctx, store, aggregateID)
			for j := 0; j < numEvents; j++ {
				c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				loadCounter(ctx, store, aggregateID)
			}
		})
	}
}

// Benchmark concurrent appends with different concurrency levels
func BenchmarkAppend_Concurrent(b *testing.B) {
	for _, concurrency := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("concurrency-%d", concurrency), func(b *testing.B) {
			store := NewInMemoryStore()
			ctx := context.Background()

			b.SetParallelism(concurrency)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				// Each goroutine gets its own aggregate to avoid conflicts
				aggregateID := fmt.Sprintf("counter-%p", &pb)
				c, _ := loadCounter(ctx, store, aggregateID)

				for pb.Next() {
					c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				}
			})
		})
	}
}

// Benchmark concurrent appends to the same aggregate (high contention)
func BenchmarkAppend_HighContention(b *testing.B) {
	for _, concurrency := range []int{2, 4, 8, 16} {
		b.Run(fmt.Sprintf("concurrency-%d", concurrency), func(b *testing.B) {
			store := NewInMemoryStore()
			ctx := context.Background()
			retry := NewRetry(20, ExponentialBackoff(0))

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
							NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
					}
				}()
			}
			wg.Wait()
		})
	}
}

// Benchmark Accept phase (validation and counter assignment)
func BenchmarkAccept_SingleEvent(b *testing.B) {
	store := NewInMemoryStore()
	ctx := context.Background()

	// Pre-create aggregates with varying event counts
	for _, eventCount := range []int{0, 10, 100, 1000} {
		b.Run(fmt.Sprintf("existing-events-%d", eventCount), func(b *testing.B) {
			aggregateID := fmt.Sprintf("counter-accept-%d", eventCount)
			c, _ := loadCounter(ctx, store, aggregateID)
			for j := 0; j < eventCount; j++ {
				c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
			}

			// Reload to ensure we're not caching
			c, _ = loadCounter(ctx, store, aggregateID)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.Accept(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
			}
		})
	}
}

// Benchmark global event loading
func BenchmarkLoadGlobalEvents(b *testing.B) {
	ctx := context.Background()

	// Pre-populate with events across multiple aggregates
	for _, totalEvents := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("total-events-%d", totalEvents), func(b *testing.B) {
			store := NewInMemoryStore()
			numAggregates := 10
			eventsPerAggregate := totalEvents / numAggregates

			for i := 0; i < numAggregates; i++ {
				c, _ := loadCounter(ctx, store, fmt.Sprintf("counter-%d", i))
				for j := 0; j < eventsPerAggregate; j++ {
					c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
				}
			}

			filters := []AggregateID{{Type: "counter"}}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				store.LoadGlobalEvents(ctx, filters, 0)
			}
		})
	}
}

// Benchmark retry mechanism under contention
func BenchmarkRetry_UnderContention(b *testing.B) {
	for _, maxRetries := range []int{3, 5, 10, 20} {
		b.Run(fmt.Sprintf("max-retries-%d", maxRetries), func(b *testing.B) {
			store := NewInMemoryStore()
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
					retry := NewRetry(maxRetries, ExponentialBackoff(0))

					for j := 0; j < opsPerGoroutine; j++ {
						c.AcceptThenApplyRetryable(ctx, *retry,
							NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
					}
				}()
			}
			wg.Wait()
		})
	}
}

// Benchmark batch appends with varying batch sizes
func BenchmarkAppend_Batches(b *testing.B) {
	for _, batchSize := range []int{2, 5, 10, 50} {
		b.Run(fmt.Sprintf("batch-size-%d", batchSize), func(b *testing.B) {
			store := NewInMemoryStore()
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				aggregateID := fmt.Sprintf("counter-%d", i)
				c, _ := loadCounter(ctx, store, aggregateID)

				// Create batch
				events := make([]Event, batchSize)
				for j := 0; j < batchSize; j++ {
					events[j] = NewEvent(eventIncremented, incrementedPayload{Amount: 1})
				}

				c.AcceptThenApply(ctx, events...)
			}
		})
	}
}

// Benchmark concurrent batch appends
func BenchmarkAppend_ConcurrentBatches(b *testing.B) {
	for _, batchSize := range []int{5, 10} {
		b.Run(fmt.Sprintf("batch-size-%d", batchSize), func(b *testing.B) {
			store := NewInMemoryStore()
			ctx := context.Background()
			retry := NewRetry(15, ExponentialBackoff(0))

			b.ResetTimer()

			var wg sync.WaitGroup
			concurrency := 8
			opsPerGoroutine := b.N / concurrency

			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					c, _ := loadCounter(ctx, store, "shared-counter")

					for j := 0; j < opsPerGoroutine; j++ {
						events := make([]Event, batchSize)
						for k := 0; k < batchSize; k++ {
							events[k] = NewEvent(eventIncremented, incrementedPayload{Amount: 1})
						}
						c.AcceptThenApplyRetryable(ctx, *retry, events...)
					}
				}()
			}
			wg.Wait()
		})
	}
}

// Benchmark throughput: events per second
func BenchmarkThroughput_EventsPerSecond(b *testing.B) {
	for _, concurrency := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("concurrency-%d", concurrency), func(b *testing.B) {
			store := NewInMemoryStore()
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
						c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
					}
				}(i)
			}
			wg.Wait()
		})
	}
}

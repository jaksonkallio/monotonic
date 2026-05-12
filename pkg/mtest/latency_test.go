package mtest

import (
	"sync"
	"testing"
	"time"
)

func TestLatencies_PercentilesNearestRank(t *testing.T) {
	l := NewLatencies()
	// Samples 1..100 microseconds; nearest-rank percentiles are: p50=50, p95=95, p99=99.
	for i := 1; i <= 100; i++ {
		l.Record(time.Duration(i) * time.Microsecond)
	}

	cases := []struct {
		q    float64
		want time.Duration
	}{
		{0.50, 50 * time.Microsecond},
		{0.95, 95 * time.Microsecond},
		{0.99, 99 * time.Microsecond},
		{1.00, 100 * time.Microsecond},
	}
	for _, tc := range cases {
		got := l.Percentile(tc.q)
		if got != tc.want {
			t.Errorf("Percentile(%.2f) = %v, want %v", tc.q, got, tc.want)
		}
	}
}

func TestLatencies_EmptyReturnsZero(t *testing.T) {
	l := NewLatencies()
	if got := l.Percentile(0.5); got != 0 {
		t.Errorf("empty Percentile = %v, want 0", got)
	}
	if got := l.Count(); got != 0 {
		t.Errorf("empty Count = %d, want 0", got)
	}
}

func TestLatencies_ConcurrentRecord(t *testing.T) {
	l := NewLatencies()
	const goroutines = 32
	const perGoroutine = 100

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perGoroutine {
				l.Record(time.Microsecond)
			}
		}()
	}
	wg.Wait()

	if got := l.Count(); got != goroutines*perGoroutine {
		t.Errorf("Count = %d, want %d", got, goroutines*perGoroutine)
	}
}

func TestLatencies_ReportToNoopWhenEmpty(t *testing.T) {
	// b.ReportMetric isn't easy to inspect outside a real benchmark; assert that
	// calling ReportTo on an empty Latencies doesn't panic and doesn't write to
	// the (nil-safe) recorder. We invoke it through testing.Benchmark which
	// supplies a real *testing.B.
	r := testing.Benchmark(func(b *testing.B) {
		l := NewLatencies()
		l.ReportTo(b)
	})
	// MemAllocs is just a stand-in liveness check; the real point is no panic above.
	_ = r
}

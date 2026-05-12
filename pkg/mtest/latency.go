package mtest

import (
	"slices"
	"sync"
	"testing"
	"time"
)

// Latencies is a thread-safe recorder of per-operation durations that reports p50/p95/p99/max via b.ReportMetric.
// Use one per benchmark sub-run, call Record from anywhere (including goroutines), then ReportTo before the sub-run returns.
type Latencies struct {
	mu      sync.Mutex
	samples []time.Duration
}

// NewLatencies creates an empty Latencies recorder.
func NewLatencies() *Latencies {
	return &Latencies{}
}

// Record appends a single observation; safe for concurrent use.
func (l *Latencies) Record(d time.Duration) {
	l.mu.Lock()
	l.samples = append(l.samples, d)
	l.mu.Unlock()
}

// Count returns the number of recorded samples.
func (l *Latencies) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.samples)
}

// ReportTo computes p50/p95/p99/max in microseconds and registers them on b via b.ReportMetric.
// A no-op when no samples have been recorded (so a benchmark that skipped work doesn't pollute the output).
func (l *Latencies) ReportTo(b *testing.B) {
	b.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.samples) == 0 {
		return
	}
	sorted := make([]time.Duration, len(l.samples))
	copy(sorted, l.samples)
	slices.Sort(sorted)

	b.ReportMetric(float64(percentile(sorted, 0.50).Microseconds()), "p50_us/op")
	b.ReportMetric(float64(percentile(sorted, 0.95).Microseconds()), "p95_us/op")
	b.ReportMetric(float64(percentile(sorted, 0.99).Microseconds()), "p99_us/op")
	b.ReportMetric(float64(sorted[len(sorted)-1].Microseconds()), "max_us/op")
}

// Percentile returns the q-quantile of the recorded samples (q in [0,1]); returns 0 when empty.
// Exposed for tests and callers that want to inspect raw values rather than reporting via b.ReportMetric.
func (l *Latencies) Percentile(q float64) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.samples) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(l.samples))
	copy(sorted, l.samples)
	slices.Sort(sorted)
	return percentile(sorted, q)
}

// percentile reads the q-quantile from an already-sorted slice using nearest-rank; q is clamped to [0,1].
func percentile(sorted []time.Duration, q float64) time.Duration {
	if q < 0 {
		q = 0
	} else if q > 1 {
		q = 1
	}
	// Nearest-rank: index = ceil(q*N) - 1, clamped to [0, N-1].
	idx := int(q*float64(len(sorted))+0.9999999) - 1
	if idx < 0 {
		idx = 0
	} else if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

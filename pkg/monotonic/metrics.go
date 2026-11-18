package monotonic

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// MetricsNamespace prefixes every metric this package exports.
const MetricsNamespace = "monotonic"

// Label values for the status dimension of ProjectorUpdatesTotal.
const (
	updateStatusSuccess = "success"
	updateStatusError   = "error"
)

var (
	ProjectorLastSuccessfulUpdateTimestampSeconds = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Subsystem: "projector",
			Name:      "last_successful_update_timestamp_seconds",
			Help:      "Unix timestamp of the last Update that returned without an error, including idle updates that found no events.",
		},
		[]string{"projector"},
	)

	ProjectorUpdatesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Subsystem: "projector",
			Name:      "updates_total",
			Help:      "Count of projector Update calls, labelled by whether the call succeeded or returned an error.",
		},
		[]string{"projector", "status"},
	)

	ProjectorAppliedEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Subsystem: "projector",
			Name:      "applied_events_total",
			Help:      "Count of events dispatched to projector handlers and committed.",
		},
		[]string{"projector"},
	)
)

type projectorMetrics struct {
	updatesSucceeded     prometheus.Counter
	updatesFailed        prometheus.Counter
	appliedEvents        prometheus.Counter
	lastSuccessfulUpdate prometheus.Gauge
}

func newProjectorMetrics(projectorName string) projectorMetrics {
	return projectorMetrics{
		updatesSucceeded:     ProjectorUpdatesTotal.WithLabelValues(projectorName, updateStatusSuccess),
		updatesFailed:        ProjectorUpdatesTotal.WithLabelValues(projectorName, updateStatusError),
		appliedEvents:        ProjectorAppliedEventsTotal.WithLabelValues(projectorName),
		lastSuccessfulUpdate: ProjectorLastSuccessfulUpdateTimestampSeconds.WithLabelValues(projectorName),
	}
}

// observeUpdate records one finished Update.
func (m projectorMetrics) observeUpdate(result ProjectorUpdateResult, err error) {
	if err != nil {
		m.updatesFailed.Inc()
		return
	}

	m.updatesSucceeded.Inc()
	m.appliedEvents.Add(float64(result.AppliedEvents))
	m.lastSuccessfulUpdate.SetToCurrentTime()
}

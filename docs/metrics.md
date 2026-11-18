# Metrics

Projectors export Prometheus metrics. They are registered with `prometheus.DefaultRegisterer` at package init via `promauto`, so importing `pkg/monotonic` is enough to get them: a process that already serves `promhttp.Handler()` over the default gatherer picks them up with no wiring at all.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `monotonic_projector_last_successful_update_timestamp_seconds` | gauge | `projector` | Unix timestamp of the last `Update` that returned no error. |
| `monotonic_projector_updates_total` | counter | `projector`, `status` | `Update` calls, split by `status="success"` or `status="error"`. |
| `monotonic_projector_applied_events_total` | counter | `projector` | Events dispatched to handlers and committed. |

Every series is labelled by projector name and bound when `NewProjector` is called, so all four series exist at zero from startup. That matters because a `rate()` over a series that does not exist yet is empty rather than zero, so an alert written against a projector that has never failed would otherwise be matching nothing at all.

## Freshness is the one to alert on

`last_successful_update_timestamp_seconds` moves on every clean `Update`, including the idle ones that found no events. It therefore means "this projector is alive and checking" rather than "this projector did work", which is exactly what makes it alertable: a caught-up projector sitting idle keeps it fresh, and a stuck or dead one does not.

```promql
# Projector has not completed an update in five minutes
time() - monotonic_projector_last_successful_update_timestamp_seconds > 300
```

Because the gauge is seeded at zero rather than left absent, a projector that was constructed but has never managed a single clean `Update` reads as stale and trips the same alert. That is deliberate: a projector that never starts working is as broken as one that stopped, and an absent series would have quietly matched nothing.

## How updates are counted

The split on `status` is there because a raw call count cannot tell a projector polling happily apart from one failing and being retried by `Drive` at the same rate. Sum across the label when you want the plain count:

```promql
# Update rate, regardless of outcome
sum by (projector) (rate(monotonic_projector_updates_total[5m]))

# Fraction of updates failing
sum by (projector) (rate(monotonic_projector_updates_total{status="error"}[5m]))
  / sum by (projector) (rate(monotonic_projector_updates_total[5m]))
```

A failed `Update` increments only `status="error"`. It records nothing else, because its transaction rolled back: the events it dispatched were never applied, so counting them would double-count them against the retry that eventually commits them. A batch is all-or-nothing, so `applied_events_total` counts committed events and never attempts.

Two consequences worth knowing:

- A context cancellation counts as an error, so expect one increment per projector per shutdown.
- A truncate counts as a successful `Update` but applies no events, so it moves the freshness gauge and the success counter without touching `applied_events_total`.

## Throughput

`applied_events_total` is a projector-side throughput signal: how fast this projector is getting through work.

```promql
# Events applied per second, per projector
sum by (projector) (rate(monotonic_projector_applied_events_total[1m]))
```

Read it as throughput, not as lag. Differencing it against the number of events written does not give you a backlog, for three separate reasons:

- **Filtering.** A projector only ever sees the event types its handlers registered, via `Dispatch.EventFilters()`. A projector subscribed to two of your twenty event types is fully caught up while applying a small fraction of what was written, so the difference grows without bound and means nothing.
- **Starting position.** A projector built with `WithInitialCounterForward()` skips everything written before its first update, and one built with `WithInitialCounterAt(n)` skips everything at or below `n`. Neither will ever account for that history.
- **Truncation.** A rebuild replays events the projector has already applied once, and both passes add to the same cumulative counter. After a truncate the counter is ahead of the number of distinct events the projector has ever seen.

For an actual backlog, compare the projector's resume counter against the head of the stream. The counter is in the backend's `projector_state` row, and `Store.MaxGlobalCounter` gives you the head.

What this metric *is* good for alongside the freshness gauge is telling apart the two ways a projector can look quiet. One whose freshness gauge keeps moving while this stays flat is caught up and idle. One where both are flat is stuck.

## Using a non-default registry

The collectors are exported, so a process that keeps its own registry can register them itself:

```go
reg := prometheus.NewRegistry()
reg.MustRegister(
	monotonic.ProjectorUpdatesTotal,
	monotonic.ProjectorAppliedEventsTotal,
	monotonic.ProjectorLastSuccessfulUpdateTimestampSeconds,
)
```

They stay registered on the default registerer as well, since that happens at package init and cannot be opted out of. To take them back off it, call `prometheus.Unregister` on each during your own setup:

```go
prometheus.Unregister(monotonic.ProjectorUpdatesTotal)
```

That only removes collectors that registered successfully. It is not an escape hatch for a name collision: if something else in your process already exports these names on the default registerer, `promauto` panics from inside this package's `init`, before any of your code gets to run. The fix there is to rename the other collector, since these names are fixed.

## Cost

Each projector binds its own children of the metric vectors at construction rather than looking them up per `Update`. A `WithLabelValues` lookup hashes the label set, and an idle projector would run three of them on every poll forever.

Measured on an idle in-memory `Update` (M-series Mac, `-benchtime 2000000x`):

| | ns/op | allocs/op |
|---|---|---|
| No metrics | ~87 | 2 |
| Bound children (what this does) | ~117 | 2 |
| `WithLabelValues` per update | ~195 | 2 |

So the instrumentation costs about 30ns per `Update` and allocates nothing. Against a Postgres-backed `Update`, which runs around 444µs, that is roughly 0.007%.

# Projection

Before reading this, you should understand what an event sourcing [aggregate](./aggregate.md) is!

A **projection** (also called a "read model") builds a query-optimized view of your data by replaying events from the global event log. While aggregates own the write side (accepting and validating events), projections own the read side — they subscribe to events across multiple aggregate types and build up whatever state is useful for queries.

## Why Projections?

Aggregates are optimized for consistency and business rules, not for queries. If you want to answer "how many checkouts happened today?" or "which items are trending?", you'd have to load and inspect every aggregate individually. Projections solve this by maintaining denormalized views that are cheap to query.

## Implementing a Projection

A projection implements the `ProjectionLogic` interface:

```go
type ProjectionLogic interface {
    Apply(event AggregateEvent)
    AggregateFilters() []AggregateID
}
```

- `AggregateFilters` declares which aggregate types (and optionally specific aggregate IDs) this projection subscribes to. Each filter is an `AggregateID`: if `ID` is empty, all aggregates of that type match; if `ID` is set, only that specific aggregate matches.
- `Apply` processes each matching event and updates the projection's state.

### Example: Order Statistics

```go
type OrderStatsProjection struct {
    TotalCheckouts int
    TotalPayments  int
    ItemsSold      map[string]int // SKU -> count
}

func NewOrderStatsProjection() *OrderStatsProjection {
    return &OrderStatsProjection{
        ItemsSold: make(map[string]int),
    }
}

func (p *OrderStatsProjection) AggregateFilters() []monotonic.AggregateID {
    // Subscribe to all cart and stock aggregates
    return []monotonic.AggregateID{
        {Type: "cart"},
        {Type: "stock"},
    }
}

func (p *OrderStatsProjection) Apply(event monotonic.AggregateEvent) {
    switch event.AggregateType {
    case "cart":
        switch event.Event.Type {
        case "checkout-started":
            p.TotalCheckouts++
        case "payment-charged":
            p.TotalPayments++
        }
    case "stock":
        switch event.Event.Type {
        case "reservation-confirmed":
            p.ItemsSold[event.AggregateID]++
        }
    }
}
```

Note that `Apply` receives an `AggregateEvent` (not just an `AcceptedEvent`), which includes the `AggregateType` and `AggregateID` so the projection knows which aggregate the event came from.

## Using a Projection

Wrap your projection logic in a `Projection` to manage catching up on events:

```go
ctx := context.TODO()
store := monotonic.NewInMemoryStore()

// Create the projection
stats := NewOrderStatsProjection()
projection := monotonic.NewProjection(store, stats)

// Catch up on all historical events
processed, err := projection.Update(ctx)
// processed = number of events applied

// Later, catch up on only new events since last Update
processed, err = projection.Update(ctx)
```

Each call to `Update` loads events from the global event log starting after the last processed global counter and applies them. This makes it cheap to call repeatedly — it only processes new events.

## Resuming a Projection

Projections track their progress via a global counter. You can persist this counter and resume from where you left off:

```go
// Save progress
savedCounter := projection.GlobalCounter()

// Later, resume from saved position
projection := monotonic.NewProjectionFrom(store, stats, savedCounter)
processed, err := projection.Update(ctx)
// Only processes events after savedCounter
```

This is useful when your projection state is persisted to a database — on restart, you resume from the last saved counter rather than replaying all events from the beginning.

## Next Steps

Once you're comfortable with projections, learn about [sagas](./saga.md) — state machines that coordinate business logic across multiple aggregates with atomic event commits.

# Aggregate

An **aggregate** is an entity with state that changes over time through events.

## Core Concepts

### Events

An **event** is an immutable fact that something happened. Events are typically written in past tense: `item-added`, `checkout-started`, `payment-charged`.

In Monotonic, there are two event types. An `Event` is a bare proposal — just a type and payload:

```go
type Event struct {
    Type    string          // Event type identifier
    Payload json.RawMessage // Event-specific data (JSON)
}
```

Once an event is accepted by an aggregate, it becomes an `AcceptedEvent` with additional metadata:

```go
type AcceptedEvent struct {
    Event
    AcceptedAt    time.Time // When the event was accepted
    Counter       int64     // Sequential position in the aggregate's history
    GlobalCounter int64     // Position in the global event log
}
```

Events are stored as JSON because they must remain immutable. If you stored Go structs directly, changing the struct definition could corrupt historical events. JSON provides a stable serialization format that can evolve over time.

### The Event Store

The **store** persists events and loads them for replay. Monotonic provides an in-memory store for development and testing:

```go
store := monotonic.NewInMemoryStore()
```

For production, you'd either implement the `Store` interface yourself, or use one of the ready-made ones (currently: Postgres, In-memory).

## Building Your First Aggregate

Let's build a shopping cart aggregate step by step.

### Step 1: Define Your Struct

In Monotonic, aggregates are plain Go structs that embed `AggregateBase`:

```go
package myapp

import (
    "errors"

    "github.com/jaksonkallio/monotonic/pkg/monotonic"
)

type Cart struct {
    *monotonic.AggregateBase

    Items           []string
    CheckoutStarted bool
}
```

The state lives in normal Go fields. No special types, no JSON marshaling for state. Just regular Go code.

Your struct embeds `*monotonic.AggregateBase`, which provides:
- `AcceptThenApply(ctx, events...)` — validate, persist, and apply events
- `Counter()` — the number of events applied
- Automatic catch-up if other processes have appended events

### Step 2: Define Typed Payloads

Define Go structs for your event payloads. This gives you type safety when creating and parsing events:

```go
type ItemAddedPayload struct {
    ItemName string `json:"item_name"`
}
```

### Step 3: Implement Apply

The `Apply` method updates your state when an event occurs. This method **cannot fail**. By the time `Apply` is called, the event has already been accepted and persisted. Note that `Apply` receives an `AcceptedEvent`, not a bare `Event`.

```go
func (c *Cart) Apply(event monotonic.AcceptedEvent) {
    switch event.Type {
    case "item-added":
        if p, ok := monotonic.ParsePayload[ItemAddedPayload](event); ok {
            c.Items = append(c.Items, p.ItemName)
        }

    case "checkout-started":
        c.CheckoutStarted = true
    }
}
```

Use the generic `ParsePayload[T]` helper to unmarshal event payloads into typed structs.

Always use the event's `AcceptedAt` timestamp for any time-based logic, not `time.Now()`. This ensures deterministic replay.

### Step 4: Implement ShouldAccept

The `ShouldAccept` method decides whether a proposed event is valid given the current state. Return an error to reject the event.

```go
func (c *Cart) ShouldAccept(event monotonic.Event) error {
    switch event.Type {
    case "item-added":
		if c.CheckoutStarted {
			return errors.New("cannot add items after checkout has started")
		}
		if len(c.Items) >= 1000 {
			return errors.New("cart cannot have more than 1000 items")
		}
        return nil

    case "checkout-started":
        if c.CheckoutStarted {
            return errors.New("checkout has already been started")
        }
        if len(c.Items) == 0 {
            return errors.New("cannot start checkout on an empty cart")
        }
        return nil
    }

    return errors.New("unrecognized event type: " + event.Type)
}
```

This is where your business rules live. Note that `ShouldAccept` receives a bare `Event` (not `AcceptedEvent`), since the event hasn't been accepted yet at this point.

### Step 5: Create a Load Function

The `Hydrate` function reconstructs an aggregate by replaying its events. Typically, you'll wrap this in a helper function for your specific aggregate types:

```go
func LoadCart(ctx context.Context, store monotonic.Store, id string) (*Cart, error) {
    return monotonic.Hydrate(ctx, store, "cart", id, func(base *monotonic.AggregateBase) *Cart {
        return &Cart{AggregateBase: base}
    })
}
```

The `"cart"` string is the aggregate type that namespaces your events in the store. Having an aggregate type namespace allows you to define your event names and aggregate IDs without prefixes or global uniqueness constraints.

### Step 6: Use Your Aggregate

```go
func main() {
    ctx := context.TODO()
    store := monotonic.NewInMemoryStore()

    // Load (or create) a cart
    cart, err := LoadCart(ctx, store, "cart-123")
    if err != nil {
        panic(err)
    }

    // Record events using NewEvent helper
    err = cart.AcceptThenApply(ctx,
        monotonic.NewEvent("item-added", ItemAddedPayload{ItemName: "Widget"}),
    )
    if err != nil {
        panic(err)
    }

    // State is updated
    fmt.Println(cart.Items) // ["Widget"]

    // Start checkout
    err = cart.AcceptThenApply(ctx, monotonic.NewEvent("checkout-started", nil))
    if err != nil {
        panic(err)
    }

    fmt.Println(cart.CheckoutStarted) // true

    // This will fail - business rule violation
    err = cart.AcceptThenApply(ctx, monotonic.NewEvent("checkout-started", nil))
    fmt.Println(err) // "checkout has already been started"
}
```

## Concurrency and Optimistic Locking

Monotonic uses **optimistic concurrency control** via event counters. Each event has a sequential counter, and appending an event fails if the counter doesn't match the expected next value.

```
Process A: Load cart (counter=5)
Process B: Load cart (counter=5)
Process B: AcceptThenApply → succeeds (counter becomes 6)
Process A: AcceptThenApply → fails! (expected counter 5 -> 6, but 6 already exists)
```

When you call `AcceptThenApply()`, Monotonic automatically catches up on any events it missed before validating. This minimizes conflicts to a milliseconds-wide window but doesn't eliminate them entirely. If two processes race, one will fail with a counter mismatch.

To alleviate this, the caller can retry using `AcceptThenApplyRetryable`:

```go
err := cart.AcceptThenApplyRetryable(ctx,
    monotonic.NewSensibleDefaultRetry(),
    monotonic.NewEvent("item-added", ItemAddedPayload{ItemName: "Widget"}),
)
```

`NewSensibleDefaultRetry` is simply a built-in retry configuration that is suitable for most use cases. It retries up to 5 times with exponential backoff starting at 100ms.

Of course, if you want to use a custom retry strategy, you can create your own `Retry` configuration:

```go
retry := monotonic.NewRetry(3, monotonic.ExponentialBackoff(100*time.Millisecond))
err := cart.AcceptThenApplyRetryable(ctx, retry,
    monotonic.NewEvent("item-added", ItemAddedPayload{ItemName: "Widget"}),
)
```

The available backoff functions are:
- `ExponentialBackoff`
- `LinearBackoff`
- `ConstantBackoff`

## Next Steps

Once you're comfortable with aggregates, learn about [projections](./projection.md) — read models that build query-optimized views by replaying events across aggregates.

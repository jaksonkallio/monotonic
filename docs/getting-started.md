# Getting Started with Monotonic

Monotonic is a lightweight event sourcing framework for Go. It's designed for small-to-medium scale projects that want the benefits of event sourcing without the complexity of distributed infrastructure or orchestration.

## What is Event Sourcing?

Traditional applications store the *current state* of their data. When you update a user's email, the old email is overwritten and lost forever.

**Event sourcing** takes a different approach: instead of storing current state, you store the *sequence of events* that led to the current state. To get the current state, you replay all the events from the beginning.

```
Traditional:  User { email: "new@example.com" }

Event Sourced:
  1. user-created { email: "old@example.com" }
  2. email-changed { email: "new@example.com" }

  Current state = replay(events) → User { email: "new@example.com" }
```

### Why Event Sourcing?

- **Complete audit trail**: Every change is recorded permanently
- **Time travel**: Reconstruct state at any point in history
- **Debugging**: See exactly what happened and when
- **Flexibility**: Build new views of data by replaying events differently
- **Natural fit for domains**: Many business processes are inherently event-driven

### The Trade-offs

Event sourcing isn't free. You trade:

- **Storage**: Events accumulate forever (though you can use snapshots)
- **Complexity**: Replaying events adds a layer of indirection
- **Event schema evolution**: Changing event formats requires care

Monotonic is designed to minimize these costs for projects where a full-blown event sourcing platform would be overkill.

---

## Installation

```bash
go get github.com/jaksonkallio/monotonic
```

```go
import "github.com/jaksonkallio/monotonic/pkg/monotonic"
```

---

## Core Concepts

### Events

An **event** is an immutable fact that something happened. Events are always written in past tense: `item-added`, `checkout-started`, `payment-charged`.

```go
type Event struct {
    AcceptedAt time.Time       // When the event was accepted
    Counter    int64           // Sequential position in the aggregate's history
    Type       string          // Event type identifier
    Payload    json.RawMessage // Event-specific data (JSON)
}
```

Events are stored as JSON because they must remain immutable. If you stored Go structs directly, changing the struct definition could corrupt historical events. JSON provides a stable serialization format that can evolve over time.

### Aggregates

An **aggregate** is an entity with state that changes over time through events. In Monotonic, aggregates are plain Go structs that embed `AggregateBase`:

```go
type Cart struct {
    *monotonic.AggregateBase

    // Your state - plain Go fields
    Items           []string
    CheckoutStarted bool
}
```

The state lives in normal Go fields. No special types, no JSON marshaling for state - just regular Go code.

### The Event Store

The **store** persists events and loads them for replay. Monotonic provides an in-memory store for development and testing:

```go
store := monotonic.NewInMemoryStore()
```

For production, you'd implement the `Store` interface with your database of choice (PostgreSQL, SQLite, etc.).

---

## Building Your First Aggregate

Let's build a shopping cart aggregate step by step.

### Step 1: Define Your Struct

```go
package myapp

import (
    "encoding/json"
    "errors"

    "github.com/jaksonkallio/monotonic/pkg/monotonic"
)

type Cart struct {
    *monotonic.AggregateBase

    Items           []string
    CheckoutStarted bool
}
```

Your struct embeds `*monotonic.AggregateBase`, which provides:
- `Record(event)` - validate, persist, and apply an event
- `Counter()` - the number of events applied
- Automatic catch-up if other processes have appended events

### Step 2: Implement Apply

The `Apply` method updates your state when an event occurs. This method **cannot fail** - by the time `Apply` is called, the event has already been accepted and persisted.

```go
func (c *Cart) Apply(event monotonic.Event) {
    switch event.Type {
    case "item-added":
        var payload struct {
            ItemName string `json:"item_name"`
        }
        json.Unmarshal(event.Payload, &payload)
        c.Items = append(c.Items, payload.ItemName)

    case "checkout-started":
        c.CheckoutStarted = true
    }
}
```

**Important**: Always use the event's `AcceptedAt` timestamp for any time-based logic, not `time.Now()`. This ensures deterministic replay - the same events always produce the same state.

### Step 3: Implement Validate

The `Validate` method decides whether an event is valid given the current state. Return an error to reject the event.

```go
func (c *Cart) Validate(event monotonic.Event) error {
    switch event.Type {
    case "item-added":
        // Anyone can add items
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

This is where your business rules live. The separation between validation (`Validate`) and application (`Apply`) is intentional - it keeps your state mutation simple and predictable.

### Step 4: Create a Load Function

The `Hydrate` function reconstructs an aggregate by replaying its events:

```go
func LoadCart(store monotonic.Store, id string) (*Cart, error) {
    return monotonic.Hydrate(store, "cart", id, func(base *monotonic.AggregateBase) *Cart {
        return &Cart{AggregateBase: base}
    })
}
```

The `"cart"` string is the aggregate type - it namespaces your events in the store.

### Step 5: Use Your Aggregate

```go
func main() {
    store := monotonic.NewInMemoryStore()

    // Load (or create) a cart
    cart, err := LoadCart(store, "cart-123")
    if err != nil {
        panic(err)
    }

    // Record events
    payload, _ := json.Marshal(map[string]string{"item_name": "Widget"})
    err = cart.Record(monotonic.Event{
        Type:    "item-added",
        Payload: payload,
    })
    if err != nil {
        panic(err)
    }

    // State is updated
    fmt.Println(cart.Items) // ["Widget"]

    // Start checkout
    err = cart.Record(monotonic.Event{Type: "checkout-started"})
    if err != nil {
        panic(err)
    }

    fmt.Println(cart.CheckoutStarted) // true

    // This will fail - business rule violation
    err = cart.Record(monotonic.Event{Type: "checkout-started"})
    fmt.Println(err) // "checkout has already been started"
}
```

---

## Concurrency and Optimistic Locking

Monotonic uses **optimistic concurrency control** via event counters. Each event has a sequential counter, and appending an event fails if the counter doesn't match the expected next value.

```
Process A: Load cart (counter=5)
Process B: Load cart (counter=5)
Process B: Record event → succeeds (counter becomes 6)
Process A: Record event → fails! (expected counter 6, got 6)
```

When you call `Record()`, Monotonic automatically catches up on any events it missed before validating:

```go
func (b *AggregateBase) Record(event Event) error {
    // Catch up first - replay any events we missed
    b.catchUp()

    // Now validate against the current state
    b.self.Validate(event)

    // Append with the correct counter
    // ...
}
```

This minimizes conflicts but doesn't eliminate them entirely. If two processes race, one will fail with a counter mismatch. The caller can retry:

```go
for retries := 0; retries < 3; retries++ {
    err := cart.Record(event)
    if err == nil {
        break
    }
    // Record will catch up on next attempt
}
```

---

## Sagas: Coordinating Multiple Aggregates

Sometimes a business process spans multiple aggregates. A checkout might need to:

1. Mark the cart as "checking out"
2. Reserve inventory for each item
3. Charge payment
4. Mark the cart as "paid"

This is where **sagas** come in. A saga is a state machine that coordinates events across multiple aggregates, with all events committed atomically.

### Why Atomicity Matters

Without atomic commits, you could end up with partial state:

```
1. Reserve inventory ✓
2. Charge payment ✓
3. *crash*
4. Mark cart as paid ✗
```

Now inventory is reserved, payment is charged, but the cart doesn't know. Monotonic's `AppendMulti` ensures all events are committed together or none are.

### Defining a Saga

A saga needs:
1. **States** - the stages of your workflow
2. **Actions** - functions that execute for each state
3. **Refs** - references to the aggregates being coordinated

```go
// Define your states
const (
    CheckoutStarted   = "started"
    CheckoutReserving = "reserving_stock"
    CheckoutCharging  = "charging_payment"
    CheckoutCompleted = "completed"
    CheckoutFailed    = "failed"
)

// Define actions for each state
func CheckoutActions() monotonic.ActionMap {
    return monotonic.ActionMap{
        CheckoutStarted:   startCheckout,
        CheckoutReserving: reserveStock,
        CheckoutCharging:  chargePayment,
        // No action for CheckoutCompleted = terminal state
    }
}
```

### Writing Action Functions

Each action function receives the saga and store, and returns:
- The next state to transition to
- Events to append to other aggregates (committed atomically with the state transition)

```go
func startCheckout(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (*monotonic.ActionResult, error) {
    // Load the cart using the saga's reference
    cart, err := LoadCart(store, saga.Refs["cart"].ID)
    if err != nil {
        return nil, err
    }

    // Prepare an event for the cart aggregate
    // Accept catches up the aggregate, validates,
    // and assigns the next counter - but doesn't append or apply yet
    cartEvent, err := cart.Accept(monotonic.Event{Type: "checkout-started"})
    if err != nil {
        return nil, err
    }

    return &monotonic.ActionResult{
        NewState: CheckoutReserving,
        Events:   []monotonic.AggregateEvent{cartEvent},
    }, nil
}
```

What's cool is that under the hood, the saga _itself_ is an event-sourced aggregate too. When you return an `ActionResult`, Monotonic:
1. Creates a `state-transitioned` event for the saga
2. Combines it with your `Events`
3. Appends all events atomically via `AppendMulti`

### Creating and Running Sagas

```go
func StartCheckoutSaga(ctx context.Context, store monotonic.Store, sagaID, cartID string) (*monotonic.Saga, error) {
    return monotonic.NewSaga(
        store,
        "checkout-saga",           // Saga type
        sagaID,                    // Unique saga ID
        CheckoutStarted,           // Initial state
        map[string]monotonic.AggregateID{
            "cart": monotonic.NewAggregateID("cart", cartID),
        },
        CheckoutActions(),
    )
}

// Usage
saga, err := StartCheckoutSaga(ctx, store, "saga-123", "cart-456")
if err != nil {
    panic(err)
}

// Run to completion
err = saga.Run(ctx)
```

### Delayed Transitions (Retries)

Actions can return a `Delay` to pause before the next step:

```go
func handlePaymentFailed(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (*monotonic.ActionResult, error) {
    return &monotonic.ActionResult{
        NewState: CheckoutCharging,  // Retry charging
        Delay:    1 * time.Minute,   // Wait 1 minute before retry
    }, nil
}
```

The saga tracks `ReadyAt` and `Step()` becomes a no-op until the delay passes:

```go
if !saga.IsReady() {
    return nil // Not ready yet, try again later
}
```

### Loading Existing Sagas

Sagas are event-sourced, so they can be loaded and resumed:

```go
saga, err := monotonic.LoadSaga(store, "checkout-saga", "saga-123", CheckoutActions())
if err != nil {
    panic(err)
}

// Continue from where we left off
saga.Run(ctx)
```

---

## External Services and Idempotency

When your saga calls external APIs (payment processors, email services, etc.), you can't include them in the atomic commit. Use **idempotency keys** to make retries safe:

```go
func chargePayment(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (*monotonic.ActionResult, error) {
    cart, _ := LoadCart(store, saga.Refs["cart"].ID)

    // Generate deterministic idempotency key from saga state
    idempotencyKey := fmt.Sprintf("%s:charge:%d", saga.ID.ID, saga.Counter())

    // External API call - safe to retry with same key
    err := paymentAPI.Charge(cart.PaymentToken, cart.Total, idempotencyKey)
    if err != nil {
        return &monotonic.ActionResult{
            NewState: CheckoutFailed,
        }, nil
    }

    // Record success
    return &monotonic.ActionResult{
        NewState: CheckoutCompleted,
        Events:   []monotonic.AggregateEvent{...},
    }, nil
}
```

Most payment processors (Stripe, Square, etc.) support idempotency keys. For services that don't, you may need to implement your own deduplication logic.

---

## Best Practices

### Event Design

- **Use past tense**: `order-placed`, not `place-order`
- **Be specific**: `item-added-to-cart`, not `cart-updated`
- **Include enough context**: Events should be self-describing
- **Think about evolution**: How will you handle old events when schemas change?

### Aggregate Design

- **Keep aggregates small**: Large aggregates with many events slow down hydration
- **One aggregate per transaction boundary**: If two things must change together, they might belong in the same aggregate (or coordinated via a saga)
- **State should be derivable**: If you can compute it from events, don't store it

### Saga Design

- **Sagas are coordinators, not data owners**: Load aggregates to read their state
- **Make actions idempotent**: A retry should produce the same result upon success
- **Plan for failure**: What happens if step 3 of 5 fails? Define compensation logic

---

## Next Steps

- Check out the `internal/cartdemo` package for a complete working example
- Implement a production `Store` backed by your database
- Add event schema versioning for long-lived systems
- Build read models (projections) for query-optimized views of your data

Happy event sourcing!

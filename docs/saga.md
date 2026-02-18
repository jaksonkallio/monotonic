# Saga

Before reading this, you should understand what an event sourcing [aggregate](./aggregate.md) is!

In event sourcing, a saga typically refers to a long-running process that coordinates business logic across multiple aggregates. In Monotonic this is true as well, but the approach here is aimed at being very lightweight and minimal. Monotonic sagas are essentially just an aggregate like any other, where we transition between states (represented by strings) and emit events for relevant aggregates that should be accepted and applied all at once, or none at all (atomicity). The saga's state transition _itself_ is an event as well, so the saga won't transition if any of the events failed to apply. The effect is that sagas allow us to coordinate complex business logic across different aggregates without worrying about locking, failures, rollbacks, concurrency, etc.

## Why Atomicity Matters

Without atomic commits, you could end up with partial state:

```
1. Reserve inventory ✓
2. Charge payment ✓
3. *crash*
4. Mark cart as paid ✗
```

Now inventory is reserved, payment is charged, but the cart doesn't know. Monotonic's `Append` ensures all events for a given step are committed together or none are.

## Defining a Saga

A saga needs:
1. **States** — the stages of your workflow
2. **Actions** — functions that execute for each state
3. **Input** — initial data (like aggregate IDs) provided when the saga starts

```go
// Define your states
const (
    CheckoutStarted   = "started"
    CheckoutReserving = "reserving_stock"
    CheckoutCharging  = "charging_payment"
    CheckoutCompleted = "completed"
    CheckoutFailed    = "failed"
)

// Define input
type CheckoutInput struct {
    CartID string `json:"cart_id"`
}

// Define actions for each state
func CheckoutActions() monotonic.ActionMap {
    return monotonic.ActionMap{
        CheckoutStarted:   monotonic.TypedAction(startCheckout),
        CheckoutReserving: monotonic.TypedAction(reserveStock),
        CheckoutCharging:  monotonic.TypedAction(chargePayment),
        CheckoutCompleted: completeCheckout,
        // No action for CheckoutFailed = terminal state
    }
}
```

## Writing Action Functions

Each action function receives the saga and store, and returns:
- The next state to transition to
- Events to append to other aggregates (committed atomically with the state transition)

Use `TypedAction` to automatically parse the saga's input into a typed struct, avoiding boilerplate in every action:

```go
func startCheckout(ctx context.Context, saga *monotonic.Saga, input CheckoutInput, store monotonic.Store) (monotonic.ActionResult, error) {
    // Load the cart using the typed input
    cart, err := LoadCart(ctx, store, input.CartID)
    if err != nil {
        return monotonic.ActionResult{}, err
    }

    // Accept prepares the event: catches up the aggregate, validates
    // via ShouldAccept, and assigns the next counter — but doesn't persist yet
    acceptedEvents, err := cart.Accept(ctx, monotonic.Event{Type: "checkout-started"})
    if err != nil {
        return monotonic.ActionResult{}, err
    }

    return monotonic.ActionResult{
        NewState: CheckoutReserving,
        Events: []monotonic.AggregateEvent{{
            AggregateType: "cart",
            AggregateID:   input.CartID,
            Event:         acceptedEvents[0],
        }},
    }, nil
}
```

Under the hood, when you return an `ActionResult`, Monotonic:
1. Creates a `state-transitioned` event for the saga
2. Combines it with your `Events`
3. Appends all events atomically via `Append`

For actions that don't need the typed input, use a plain `ActionFunc`:

```go
func completeCheckout(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
    return monotonic.ActionResult{Complete: true}, nil
}
```

Returning `Complete: true` marks the saga as finished. No further steps will execute.

## Creating and Running Sagas

Note that sagas require a `SagaStore` (which extends `Store` with saga lifecycle operations like listing active sagas and marking them completed). The in-memory store implements both interfaces.

```go
func StartCheckoutSaga(ctx context.Context, store monotonic.SagaStore, sagaID, cartID string) (*monotonic.Saga, error) {
    input, _ := json.Marshal(CheckoutInput{CartID: cartID})

    return monotonic.NewSaga(
        store,
        "checkout-saga",           // Saga type
        sagaID,                    // Unique saga ID
        CheckoutStarted,           // Initial state
        input,                     // JSON input data
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

## Saga Drivers

For production use, you'll want a `SagaDriver` to continuously poll and step sagas forward:

```go
driver := monotonic.NewSagaDriver(monotonic.SagaDriverConfig{
    Store:    store,
    SagaType: "checkout-saga",
    Actions:  CheckoutActions(),
    Interval: 1 * time.Second,
})

// Run in a goroutine — polls and steps all active sagas
go driver.Run(ctx)
```

Multiple drivers can run simultaneously without coordination. Optimistic concurrency ensures only one driver succeeds per saga step.

## Delayed Transitions (Retries)

Actions can return a `Delay` to pause before the next step:

```go
func handlePaymentFailed(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
    return monotonic.ActionResult{
        NewState: CheckoutCharging,  // Retry charging
        Delay:    1 * time.Minute,   // Wait 1 minute before retry
    }, nil
}
```

The saga tracks when it's ready, and `Step()` becomes a no-op until the delay passes:

```go
if !saga.IsReady() {
    return // Not ready yet, try again later
}
```

## Loading Existing Sagas

Sagas are event-sourced, so they can be loaded and resumed:

```go
saga, err := monotonic.LoadSaga(store, "checkout-saga", "saga-123", CheckoutActions())
if err != nil {
    panic(err)
}

// Continue from where we left off
saga.Run(ctx)
```

## External Services and Idempotency

When your saga calls external APIs (payment processors, email services, etc.), you can't include them in the atomic commit. Use **idempotency keys** to make retries safe:

```go
func chargePayment(ctx context.Context, saga *monotonic.Saga, input CheckoutInput, store monotonic.Store) (monotonic.ActionResult, error) {
    cart, _ := LoadCart(ctx, store, input.CartID)

    // Generate deterministic idempotency key from saga state
    idempotencyKey := fmt.Sprintf("%s:charge:%d", saga.ID.ID, saga.Counter())

    // External API call - safe to retry with same key
    err := paymentAPI.Charge(cart.PaymentToken, cart.Total, idempotencyKey)
    if err != nil {
        return monotonic.ActionResult{
            NewState: CheckoutFailed,
        }, nil
    }

    // Record success
    return monotonic.ActionResult{
        NewState: CheckoutCompleted,
        Events:   []monotonic.AggregateEvent{...},
    }, nil
}
```

Most payment processors (Stripe, Square, etc.) support idempotency keys. For services that don't, you may need to implement your own deduplication logic.

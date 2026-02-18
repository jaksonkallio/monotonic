# Getting Started with Monotonic

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

- **Storage**: Events accumulate forever
- **Complexity**: Replaying events adds a layer of indirection
- **Event schema evolution**: Changing event formats requires care

Monotonic is designed to minimize these costs for projects where a full-blown event sourcing platform would be overkill.

## Installation

```bash
go get github.com/jaksonkallio/monotonic
```

```go
import "github.com/jaksonkallio/monotonic/pkg/monotonic"
```

## Concepts

We recommend learning the concepts in order:

1. [Aggregates](./aggregate.md): event-sourced entities with state, business rules, and optimistic concurrency
2. [Projections](./projection.md): read models built by replaying events across aggregates
3. [Sagas](./saga.md): state machines that coordinate business logic across multiple aggregates

## Best Practices

- **Use past tense**: `order-placed`, not `place-order`
- **Be specific**: `item-added-to-cart`, not `cart-updated`
- **Include enough context**: Events should be self-describing
- **Think about evolution**: How will you handle old events when schemas change?
- **Sagas are coordinators, not data owners**: Load aggregates to read their state
- **Keep aggregates small**: Large aggregates with many events slow down hydration

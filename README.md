<p align="center">
  <img src="./docs/logo.png" alt="Monotonic" />
</p>

A lightweight event-sourcing framework for Go with aggregates, sagas, and projections.

Simple example below, see [docs/getting-started.md](./docs/getting-started.md) for a deep dive.

```go
package main

import (
	"context"
	"errors"
	"fmt"

	m "github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// Define your aggregate: a shopping cart
type Cart struct {
	*m.AggregateBase
	Items           []string
	CheckoutStarted bool
}

// Apply replays a persisted event onto the aggregate's state
func (c *Cart) Apply(event m.AcceptedEvent) {
	switch event.Type {
	case "item-added":
		if p, ok := m.ParsePayload[ItemAdded](event); ok {
			c.Items = append(c.Items, p.Name)
		}
	case "checkout-started":
		c.CheckoutStarted = true
	}
}

// ShouldAccept enforces business invariants before an event is persisted
func (c *Cart) ShouldAccept(event m.Event) error {
	if event.Type == "checkout-started" && len(c.Items) == 0 {
		return errors.New("cannot checkout an empty cart")
	}
	return nil
}

type ItemAdded struct {
	Name string `json:"name"`
}

func main() {
	store := m.NewInMemoryStore()
	ctx := context.TODO()

	// Hydrate/create a cart aggregate
	cart, _ := m.Hydrate(ctx, store, "cart", "cart-1", func(base *m.AggregateBase) *Cart {
		return &Cart{AggregateBase: base}
	})

	// Persist events — state updates automatically
	cart.AcceptThenApply(ctx, m.NewEvent("item-added", ItemAdded{Name: "widget"}))
	cart.AcceptThenApply(ctx, m.NewEvent("item-added", ItemAdded{Name: "gadget"}))
	cart.AcceptThenApply(ctx, m.NewEvent("checkout-started", nil))

	fmt.Println(cart.Items)           // [widget gadget]
	fmt.Println(cart.CheckoutStarted) // true

	// Rehydrate from the store, state is rebuilt from all stored events
	fresh, _ := m.Hydrate(ctx, store, "cart", "cart-1", func(base *m.AggregateBase) *Cart {
		return &Cart{AggregateBase: base}
	})

	fmt.Println(fresh.Items)           // [widget gadget]
	fmt.Println(fresh.CheckoutStarted) // true
}
```

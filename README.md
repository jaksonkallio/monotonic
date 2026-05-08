<p align="center">
  <img src="./docs/logo.png" alt="Monotonic" />
</p>

A lightweight event-sourcing framework for Go with aggregates and projections. It's designed for small-to-medium throughput projects that want the benefits of event sourcing without the complexity of dedicated orchestration components.

Simple example below, see [docs/getting-started.md](./docs/getting-started.md) for a deep dive.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	m "github.com/jaksonkallio/monotonic/pkg/monotonic"
)

func main() {
	store := m.NewInMemoryStore()
	ctx := context.TODO()

	// Hydrate/create an account aggregate.
	account, _ := m.Hydrate(ctx, store, "account", "alice", func(base *m.AggregateBase) *Account {
		return &Account{AggregateBase: base}
	})

	// Accept and apply some domain events.
	// These business operations are globally concurrent-safe, business logic invariants are enforced in a strict order.
	account.AcceptThenApply(ctx, m.NewEvent("account-opened", AccountOpened{HolderName: "Alice"}))
	account.AcceptThenApply(ctx, m.NewEvent("funds-deposited", FundsMoved{Amount: 100}))
	account.AcceptThenApply(ctx, m.NewEvent("funds-withdrawn", FundsMoved{Amount: 30}))
	fmt.Println(account.HolderName) // Alice
	fmt.Println(account.Balance)    // 70

	// Rehydrate account from the store, state is consistent after being rebuilt from all stored events.
	fresh, _ := m.Hydrate(ctx, store, "account", "alice", func(base *m.AggregateBase) *Account {
		return &Account{AggregateBase: base}
	})
	fmt.Println(fresh.HolderName) // Alice
	fmt.Println(fresh.Balance)    // 70

	// Close the account, which is an event that inherently drains any remaining balance and marks as closed.
	fresh.AcceptThenApply(ctx, m.NewEvent("account-closed", nil))
	fmt.Println(fresh.Balance) // 0
	fmt.Println(fresh.Closed)  // true

	// Open additional accounts to illustrate a projection with multiple rows.
	bob, _ := m.Hydrate(ctx, store, "account", "bob", func(base *m.AggregateBase) *Account {
		return &Account{AggregateBase: base}
	})
	bob.AcceptThenApply(ctx, m.NewEvent("account-opened", AccountOpened{HolderName: "Bob"}))
	bob.AcceptThenApply(ctx, m.NewEvent("funds-deposited", FundsMoved{Amount: 200}))
	carol, _ := m.Hydrate(ctx, store, "account", "carol", func(base *m.AggregateBase) *Account {
		return &Account{AggregateBase: base}
	})
	carol.AcceptThenApply(ctx, m.NewEvent("account-opened", AccountOpened{HolderName: "Carol"}))
	carol.AcceptThenApply(ctx, m.NewEvent("funds-deposited", FundsMoved{Amount: 50}))

	// Build a per-account projection of holder and balance, then catch up on every event in the store.
	summaries := m.NewInMemoryProjectionPersistence[AccountSummary]()
	projector, _ := m.NewProjector(ctx, store, &AccountSummaryLogic{}, summaries)
	projector.Update(ctx)

	// Projected rows, from our account summary projection logic.
	// This could be persisted as a plain ol' Postgres table, Redis key value, in-memory table, etc. for easy querying/reading.
	// One common pattern is to persist projection as a Postgres table and use something like `sqlc` to generate type-safe read queries.
	// | key   | HolderName | Balance | Closed |
	// |-------|------------|---------|--------|
	// | alice | Alice      | 0       | true   |
	// | bob   | Bob        | 200     | false  |
	// | carol | Carol      | 50      | false  |
}

// Account aggregate representing a bank account.
type Account struct {
	*m.AggregateBase
	HolderName string
	Balance    int64
	Opened     bool
	Closed     bool
}

// Apply replays a persisted event onto the aggregate's state.
func (a *Account) Apply(event m.AcceptedEvent) {
	switch event.Type {
	case "account-opened":
		if p, ok := m.ParsePayload[AccountOpened](event); ok {
			a.HolderName = p.HolderName
			a.Opened = true
		}
	case "funds-deposited":
		if p, ok := m.ParsePayload[FundsMoved](event); ok {
			a.Balance += p.Amount
		}
	case "funds-withdrawn":
		if p, ok := m.ParsePayload[FundsMoved](event); ok {
			a.Balance -= p.Amount
		}
	case "account-closed":
		a.Balance = 0
		a.Closed = true
	}
}

// ShouldAccept enforces business invariants before an event is persisted.
func (a *Account) ShouldAccept(event m.Event) error {
	switch event.Type {
	case "account-opened":
		if a.Opened {
			return errors.New("account is already opened")
		}
		var p AccountOpened
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		if p.HolderName == "" {
			return errors.New("holder name is required")
		}
	case "funds-deposited", "funds-withdrawn":
		if !a.Opened {
			return errors.New("account is not opened")
		}
		if a.Closed {
			return errors.New("account is closed")
		}
		var p FundsMoved
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		if p.Amount <= 0 {
			return errors.New("amount must be positive")
		}
		if event.Type == "funds-withdrawn" && a.Balance < p.Amount {
			return errors.New("insufficient funds")
		}
	case "account-closed":
		if !a.Opened {
			return errors.New("account is not opened")
		}
		if a.Closed {
			return errors.New("account is already closed")
		}
	}
	return nil
}

type AccountOpened struct {
	HolderName string `json:"holderName"`
}

type FundsMoved struct {
	Amount int64 `json:"amount"`
}

// AccountSummary is a per-account projection row exposing holder, balance, and closed state.
type AccountSummary struct {
	HolderName string
	Balance    int64
	Closed     bool
}

// AccountSummaryLogic projects every account event into the AccountSummary table, keyed by aggregate ID.
type AccountSummaryLogic struct{}

func (l *AccountSummaryLogic) EventFilters() []m.EventFilter {
	return []m.EventFilter{{AggregateType: "account"}}
}

func (l *AccountSummaryLogic) Apply(ctx context.Context, reader m.ProjectionReader[AccountSummary], event m.AggregateEvent) ([]m.Projected[AccountSummary], error) {
	key := m.ProjectionKey(event.AggregateID)
	switch event.Event.Type {
	case "account-opened":
		if p, ok := m.ParsePayload[AccountOpened](event.Event); ok {
			return []m.Projected[AccountSummary]{{Key: key, Value: AccountSummary{HolderName: p.HolderName}}}, nil
		}
	case "funds-deposited":
		if p, ok := m.ParsePayload[FundsMoved](event.Event); ok {
			return m.MutateByKey(ctx, reader, key, func(s *AccountSummary) error {
				s.Balance += p.Amount
				return nil
			})
		}
	case "funds-withdrawn":
		if p, ok := m.ParsePayload[FundsMoved](event.Event); ok {
			return m.MutateByKey(ctx, reader, key, func(s *AccountSummary) error {
				s.Balance -= p.Amount
				return nil
			})
		}
	case "account-closed":
		return m.MutateByKey(ctx, reader, key, func(s *AccountSummary) error {
			s.Balance = 0
			s.Closed = true
			return nil
		})
	}
	return nil, nil
}
```

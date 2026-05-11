<p align="center">
  <img src="./docs/logo.png" alt="Monotonic" />
</p>

A lightweight event-sourcing framework for Go with aggregates and projections. It's designed for small-to-medium throughput projects that want the benefits of event sourcing without the complexity of dedicated orchestration components.

## Example
_Note: this entire README file is human-written! No AI._

First off, what is **event sourcing**? The quick answer is "a system that derives state from past events". In most traditional applications, you are storing the current state of the system, probably in some sort of database. If you were running a bank software, this might look like storing a table of current balances somewhere, with a column for the account number and another column for the current balance. As transactions occur, you might be incrementing or decrementing this value. The event sourcing approach flips this on its head, by instead just tracking the **historic immutable events** of what has already happened, and deriving the current state from that. An event sourced bank software would just store records of transactions, then at any time we can **project** the current balance from all of the deposits and withdrawals that occurred for an account. The "current balance" of an account becomes just a passive result of all of the events that have happened previously, rather than an explicit value our software increments or decrements.

An **aggregate** is a single compartmentalized entity within an event sourcing system that has the ability to enforce invariants within it. Monotonic uses an "aggregate type" + "aggregate ID" namespacing approach to make it easy to give specific aggregates a string key. Here's what an appended event for Alice's `account` aggregate might look like:

```json
{
	"AggregateType": "account",
	"AggregateID": "alice",
	"Event": {
		"Type": "funds-withdrawn",
		"Payload": {
			"Amount": 100
		},
		"AcceptedAt": "2026-05-11T08:39:00Z",
		"Counter": 12,
		"GlobalCounter": 1083
	}
}
```

Importantly, you'll notice that there is a `Counter` value on the event. Counters are critical to event sourcing systems, so important in fact that they're the reason this project is called "Monotonic"! All events are assigned a dense, strictly-increasing counter integer at append-time. This allows us to enforce business invariants using optimistic concurrency, we can assert that a new event may be appended based on its counter value within the aggregate. If two instances try to append an event with the same counter at the exact same time, exactly one will succeed and the other will fail. The solution is often just to immediately retry the business logic operation. As a user of the framework, you wouldn't have to worry about counters beyond what they provide to you: concurrent-safe business logic enforcement. Whether you have 1x or 200x running instances with Monotonic appending events, you don't have to worry about any distributed coordination because the framework handles it for you.

Aggregate implementations for your business logic are usually just plain Go structs with an embedded `AggregateBase`. Monotonic uses the approach of separating new-event invariant enforcement and applying historic immutable events as two different methods: `ShouldAccept` and `Apply`. You'll see more about this in the example.

Event sourcing is generally coupled to the idea of CQRS, which stands for **command query read separation**. This principle is that we should be modeling the "command" (write) side of our domain logic differently than our "read" side of the domain logic, where the read side is often inextricably tied to the presentation layer of the application. If you've ever built medium/high complexity software, you'll recognize this frustration of trying to make your domain logic layer also work as a good read layer (e.g. ORMs). CQRS is freeing because we can just admit that modifying domain objects and reading information about domain objects are totally different concerns right from the get-go.

A common pattern for event-sourced applications is to have a domain layer that exclusively handles the implementation of business logic **invariants** (invariants are business logic rules that would prevent new events from being appended), basically, the "command" side of CQRS. Then, completely separately, you can build out **projections** which are (often tabular) data structures that simply react to these events that have happened, building up the projected state that will be shown in the application's presentation-layer.

Now that we have some of the basics down, let's look at a practical code example! There are tons of event sourcing frameworks out there with varying degrees of complexity tradeoffs, but of course since this is the Monotonic framework repo, we'll be using that for our example.

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
	// Store is where the actual events are stored, there's an in-memory store for testing/experimenting, and a Postgres one for production.
	// It's easy to implement your own store! You just need some sort of database that can read + insert counter values atomically.
	store := m.NewInMemoryStore()
	ctx := context.TODO()

	// "Hydrate" function replays all events for an aggregate from the store into an in-memory representation needed to enforce invariants.
	// The implementer usually wraps `Hydrate` functions into simpler function of their own like `HydrateAccount`.
	// Your aggregate structs are just normal Go structs with an embedded `AggregateBase` provided by Monotonic.
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

	// Rehydrate account from the store, state is consistent after being replayed from all appended events.
	fresh, _ := m.Hydrate(ctx, store, "account", "alice", func(base *m.AggregateBase) *Account {
		return &Account{AggregateBase: base}
	})
	fmt.Println(fresh.HolderName) // Alice
	fmt.Println(fresh.Balance)    // 70

	// Close the account, this sets the closed flag and drains the balance to zero.
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

	// `AcceptThenApply` is just a convenience method, you can also manually do `Accept` and `Apply` steps separately.
	// This allows you to accept+apply events atomically in a batch across multiple different aggregates.
	// Either both events in the batch succeed, or both events are rejected.
	transferWithdraw, _ := bob.Accept(ctx, m.NewEvent("funds-withdrawn", FundsMoved{Amount: 25, Memo: "transferred to carol" }))
	transferDeposit, _ := carol.Accept(ctx, m.NewEvent("funds-deposited", FundsMoved{Amount: 25, Memo: "transferred from bob" }))
	store.Append(ctx,
		m.AggregateEvent{AggregateType: bob.ID.Type, AggregateID: bob.ID.ID, Event: transferWithdraw[0]},
		m.AggregateEvent{AggregateType: carol.ID.Type, AggregateID: carol.ID.ID, Event: transferDeposit[0]},
	)

	// Build a per-account projection of holder and balance, then catch up on every event in the store.
	summaries := m.NewInMemoryProjectionPersistence[AccountSummary]()
	projector, _ := m.NewProjector(ctx, store, NewAccountSummaryLogic(), summaries)
	projector.Update(ctx)

	// Projected rows, from our account summary projection logic.
	// This could be persisted as a plain ol' Postgres table, Redis key value, in-memory table, etc. for easy querying/reading.
	// One common pattern is to persist projection as a Postgres table and use something like `sqlc` to generate type-safe read queries.
	// | key   | HolderName | Balance | Closed |
	// |-------|------------|---------|--------|
	// | alice | Alice      | 0       | true   |
	// | bob   | Bob        | 175     | false  |
	// | carol | Carol      | 75      | false  |
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
		if p, err := m.ParsePayload[AccountOpened](event); err == nil {
			a.HolderName = p.HolderName
			a.Opened = true
		}
	case "funds-deposited":
		if p, err := m.ParsePayload[FundsMoved](event); err == nil {
			a.Balance += p.Amount
		}
	case "funds-withdrawn":
		if p, err := m.ParsePayload[FundsMoved](event); err == nil {
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
	Amount int64  `json:"amount"`
	Memo   string `json:"memo"`
}

// AccountSummary is a per-account projection row exposing holder, balance, and closed state.
type AccountSummary struct {
	HolderName string
	Balance    int64
	Closed     bool
}

// NewAccountSummaryLogic builds a dispatch that routes each account event type to its projection handler.
func NewAccountSummaryLogic() m.ProjectorLogic[AccountSummary] {
	return m.NewDispatch[AccountSummary]().
		On("account", "account-opened", applyAccountOpened).
		On("account", "funds-deposited", applyFundsDeposited).
		On("account", "funds-withdrawn", applyFundsWithdrawn).
		On("account", "account-closed", applyAccountClosed)
}

func applyAccountOpened(ctx context.Context, reader m.ProjectionReader[AccountSummary], event m.AggregateEvent) ([]m.Projected[AccountSummary], error) {
	p, err := m.ParsePayload[AccountOpened](event.Event)
	if err != nil {
		return nil, err
	}
	return []m.Projected[AccountSummary]{
		{
			Key: m.ProjectionKey(event.AggregateID),
			Value: AccountSummary{
				HolderName: p.HolderName
			}
		}
	}, nil
}

func applyFundsDeposited(ctx context.Context, reader m.ProjectionReader[AccountSummary], event m.AggregateEvent) ([]m.Projected[AccountSummary], error) {
	p, err := m.ParsePayload[FundsMoved](event.Event)
	if err != nil {
		return nil, err
	}
	return m.MutateByKey(ctx, reader, m.ProjectionKey(event.AggregateID), func(s *AccountSummary) error {
		s.Balance += p.Amount
		return nil
	})
}

func applyFundsWithdrawn(ctx context.Context, reader m.ProjectionReader[AccountSummary], event m.AggregateEvent) ([]m.Projected[AccountSummary], error) {
	p, err := m.ParsePayload[FundsMoved](event.Event)
	if err != nil {
		return nil, err
	}
	return m.MutateByKey(ctx, reader, m.ProjectionKey(event.AggregateID), func(s *AccountSummary) error {
		s.Balance -= p.Amount
		return nil
	})
}

func applyAccountClosed(ctx context.Context, reader m.ProjectionReader[AccountSummary], event m.AggregateEvent) ([]m.Projected[AccountSummary], error) {
	return m.MutateByKey(ctx, reader, m.ProjectionKey(event.AggregateID), func(s *AccountSummary) error {
		s.Balance = 0
		s.Closed = true
		return nil
	})
}
```

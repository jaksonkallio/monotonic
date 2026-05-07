// Package ledgerdemo is the example event-sourced ledger that serves as the project's primary demo of monotonic.
package ledgerdemo

import (
	"context"
	"errors"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// Aggregate types.
const (
	AggregateAccount  = "account"
	AggregateTransfer = "transfer"
)

// Event types.
const (
	EventAccountOpened     = "account-opened"
	EventFundsDeposited    = "funds-deposited"
	EventFundsWithdrawn    = "funds-withdrawn"
	EventTransferCompleted = "transfer-completed"
)

// AccountOpenedPayload carries the human-readable name when an account is opened.
type AccountOpenedPayload struct {
	HolderName string
}

// FundsMovedPayload carries the amount for deposits and withdrawals.
type FundsMovedPayload struct {
	Amount int64
}

// TransferCompletedPayload describes a completed transfer between two accounts.
type TransferCompletedPayload struct {
	FromAccount string
	ToAccount   string
	Amount      int64
}

// account is a minimal aggregate that tracks balance for ShouldAccept invariants.
type account struct {
	*monotonic.AggregateBase
	Balance int64
}

func (a *account) Apply(event monotonic.AcceptedEvent) {
	switch event.Type {
	case EventFundsDeposited:
		if p, ok := monotonic.ParsePayload[FundsMovedPayload](event); ok {
			a.Balance += p.Amount
		}
	case EventFundsWithdrawn:
		if p, ok := monotonic.ParsePayload[FundsMovedPayload](event); ok {
			a.Balance -= p.Amount
		}
	}
}

func (a *account) ShouldAccept(event monotonic.Event) error {
	if event.Type == EventFundsWithdrawn {
		p, ok := monotonic.ParsePayload[FundsMovedPayload](monotonic.AcceptedEvent{Event: event})
		if !ok {
			return errors.New("invalid withdraw payload")
		}
		if a.Balance < p.Amount {
			return errors.New("insufficient funds")
		}
	}
	return nil
}

// loadAccount hydrates an account aggregate from the store.
func loadAccount(ctx context.Context, store monotonic.Store, id string) (*account, error) {
	return monotonic.Hydrate(ctx, store, AggregateAccount, id, func(base *monotonic.AggregateBase) *account {
		return &account{AggregateBase: base}
	})
}

// transfer is a minimal aggregate; transfers are immutable once completed.
type transfer struct {
	*monotonic.AggregateBase
}

func (t *transfer) Apply(event monotonic.AcceptedEvent) {}

func (t *transfer) ShouldAccept(event monotonic.Event) error { return nil }

func loadTransfer(ctx context.Context, store monotonic.Store, id string) (*transfer, error) {
	return monotonic.Hydrate(ctx, store, AggregateTransfer, id, func(base *monotonic.AggregateBase) *transfer {
		return &transfer{AggregateBase: base}
	})
}

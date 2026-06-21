// Package ledgerdemo is the example event-sourced ledger that serves as the project's primary demo of monotonic.
package ledgerdemo

import (
	"context"
	"fmt"

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

// Account is a minimal aggregate that tracks balance for ShouldAccept invariants.
type Account struct {
	*monotonic.AggregateBase
	Balance int64
}

func (a *Account) Apply(event monotonic.AcceptedEvent) {
	switch event.Type {
	case EventFundsDeposited:
		if p, err := monotonic.ParsePayload[FundsMovedPayload](event); err == nil {
			a.Balance += p.Amount
		}
	case EventFundsWithdrawn:
		if p, err := monotonic.ParsePayload[FundsMovedPayload](event); err == nil {
			a.Balance -= p.Amount
		}
	}
}

func (a *Account) ShouldAccept(event monotonic.Event) error {
	if event.Type == EventFundsWithdrawn {
		p, err := monotonic.ParsePayload[FundsMovedPayload](monotonic.AcceptedEvent{Event: event})
		if err != nil {
			return fmt.Errorf("parsing withdraw payload: %w", err)
		}
		if a.Balance < p.Amount {
			return fmt.Errorf("insufficient funds")
		}
	}
	return nil
}

// LoadAccount hydrates an Account aggregate from the store.
func LoadAccount(ctx context.Context, store monotonic.Store, id string) (*Account, error) {
	return monotonic.Hydrate(ctx, store, AggregateAccount, id, func(base *monotonic.AggregateBase) *Account {
		return &Account{AggregateBase: base}
	})
}

// Transfer is a minimal aggregate; transfers are immutable once completed.
type Transfer struct {
	*monotonic.AggregateBase
}

func (t *Transfer) Apply(event monotonic.AcceptedEvent) {}

func (t *Transfer) ShouldAccept(event monotonic.Event) error { return nil }

// LoadTransfer hydrates a Transfer aggregate from the store.
func LoadTransfer(ctx context.Context, store monotonic.Store, id string) (*Transfer, error) {
	return monotonic.Hydrate(ctx, store, AggregateTransfer, id, func(base *monotonic.AggregateBase) *Transfer {
		return &Transfer{AggregateBase: base}
	})
}

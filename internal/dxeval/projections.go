package dxeval

import (
	"context"
	"fmt"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// AccountBalance is one row in the per-account balance projection.
type AccountBalance struct {
	AccountID  string
	HolderName string
	Balance    int64
}

// NewBalanceLogic builds the per-account balance ProjectorLogic; the transfer-completed handler emits two Projected (source and destination).
func NewBalanceLogic() monotonic.ProjectorLogic[AccountBalance] {
	return monotonic.NewDispatch[AccountBalance]().
		On(AggregateAccount, EventAccountOpened, balanceOnAccountOpened).
		On(AggregateAccount, EventFundsDeposited, balanceOnFundsDeposited).
		On(AggregateAccount, EventFundsWithdrawn, balanceOnFundsWithdrawn).
		On(AggregateTransfer, EventTransferCompleted, balanceOnTransferCompleted)
}

func balanceOnAccountOpened(ctx context.Context, reader monotonic.ProjectionReader[AccountBalance], event monotonic.AggregateEvent) ([]monotonic.Projected[AccountBalance], error) {
	payload, ok := monotonic.ParsePayload[AccountOpenedPayload](event.Event)
	if !ok {
		return nil, fmt.Errorf("invalid account-opened payload")
	}
	return []monotonic.Projected[AccountBalance]{{
		Key: monotonic.ProjectionKey(event.AggregateID),
		Value: AccountBalance{
			AccountID:  event.AggregateID,
			HolderName: payload.HolderName,
		},
	}}, nil
}

func balanceOnFundsDeposited(ctx context.Context, reader monotonic.ProjectionReader[AccountBalance], event monotonic.AggregateEvent) ([]monotonic.Projected[AccountBalance], error) {
	payload, ok := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if !ok {
		return nil, fmt.Errorf("invalid deposit payload")
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKey(event.AggregateID), func(v *AccountBalance) error {
		v.AccountID = event.AggregateID
		v.Balance += payload.Amount
		return nil
	})
}

func balanceOnFundsWithdrawn(ctx context.Context, reader monotonic.ProjectionReader[AccountBalance], event monotonic.AggregateEvent) ([]monotonic.Projected[AccountBalance], error) {
	payload, ok := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if !ok {
		return nil, fmt.Errorf("invalid withdraw payload")
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKey(event.AggregateID), func(v *AccountBalance) error {
		v.AccountID = event.AggregateID
		v.Balance -= payload.Amount
		return nil
	})
}

func balanceOnTransferCompleted(ctx context.Context, reader monotonic.ProjectionReader[AccountBalance], event monotonic.AggregateEvent) ([]monotonic.Projected[AccountBalance], error) {
	payload, ok := monotonic.ParsePayload[TransferCompletedPayload](event.Event)
	if !ok {
		return nil, fmt.Errorf("invalid transfer payload")
	}

	fromKey := monotonic.ProjectionKey(payload.FromAccount)
	toKey := monotonic.ProjectionKey(payload.ToAccount)

	from, _, err := reader.Get(ctx, fromKey)
	if err != nil {
		return nil, err
	}
	to, _, err := reader.Get(ctx, toKey)
	if err != nil {
		return nil, err
	}

	from.AccountID = payload.FromAccount
	to.AccountID = payload.ToAccount
	from.Balance -= payload.Amount
	to.Balance += payload.Amount

	return []monotonic.Projected[AccountBalance]{
		{Key: fromKey, Value: from},
		{Key: toKey, Value: to},
	}, nil
}

// LedgerStats is the single-row summary projection across all account/transfer activity.
type LedgerStats struct {
	AccountsOpened      int64
	TotalDeposited      int64
	TotalWithdrawn      int64
	TransfersCompleted  int64
	TotalTransferAmount int64
}

// NewStatsLogic builds the summary stats ProjectorLogic; each handler emits one Projected into ProjectionKeySummary.
func NewStatsLogic() monotonic.ProjectorLogic[LedgerStats] {
	return monotonic.NewDispatch[LedgerStats]().
		On(AggregateAccount, EventAccountOpened, statsOnAccountOpened).
		On(AggregateAccount, EventFundsDeposited, statsOnFundsDeposited).
		On(AggregateAccount, EventFundsWithdrawn, statsOnFundsWithdrawn).
		On(AggregateTransfer, EventTransferCompleted, statsOnTransferCompleted)
}

func statsOnAccountOpened(ctx context.Context, reader monotonic.ProjectionReader[LedgerStats], event monotonic.AggregateEvent) ([]monotonic.Projected[LedgerStats], error) {
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *LedgerStats) error {
		s.AccountsOpened++
		return nil
	})
}

func statsOnFundsDeposited(ctx context.Context, reader monotonic.ProjectionReader[LedgerStats], event monotonic.AggregateEvent) ([]monotonic.Projected[LedgerStats], error) {
	payload, ok := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if !ok {
		return nil, nil
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *LedgerStats) error {
		s.TotalDeposited += payload.Amount
		return nil
	})
}

func statsOnFundsWithdrawn(ctx context.Context, reader monotonic.ProjectionReader[LedgerStats], event monotonic.AggregateEvent) ([]monotonic.Projected[LedgerStats], error) {
	payload, ok := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if !ok {
		return nil, nil
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *LedgerStats) error {
		s.TotalWithdrawn += payload.Amount
		return nil
	})
}

func statsOnTransferCompleted(ctx context.Context, reader monotonic.ProjectionReader[LedgerStats], event monotonic.AggregateEvent) ([]monotonic.Projected[LedgerStats], error) {
	payload, ok := monotonic.ParsePayload[TransferCompletedPayload](event.Event)
	if !ok {
		return nil, nil
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *LedgerStats) error {
		s.TransfersCompleted++
		s.TotalTransferAmount += payload.Amount
		return nil
	})
}

package ledgerdemo

import (
	"context"
	"fmt"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// AccountBalance is one row in the per-account balance projection; the row is keyed by aggregate ID, so AccountID isn't repeated as a field.
type AccountBalance struct {
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
	payload, err := monotonic.ParsePayload[AccountOpenedPayload](event.Event)
	if err != nil {
		return nil, fmt.Errorf("parsing account-opened payload: %w", err)
	}
	return []monotonic.Projected[AccountBalance]{{
		Key:   monotonic.ProjectionKey(event.AggregateID),
		Value: AccountBalance{HolderName: payload.HolderName},
	}}, nil
}

func balanceOnFundsDeposited(ctx context.Context, reader monotonic.ProjectionReader[AccountBalance], event monotonic.AggregateEvent) ([]monotonic.Projected[AccountBalance], error) {
	payload, err := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if err != nil {
		return nil, fmt.Errorf("parsing deposit payload: %w", err)
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKey(event.AggregateID), func(v *AccountBalance) error {
		v.Balance += payload.Amount
		return nil
	})
}

func balanceOnFundsWithdrawn(ctx context.Context, reader monotonic.ProjectionReader[AccountBalance], event monotonic.AggregateEvent) ([]monotonic.Projected[AccountBalance], error) {
	payload, err := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if err != nil {
		return nil, fmt.Errorf("parsing withdraw payload: %w", err)
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKey(event.AggregateID), func(v *AccountBalance) error {
		v.Balance -= payload.Amount
		return nil
	})
}

func balanceOnTransferCompleted(ctx context.Context, reader monotonic.ProjectionReader[AccountBalance], event monotonic.AggregateEvent) ([]monotonic.Projected[AccountBalance], error) {
	payload, err := monotonic.ParsePayload[TransferCompletedPayload](event.Event)
	if err != nil {
		return nil, fmt.Errorf("parsing transfer-completed payload: %w", err)
	}

	fromKey := monotonic.ProjectionKey(payload.FromAccount)
	toKey := monotonic.ProjectionKey(payload.ToAccount)

	from, err := reader.Get(ctx, fromKey)
	if err != nil {
		return nil, err
	}
	to, err := reader.Get(ctx, toKey)
	if err != nil {
		return nil, err
	}

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
	payload, err := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if err != nil {
		return nil, fmt.Errorf("parsing funds-deposited payload: %w", err)
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *LedgerStats) error {
		s.TotalDeposited += payload.Amount
		return nil
	})
}

func statsOnFundsWithdrawn(ctx context.Context, reader monotonic.ProjectionReader[LedgerStats], event monotonic.AggregateEvent) ([]monotonic.Projected[LedgerStats], error) {
	payload, err := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if err != nil {
		return nil, fmt.Errorf("parsing funds-withdrawn payload: %w", err)
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *LedgerStats) error {
		s.TotalWithdrawn += payload.Amount
		return nil
	})
}

func statsOnTransferCompleted(ctx context.Context, reader monotonic.ProjectionReader[LedgerStats], event monotonic.AggregateEvent) ([]monotonic.Projected[LedgerStats], error) {
	payload, err := monotonic.ParsePayload[TransferCompletedPayload](event.Event)
	if err != nil {
		return nil, fmt.Errorf("parsing transfer-completed payload: %w", err)
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *LedgerStats) error {
		s.TransfersCompleted++
		s.TotalTransferAmount += payload.Amount
		return nil
	})
}

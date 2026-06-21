package ledgerdemo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
	pgstore "github.com/jaksonkallio/monotonic/pkg/store/postgres"
)

// Projector names registered in projector_state.
const (
	ProjectorBalance = "ledgerdemo_balance"
	ProjectorStats   = "ledgerdemo_stats"
)

// MigrateProjections creates the demo projection tables. The framework owns projector_state;
// each implementer owns and migrates whatever shape they want.
func MigrateProjections(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS account_balances (
			account_id  TEXT NOT NULL PRIMARY KEY,
			holder_name TEXT NOT NULL,
			balance     BIGINT NOT NULL
		);
	`); err != nil {
		return fmt.Errorf("migrate account_balances: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS ledger_stats (
			id                    INT NOT NULL PRIMARY KEY,
			accounts_opened       BIGINT NOT NULL,
			total_deposited       BIGINT NOT NULL,
			total_withdrawn       BIGINT NOT NULL,
			transfers_completed   BIGINT NOT NULL,
			total_transfer_amount BIGINT NOT NULL
		);
		INSERT INTO ledger_stats (id, accounts_opened, total_deposited, total_withdrawn, transfers_completed, total_transfer_amount)
		VALUES (1, 0, 0, 0, 0, 0)
		ON CONFLICT (id) DO NOTHING;
	`); err != nil {
		return fmt.Errorf("migrate ledger_stats: %w", err)
	}
	return nil
}

// NewBalanceDispatch builds the per-account balance Dispatch.
func NewBalanceDispatch() *monotonic.Dispatch {
	return monotonic.NewDispatch().
		On(AggregateAccount, EventAccountOpened, pgstore.TxHandler(balanceOnAccountOpened)).
		On(AggregateAccount, EventFundsDeposited, pgstore.TxHandler(balanceOnFundsDeposited)).
		On(AggregateAccount, EventFundsWithdrawn, pgstore.TxHandler(balanceOnFundsWithdrawn)).
		On(AggregateTransfer, EventTransferCompleted, pgstore.TxHandler(balanceOnTransferCompleted))
}

func balanceOnAccountOpened(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
	payload, err := monotonic.ParsePayload[AccountOpenedPayload](event.Event)
	if err != nil {
		return fmt.Errorf("parsing account-opened payload: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO account_balances (account_id, holder_name, balance)
		VALUES ($1, $2, 0)
		ON CONFLICT (account_id) DO UPDATE SET holder_name = EXCLUDED.holder_name
	`, event.AggregateID, payload.HolderName)
	return err
}

func balanceOnFundsDeposited(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
	payload, err := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if err != nil {
		return fmt.Errorf("parsing deposit payload: %w", err)
	}
	_, err = tx.Exec(ctx,
		`UPDATE account_balances SET balance = balance + $2 WHERE account_id = $1`,
		event.AggregateID, payload.Amount,
	)
	return err
}

func balanceOnFundsWithdrawn(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
	payload, err := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if err != nil {
		return fmt.Errorf("parsing withdraw payload: %w", err)
	}
	_, err = tx.Exec(ctx,
		`UPDATE account_balances SET balance = balance - $2 WHERE account_id = $1`,
		event.AggregateID, payload.Amount,
	)
	return err
}

func balanceOnTransferCompleted(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
	payload, err := monotonic.ParsePayload[TransferCompletedPayload](event.Event)
	if err != nil {
		return fmt.Errorf("parsing transfer-completed payload: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE account_balances SET balance = balance - $2 WHERE account_id = $1`,
		payload.FromAccount, payload.Amount,
	); err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`UPDATE account_balances SET balance = balance + $2 WHERE account_id = $1`,
		payload.ToAccount, payload.Amount,
	)
	return err
}

// NewStatsDispatch builds the summary stats Dispatch.
func NewStatsDispatch() *monotonic.Dispatch {
	return monotonic.NewDispatch().
		On(AggregateAccount, EventAccountOpened, pgstore.TxHandler(statsOnAccountOpened)).
		On(AggregateAccount, EventFundsDeposited, pgstore.TxHandler(statsOnFundsDeposited)).
		On(AggregateAccount, EventFundsWithdrawn, pgstore.TxHandler(statsOnFundsWithdrawn)).
		On(AggregateTransfer, EventTransferCompleted, pgstore.TxHandler(statsOnTransferCompleted))
}

func statsOnAccountOpened(ctx context.Context, tx pgx.Tx, _ monotonic.AggregateEvent) error {
	_, err := tx.Exec(ctx, `UPDATE ledger_stats SET accounts_opened = accounts_opened + 1 WHERE id = 1`)
	return err
}

func statsOnFundsDeposited(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
	payload, err := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if err != nil {
		return fmt.Errorf("parsing funds-deposited payload: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE ledger_stats SET total_deposited = total_deposited + $1 WHERE id = 1`, payload.Amount)
	return err
}

func statsOnFundsWithdrawn(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
	payload, err := monotonic.ParsePayload[FundsMovedPayload](event.Event)
	if err != nil {
		return fmt.Errorf("parsing funds-withdrawn payload: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE ledger_stats SET total_withdrawn = total_withdrawn + $1 WHERE id = 1`, payload.Amount)
	return err
}

func statsOnTransferCompleted(ctx context.Context, tx pgx.Tx, event monotonic.AggregateEvent) error {
	payload, err := monotonic.ParsePayload[TransferCompletedPayload](event.Event)
	if err != nil {
		return fmt.Errorf("parsing transfer-completed payload: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE ledger_stats
		SET transfers_completed = transfers_completed + 1,
		    total_transfer_amount = total_transfer_amount + $1
		WHERE id = 1
	`, payload.Amount)
	return err
}

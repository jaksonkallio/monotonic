package postgres_integration_test

import (
	"context"
	"testing"

	"github.com/jaksonkallio/monotonic/internal/ledgerdemo"
	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

func TestLedgerDemo_EndToEnd(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	backend := resetProjectorState(t)

	// Migrate the demo's own projection tables.
	if _, err := sharedPool.Exec(ctx, `DROP TABLE IF EXISTS account_balances; DROP TABLE IF EXISTS ledger_stats`); err != nil {
		t.Fatalf("drop demo tables: %v", err)
	}
	tx, err := sharedPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := ledgerdemo.MigrateProjections(ctx, tx); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("MigrateProjections: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit migrate: %v", err)
	}
	t.Cleanup(func() {
		sharedPool.Exec(context.Background(), `DROP TABLE IF EXISTS account_balances; DROP TABLE IF EXISTS ledger_stats`)
	})

	// Drive the aggregate side: open two accounts, deposit, transfer, withdraw.
	alice, err := ledgerdemo.LoadAccount(ctx, store, "alice")
	if err != nil {
		t.Fatalf("load alice: %v", err)
	}
	if err := alice.AcceptThenApply(ctx, monotonic.NewEvent(ledgerdemo.EventAccountOpened, ledgerdemo.AccountOpenedPayload{HolderName: "Alice"})); err != nil {
		t.Fatalf("open alice: %v", err)
	}
	bob, err := ledgerdemo.LoadAccount(ctx, store, "bob")
	if err != nil {
		t.Fatalf("load bob: %v", err)
	}
	if err := bob.AcceptThenApply(ctx, monotonic.NewEvent(ledgerdemo.EventAccountOpened, ledgerdemo.AccountOpenedPayload{HolderName: "Bob"})); err != nil {
		t.Fatalf("open bob: %v", err)
	}
	if err := alice.AcceptThenApply(ctx, monotonic.NewEvent(ledgerdemo.EventFundsDeposited, ledgerdemo.FundsMovedPayload{Amount: 100})); err != nil {
		t.Fatalf("deposit alice: %v", err)
	}
	xfer, err := ledgerdemo.LoadTransfer(ctx, store, "xfer-1")
	if err != nil {
		t.Fatalf("load transfer: %v", err)
	}
	if err := xfer.AcceptThenApply(ctx, monotonic.NewEvent(ledgerdemo.EventTransferCompleted, ledgerdemo.TransferCompletedPayload{
		FromAccount: "alice", ToAccount: "bob", Amount: 30,
	})); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if err := alice.AcceptThenApply(ctx, monotonic.NewEvent(ledgerdemo.EventFundsWithdrawn, ledgerdemo.FundsMovedPayload{Amount: 20})); err != nil {
		t.Fatalf("withdraw: %v", err)
	}

	// Run both projectors.
	balanceProj, err := monotonic.NewProjector(ctx, ledgerdemo.ProjectorBalance, store, ledgerdemo.NewBalanceDispatch(), backend, 0)
	if err != nil {
		t.Fatalf("balance projector: %v", err)
	}
	statsProj, err := monotonic.NewProjector(ctx, ledgerdemo.ProjectorStats, store, ledgerdemo.NewStatsDispatch(), backend, 0)
	if err != nil {
		t.Fatalf("stats projector: %v", err)
	}
	if _, err := balanceProj.Update(ctx); err != nil {
		t.Fatalf("balance Update: %v", err)
	}
	if _, err := statsProj.Update(ctx); err != nil {
		t.Fatalf("stats Update: %v", err)
	}

	// Verify per-account balances.
	var aliceBal, bobBal int64
	var aliceName, bobName string
	if err := sharedPool.QueryRow(ctx, `SELECT holder_name, balance FROM account_balances WHERE account_id = 'alice'`).Scan(&aliceName, &aliceBal); err != nil {
		t.Fatalf("query alice balance: %v", err)
	}
	if err := sharedPool.QueryRow(ctx, `SELECT holder_name, balance FROM account_balances WHERE account_id = 'bob'`).Scan(&bobName, &bobBal); err != nil {
		t.Fatalf("query bob balance: %v", err)
	}
	if aliceBal != 50 {
		t.Errorf("alice balance: want 50, got %d", aliceBal)
	}
	if aliceName != "Alice" {
		t.Errorf("alice name: want Alice, got %q", aliceName)
	}
	if bobBal != 30 {
		t.Errorf("bob balance: want 30, got %d", bobBal)
	}
	if bobName != "Bob" {
		t.Errorf("bob name: want Bob, got %q", bobName)
	}

	// Verify summary stats.
	var stats struct {
		AccountsOpened, TotalDeposited, TotalWithdrawn, TransfersCompleted, TotalTransferAmount int64
	}
	if err := sharedPool.QueryRow(ctx, `
		SELECT accounts_opened, total_deposited, total_withdrawn, transfers_completed, total_transfer_amount
		FROM ledger_stats WHERE id = 1
	`).Scan(&stats.AccountsOpened, &stats.TotalDeposited, &stats.TotalWithdrawn, &stats.TransfersCompleted, &stats.TotalTransferAmount); err != nil {
		t.Fatalf("query ledger_stats: %v", err)
	}

	if stats.AccountsOpened != 2 || stats.TotalDeposited != 100 || stats.TotalWithdrawn != 20 ||
		stats.TransfersCompleted != 1 || stats.TotalTransferAmount != 30 {
		t.Errorf("stats mismatch: %+v", stats)
	}

	// Resume scenario: append another deposit and verify a fresh balance projector
	// picks up exactly one new event.
	if err := alice.AcceptThenApply(ctx, monotonic.NewEvent(ledgerdemo.EventFundsDeposited, ledgerdemo.FundsMovedPayload{Amount: 5})); err != nil {
		t.Fatalf("second deposit: %v", err)
	}
	resumed, err := monotonic.NewProjector(ctx, ledgerdemo.ProjectorBalance, store, ledgerdemo.NewBalanceDispatch(), backend, 0)
	if err != nil {
		t.Fatalf("resume balance: %v", err)
	}
	if n, err := resumed.Update(ctx); err != nil || n != 1 {
		t.Errorf("resumed Update n=%d err=%v, want n=1", n, err)
	}
	if err := sharedPool.QueryRow(ctx, `SELECT balance FROM account_balances WHERE account_id = 'alice'`).Scan(&aliceBal); err != nil {
		t.Fatalf("query alice balance after resume: %v", err)
	}
	if aliceBal != 55 {
		t.Errorf("alice balance after resume: want 55, got %d", aliceBal)
	}
}

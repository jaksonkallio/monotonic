package ledgerdemo

import (
	"context"
	"testing"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

func TestLedgerScenario(t *testing.T) {
	ctx := context.Background()
	store := monotonic.NewInMemoryStore()

	// Open two accounts.
	alice, err := loadAccount(ctx, store, "alice")
	if err != nil {
		t.Fatalf("load alice: %v", err)
	}
	if err := alice.AcceptThenApply(ctx, monotonic.NewEvent(EventAccountOpened, AccountOpenedPayload{HolderName: "Alice"})); err != nil {
		t.Fatalf("open alice: %v", err)
	}

	bob, err := loadAccount(ctx, store, "bob")
	if err != nil {
		t.Fatalf("load bob: %v", err)
	}
	if err := bob.AcceptThenApply(ctx, monotonic.NewEvent(EventAccountOpened, AccountOpenedPayload{HolderName: "Bob"})); err != nil {
		t.Fatalf("open bob: %v", err)
	}

	// Deposit $100 to Alice.
	if err := alice.AcceptThenApply(ctx, monotonic.NewEvent(EventFundsDeposited, FundsMovedPayload{Amount: 100})); err != nil {
		t.Fatalf("deposit alice: %v", err)
	}

	// Transfer $30 from Alice to Bob.
	xfer, err := loadTransfer(ctx, store, "xfer-1")
	if err != nil {
		t.Fatalf("load transfer: %v", err)
	}
	if err := xfer.AcceptThenApply(ctx, monotonic.NewEvent(EventTransferCompleted, TransferCompletedPayload{
		FromAccount: "alice",
		ToAccount:   "bob",
		Amount:      30,
	})); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	// Withdraw $20 from Alice. Note: must come from Alice's own aggregate-tracked balance ($100),
	// not the projection's view, because account.ShouldAccept doesn't see cross-aggregate transfer events.
	if err := alice.AcceptThenApply(ctx, monotonic.NewEvent(EventFundsWithdrawn, FundsMovedPayload{Amount: 20})); err != nil {
		t.Fatalf("withdraw alice: %v", err)
	}

	// Spin up both projectors against the same store.
	balancePersist := monotonic.NewInMemoryProjectionPersistence[AccountBalance]()
	balanceProjector, err := monotonic.NewProjector(ctx, store, NewBalanceLogic(), balancePersist, 0)
	if err != nil {
		t.Fatalf("NewProjector balance: %v", err)
	}

	statsPersist := monotonic.NewInMemoryProjectionPersistence[LedgerStats]()
	statsProjector, err := monotonic.NewProjector(ctx, store, NewStatsLogic(), statsPersist, 0)
	if err != nil {
		t.Fatalf("NewProjector stats: %v", err)
	}

	if processed, err := balanceProjector.Update(ctx); err != nil {
		t.Fatalf("balance Update: %v", err)
	} else if processed == 0 {
		t.Fatal("balance projector processed nothing")
	}

	if processed, err := statsProjector.Update(ctx); err != nil {
		t.Fatalf("stats Update: %v", err)
	} else if processed == 0 {
		t.Fatal("stats projector processed nothing")
	}

	// Verify per-account balances.
	// Alice: +100 deposit, -30 transfer out, -20 withdrawal = 50.
	aliceBalance, _ := balancePersist.Get(ctx, "alice")
	if aliceBalance.Balance != 50 {
		t.Errorf("alice balance: want 50, got %d", aliceBalance.Balance)
	}
	if aliceBalance.HolderName != "Alice" {
		t.Errorf("alice holder name: want Alice, got %q", aliceBalance.HolderName)
	}

	// Bob: +30 transfer in = 30. HolderName populated by Bob's account-opened.
	bobBalance, _ := balancePersist.Get(ctx, "bob")
	if bobBalance.Balance != 30 {
		t.Errorf("bob balance: want 30, got %d", bobBalance.Balance)
	}
	if bobBalance.HolderName != "Bob" {
		t.Errorf("bob holder name: want Bob, got %q", bobBalance.HolderName)
	}

	// Verify summary stats.
	stats, _ := statsPersist.Get(ctx, monotonic.ProjectionKeySummary)
	if stats.AccountsOpened != 2 {
		t.Errorf("accounts opened: want 2, got %d", stats.AccountsOpened)
	}
	if stats.TotalDeposited != 100 {
		t.Errorf("total deposited: want 100, got %d", stats.TotalDeposited)
	}
	if stats.TotalWithdrawn != 20 {
		t.Errorf("total withdrawn: want 20, got %d", stats.TotalWithdrawn)
	}
	if stats.TransfersCompleted != 1 {
		t.Errorf("transfers completed: want 1, got %d", stats.TransfersCompleted)
	}
	if stats.TotalTransferAmount != 30 {
		t.Errorf("total transfer amount: want 30, got %d", stats.TotalTransferAmount)
	}

	// Resume scenario: append a new event, build fresh projectors against same persistences, expect them to pick up only the new event.
	if err := alice.AcceptThenApply(ctx, monotonic.NewEvent(EventFundsDeposited, FundsMovedPayload{Amount: 5})); err != nil {
		t.Fatalf("second deposit: %v", err)
	}

	resumedBalance, err := monotonic.NewProjector(ctx, store, NewBalanceLogic(), balancePersist, 0)
	if err != nil {
		t.Fatalf("resume balance: %v", err)
	}
	if processed, err := resumedBalance.Update(ctx); err != nil {
		t.Fatalf("resumed balance Update: %v", err)
	} else if processed != 1 {
		t.Errorf("resumed balance processed: want 1, got %d", processed)
	}

	aliceBalance, _ = balancePersist.Get(ctx, "alice")
	if aliceBalance.Balance != 55 {
		t.Errorf("alice balance after resume: want 55, got %d", aliceBalance.Balance)
	}
}

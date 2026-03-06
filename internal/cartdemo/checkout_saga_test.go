package cartdemo

import (
	"context"
	"testing"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

func TestCheckoutSaga(t *testing.T) {
	store := monotonic.NewInMemoryStore()
	ctx := context.Background()

	// First, set up stock for the items
	widgetStock, _ := LoadStock(ctx, store, "widget")
	widgetStock.AcceptThenApply(ctx, monotonic.NewEvent(EventStockAdded, StockAddedPayload{Quantity: 10}))
	gadgetStock, _ := LoadStock(ctx, store, "gadget")
	gadgetStock.AcceptThenApply(ctx, monotonic.NewEvent(EventStockAdded, StockAddedPayload{Quantity: 5}))

	// Create a cart with items
	cart, _ := LoadCart(ctx, store, "cart-123")
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "widget"}))
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "gadget"}))

	// Start a checkout saga for this cart
	saga, err := StartCheckoutSaga(ctx, store, "checkout-1", "cart-123")
	if err != nil {
		t.Fatalf("StartCheckoutSaga failed: %v", err)
	}

	// Verify initial state
	if saga.State() != CheckoutStarted {
		t.Errorf("expected state %s, got %s", CheckoutStarted, saga.State())
	}
	var sagaInput checkoutSagaInput
	if err := saga.InputAs(&sagaInput); err != nil {
		t.Fatalf("InputAs failed: %v", err)
	}
	if sagaInput.CartID != "cart-123" {
		t.Errorf("expected cart ID cart-123, got %s", sagaInput.CartID)
	}

	// Step 1: started -> reserving_stock
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 1 failed: %v", err)
	}
	if saga.State() != CheckoutReservingStock {
		t.Errorf("expected state %s, got %s", CheckoutReservingStock, saga.State())
	}

	// Verify cart checkout-started event was created
	cartEvents, _ := store.LoadAggregateEvents(ctx, "cart", "cart-123", 0)
	if len(cartEvents) != 3 { // 2 item-added + 1 checkout-started
		t.Errorf("expected 3 cart events, got %d", len(cartEvents))
	}

	// Step 2: reserving_stock -> creating_payment_token
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 2 failed: %v", err)
	}
	if saga.State() != CheckoutCreatingToken {
		t.Errorf("expected state %s, got %s", CheckoutCreatingToken, saga.State())
	}

	// Verify stock reservation events (1 stock-added + 1 stock-reserved = 2 each)
	widgetStockEvents, _ := store.LoadAggregateEvents(ctx, "stock", "widget", 0)
	gadgetStockEvents, _ := store.LoadAggregateEvents(ctx, "stock", "gadget", 0)
	if len(widgetStockEvents) != 2 {
		t.Errorf("expected 2 widget stock events, got %d", len(widgetStockEvents))
	}
	if len(gadgetStockEvents) != 2 {
		t.Errorf("expected 2 gadget stock events, got %d", len(gadgetStockEvents))
	}

	// Step 3: creating_payment_token -> charging_payment
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 3 failed: %v", err)
	}
	if saga.State() != CheckoutChargingPayment {
		t.Errorf("expected state %s, got %s", CheckoutChargingPayment, saga.State())
	}

	// Step 4: charging_payment -> confirming_stock
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 4 failed: %v", err)
	}
	if saga.State() != CheckoutConfirmingStock {
		t.Errorf("expected state %s, got %s", CheckoutConfirmingStock, saga.State())
	}

	// Step 5: confirming_stock -> completed
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 5 failed: %v", err)
	}
	if saga.State() != CheckoutCompleted {
		t.Errorf("expected state %s, got %s", CheckoutCompleted, saga.State())
	}

	// Verify stock reservations were confirmed (reservation tracking cleared)
	finalWidgetStock, _ := LoadStock(ctx, store, "widget")
	finalGadgetStock, _ := LoadStock(ctx, store, "gadget")
	if finalWidgetStock.TotalReserved() != 0 {
		t.Errorf("expected widget reservations to be confirmed, got %d reserved", finalWidgetStock.TotalReserved())
	}
	if finalGadgetStock.TotalReserved() != 0 {
		t.Errorf("expected gadget reservations to be confirmed, got %d reserved", finalGadgetStock.TotalReserved())
	}

	// Step 6: complete the saga
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 6 failed: %v", err)
	}
	if !saga.Completed() {
		t.Error("expected saga to be closed")
	}

	// Step 7: closed saga, should be no-op
	err = saga.Step(ctx)
	if err != nil {
		t.Fatalf("Step 7 failed: %v", err)
	}
	if saga.State() != CheckoutCompleted {
		t.Errorf("expected state to remain %s, got %s", CheckoutCompleted, saga.State())
	}

	// Verify saga events
	sagaEvents, _ := store.LoadAggregateEvents(ctx, "checkout-saga", "checkout-1", 0)
	// 1 start + 5 transitions + 1 close = 7 events
	if len(sagaEvents) != 7 {
		t.Errorf("expected 7 saga events, got %d", len(sagaEvents))
	}
}

func TestCheckoutSagaHydration(t *testing.T) {
	store := monotonic.NewInMemoryStore()
	ctx := context.Background()

	// Set up stock
	thingStock, _ := LoadStock(ctx, store, "thing")
	thingStock.AcceptThenApply(ctx, monotonic.NewEvent(EventStockAdded, StockAddedPayload{Quantity: 10}))

	// Create cart
	cart, _ := LoadCart(ctx, store, "cart-456")
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "thing"}))

	// Start and run saga partway
	saga, _ := StartCheckoutSaga(ctx, store, "checkout-2", "cart-456")
	saga.Step(ctx) // started -> reserving_stock
	saga.Step(ctx) // reserving_stock -> creating_payment_token

	// Hydrate a fresh instance
	saga2, err := LoadCheckoutSaga(store, "checkout-2")
	if err != nil {
		t.Fatalf("LoadCheckoutSaga failed: %v", err)
	}

	// Should have same state
	if saga2.State() != CheckoutCreatingToken {
		t.Errorf("expected state %s, got %s", CheckoutCreatingToken, saga2.State())
	}
	var sagaInput checkoutSagaInput
	if err := saga.InputAs(&sagaInput); err != nil {
		t.Fatalf("InputAs failed: %v", err)
	}
	if sagaInput.CartID != "cart-456" {
		t.Errorf("expected cart ID cart-456, got %s", sagaInput.CartID)
	}
	if saga2.Counter() != saga.Counter() {
		t.Errorf("expected counter %d, got %d", saga.Counter(), saga2.Counter())
	}

	// Continue from hydrated instance
	err = saga2.Step(ctx)
	if err != nil {
		t.Fatalf("Step from hydrated saga failed: %v", err)
	}
	if saga2.State() != CheckoutChargingPayment {
		t.Errorf("expected state %s, got %s", CheckoutChargingPayment, saga2.State())
	}
}

func TestCheckoutSagaRun(t *testing.T) {
	store := monotonic.NewInMemoryStore()
	ctx := context.Background()

	// Set up stock
	itemStock, _ := LoadStock(ctx, store, "item")
	itemStock.AcceptThenApply(ctx, monotonic.NewEvent(EventStockAdded, StockAddedPayload{Quantity: 10}))

	// Create cart
	cart, _ := LoadCart(ctx, store, "cart-789")
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "item"}))

	// Start saga and run to completion
	saga, _ := StartCheckoutSaga(ctx, store, "checkout-3", "cart-789")

	err := saga.Run(ctx)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if saga.State() != CheckoutCompleted {
		t.Errorf("expected state %s, got %s", CheckoutCompleted, saga.State())
	}

	if !saga.Completed() {
		t.Error("expected saga to be closed")
	}
}

func TestSagaDelayedTransition(t *testing.T) {
	store := monotonic.NewInMemoryStore()
	ctx := context.Background()

	// Create a simple saga with a delayed transition
	actions := monotonic.ActionMap{
		"start": func(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
			return monotonic.ActionResult{
				NewState: "waiting",
				ReadyAt:  time.Now().Add(100 * time.Millisecond),
			}, nil
		},
		"waiting": func(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
			return monotonic.ActionResult{NewState: "done"}, nil
		},
		"done": func(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
			return monotonic.ActionResult{Complete: true}, nil
		},
	}

	saga, _ := monotonic.NewSaga(store, "test-saga", "saga-1", "start", nil, actions)

	// First step should transition to "waiting" with delay
	saga.Step(ctx)
	if saga.State() != "waiting" {
		t.Errorf("expected state 'waiting', got %s", saga.State())
	}

	// Saga should not be ready yet
	if saga.IsReady() {
		t.Error("expected saga to not be ready (delayed)")
	}

	// Step should be no-op when not ready
	saga.Step(ctx)
	if saga.State() != "waiting" {
		t.Errorf("expected state to remain 'waiting', got %s", saga.State())
	}

	// Wait for delay
	time.Sleep(150 * time.Millisecond)

	// Now it should be ready
	if !saga.IsReady() {
		t.Error("expected saga to be ready after delay")
	}

	// Step should now transition to done
	saga.Step(ctx)
	if saga.State() != "done" {
		t.Errorf("expected state 'done', got %s", saga.State())
	}

	// Step again to close
	saga.Step(ctx)
	if !saga.Completed() {
		t.Error("expected saga to be completed")
	}
}

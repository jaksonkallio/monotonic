package cartdemo

import (
	"context"
	"testing"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

func TestCheckoutPaymentFailed(t *testing.T) {
	store := monotonic.NewInMemoryStore()
	ctx := context.Background()

	// Set up stock
	widget, _ := LoadStock(ctx, store, "widget")
	widget.AcceptThenApply(ctx, monotonic.NewEvent(EventStockAdded, StockAddedPayload{Quantity: 10}))

	// Create a cart with one item
	cart, _ := LoadCart(ctx, store, "cart-fail")
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "widget"}))

	// Start checkout saga
	saga, err := StartCheckoutSaga(ctx, store, "checkout-fail", "cart-fail")
	if err != nil {
		t.Fatalf("StartCheckoutSaga: %v", err)
	}

	// Step through: started -> reserving_stock -> creating_payment_token -> charging_payment
	for _, expectedState := range []string{
		CheckoutReservingStock,
		CheckoutCreatingToken,
		CheckoutChargingPayment,
	} {
		if err := saga.Step(ctx); err != nil {
			t.Fatalf("Step to %s: %v", expectedState, err)
		}
		if saga.State() != expectedState {
			t.Fatalf("expected state %s, got %s", expectedState, saga.State())
		}
	}

	// Now manually simulate a payment failure by directly transitioning the saga
	// to the payment_failed state. In the real checkout flow, checkoutChargePayment
	// would return NewState: CheckoutPaymentFailed when the payment API fails.
	// We can't easily make that happen with the current hard-coded success,
	// so we inject a payment_failed transition event directly into the store.
	sagaCounter := saga.Counter()
	transitionPayload := monotonic.NewEvent(monotonic.EventTypeStateTransitioned,
		monotonic.SagaStateTransitionPayload{ToState: CheckoutPaymentFailed})
	failEvent := monotonic.AggregateEvent{
		AggregateType: "checkout-saga",
		AggregateID:   "checkout-fail",
		Event: monotonic.AcceptedEvent{
			Event:   transitionPayload,
			Counter: sagaCounter + 1,
		},
	}
	if err := store.Append(ctx, failEvent); err != nil {
		t.Fatalf("append payment_failed transition: %v", err)
	}

	// Reload the saga so it picks up the injected event
	saga, err = LoadCheckoutSaga(store, "checkout-fail")
	if err != nil {
		t.Fatalf("LoadCheckoutSaga: %v", err)
	}
	if saga.State() != CheckoutPaymentFailed {
		t.Fatalf("expected state %s, got %s", CheckoutPaymentFailed, saga.State())
	}

	// Step: payment_failed action should transition back to charging_payment with a delay
	if err := saga.Step(ctx); err != nil {
		t.Fatalf("Step payment_failed: %v", err)
	}
	if saga.State() != CheckoutChargingPayment {
		t.Errorf("expected state %s, got %s", CheckoutChargingPayment, saga.State())
	}

	// The saga should NOT be ready (1-minute delay)
	if saga.IsReady() {
		t.Error("expected saga to be delayed after payment failure retry")
	}

	// Saga should not be completed
	if saga.Completed() {
		t.Error("expected saga to not be completed")
	}
}

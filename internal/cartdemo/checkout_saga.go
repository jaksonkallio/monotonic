package cartdemo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// Checkout saga states
const (
	CheckoutStarted          = "started"
	CheckoutReservingStock   = "reserving_stock"
	CheckoutCreatingToken    = "creating_payment_token"
	CheckoutChargingPayment  = "charging_payment"
	CheckoutPaymentFailed    = "payment_failed"
	CheckoutConfirmingStock  = "confirming_stock"
	CheckoutCompleted        = "completed"
)

// checkoutSagaInput is the typed input for checkout saga actions
type checkoutSagaInput struct {
	CartID string `json:"cart_id"`
}

// CheckoutActions returns the action map for checkout sagas
func CheckoutActions() monotonic.ActionMap {
	return monotonic.ActionMap{
		CheckoutStarted:         monotonic.TypedAction(checkoutStart),
		CheckoutReservingStock:  monotonic.TypedAction(checkoutReserveStock),
		CheckoutCreatingToken:   monotonic.TypedAction(checkoutCreatePaymentToken),
		CheckoutChargingPayment: monotonic.TypedAction(checkoutChargePayment),
		CheckoutPaymentFailed:   checkoutPaymentFailed, // no input needed
		CheckoutConfirmingStock: monotonic.TypedAction(checkoutConfirmStock),
		CheckoutCompleted:       checkoutComplete, // no input needed
	}
}

// StartCheckoutSaga creates a new checkout saga for a cart
func StartCheckoutSaga(ctx context.Context, store monotonic.SagaStore, sagaID string, cartID string) (*monotonic.Saga, error) {
	input, err := json.Marshal(checkoutSagaInput{CartID: cartID})
	if err != nil {
		return nil, err
	}

	return monotonic.NewSaga(
		store,
		"checkout-saga",
		sagaID,
		CheckoutStarted,
		input,
		CheckoutActions(),
	)
}

// LoadCheckoutSaga loads an existing checkout saga
func LoadCheckoutSaga(store monotonic.SagaStore, sagaID string) (*monotonic.Saga, error) {
	return monotonic.LoadSaga(store, "checkout-saga", sagaID, CheckoutActions())
}

func checkoutStart(ctx context.Context, saga *monotonic.Saga, input checkoutSagaInput, store monotonic.Store) (monotonic.ActionResult, error) {
	cart, err := LoadCart(ctx, store, input.CartID)
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	acceptedEvents, err := cart.Accept(ctx,monotonic.Event{Type: EventCheckoutStarted})
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	return monotonic.ActionResult{
		NewState: CheckoutReservingStock,
		Events: []monotonic.AggregateEvent{{
			AggregateType: "cart",
			AggregateID:   input.CartID,
			Event:         acceptedEvents[0],
		}},
	}, nil
}

func checkoutReserveStock(ctx context.Context, saga *monotonic.Saga, input checkoutSagaInput, store monotonic.Store) (monotonic.ActionResult, error) {
	cart, err := LoadCart(ctx, store, input.CartID)
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	events := []monotonic.AggregateEvent{}

	for _, sku := range cart.Items {
		stock, err := LoadStock(ctx, store, sku)
		if err != nil {
			return monotonic.ActionResult{}, err
		}

		acceptedEvents, err := stock.Accept(ctx,monotonic.NewEvent(EventStockReserved, StockReservedPayload{
			SagaID:   saga.ID.ID,
			Quantity: 1,
		}))
		if err != nil {
			return monotonic.ActionResult{}, err
		}

		events = append(events, monotonic.AggregateEvent{
			AggregateType: "stock",
			AggregateID:   sku,
			Event:         acceptedEvents[0],
		})
	}

	return monotonic.ActionResult{
		NewState: CheckoutCreatingToken,
		Events:   events,
	}, nil
}

func checkoutCreatePaymentToken(ctx context.Context, saga *monotonic.Saga, input checkoutSagaInput, store monotonic.Store) (monotonic.ActionResult, error) {
	cart, err := LoadCart(ctx, store, input.CartID)
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	// Generate a payment token (in real code, this might call a payment service)
	token := "tok_" + saga.ID.ID

	acceptedEvents, err := cart.Accept(ctx,monotonic.NewEvent(EventPaymentTokenSet, PaymentTokenSetPayload{Token: token}))
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	return monotonic.ActionResult{
		NewState: CheckoutChargingPayment,
		Events: []monotonic.AggregateEvent{{
			AggregateType: "cart",
			AggregateID:   input.CartID,
			Event:         acceptedEvents[0],
		}},
	}, nil
}

func checkoutChargePayment(ctx context.Context, saga *monotonic.Saga, input checkoutSagaInput, store monotonic.Store) (monotonic.ActionResult, error) {
	cart, err := LoadCart(ctx, store, input.CartID)
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	// In real code:
	// 1. Load cart to get payment token and total
	// 2. Call external payment API with idempotency key
	// 3. Handle success/failure

	// Simulating success for this example
	paymentSucceeded := true

	if !paymentSucceeded {
		return monotonic.ActionResult{NewState: CheckoutPaymentFailed}, nil
	}

	acceptedEvents, err := cart.Accept(ctx,monotonic.Event{Type: EventPaymentCharged})
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	return monotonic.ActionResult{
		NewState: CheckoutConfirmingStock,
		Events: []monotonic.AggregateEvent{{
			AggregateType: "cart",
			AggregateID:   input.CartID,
			Event:         acceptedEvents[0],
		}},
	}, nil
}

func checkoutConfirmStock(ctx context.Context, saga *monotonic.Saga, input checkoutSagaInput, store monotonic.Store) (monotonic.ActionResult, error) {
	cart, err := LoadCart(ctx, store, input.CartID)
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	events := []monotonic.AggregateEvent{}

	for _, sku := range cart.Items {
		stock, err := LoadStock(ctx, store, sku)
		if err != nil {
			return monotonic.ActionResult{}, err
		}

		acceptedEvents, err := stock.Accept(ctx,monotonic.NewEvent(EventReservationConfirmed, ReservationPayload{
			SagaID: saga.ID.ID,
		}))
		if err != nil {
			return monotonic.ActionResult{}, err
		}

		events = append(events, monotonic.AggregateEvent{
			AggregateType: "stock",
			AggregateID:   sku,
			Event:         acceptedEvents[0],
		})
	}

	return monotonic.ActionResult{
		NewState: CheckoutCompleted,
		Events:   events,
	}, nil
}

func checkoutPaymentFailed(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
	// Retry payment after a delay
	return monotonic.ActionResult{
		NewState: CheckoutChargingPayment,
		ReadyAt:  time.Now().Add(1 * time.Minute),
	}, nil
}

func checkoutComplete(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
	return monotonic.ActionResult{Complete: true}, nil
}

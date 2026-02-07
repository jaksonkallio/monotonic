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

// CheckoutActions returns the action map for checkout sagas
func CheckoutActions() monotonic.ActionMap {
	return monotonic.ActionMap{
		CheckoutStarted:         checkoutStart,
		CheckoutReservingStock:  checkoutReserveStock,
		CheckoutCreatingToken:   checkoutCreatePaymentToken,
		CheckoutChargingPayment: checkoutChargePayment,
		CheckoutPaymentFailed:   checkoutPaymentFailed,
		CheckoutConfirmingStock: checkoutConfirmStock,
		CheckoutCompleted:       checkoutComplete, // explicitly close
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

type checkoutSagaInput struct {
	CartID string `json:"cart_id"`
}

func checkoutStart(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
	// Load the cart and prepare checkout-started event
	var input checkoutSagaInput
	if err := json.Unmarshal(saga.Input(), &input); err != nil {
		return monotonic.ActionResult{}, err
	}

	cart, err := LoadCart(store, input.CartID)
	if err != nil {
		return monotonic.ActionResult{}, err
	}
	acceptedEvents, err := cart.Accept(monotonic.Event{Type: EventCheckoutStarted})
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

func checkoutReserveStock(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
	var input checkoutSagaInput
	if err := json.Unmarshal(saga.Input(), &input); err != nil {
		return monotonic.ActionResult{}, err
	}

	// Load the cart
	cart, err := LoadCart(store, input.CartID)
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	// Reserve stock for each item in the cart
	events := []monotonic.AggregateEvent{}

	for _, sku := range cart.Items {
		stock, err := LoadStock(store, sku)
		if err != nil {
			return monotonic.ActionResult{}, err
		}

		acceptedEvents, err := stock.Accept(monotonic.NewEvent(EventStockReserved, StockReservedPayload{
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

func checkoutCreatePaymentToken(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
	// Unmarshal saga input
	var input checkoutSagaInput
	if err := json.Unmarshal(saga.Input(), &input); err != nil {
		return monotonic.ActionResult{}, err
	}

	// Load the cart
	cart, err := LoadCart(store, input.CartID)
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	// Generate a payment token (in real code, this might call a payment service)
	token := "tok_" + saga.ID.ID

	// Store the token on the cart
	acceptedEvents, err := cart.Accept(monotonic.NewEvent(EventPaymentTokenSet, PaymentTokenSetPayload{Token: token}))
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

func checkoutChargePayment(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
	// Unmarshal saga input
	var input checkoutSagaInput
	if err := json.Unmarshal(saga.Input(), &input); err != nil {
		return monotonic.ActionResult{}, err
	}

	// Load the cart
	cart, err := LoadCart(store, input.CartID)
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

	acceptedEvents, err := cart.Accept(monotonic.Event{Type: EventPaymentCharged})
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	// Transition to confirming stock (finalize the reservations)
	return monotonic.ActionResult{
		NewState: CheckoutConfirmingStock,
		Events: []monotonic.AggregateEvent{{
			AggregateType: "cart",
			AggregateID:   input.CartID,
			Event:         acceptedEvents[0],
		}},
	}, nil
}

func checkoutConfirmStock(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
	var input checkoutSagaInput
	if err := json.Unmarshal(saga.Input(), &input); err != nil {
		return monotonic.ActionResult{}, err
	}

	// Load the cart to get the items
	cart, err := LoadCart(store, input.CartID)
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	// Confirm each stock reservation
	events := []monotonic.AggregateEvent{}

	for _, sku := range cart.Items {
		stock, err := LoadStock(store, sku)
		if err != nil {
			return monotonic.ActionResult{}, err
		}

		acceptedEvents, err := stock.Accept(monotonic.NewEvent(EventReservationConfirmed, ReservationPayload{
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
		Delay:    1 * time.Minute,
	}, nil
}

func checkoutComplete(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (monotonic.ActionResult, error) {
	// Close the saga, no other fields allowed
	return monotonic.ActionResult{Complete: true}, nil
}

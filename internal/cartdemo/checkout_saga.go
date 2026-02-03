package cartdemo

import (
	"context"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// Checkout saga states
const (
	CheckoutStarted         = "started"
	CheckoutReservingStock  = "reserving_stock"
	CheckoutCreatingToken   = "creating_payment_token"
	CheckoutChargingPayment = "charging_payment"
	CheckoutPaymentFailed   = "payment_failed"
	CheckoutCompleted       = "completed"
)

// CheckoutActions returns the action map for checkout sagas
func CheckoutActions() monotonic.ActionMap {
	return monotonic.ActionMap{
		CheckoutStarted:         checkoutStart,
		CheckoutReservingStock:  checkoutReserveStock,
		CheckoutCreatingToken:   checkoutCreatePaymentToken,
		CheckoutChargingPayment: checkoutChargePayment,
		CheckoutPaymentFailed:   checkoutPaymentFailed,
		CheckoutCompleted:       checkoutComplete, // explicitly close
	}
}

// StartCheckoutSaga creates a new checkout saga for a cart
func StartCheckoutSaga(ctx context.Context, store monotonic.Store, sagaID string, cartID string) (*monotonic.Saga, error) {
	return monotonic.NewSaga(
		store,
		"checkout-saga",
		sagaID,
		CheckoutStarted,
		map[string]monotonic.AggregateID{
			"cart": {Type: "cart", ID: cartID},
		},
		CheckoutActions(),
	)
}

// LoadCheckoutSaga loads an existing checkout saga
func LoadCheckoutSaga(store monotonic.Store, sagaID string) (*monotonic.Saga, error) {
	return monotonic.LoadSaga(store, "checkout-saga", sagaID, CheckoutActions())
}

func checkoutStart(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (*monotonic.ActionResult, error) {
	// Load the cart to mark checkout started
	cart, err := LoadCart(store, saga.Refs["cart"].ID)
	if err != nil {
		return nil, err
	}

	// Prepare checkout-started event for the cart
	cartEvent, err := monotonic.PrepareEvent(
		saga.Refs["cart"].Type,
		saga.Refs["cart"].ID,
		cart.Counter()+1,
		"checkout-started",
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &monotonic.ActionResult{
		NewState:    CheckoutReservingStock,
		ExtraEvents: []monotonic.AggregateEvent{cartEvent},
	}, nil
}

func checkoutReserveStock(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (*monotonic.ActionResult, error) {
	// Load cart to get items
	cart, err := LoadCart(store, saga.Refs["cart"].ID)
	if err != nil {
		return nil, err
	}

	// Each item is a separate stock aggregate, so each gets counter=1
	events := []monotonic.AggregateEvent{}

	for _, item := range cart.Items {
		event, err := monotonic.PrepareEvent(
			"stock",
			item, // item SKU is the stock aggregate ID
			1,    // each stock aggregate is new, starts at counter 1
			"reserved",
			map[string]any{
				"saga_id":  saga.ID.ID,
				"quantity": 1,
			},
		)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return &monotonic.ActionResult{
		NewState:    CheckoutCreatingToken,
		ExtraEvents: events,
	}, nil
}

func checkoutCreatePaymentToken(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (*monotonic.ActionResult, error) {
	cart, err := LoadCart(store, saga.Refs["cart"].ID)
	if err != nil {
		return nil, err
	}

	// Generate a payment token (in real code, this might call a payment service)
	token := "tok_" + saga.ID.ID

	// Store the token on the cart
	cartEvent, err := monotonic.PrepareEvent(
		saga.Refs["cart"].Type,
		saga.Refs["cart"].ID,
		cart.Counter()+1,
		"payment-token-set",
		map[string]string{"token": token},
	)
	if err != nil {
		return nil, err
	}

	return &monotonic.ActionResult{
		NewState:    CheckoutChargingPayment,
		ExtraEvents: []monotonic.AggregateEvent{cartEvent},
	}, nil
}

func checkoutChargePayment(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (*monotonic.ActionResult, error) {
	// In real code:
	// 1. Load cart to get payment token and total
	// 2. Call external payment API with idempotency key
	// 3. Handle success/failure

	// Simulating success for this example
	paymentSucceeded := true

	if !paymentSucceeded {
		return &monotonic.ActionResult{NewState: CheckoutPaymentFailed}, nil
	}

	// Mark cart as paid
	cart, err := LoadCart(store, saga.Refs["cart"].ID)
	if err != nil {
		return nil, err
	}

	cartEvent, err := monotonic.PrepareEvent(
		saga.Refs["cart"].Type,
		saga.Refs["cart"].ID,
		cart.Counter()+1,
		"payment-charged",
		nil,
	)
	if err != nil {
		return nil, err
	}

	// Transition to completed state (close happens in next step)
	return &monotonic.ActionResult{
		NewState:    CheckoutCompleted,
		ExtraEvents: []monotonic.AggregateEvent{cartEvent},
	}, nil
}

func checkoutPaymentFailed(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (*monotonic.ActionResult, error) {
	// Retry payment after a delay
	return &monotonic.ActionResult{
		NewState: CheckoutChargingPayment,
		Delay:    1 * time.Minute,
	}, nil
}

func checkoutComplete(ctx context.Context, saga *monotonic.Saga, store monotonic.Store) (*monotonic.ActionResult, error) {
	// Close the saga - no other fields allowed
	return &monotonic.ActionResult{Close: true}, nil
}

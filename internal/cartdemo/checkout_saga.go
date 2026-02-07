package cartdemo

import (
	"context"
	"encoding/json"
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
	acceptedEvents, err := cart.AggregateBase.Accept(monotonic.Event{Type: "checkout-started"})
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

	// Each item is a separate stock aggregate, so each gets counter=1
	// Stock doesn't have a full aggregate implementation, so we create events directly
	events := []monotonic.AggregateEvent{}

	for _, item := range cart.Items {
		payload, _ := json.Marshal(map[string]any{
			"saga_id":  saga.ID.ID,
			"quantity": 1,
		})
		events = append(events, monotonic.AggregateEvent{
			AggregateType: "stock",
			AggregateID:   item, // item SKU is the stock aggregate ID
			Event: monotonic.AcceptedEvent{
				Event: monotonic.Event{
					Type:    "reserved",
					Payload: payload,
				},
				Counter:    1, // each stock aggregate is new
				AcceptedAt: time.Now(),
			},
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
	payload, _ := json.Marshal(map[string]string{"token": token})
	acceptedEvents, err := cart.AggregateBase.Accept(monotonic.Event{
		Type:    "payment-token-set",
		Payload: payload,
	})
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

	acceptedEvents, err := cart.AggregateBase.Accept(monotonic.Event{Type: "payment-charged"})
	if err != nil {
		return monotonic.ActionResult{}, err
	}

	// Transition to completed state (close happens in next step)
	return monotonic.ActionResult{
		NewState: CheckoutCompleted,
		Events: []monotonic.AggregateEvent{{
			AggregateType: "cart",
			AggregateID:   input.CartID,
			Event:         acceptedEvents[0],
		}},
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

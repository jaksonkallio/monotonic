package cartdemo

import (
	"context"
	"errors"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// Cart event types
const (
	EventItemAdded       = "item-added"
	EventCheckoutStarted = "checkout-started"
	EventPaymentTokenSet = "payment-token-set"
	EventPaymentCharged  = "payment-charged"
)

type ItemAddedPayload struct {
	ItemName string `json:"item_name"`
}

type PaymentTokenSetPayload struct {
	Token string `json:"token"`
}

// Cart is an example aggregate demonstrating the framework pattern.
// It embeds AggregateBase and uses plain Go fields for state.
type Cart struct {
	*monotonic.AggregateBase

	// State - plain Go fields
	Items           []string
	CheckoutStarted bool
	PaymentToken    string
	PaymentCharged  bool
}

// LoadCart hydrates a Cart aggregate from the store
func LoadCart(ctx context.Context, store monotonic.Store, id string) (*Cart, error) {
	return monotonic.Hydrate(ctx, store, "cart", id, func(base *monotonic.AggregateBase) *Cart {
		return &Cart{AggregateBase: base}
	})
}

func (c *Cart) Apply(event monotonic.AcceptedEvent) {
	switch event.Type {
	case EventItemAdded:
		if p, ok := monotonic.ParsePayload[ItemAddedPayload](event); ok {
			c.Items = append(c.Items, p.ItemName)
		}

	case EventCheckoutStarted:
		c.CheckoutStarted = true

	case EventPaymentTokenSet:
		if p, ok := monotonic.ParsePayload[PaymentTokenSetPayload](event); ok {
			c.PaymentToken = p.Token
		}

	case EventPaymentCharged:
		c.PaymentCharged = true
	}
}

func (c *Cart) ShouldAccept(event monotonic.Event) error {
	switch event.Type {
	case EventItemAdded:
		return nil

	case EventCheckoutStarted:
		if c.CheckoutStarted {
			return errors.New("checkout has already been started")
		}
		if len(c.Items) == 0 {
			return errors.New("cannot start checkout on an empty cart")
		}
		return nil

	case EventPaymentTokenSet:
		if !c.CheckoutStarted {
			return errors.New("checkout has not been started")
		}
		return nil

	case EventPaymentCharged:
		if c.PaymentToken == "" {
			return errors.New("no payment token set")
		}
		if c.PaymentCharged {
			return errors.New("payment has already been charged")
		}
		return nil
	}

	return errors.New("unrecognized event type: " + event.Type)
}

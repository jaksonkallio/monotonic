package cartdemo

import (
	"encoding/json"
	"errors"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

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
func LoadCart(store monotonic.Store, id string) (*Cart, error) {
	return monotonic.Hydrate(store, "cart", id, func(base *monotonic.AggregateBase) *Cart {
		return &Cart{AggregateBase: base}
	})
}

func (c *Cart) Apply(event monotonic.Event) {
	switch event.Type {
	case "item-added":
		var payload struct {
			ItemName string `json:"item_name"`
		}
		json.Unmarshal(event.Payload, &payload)
		c.Items = append(c.Items, payload.ItemName)

	case "checkout-started":
		c.CheckoutStarted = true

	case "payment-token-set":
		var payload struct {
			Token string `json:"token"`
		}
		json.Unmarshal(event.Payload, &payload)
		c.PaymentToken = payload.Token

	case "payment-charged":
		c.PaymentCharged = true
	}
}

func (c *Cart) Validate(event monotonic.Event) error {
	switch event.Type {
	case "item-added":
		return nil

	case "checkout-started":
		if c.CheckoutStarted {
			return errors.New("checkout has already been started")
		}
		if len(c.Items) == 0 {
			return errors.New("cannot start checkout on an empty cart")
		}
		return nil

	case "payment-token-set":
		if !c.CheckoutStarted {
			return errors.New("checkout has not been started")
		}
		return nil

	case "payment-charged":
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

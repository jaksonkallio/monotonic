package cartdemo

import (
	"context"
	"testing"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// OrderStatsProjection tracks order statistics across carts and stock
type OrderStatsProjection struct {
	TotalCheckouts int
	TotalPayments  int
	ItemsReserved  map[string]int // SKU -> count
	ItemsSold      map[string]int // SKU -> count
}

func NewOrderStatsProjection() *OrderStatsProjection {
	return &OrderStatsProjection{
		ItemsReserved: make(map[string]int),
		ItemsSold:     make(map[string]int),
	}
}

func (p *OrderStatsProjection) AggregateTypes() []string {
	return []string{"cart", "stock"}
}

func (p *OrderStatsProjection) Apply(event monotonic.AggregateEvent) {
	switch event.AggregateType {
	case "cart":
		switch event.Event.Type {
		case EventCheckoutStarted:
			p.TotalCheckouts++
		case EventPaymentCharged:
			p.TotalPayments++
		}
	case "stock":
		switch event.Event.Type {
		case EventStockReserved:
			if payload, ok := monotonic.ParsePayload[StockReservedPayload](event.Event); ok {
				p.ItemsReserved[event.AggregateID] += payload.Quantity
			}
		case EventReservationConfirmed:
			if payload, ok := monotonic.ParsePayload[ReservationPayload](event.Event); ok {
				// Move from reserved to sold
				reserved := p.ItemsReserved[event.AggregateID]
				if reserved > 0 {
					p.ItemsReserved[event.AggregateID]--
					p.ItemsSold[event.AggregateID]++
				}
				_ = payload // payload has saga_id if needed
			}
		}
	}
}

func TestProjection(t *testing.T) {
	store := monotonic.NewInMemoryStore()
	ctx := context.Background()

	// Set up some stock
	widget, _ := LoadStock(ctx, store, "widget")
	widget.AcceptThenApply(ctx, monotonic.NewEvent(EventStockAdded, StockAddedPayload{Quantity: 100}))

	gadget, _ := LoadStock(ctx, store, "gadget")
	gadget.AcceptThenApply(ctx, monotonic.NewEvent(EventStockAdded, StockAddedPayload{Quantity: 50}))

	// Create a cart and go through checkout
	cart, _ := LoadCart(ctx, store, "cart-1")
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "widget"}))
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "gadget"}))

	// Simulate checkout steps (normally done by saga, but we'll do directly for testing)
	cart.AcceptThenApply(ctx, monotonic.Event{Type: EventCheckoutStarted})

	// Reserve stock
	widget.AcceptThenApply(ctx, monotonic.NewEvent(EventStockReserved, StockReservedPayload{SagaID: "saga-1", Quantity: 1}))
	gadget.AcceptThenApply(ctx, monotonic.NewEvent(EventStockReserved, StockReservedPayload{SagaID: "saga-1", Quantity: 1}))

	// Payment
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventPaymentTokenSet, PaymentTokenSetPayload{Token: "tok_123"}))
	cart.AcceptThenApply(ctx, monotonic.Event{Type: EventPaymentCharged})

	// Confirm reservations
	widget.AcceptThenApply(ctx, monotonic.NewEvent(EventReservationConfirmed, ReservationPayload{SagaID: "saga-1"}))
	gadget.AcceptThenApply(ctx, monotonic.NewEvent(EventReservationConfirmed, ReservationPayload{SagaID: "saga-1"}))

	// Create projection and catch up
	stats := NewOrderStatsProjection()
	projection := monotonic.NewProjection(store, stats)

	processed, err := projection.Update(ctx)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// We should have processed: 2 stock-added + 2 item-added + 1 checkout-started +
	// 2 stock-reserved + 1 payment-token-set + 1 payment-charged + 2 reservation-confirmed = 11 events
	// But projection only subscribes to cart and stock, so all 11 are relevant
	if processed != 11 {
		t.Errorf("expected 11 events processed, got %d", processed)
	}

	// Verify projection state
	if stats.TotalCheckouts != 1 {
		t.Errorf("expected 1 checkout, got %d", stats.TotalCheckouts)
	}
	if stats.TotalPayments != 1 {
		t.Errorf("expected 1 payment, got %d", stats.TotalPayments)
	}
	if stats.ItemsSold["widget"] != 1 {
		t.Errorf("expected 1 widget sold, got %d", stats.ItemsSold["widget"])
	}
	if stats.ItemsSold["gadget"] != 1 {
		t.Errorf("expected 1 gadget sold, got %d", stats.ItemsSold["gadget"])
	}

	// Add more events and catch up again
	cart2, _ := LoadCart(ctx, store, "cart-2")
	cart2.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "widget"}))
	cart2.AcceptThenApply(ctx, monotonic.Event{Type: EventCheckoutStarted})

	processed, err = projection.Update(ctx)
	if err != nil {
		t.Fatalf("second CatchUp failed: %v", err)
	}

	if processed != 2 {
		t.Errorf("expected 2 new events processed, got %d", processed)
	}

	if stats.TotalCheckouts != 2 {
		t.Errorf("expected 2 total checkouts, got %d", stats.TotalCheckouts)
	}

	// Verify global counter tracking
	if projection.GlobalCounter() == 0 {
		t.Error("expected global counter to be updated")
	}
}

func TestProjectionFromCounter(t *testing.T) {
	store := monotonic.NewInMemoryStore()
	ctx := context.Background()

	// Create some events
	cart, _ := LoadCart(ctx, store, "cart-1")
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "widget"}))
	cart.AcceptThenApply(ctx, monotonic.Event{Type: EventCheckoutStarted})
	cart.AcceptThenApply(ctx, monotonic.Event{Type: EventPaymentCharged})

	// Create projection and catch up
	stats1 := NewOrderStatsProjection()
	proj1 := monotonic.NewProjection(store, stats1)
	proj1.Update(ctx)

	savedCounter := proj1.GlobalCounter()

	// Add more events
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "gadget"}))

	// Create a new projection starting from saved counter (simulating resume)
	stats2 := NewOrderStatsProjection()
	proj2 := monotonic.NewProjectionFrom(store, stats2, savedCounter)

	processed, _ := proj2.Update(ctx)

	// Should only process the new event
	if processed != 1 {
		t.Errorf("expected 1 event processed from saved counter, got %d", processed)
	}
}

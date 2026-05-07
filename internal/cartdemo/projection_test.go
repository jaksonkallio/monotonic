package cartdemo

import (
	"context"
	"testing"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// OrderStatsSummary tracks order statistics across carts and stock as a summary projection.
type OrderStatsSummary struct {
	TotalCheckouts int
	TotalPayments  int
	ItemsReserved  map[string]int
	ItemsSold      map[string]int
}

// orderStatsLogic is the ProjectorLogic that folds cart and stock events into an OrderStatsSummary.
type orderStatsLogic struct{}

func (l *orderStatsLogic) Apply(ctx context.Context, reader monotonic.ProjectionReader[OrderStatsSummary], event monotonic.AggregateEvent) ([]monotonic.Projected[OrderStatsSummary], error) {
	summary, _, err := reader.Get(ctx, monotonic.ProjectionKeySummary)
	if err != nil {
		return nil, err
	}
	// Zero-value summary has nil maps; initialize on first write.
	if summary.ItemsReserved == nil {
		summary.ItemsReserved = make(map[string]int)
	}
	if summary.ItemsSold == nil {
		summary.ItemsSold = make(map[string]int)
	}

	switch event.AggregateType {
	case "cart":
		switch event.Event.Type {
		case EventCheckoutStarted:
			summary.TotalCheckouts++
		case EventPaymentCharged:
			summary.TotalPayments++
		}
	case "stock":
		switch event.Event.Type {
		case EventStockReserved:
			if payload, ok := monotonic.ParsePayload[StockReservedPayload](event.Event); ok {
				summary.ItemsReserved[event.AggregateID] += payload.Quantity
			}
		case EventReservationConfirmed:
			if reserved := summary.ItemsReserved[event.AggregateID]; reserved > 0 {
				summary.ItemsReserved[event.AggregateID]--
				summary.ItemsSold[event.AggregateID]++
			}
		}
	}

	return []monotonic.Projected[OrderStatsSummary]{{Key: monotonic.ProjectionKeySummary, Value: summary}}, nil
}

func TestSummaryProjector(t *testing.T) {
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

	persistence := monotonic.NewInMemoryProjectionPersistence[OrderStatsSummary]()
	filters := []monotonic.EventFilter{{AggregateType: "cart"}, {AggregateType: "stock"}}

	projector, err := monotonic.NewProjector(ctx, store, filters, &orderStatsLogic{}, persistence)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	processed, err := projector.Update(ctx)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	// 2 stock-added + 2 item-added + 1 checkout-started + 2 stock-reserved + 1 payment-token-set + 1 payment-charged + 2 reservation-confirmed = 11
	if processed != 11 {
		t.Errorf("expected 11 events processed, got %d", processed)
	}

	summary, _, _ := persistence.Get(ctx, monotonic.ProjectionKeySummary)
	if summary.TotalCheckouts != 1 {
		t.Errorf("expected 1 checkout, got %d", summary.TotalCheckouts)
	}
	if summary.TotalPayments != 1 {
		t.Errorf("expected 1 payment, got %d", summary.TotalPayments)
	}
	if summary.ItemsSold["widget"] != 1 {
		t.Errorf("expected 1 widget sold, got %d", summary.ItemsSold["widget"])
	}
	if summary.ItemsSold["gadget"] != 1 {
		t.Errorf("expected 1 gadget sold, got %d", summary.ItemsSold["gadget"])
	}

	// Add more events and catch up again
	cart2, _ := LoadCart(ctx, store, "cart-2")
	cart2.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "widget"}))
	cart2.AcceptThenApply(ctx, monotonic.Event{Type: EventCheckoutStarted})

	processed, err = projector.Update(ctx)
	if err != nil {
		t.Fatalf("second Update failed: %v", err)
	}
	if processed != 2 {
		t.Errorf("expected 2 new events processed, got %d", processed)
	}

	summary, _, _ = persistence.Get(ctx, monotonic.ProjectionKeySummary)
	if summary.TotalCheckouts != 2 {
		t.Errorf("expected 2 total checkouts, got %d", summary.TotalCheckouts)
	}
	if projector.GlobalCounter() == 0 {
		t.Error("expected global counter to be updated")
	}
}

func TestSummaryProjectorResume(t *testing.T) {
	store := monotonic.NewInMemoryStore()
	ctx := context.Background()

	cart, _ := LoadCart(ctx, store, "cart-1")
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "widget"}))
	cart.AcceptThenApply(ctx, monotonic.Event{Type: EventCheckoutStarted})
	cart.AcceptThenApply(ctx, monotonic.Event{Type: EventPaymentCharged})

	persistence := monotonic.NewInMemoryProjectionPersistence[OrderStatsSummary]()
	filters := []monotonic.EventFilter{{AggregateType: "cart"}, {AggregateType: "stock"}}

	first, err := monotonic.NewProjector(ctx, store, filters, &orderStatsLogic{}, persistence)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := first.Update(ctx); err != nil {
		t.Fatalf("first Update: %v", err)
	}

	// Add a new event after the first projector caught up.
	cart.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "gadget"}))

	// A new projector against the same persistence resumes from the stored counter.
	resumed, err := monotonic.NewProjector(ctx, store, filters, &orderStatsLogic{}, persistence)
	if err != nil {
		t.Fatalf("NewProjector resume: %v", err)
	}

	processed, err := resumed.Update(ctx)
	if err != nil {
		t.Fatalf("resumed Update: %v", err)
	}
	if processed != 1 {
		t.Errorf("expected 1 event processed from saved counter, got %d", processed)
	}
}

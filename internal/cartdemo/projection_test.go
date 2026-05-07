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

// newOrderStatsLogic builds the ProjectorLogic that folds cart and stock events into an OrderStatsSummary.
func newOrderStatsLogic() monotonic.ProjectorLogic[OrderStatsSummary] {
	return monotonic.NewDispatch[OrderStatsSummary]().
		On("cart", EventCheckoutStarted, statsOnCheckoutStarted).
		On("cart", EventPaymentCharged, statsOnPaymentCharged).
		On("stock", EventStockReserved, statsOnStockReserved).
		On("stock", EventReservationConfirmed, statsOnReservationConfirmed)
}

// ensureStatsMaps lazily initializes the nil maps on a zero-value summary.
func ensureStatsMaps(s *OrderStatsSummary) {
	if s.ItemsReserved == nil {
		s.ItemsReserved = make(map[string]int)
	}
	if s.ItemsSold == nil {
		s.ItemsSold = make(map[string]int)
	}
}

func statsOnCheckoutStarted(ctx context.Context, reader monotonic.ProjectionReader[OrderStatsSummary], event monotonic.AggregateEvent) ([]monotonic.Projected[OrderStatsSummary], error) {
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *OrderStatsSummary) error {
		ensureStatsMaps(s)
		s.TotalCheckouts++
		return nil
	})
}

func statsOnPaymentCharged(ctx context.Context, reader monotonic.ProjectionReader[OrderStatsSummary], event monotonic.AggregateEvent) ([]monotonic.Projected[OrderStatsSummary], error) {
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *OrderStatsSummary) error {
		ensureStatsMaps(s)
		s.TotalPayments++
		return nil
	})
}

func statsOnStockReserved(ctx context.Context, reader monotonic.ProjectionReader[OrderStatsSummary], event monotonic.AggregateEvent) ([]monotonic.Projected[OrderStatsSummary], error) {
	payload, ok := monotonic.ParsePayload[StockReservedPayload](event.Event)
	if !ok {
		return nil, nil
	}
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *OrderStatsSummary) error {
		ensureStatsMaps(s)
		s.ItemsReserved[event.AggregateID] += payload.Quantity
		return nil
	})
}

func statsOnReservationConfirmed(ctx context.Context, reader monotonic.ProjectionReader[OrderStatsSummary], event monotonic.AggregateEvent) ([]monotonic.Projected[OrderStatsSummary], error) {
	return monotonic.MutateByKey(ctx, reader, monotonic.ProjectionKeySummary, func(s *OrderStatsSummary) error {
		ensureStatsMaps(s)
		if reserved := s.ItemsReserved[event.AggregateID]; reserved > 0 {
			s.ItemsReserved[event.AggregateID]--
			s.ItemsSold[event.AggregateID]++
		}
		return nil
	})
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

	projector, err := monotonic.NewProjector(ctx, store, newOrderStatsLogic(), persistence)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	processed, err := projector.Update(ctx)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	// Dispatch subscribes to: checkout-started (1), stock-reserved (2), payment-charged (1), reservation-confirmed (2) = 6.
	if processed != 6 {
		t.Errorf("expected 6 events processed, got %d", processed)
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

	// Add more events and catch up again. Only checkout-started is subscribed; item-added is filtered out by the dispatch.
	cart2, _ := LoadCart(ctx, store, "cart-2")
	cart2.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "widget"}))
	cart2.AcceptThenApply(ctx, monotonic.Event{Type: EventCheckoutStarted})

	processed, err = projector.Update(ctx)
	if err != nil {
		t.Fatalf("second Update failed: %v", err)
	}
	if processed != 1 {
		t.Errorf("expected 1 new event processed, got %d", processed)
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

	first, err := monotonic.NewProjector(ctx, store, newOrderStatsLogic(), persistence)
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}
	if _, err := first.Update(ctx); err != nil {
		t.Fatalf("first Update: %v", err)
	}

	// Add an event the dispatch is subscribed to. Cart requires an item before checkout.
	cart2, _ := LoadCart(ctx, store, "cart-2")
	if err := cart2.AcceptThenApply(ctx, monotonic.NewEvent(EventItemAdded, ItemAddedPayload{ItemName: "widget"})); err != nil {
		t.Fatalf("add item to cart-2: %v", err)
	}
	if err := cart2.AcceptThenApply(ctx, monotonic.Event{Type: EventCheckoutStarted}); err != nil {
		t.Fatalf("checkout cart-2: %v", err)
	}

	// A new projector against the same persistence resumes from the stored counter.
	resumed, err := monotonic.NewProjector(ctx, store, newOrderStatsLogic(), persistence)
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

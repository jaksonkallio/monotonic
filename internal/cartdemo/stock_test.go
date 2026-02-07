package cartdemo

import (
	"testing"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

func TestStock(t *testing.T) {
	store := monotonic.NewInMemoryStore()

	// Load stock for a new SKU (starts empty)
	stock, err := LoadStock(store, "widget-001")
	if err != nil {
		t.Fatalf("LoadStock failed: %v", err)
	}

	if stock.Available != 0 {
		t.Errorf("expected 0 available, got %d", stock.Available)
	}

	// Add stock
	err = stock.AcceptThenApply(monotonic.Event{
		Type:    "stock-added",
		Payload: []byte(`{"quantity":100}`),
	})
	if err != nil {
		t.Fatalf("stock-added failed: %v", err)
	}

	if stock.Available != 100 {
		t.Errorf("expected 100 available, got %d", stock.Available)
	}

	// Reserve some stock
	err = stock.AcceptThenApply(monotonic.Event{
		Type:    "stock-reserved",
		Payload: []byte(`{"saga_id":"checkout-1","quantity":10}`),
	})
	if err != nil {
		t.Fatalf("stock-reserved failed: %v", err)
	}

	if stock.Available != 90 {
		t.Errorf("expected 90 available after reservation, got %d", stock.Available)
	}
	if stock.Reserved["checkout-1"] != 10 {
		t.Errorf("expected 10 reserved for checkout-1, got %d", stock.Reserved["checkout-1"])
	}

	// Try to reserve more than available - should fail
	err = stock.AcceptThenApply(monotonic.Event{
		Type:    "stock-reserved",
		Payload: []byte(`{"saga_id":"checkout-2","quantity":100}`),
	})
	if err == nil {
		t.Error("expected error when reserving more than available")
	}

	// Confirm the first reservation
	err = stock.AcceptThenApply(monotonic.Event{
		Type:    "reservation-confirmed",
		Payload: []byte(`{"saga_id":"checkout-1"}`),
	})
	if err != nil {
		t.Fatalf("reservation-confirmed failed: %v", err)
	}

	if stock.Available != 90 {
		t.Errorf("expected 90 available after confirmation, got %d", stock.Available)
	}
	if _, exists := stock.Reserved["checkout-1"]; exists {
		t.Error("expected reservation to be removed after confirmation")
	}

	// Hydrate and verify state persists
	stock2, err := LoadStock(store, "widget-001")
	if err != nil {
		t.Fatalf("LoadStock for hydration failed: %v", err)
	}
	if stock2.Available != 90 {
		t.Errorf("expected hydrated stock to have 90 available, got %d", stock2.Available)
	}
	if stock2.TotalReserved() != 0 {
		t.Errorf("expected hydrated stock to have 0 reserved, got %d", stock2.TotalReserved())
	}
}

func TestStockRelease(t *testing.T) {
	store := monotonic.NewInMemoryStore()

	stock, _ := LoadStock(store, "gadget-001")
	stock.AcceptThenApply(monotonic.Event{
		Type:    "stock-added",
		Payload: []byte(`{"quantity":50}`),
	})
	stock.AcceptThenApply(monotonic.Event{
		Type:    "stock-reserved",
		Payload: []byte(`{"saga_id":"order-1","quantity":20}`),
	})

	if stock.Available != 30 {
		t.Errorf("expected 30 available, got %d", stock.Available)
	}

	// Release the reservation (e.g., order cancelled)
	err := stock.AcceptThenApply(monotonic.Event{
		Type:    "reservation-released",
		Payload: []byte(`{"saga_id":"order-1"}`),
	})
	if err != nil {
		t.Fatalf("reservation-released failed: %v", err)
	}

	if stock.Available != 50 {
		t.Errorf("expected 50 available after release, got %d", stock.Available)
	}
	if stock.TotalReserved() != 0 {
		t.Errorf("expected 0 reserved after release, got %d", stock.TotalReserved())
	}
}

package cartdemo

import (
	"encoding/json"
	"testing"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

func TestCartCatchUp(t *testing.T) {
	store := monotonic.NewInMemoryStore()

	// Process A loads cart
	cartA, _ := LoadCart(store, "user-123")

	// Process B loads the same cart
	cartB, _ := LoadCart(store, "user-123")

	// Process A adds an item
	payload, _ := json.Marshal(map[string]string{"item_name": "Widget"})
	cartA.Record(monotonic.Event{Type: "item-added", Payload: payload})

	// cartA sees the item, cartB doesn't (yet)
	if len(cartA.Items) != 1 {
		t.Errorf("cartA: expected 1 item, got %d", len(cartA.Items))
	}
	if len(cartB.Items) != 0 {
		t.Errorf("cartB: expected 0 items before catch-up, got %d", len(cartB.Items))
	}

	// Process B proposes another event - this should catch up first
	payload, _ = json.Marshal(map[string]string{"item_name": "Gadget"})
	err := cartB.Record(monotonic.Event{Type: "item-added", Payload: payload})
	if err != nil {
		t.Fatalf("cartB Record failed: %v", err)
	}

	// Now cartB should have both items (caught up + its own)
	if len(cartB.Items) != 2 {
		t.Errorf("cartB: expected 2 items after propose, got %d", len(cartB.Items))
	}
	if cartB.Counter() != 2 {
		t.Errorf("cartB: expected counter 2, got %d", cartB.Counter())
	}

	// Verify the store has both events
	events, _ := store.Load("cart", "user-123")
	if len(events) != 2 {
		t.Errorf("store: expected 2 events, got %d", len(events))
	}
}

func TestCart(t *testing.T) {
	store := monotonic.NewInMemoryStore()

	// Load a new cart (no events yet)
	cart, err := LoadCart(store, "user-123")
	if err != nil {
		t.Fatalf("LoadCart failed: %v", err)
	}

	// Verify initial state
	if len(cart.Items) != 0 {
		t.Errorf("expected empty cart, got %d items", len(cart.Items))
	}
	if cart.CheckoutStarted {
		t.Error("expected checkout not started")
	}

	// Try to checkout empty cart - should fail
	err = cart.Record(monotonic.Event{Type: "checkout-started"})
	if err == nil {
		t.Error("expected error when checking out empty cart")
	}

	// Add an item
	payload, _ := json.Marshal(map[string]string{"item_name": "Widget"})
	err = cart.Record(monotonic.Event{Type: "item-added", Payload: payload})
	if err != nil {
		t.Fatalf("Record item-added failed: %v", err)
	}

	// Verify state updated
	if len(cart.Items) != 1 || cart.Items[0] != "Widget" {
		t.Errorf("expected [Widget], got %v", cart.Items)
	}
	if cart.Counter() != 1 {
		t.Errorf("expected counter 1, got %d", cart.Counter())
	}

	// Add another item
	payload, _ = json.Marshal(map[string]string{"item_name": "Gadget"})
	err = cart.Record(monotonic.Event{Type: "item-added", Payload: payload})
	if err != nil {
		t.Fatalf("Record item-added failed: %v", err)
	}

	// Start checkout - should succeed now
	err = cart.Record(monotonic.Event{Type: "checkout-started"})
	if err != nil {
		t.Fatalf("Record checkout-started failed: %v", err)
	}

	if !cart.CheckoutStarted {
		t.Error("expected checkout to be started")
	}

	// Try to start checkout again - should fail
	err = cart.Record(monotonic.Event{Type: "checkout-started"})
	if err == nil {
		t.Error("expected error when starting checkout twice")
	}

	// Verify counter
	if cart.Counter() != 3 {
		t.Errorf("expected counter 3, got %d", cart.Counter())
	}

	// Test hydration - load the same cart fresh and verify state is rebuilt
	cart2, err := LoadCart(store, "user-123")
	if err != nil {
		t.Fatalf("LoadCart (hydrate) failed: %v", err)
	}

	if len(cart2.Items) != 2 {
		t.Errorf("hydrated cart: expected 2 items, got %d", len(cart2.Items))
	}
	if !cart2.CheckoutStarted {
		t.Error("hydrated cart: expected checkout started")
	}
	if cart2.Counter() != 3 {
		t.Errorf("hydrated cart: expected counter 3, got %d", cart2.Counter())
	}
}

package monotonic

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

const (
	eventIncremented = "incremented"
	eventDecremented = "decremented"
)

type incrementedPayload struct {
	Amount int `json:"amount"`
}

type counter struct {
	*AggregateBase
	Value int
}

func (c *counter) Apply(event AcceptedEvent) {
	switch event.Type {
	case eventIncremented:
		if p, ok := ParsePayload[incrementedPayload](event); ok {
			c.Value += p.Amount
		}
	case eventDecremented:
		if p, ok := ParsePayload[incrementedPayload](event); ok {
			c.Value -= p.Amount
		}
	}
}

func (c *counter) ShouldAccept(event Event) error {
	switch event.Type {
	case eventIncremented:
		return nil
	case eventDecremented:
		var p incrementedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		if c.Value-p.Amount < 0 {
			return errors.New("counter would go negative")
		}
		return nil
	}
	return errors.New("unknown event type")
}

func loadCounter(ctx context.Context, store Store, id string) (*counter, error) {
	return Hydrate(ctx, store, "counter", id, func(base *AggregateBase) *counter {
		return &counter{AggregateBase: base}
	})
}

func TestNewEvent(t *testing.T) {
	e := NewEvent("incremented", incrementedPayload{Amount: 5})
	if e.Type != "incremented" {
		t.Errorf("expected type 'incremented', got %q", e.Type)
	}
	var p incrementedPayload
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if p.Amount != 5 {
		t.Errorf("expected amount 5, got %d", p.Amount)
	}
}

func TestHydrate(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, err := loadCounter(ctx, store, "c1")
	if err != nil {
		t.Fatalf("loadCounter: %v", err)
	}

	// Apply some events
	if err := c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 3})); err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 7})); err != nil {
		t.Fatal(err)
	}
	if c.Value != 10 {
		t.Fatalf("expected 10, got %d", c.Value)
	}

	// Hydrate a fresh instance and verify state rebuilt
	c2, err := loadCounter(ctx, store, "c1")
	if err != nil {
		t.Fatalf("loadCounter c2: %v", err)
	}
	if c2.Value != 10 {
		t.Errorf("hydrated value: expected 10, got %d", c2.Value)
	}
	if c2.Counter() != c.Counter() {
		t.Errorf("counter mismatch: expected %d, got %d", c.Counter(), c2.Counter())
	}
}

func TestAcceptThenApply(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "c1")

	if err := c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 2})); err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 3})); err != nil {
		t.Fatal(err)
	}

	if c.Value != 5 {
		t.Errorf("expected 5, got %d", c.Value)
	}
	if c.Counter() != 2 {
		t.Errorf("expected counter 2, got %d", c.Counter())
	}
}

func TestAcceptThenApply_Rejection(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "c1")

	// Decrement on a zero counter should be rejected
	err := c.AcceptThenApply(ctx, NewEvent(eventDecremented, incrementedPayload{Amount: 1}))
	if err == nil {
		t.Fatal("expected rejection error, got nil")
	}
	if c.Value != 0 {
		t.Errorf("state should be unchanged, got value %d", c.Value)
	}
	if c.Counter() != 0 {
		t.Errorf("counter should be unchanged, got %d", c.Counter())
	}
}

func TestAcceptThenApply_NoEvents(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "c1")

	err := c.AcceptThenApply(ctx)
	if err != nil {
		t.Fatalf("no-op should not error: %v", err)
	}
	if c.Counter() != 0 {
		t.Errorf("counter should remain 0, got %d", c.Counter())
	}
}

func TestAccept(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "c1")

	accepted, err := c.Accept(ctx,
		NewEvent(eventIncremented, incrementedPayload{Amount: 1}),
		NewEvent(eventIncremented, incrementedPayload{Amount: 2}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 2 {
		t.Fatalf("expected 2 accepted events, got %d", len(accepted))
	}
	if accepted[0].Counter != 1 {
		t.Errorf("first event counter: expected 1, got %d", accepted[0].Counter)
	}
	if accepted[1].Counter != 2 {
		t.Errorf("second event counter: expected 2, got %d", accepted[1].Counter)
	}

	// State should NOT have changed (Accept doesn't apply)
	if c.Value != 0 {
		t.Errorf("value should be 0 after Accept, got %d", c.Value)
	}
}

func TestAcceptThenApplyRetryable(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	c, _ := loadCounter(ctx, store, "c1")

	// Apply an event directly via another instance to simulate a concurrent write
	c2, _ := loadCounter(ctx, store, "c1")
	if err := c2.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 10})); err != nil {
		t.Fatal(err)
	}

	// c is now stale (counter=0, store has counter=1).
	// AcceptThenApplyRetryable should catch up and succeed on retry.
	retry := NewRetry(3, ExponentialBackoff(0))
	err := c.AcceptThenApplyRetryable(ctx, *retry, NewEvent(eventIncremented, incrementedPayload{Amount: 5}))
	if err != nil {
		t.Fatalf("retryable apply failed: %v", err)
	}

	if c.Value != 15 {
		t.Errorf("expected 15, got %d", c.Value)
	}
}

func TestProjection(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	// Set up some counter events
	c1, _ := loadCounter(ctx, store, "c1")
	c1.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 1}))
	c1.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 2}))

	c2, _ := loadCounter(ctx, store, "c2")
	c2.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 10}))

	// Simple projection that counts total increments
	proj := &incrementProjection{}
	p := NewProjection(store, proj)

	processed, err := p.Update(ctx)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if processed != 3 {
		t.Errorf("expected 3 events processed, got %d", processed)
	}
	if proj.total != 13 {
		t.Errorf("expected total 13, got %d", proj.total)
	}
	if p.GlobalCounter() == 0 {
		t.Error("global counter should be > 0")
	}

	// Add more events, update again
	c1.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 100}))
	processed, err = p.Update(ctx)
	if err != nil {
		t.Fatalf("second Update: %v", err)
	}
	if processed != 1 {
		t.Errorf("expected 1 new event, got %d", processed)
	}
	if proj.total != 113 {
		t.Errorf("expected total 113, got %d", proj.total)
	}

	// NewProjectionFrom — resume from a saved counter
	savedCounter := p.GlobalCounter()
	c2.AcceptThenApply(ctx, NewEvent(eventIncremented, incrementedPayload{Amount: 50}))

	proj2 := &incrementProjection{}
	p2 := NewProjectionFrom(store, proj2, savedCounter)
	processed, err = p2.Update(ctx)
	if err != nil {
		t.Fatalf("Update from counter: %v", err)
	}
	if processed != 1 {
		t.Errorf("expected 1 event from saved counter, got %d", processed)
	}
	if proj2.total != 50 {
		t.Errorf("expected total 50, got %d", proj2.total)
	}
}

func TestSagaRunToCompletion(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	actions := ActionMap{
		"start": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{NewState: "done"}, nil
		},
		"done": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{Complete: true}, nil
		},
	}

	saga, err := NewSaga(store, "test-saga", "s1", "start", nil, actions)
	if err != nil {
		t.Fatal(err)
	}

	if err := saga.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !saga.Completed() {
		t.Error("expected saga to be completed")
	}
	if saga.State() != "done" {
		t.Errorf("expected state 'done', got %q", saga.State())
	}
}

func TestSagaInput(t *testing.T) {
	store := NewInMemoryStore()

	type testInput struct {
		Name string `json:"name"`
	}
	inputData, _ := json.Marshal(testInput{Name: "hello"})

	actions := ActionMap{
		"start": func(ctx context.Context, saga *Saga, store Store) (ActionResult, error) {
			return ActionResult{Complete: true}, nil
		},
	}

	saga, err := NewSaga(store, "test-saga", "s2", "start", inputData, actions)
	if err != nil {
		t.Fatal(err)
	}

	// Input() returns a copy
	raw := saga.Input()
	if raw == nil {
		t.Fatal("Input() returned nil")
	}

	// InputAs() unmarshals correctly
	var got testInput
	if err := saga.InputAs(&got); err != nil {
		t.Fatalf("InputAs: %v", err)
	}
	if got.Name != "hello" {
		t.Errorf("expected name 'hello', got %q", got.Name)
	}

	// Verify Input() returns a copy (modifying it shouldn't affect saga)
	raw[0] = 0
	var got2 testInput
	saga.InputAs(&got2)
	if got2.Name != "hello" {
		t.Error("Input() did not return a defensive copy")
	}
}

type incrementProjection struct {
	total int
}

func (p *incrementProjection) AggregateFilters() []AggregateID {
	return []AggregateID{{Type: "counter"}}
}

func (p *incrementProjection) Apply(event AggregateEvent) {
	if event.Event.Type == eventIncremented {
		if payload, ok := ParsePayload[incrementedPayload](event.Event); ok {
			p.total += payload.Amount
		}
	}
}

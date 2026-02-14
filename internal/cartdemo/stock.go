package cartdemo

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// Stock event types
const (
	EventStockAdded           = "stock-added"
	EventStockReserved        = "stock-reserved"
	EventReservationReleased  = "reservation-released"
	EventReservationConfirmed = "reservation-confirmed"
)

// Stock event payloads
type StockAddedPayload struct {
	Quantity int `json:"quantity"`
}

type StockReservedPayload struct {
	SagaID   string `json:"saga_id"`
	Quantity int    `json:"quantity"`
}

type ReservationPayload struct {
	SagaID string `json:"saga_id"`
}

// Stock represents inventory for a single SKU.
// Each SKU has its own Stock aggregate.
type Stock struct {
	*monotonic.AggregateBase

	// Available is the quantity available for reservation
	Available int

	// Reserved tracks reservations by saga ID
	// This allows us to release specific reservations on cancellation
	Reserved map[string]int
}

// LoadStock hydrates a Stock aggregate from the store
func LoadStock(ctx context.Context, store monotonic.Store, sku string) (*Stock, error) {
	return monotonic.Hydrate(ctx, store, "stock", sku, func(base *monotonic.AggregateBase) *Stock {
		return &Stock{
			AggregateBase: base,
			Reserved:      make(map[string]int),
		}
	})
}

func (s *Stock) Apply(event monotonic.AcceptedEvent) {
	switch event.Type {
	case EventStockAdded:
		if p, ok := monotonic.ParsePayload[StockAddedPayload](event); ok {
			s.Available += p.Quantity
		}

	case EventStockReserved:
		if p, ok := monotonic.ParsePayload[StockReservedPayload](event); ok {
			s.Available -= p.Quantity
			s.Reserved[p.SagaID] += p.Quantity
		}

	case EventReservationReleased:
		if p, ok := monotonic.ParsePayload[ReservationPayload](event); ok {
			quantity := s.Reserved[p.SagaID]
			s.Available += quantity
			delete(s.Reserved, p.SagaID)
		}

	case EventReservationConfirmed:
		if p, ok := monotonic.ParsePayload[ReservationPayload](event); ok {
			// Reservation becomes a sale, just remove from reserved tracking
			delete(s.Reserved, p.SagaID)
		}
	}
}

func (s *Stock) ShouldAccept(event monotonic.Event) error {
	switch event.Type {
	case EventStockAdded:
		var p StockAddedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		if p.Quantity <= 0 {
			return errors.New("quantity must be positive")
		}
		return nil

	case EventStockReserved:
		var p StockReservedPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		if p.Quantity <= 0 {
			return errors.New("quantity must be positive")
		}
		if p.SagaID == "" {
			return errors.New("saga_id is required")
		}
		if s.Available < p.Quantity {
			return errors.New("insufficient stock")
		}
		return nil

	case EventReservationReleased:
		var p ReservationPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		if _, exists := s.Reserved[p.SagaID]; !exists {
			return errors.New("no reservation found for saga")
		}
		return nil

	case EventReservationConfirmed:
		var p ReservationPayload
		if err := json.Unmarshal(event.Payload, &p); err != nil {
			return err
		}
		if _, exists := s.Reserved[p.SagaID]; !exists {
			return errors.New("no reservation found for saga")
		}
		return nil
	}

	return errors.New("unrecognized event type: " + event.Type)
}

// TotalReserved returns the total quantity currently reserved across all sagas
func (s *Stock) TotalReserved() int {
	total := 0
	for _, qty := range s.Reserved {
		total += qty
	}
	return total
}

package cartdemo

import (
	"encoding/json"
	"errors"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

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
func LoadStock(store monotonic.Store, sku string) (*Stock, error) {
	return monotonic.Hydrate(store, "stock", sku, func(base *monotonic.AggregateBase) *Stock {
		return &Stock{
			AggregateBase: base,
			Reserved:      make(map[string]int),
		}
	})
}

func (s *Stock) Apply(event monotonic.AcceptedEvent) {
	switch event.Type {
	case "stock-added":
		var payload struct {
			Quantity int `json:"quantity"`
		}
		json.Unmarshal(event.Payload, &payload)
		s.Available += payload.Quantity

	case "stock-reserved":
		var payload struct {
			SagaID   string `json:"saga_id"`
			Quantity int    `json:"quantity"`
		}
		json.Unmarshal(event.Payload, &payload)
		s.Available -= payload.Quantity
		s.Reserved[payload.SagaID] += payload.Quantity

	case "reservation-released":
		var payload struct {
			SagaID string `json:"saga_id"`
		}
		json.Unmarshal(event.Payload, &payload)
		quantity := s.Reserved[payload.SagaID]
		s.Available += quantity
		delete(s.Reserved, payload.SagaID)

	case "reservation-confirmed":
		var payload struct {
			SagaID string `json:"saga_id"`
		}
		json.Unmarshal(event.Payload, &payload)
		// Reservation becomes a sale, just remove from reserved tracking
		delete(s.Reserved, payload.SagaID)
	}
}

func (s *Stock) ShouldAccept(event monotonic.Event) error {
	switch event.Type {
	case "stock-added":
		var payload struct {
			Quantity int `json:"quantity"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		if payload.Quantity <= 0 {
			return errors.New("quantity must be positive")
		}
		return nil

	case "stock-reserved":
		var payload struct {
			SagaID   string `json:"saga_id"`
			Quantity int    `json:"quantity"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		if payload.Quantity <= 0 {
			return errors.New("quantity must be positive")
		}
		if payload.SagaID == "" {
			return errors.New("saga_id is required")
		}
		if s.Available < payload.Quantity {
			return errors.New("insufficient stock")
		}
		return nil

	case "reservation-released":
		var payload struct {
			SagaID string `json:"saga_id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		if _, exists := s.Reserved[payload.SagaID]; !exists {
			return errors.New("no reservation found for saga")
		}
		return nil

	case "reservation-confirmed":
		var payload struct {
			SagaID string `json:"saga_id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		if _, exists := s.Reserved[payload.SagaID]; !exists {
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

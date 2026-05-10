package monotonic

import (
	"encoding/json"
	"fmt"
	"time"
)

// EventFilter selects events from the global stream by aggregate type, aggregate ID, and/or event type.
// Non-empty fields must match for an event to pass the filter.
// `EventType` may only be provided if `AggregateType` is also provided, because event types are namespaced by the aggregate that emits them.
type EventFilter struct {
	AggregateType string
	AggregateID   string
	EventType     string
}

func (f EventFilter) Validate() error {
	if f.EventType != "" && f.AggregateType == "" {
		return fmt.Errorf("event filter with event type must also specify aggregate type")
	}
	return nil
}

// Event represents pure, ephemeral event data
type Event struct {
	Type    string
	Payload json.RawMessage
}

type Payload any

// NewEvent creates an event with a typed payload
// The payload will be marshaled to JSON and stored as raw bytes in the event
func NewEvent[P Payload](eventType string, payload P) Event {
	data, _ := json.Marshal(payload)
	return Event{Type: eventType, Payload: data}
}

// ParsePayload unmarshals an event's payload into the specified type
func ParsePayload[P Payload](event AcceptedEvent) (P, error) {
	var payload P
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return payload, err
	}
	return payload, nil
}

// AcceptedEvent represents an event that has been accepted in an event stream with an assigned counter
// May or may not have been persisted or applied, depending on the context
type AcceptedEvent struct {
	Event
	AcceptedAt    time.Time
	Counter       int64
	GlobalCounter int64
}

// AggregateEvent pairs an event with its target aggregate
type AggregateEvent struct {
	Event         AcceptedEvent
	AggregateType string
	AggregateID   string
}

package monotonic

import (
	"encoding/json"
	"time"
)

// Event represents pure, ephemeral event data
type Event struct {
	Type    string
	Payload json.RawMessage
}

// NewEvent creates an event with a typed payload
// The payload will be marshaled to JSON and stored as raw bytes in the event
func NewEvent[P any](eventType string, payload P) Event {
	data, _ := json.Marshal(payload)
	return Event{Type: eventType, Payload: data}
}

// ParsePayload unmarshals an event's payload into the specified type
func ParsePayload[P any](event AcceptedEvent) (P, bool) {
	var payload P
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return payload, false
	}
	return payload, true
}

// AcceptedEvent represents an event that has been accepted in an event stream with an assigned counter
// May or may not have been persisted or applied, depending on the context
type AcceptedEvent struct {
	Event
	AcceptedAt time.Time
	Counter    int64
}

// AggregateEvent pairs an event with its target aggregate
type AggregateEvent struct {
	Event         AcceptedEvent
	AggregateType string
	AggregateID   string
}

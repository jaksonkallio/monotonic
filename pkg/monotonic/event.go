package monotonic

import (
	"cmp"
	"encoding/json"
	"time"
)

const (
	AnyCounter int64 = -1
)

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
func ParsePayload[P Payload](event AcceptedEvent) (P, bool) {
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
	AcceptedAt    time.Time
	Counter       int64
	GlobalCounter int64
}

func AcceptedEventLess(a AcceptedEvent, b AcceptedEvent) int {
	return cmp.Compare(a.GlobalCounter, b.GlobalCounter)
}

// AggregateEvent pairs an event with its target aggregate
type AggregateEvent struct {
	Event         AcceptedEvent
	AggregateType string
	AggregateID   string
}

// EventFilter is used to filter events based on (optionally) aggregate type and aggregate ID
// If both aggregate type and ID are blank, all global events are returned
type EventFilter struct {
	AggregateType string
	AggregateID   string
}

type ProposedEvent struct {
	ExpectedCounter int64
	AggregateType   string
	AggregateID     string
	Event           Event
}

type ProposedEventBatch struct {
	ProposalID     string
	ExpiresAt      time.Time
	ProposedEvents []ProposedEvent
}

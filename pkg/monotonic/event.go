package monotonic

import (
	"encoding/json"
	"time"
)

type Event struct {
	Type    string
	Payload json.RawMessage
}

// AcceptedEvent represents an event that has been accepted with an assigned counter
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

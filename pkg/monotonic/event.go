package monotonic

import (
	"encoding/json"
	"time"
)

// Event represents an pure, ephemeral event data
type Event struct {
	Type    string
	Payload json.RawMessage
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

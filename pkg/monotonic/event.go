package monotonic

import (
	"encoding/json"
	"time"
)

type Event struct {
	AcceptedAt time.Time
	Counter    int64
	Type       string
	Payload    json.RawMessage
}

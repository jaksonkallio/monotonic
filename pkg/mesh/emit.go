package mesh

import (
	"context"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// BroadcastEmitter is a transport that supports emitting broadcast communication
type BroadcastEmitter interface {
	// Emit broadcasts a message
	Emit(message any) error
}

// BroadcastListener is a transport that supports listening for broadcast communications
type BroadcastListener interface {
	// Listen will start listening for broadcasted messages and handling them
	Listen(context.Context)
}

type Broadcaster interface {
	BroadcastEmitter
	BroadcastListener
}

// SynchronousRelay is a transport for synchronous connection communication
type SynchronousRelay interface {
	// PollEvents will poll for events matching filter
	PollEvents(filter monotonic.EventFilter, after int64) ([]monotonic.AcceptedEvent, error)
}

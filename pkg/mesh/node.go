// Package mesh is responsible for optionally extending the normal Monotonic event sourcing capabilities into a wider distributed mesh network.
package mesh

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

type Store interface {
	Append(ctx context.Context, events ...monotonic.ProposedEvent) error
	FindEvents(ctx context.Context, filter monotonic.EventFilter, afterGlobalCounter int64) ([]monotonic.AcceptedEvent, error)
}

type Node struct {
	// Store is an optional reference to a store implementation, if this node is directly able to handle store-related actions
	Store Store

	// Relays
	Relays []Relay
}

// ProposeEvent will propose a batch of events to the node, attempting to append directly to the store if available or propagate across relays
func (n Node) ProposeEvent(batch monotonic.ProposedEventBatch) error {
	// IDEA: persist the proposed event batch so that we can try storing or relaying it even after the node reboots

	if n.Store != nil {
		// Store is configured, try to append the events to the store directly
		if err := n.attemptStoreAppend(batch); err != nil {
			return fmt.Errorf("store append: %w", err)
		}
	}

	go n.relay(batch)

	return nil
}

// attemptStoreAppend attempts to append the batch in the store, returning any invariant-violating append error
func (n Node) attemptStoreAppend(batch monotonic.ProposedEventBatch) error {
	// TODO: configurable timeout for appending
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := n.Store.Append(ctx, batch.ProposedEvents...); err != nil {
		if errors.Is(err, monotonic.ErrCounterConflict) {
			// Irrecoverable append errors, should not keep trying or relaying the proposed event
			return err
		}

		// Log and continue for transient errors
		slog.Error("store append failed", "error", err)
	}

	return nil
}

// relay will continuously relay the proposed event batch to all configured relays, until the event is accepted or expires
func (n Node) relay(batch monotonic.ProposedEventBatch) {
	return
}

// Package mesh is responsible for optionally extending the normal Monotonic event sourcing capabilities into a wider distributed mesh network.
package mesh

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
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

	// Cache for events
	cache EventCache
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

	n.relayProposedEventsBatch(batch)

	return nil
}

// AcceptedEvents returns a dense stream of events matching the provided filter, strictly after the provided global counter
func (n Node) AcceptedEvents(filter monotonic.EventFilter, after int64) []monotonic.AcceptedEvent {
	var polledRelay bool
	var acceptedEvents []monotonic.AcceptedEvent
	for _, relay := range SortPrioritizedRelays(rand.NewSource(time.Now().UnixNano()), n.Relays) {
		polledAcceptedEvents, err := relay.PollEvents(filter, after)
		if err != nil {
			// Failed to poll the relay, gracefully log and continue with another relay
			slog.Warn("relay poll events failed", "filter", filter, "after", after)
			continue
		}
		acceptedEvents = polledAcceptedEvents
		polledRelay = true
		break
	}

	if polledRelay {
		// Successfully polled relay, add events to local cache
		n.cache.Add(EventCacheKey(filter), acceptedEvents)
	} else {
		// All relays failed, fall back to this node's local cache
		acceptedEvents = n.cache.Get(EventCacheKey(filter), after)
	}

	return acceptedEvents
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
func (n Node) relayProposedEventsBatch(batch monotonic.ProposedEventBatch) {
	relays := SortPrioritizedRelays(rand.NewSource(time.Now().UnixNano()), n.Relays)

	var i int64
	for !n.accepted(batch.ProposalID) && time.Now().Before(batch.ExpiresAt) {
		// Iteratively relay until accepted or expired
		relay := relays[i%int64(len(relays))]
		n.relayProposedEventsBatchTo(relay, batch)
		i += 1
	}
}

// relayTo will relay the batch to the specified relay
func (n Node) relayProposedEventsBatchTo(relay Relay, batch monotonic.ProposedEventBatch) error {
	ctx, cancel := context.WithTimeout(context.Background(), relay.Timeout)
	defer cancel()

	if err := relay.Transport.Propose(ctx, batch); err != nil {
		return fmt.Errorf("relay propose: %w", err)
	}

	return nil
}

func (n Node) accepted(proposalID string) bool {
	// TODO: check local cache and/or poll relays
	return false
}

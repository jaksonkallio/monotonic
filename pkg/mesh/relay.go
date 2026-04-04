package mesh

import (
	"context"
	"math/rand"
	"slices"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// RelayTransport is a transport implementation for connecting to a relay
type RelayTransport interface {
	// Poll for events matching the provided filter and minimum global counter
	Poll(ctx context.Context, filter monotonic.EventFilter, afterGlobalCounter int64) ([]monotonic.AcceptedEvent, error)

	// Propose an event batch to the relay
	Propose(ctx context.Context, batch monotonic.ProposedEventBatch) error
}

type Relay struct {
	Name      string
	Transport RelayTransport
	Timeout   time.Duration
	Priority  int
}

// SortPrioritizedRelays returns the slice of relays sorted by priority, which relays of the same priority using seeded-random selection
func SortPrioritizedRelays(seed rand.Source, relays []Relay) []Relay {
	// Build a set of tiebreakers, which are just the relays with a seeded randomized tiebreaker integer
	type randomTiebreaker struct {
		Relay
		Tiebreaker    int32
		OriginalIndex int
	}
	rand := rand.New(seed)
	tiebrokenRelays := make([]randomTiebreaker, len(relays))
	for i, relay := range relays {
		tiebrokenRelays[i] = randomTiebreaker{
			Relay:         relay,
			Tiebreaker:    rand.Int31(),
			OriginalIndex: i,
		}
	}

	// Do a plain sort of the relays using the tiebreaker values when priority is the same
	slices.SortFunc(tiebrokenRelays, func(a, b randomTiebreaker) int {
		if a.Priority < b.Priority {
			return -1
		} else if a.Priority > b.Priority {
			return 1
		} else if a.Tiebreaker < b.Tiebreaker {
			return -1
		} else if a.Tiebreaker > b.Tiebreaker {
			return 1
		} else if a.OriginalIndex < b.OriginalIndex {
			return -1
		} else if a.OriginalIndex > b.OriginalIndex {
			return 1
		}
		return 0
	})

	sorted := make([]Relay, len(relays))
	for i, t := range tiebrokenRelays {
		sorted[i] = t.Relay
	}

	return sorted
}

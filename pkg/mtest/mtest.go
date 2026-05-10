package mtest

import (
	"context"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

// EmitEvents appends events directly to the store for the given aggregate, bypassing aggregate validation.
// Useful in projection tests where you want to seed events without wiring up a real aggregate.
func EmitEvents(ctx context.Context, store monotonic.Store, aggType, aggID string, events ...monotonic.Event) error {
	existing, err := store.LoadAggregateEvents(ctx, aggType, aggID, 0)
	if err != nil {
		return err
	}

	nextCounter := int64(len(existing)) + 1
	now := time.Now()

	aggEvents := make([]monotonic.AggregateEvent, len(events))
	for i, e := range events {
		aggEvents[i] = monotonic.AggregateEvent{
			Event: monotonic.AcceptedEvent{
				Event:      e,
				AcceptedAt: now,
				Counter:    nextCounter + int64(i),
			},
			AggregateType: aggType,
			AggregateID:   aggID,
		}
	}

	return store.Append(ctx, aggEvents...)
}

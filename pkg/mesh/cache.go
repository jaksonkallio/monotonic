package mesh

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/jaksonkallio/monotonic/pkg/monotonic"
)

type EventCacheKey monotonic.EventFilter

type EventCache interface {
	Get(key EventCacheKey, after int64) []monotonic.AcceptedEvent
	Add(key EventCacheKey, events []monotonic.AcceptedEvent)
}

// EventStream is a dense ordered list of events matching a particular filter
type EventStream struct {
	events []monotonic.AcceptedEvent
	lock   sync.RWMutex
}

func NewEventStream() *EventStream {
	return &EventStream{
		events: make([]monotonic.AcceptedEvent, 0),
	}
}

func (es *EventStream) LatestCounter() int64 {
	es.lock.RLock()
	defer es.lock.RUnlock()
	return es.latestCounter()
}

func (es *EventStream) Count() int {
	es.lock.RLock()
	defer es.lock.RUnlock()
	return len(es.events)
}

func (es *EventStream) latestCounter() int64 {
	if len(es.events) == 0 {
		return -1
	}
	return es.events[len(es.events)-1].GlobalCounter
}

func (es *EventStream) After(counter int64) []monotonic.AcceptedEvent {
	es.lock.RLock()
	defer es.lock.RUnlock()
	events := make([]monotonic.AcceptedEvent, 0)
	for _, event := range es.events {
		if event.GlobalCounter > counter {
			events = append(events, event)
		}
	}
	return events
}

func (es *EventStream) Add(events []monotonic.AcceptedEvent) {
	if len(events) == 0 {
		// No-op
		return
	}
	es.lock.Lock()
	defer es.lock.Unlock()
	slices.SortFunc(events, monotonic.AcceptedEventLess)
	for _, event := range events {
		if event.GlobalCounter > es.latestCounter() {
			// Add any new events to this stream by global counter
			// Appended events were previously sorted, so we know that appended events order will be maintained
			es.events = append(es.events, event)
		}
	}
}

type cachedStream struct {
	EventStream  EventStream
	LastAccessed time.Time
}

type InMemEventCache struct {
	streams   map[EventCacheKey]*cachedStream
	lock      sync.RWMutex
	maxEvents int
}

func NewInMemEventCache(ctx context.Context, maxEvents int) *InMemEventCache {
	return &InMemEventCache{
		streams:   make(map[EventCacheKey]*cachedStream, 0),
		maxEvents: maxEvents,
	}
}

func (m *InMemEventCache) Get(key EventCacheKey, after int64) []monotonic.AcceptedEvent {
	m.lock.RLock()
	defer m.lock.RUnlock()

	stream, hasKey := m.streams[key]
	if !hasKey || stream == nil {
		// Key doesn't exist, no accepted events to return
		return nil
	}

	return stream.EventStream.After(after)
}

func (m *InMemEventCache) Add(key EventCacheKey, events []monotonic.AcceptedEvent) {
	m.lock.Lock()
	defer m.lock.Unlock()
	stream, hasKey := m.streams[key]
	if !hasKey || stream == nil {
		// Stream doesn't exist, create and add to map
		stream = &cachedStream{
			EventStream:  *NewEventStream(),
			LastAccessed: time.Now(),
		}
		m.streams[key] = stream
	}
	stream.EventStream.Add(events)
	stream.LastAccessed = time.Now()
	// IDEA: Use a real LRU implementation rather than this inefficient garbage collect approach
	m.garbageCollect()
}

func (m *InMemEventCache) Count() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.count()
}

func (m *InMemEventCache) count() int {
	var sum int
	for _, stream := range m.streams {
		sum += stream.EventStream.Count()
	}
	return sum
}

func (m *InMemEventCache) garbageCollect() {
	for m.count() > m.maxEvents {
		if len(m.streams) == 0 {
			// Nothing to possibly garbage collect
			break
		}

		var lruStreamKey EventCacheKey
		var lruLastAccessed time.Time
		for key, stream := range m.streams {
			if lruLastAccessed.IsZero() || stream.LastAccessed.Before(lruLastAccessed) {
				lruStreamKey = key
				lruLastAccessed = stream.LastAccessed
			}
		}

		delete(m.streams, lruStreamKey)
	}
}

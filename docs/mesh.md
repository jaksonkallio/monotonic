# Monotonic Mesh

Monotonic is a lightweight event sourcing framework. Monotonic also has mesh-networking capabilities as a completely optional add-on to extend upon the transmission of events between nodes in a network.

# Benefits and Use Case
The main benefit of Monotonic Mesh networking is that we get to combine event sourcing with a mesh-based networking protocol. We can build extremely durable applications with strong consistency guarantees over the top of brittle physical network transport mediums.

Everything is designed with asynchronicity in mind. Is a relay offline? That's fine, I'll queue up the event to try transmitting again later. When I finally do get online, I'll handle all of events that have happened since I was last connected.

What might this be useful for?
- Extremely remote sensors and controls connected only by occasionally-available radios
- Applications built over mesh-based radio protocols like Meshtastic
- Space travel where it takes minutes or hours for signals to arrive, and connectivity is spotty
- Military uses where there is adversarial jamming and mesh nodes can be destroyed

# Concepts

## Store
The "store" is some database implementation that supports transactions and guaranteed consistency. Most commonly, an SQL database. The only requirement is that the store can enforce the monotonically-increasing counters that the framework uses. In a large-scale mesh-based network, there would probably only be a handful of nodes that are connected to the actual store for handling accepting/appending events in the store.

## Nodes
A node is the runtime of Monotonic embedded in an application. The node is what communicates either directly with the store or communicates with other nodes. Anything that does stuff with Monotonic events will be a node.

A node MAY be connected to the store directly, in such case the node is able to actually accept/append new events from proposed events.

A node MAY have relay relationships with other nodes, which essentially just means that the node can be poll these other relay nodes for new events.

Generally, a node SHOULD have either (1) a configured connection to a store, or (2) configured relay connection(s) to other nodes.

![Flat Node Topology](./monotonic-flat.excalidraw.png)

![Hierarchical Node Topology](./monotonic-hierarchical.excalidraw.png)

![Mesh Node Topology](./monotonic-mesh.excalidraw.png)


# Events
An event is a unit of communication in Monotonic. Events have a strict monotonically-increasing counter attached to them, such that an event can only be appended if it exactly has N+1 counter value from the previous event. The node proposing a new event MAY provide an expected counter to guarantee that their new event is being written in the context of having seen exactly all previous events. This is an optimistic concurrency system. When `ExpectedCounter` is omitted, its value MUST be interpreted as `0`.

At its most basic level, an event contains a unique ID, categorical information about the aggregate, an event type, and optionally a payload. Throughout this document, I will represent events as JSON for easier readability, but in a production environment a binary format would be used.

```json
{
    "ID": "206e3afa-663a-4da0-ab9b-97c501bd48d7",
    "Aggregate": "sensor",
    "AggregateID": "my-sensor-3458",
    "Type": "something-happened",
    "Payload": {
        "Foo": "bar",
        "Baz": 12
    }
}
```

The `ID` is a unique ID to identify this specific proposed event. It will be used as a key for the proposed event when the event is being relayed. Nodes MUST NOT update an existing known proposed event if it sees another event with the same ID, but other values are different.

`Aggregate` and `AggregateID` MAY be included as information about the relevant aggregate for the event. This is how events are namespaced. You can think of the aggregate like an entity type like "sensor", "item", or "building", and a specific aggregate ID as one instance of that entity type, like "my-sensor-3458", "1234", or "123-main-street". It is valid to leave aggregate information blank, which SHOULD be treated like globally-relevant event without namespacing.

`Type` is an event type that describes what type of event took place.

`Payload` is an optional, arbitrary payload of information to include about the event. Sometimes, the event type doesn't require any additional information.

Surrounding this event information is either a "Proposed Event" or an "Accepted Event" envelope.

## Proposed Event
Proposed events are created and are waiting to be officially added to the store. Proposed events flow up from nodes until they eventually reach a node that is connected to a store. From there, the node will try to accept/append the event into the store. Assuming the proposed event counters are expected (optimistic concurrency), the event will be added and accepted, turning into an "Accepted Event".

Providing an `ExpectedCounter` value for a proposed event will tell the node trying to accept this event that it should only be accepted if the expected counter is exactly equal to the previous counter for the aggregate + 1.

```json
{
    "ProposedEvents": [
        {
            // "Open vent 4c160adc"
            "ExpectedCounter": 6893,
            "Event": {
                "ID": "206e3afa-663a-4da0-ab9b-97c501bd48d7",
                "Aggregate": "vent",
                "AggregateID": "4c160adc",
                "Type": "opened"
            }
        },
        {
            // "Open vent 331f8c12"
            "ExpectedCounter": 4023,
            "Event": {
                "ID": "2eb1ba74-e193-442c-9ae4-a3a5c980e792",
                "Aggregate": "vent",
                "AggregateID": "331f8c12",
                "Type": "opened"
            }
        },
        {
            // "Start fan ce725181 with 60% speed level"
            "ExpectedCounter": 18598,
            "Event": {
                "ID": "9da807a9-86e2-4b29-8818-926358d97968",
                "Aggregate": "fan",
                "AggregateID": "ce725181",
                "Type": "started",
                "Payload": {
                    "Speed": 0.6
                }
            }
        }
    ]
}
```

You'll notice that proposed events are specified as arrays. Proposed events can be accepted in atomic batches. A node MAY propose multiple events (listed in `ProposedEvents` array) that are either _all_ accepted at once, or _all_ rejected at once. The node proposing the events MUST provide explicit `ExpectedCounter` values (where omitting is interpreted as `0`), even on subsequent proposed events after the first even though one could infer what the subsequent expected counter values would be.

If the node proposing the event does not care about guaranteeing the event is exactly the next event in the sequence, the proposer can use an `ExpectedCounter` value of `-1`. In this case the node accepting the event will simply append without any counter check. This is useful if the proposing node just wants to fire-and-forget events without caring about their ordering or retries, such as sensor readings.

```json
{
    "ProposedEvents": [
        {
            // "Record temperature of 21.78 C for thermometer 131ec3e1"
            "ExpectedCounter": -1,
            "Event": {
                "ID": "206e3afa-663a-4da0-ab9b-97c501bd48d7",
                "Aggregate": "thermometer",
                "AggregateID": "131ec3e1",
                "Type": "recorded-temperature",
                "Payload": {
                    "Temperature": 21.78
                }
            }
        },
        {
            // "Record humidity of 54% for thermometer 131ec3e1"
            "ExpectedCounter": -1,
            "Event": {
                "ID": "8754de95-071a-498e-a33d-d7a9e97b1e3f",
                "Aggregate": "thermometer",
                "AggregateID": "131ec3e1",
                "Type": "recorded-humidity",
                "Payload": {
                    "Humidity": 0.54
                }
            }
        }
    ]
}
```

# Invariants
Invariants are business logic rules that determine whether a proposed event is allowed to become an accepted event or rejected. Invariants are enforced by the nodes that are accepting the event at the store. Oftentimes, however, the nodes proposing events MAY make a best-effort attempt to validate the events before proposing via guard functions, as this can help cut down on latency and volume on the network.

Invariant enforcement isn't outlined in this document because it is a standard part of the Monotonic framework not specific to Monotonic Mesh features. The general process involves hydrating the aggregate and its events from store, and attempting to `AcceptThenApply` the proposed events in one atomic batch. This indirectly calls the `ShouldAccept` method on the aggregate implementation, which can use plain Go code to make an acceptance determination in accordance with any invariant logic.

# Event Reads
## Polling
Reads are entirely poll-based. This is for a few reasons:
1. Simplicity. Poll-based systems are often more simple.
2. The reader node can only decide when it needs to poll for new events, instead of events preemptively being pushed to the reader when unnecessary. This reduces network chatter, since nodes only request event information when needed.
3. Lack of connectivity is often the reader's problem, so the reader should determine when it can connect and poll for new events. Generally, as you trace the network graph from edge nodes to nodes connected to the store, the connection consistency improves.

Nodes should poll on a schedule in the case of maintaining local projections, or poll as necessary to propose events in response to some trigger.

## Poll Order
When a node receives an event poll for events greater than some counter, it SHOULD either respond directly with information from the store if directly connected to the store, or recursively poll its own relays before responding if no direct connection to the store. This ensures that the event log is as up to date as currently possible.

In the case of a node that is not directly connected to a store receiving an event poll, the node will respond with either the set of new events from its own successful recursive event polls to a relay, or a set of new events from its own local cache if failed to communicate with all relays. In a sense, the node's own accepted event cache is the lowest-priority "relay" to check for new events.

1. Node receives an event poll for all events with counter greater than X.
2. If node is connected to a store, simply query and return events with counter >X from the store.
3. Build a "relay attempt order" queue, and iterate through this attempt queue.
4. If node successfully fetches the events from one of its relays, passthrough the response (and possibly cache the events locally).
5. If node fails to fetch the events from all of its relays, query the local cache for events with counter >X and return.

Relay nodes for a particular node MAY be configured with a `Priority` integer. The ordering of which relay nodes will be attempted to be polled will prioritize by `Priority` ascending. If two configured relay nodes have the same `Priority` value, node MUST try the nodes in a pseudorandom or round-robin order. `Timeout` MUST be provided greater than `0` and indicates the number of milliseconds for the node to wait for before considering the relay unreachable. It is important that `Timeout` values remain relatively low so that the node can quickly give up on failing relays. For example, a relay connected via TCP over the traditional Internet will probably have much lower timeout than a relay connected over LoRa.

```json
{
    "Relays": [
        {
            "Priority": 0,
            "Address": "tcp://some-host-1:15001",
            "Timeout": 750
        },
        {
            "Priority": 0,
            "Address": "tcp://some-host-2:15001",
            "Timeout": 750
        },
        {
            "Priority": 1,
            "Address": "tcp://some-host-3:15001",
            "Timeout": 5000
        },
        {
            "Priority": 2,
            "Address": "tcp://some-host-3:15001",
            "Timeout": 5000
        },
    ]
}
```

In this example, the node will attempt polling from the first two relays in a random order. If both fail, then attempt polling from the third relay. If the third fails, attempt polling from the fourth relay.

If setting different `Priority` values isn't relevant to a node's use case, `Priority` MAY be omitted (which is interpreted as `0`) and therefore all relays will have the same `Priority` value, naturally utilizing using a random attempt order for all relays.

## Caching
Nodes MAY cache accepted events. Accepted events are immutable and can therefore be safely cached indefinitely. Usually, it makes sense to keep some number of events cached locally to serve up from 

# Transport
One of the major benefits of Monotonic Mesh networks is that a wide variety of transport mediums are available due to the asynchronous best-effort nature of the system. Asynchronicity gives implementers a lot of flexibility because failing to establish synchronous connections becomes a reasonably tolerated failure mode.

TODO: actually implement some of these! just some ideas:
- TCP, via gRPC, probably the most common protocol for central trunk and store-connected nodes
- LoRa, via Meshtastic
- Bluetooth
- Audio speaker + microphone encoding
- Filesystem, where new events can be polled from a local filesystem like a mounted mobile physical USB drive (cool huh!)

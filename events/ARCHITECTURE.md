# orbit-events - Architecture

`events` is the reusable event-stream semantic layer over `orbit-core`. It
turns one per-node Orbit ring into a raw topic and byte-payload bus without
adding application lifecycle or serialization policy.

## Role

The crate answers:

> Which event frames have appeared since this subscriber's cursor?

That differs from cache and metrics semantics:

- cache asks for the newest value for a key;
- metrics asks for the newest sample for each node or metric key;
- event consumers walk every retained frame after their cursor.

## Ownership Boundary

`orbit-core` owns:

- `Fleet` and node identity;
- ring allocation and SHM layout;
- per-node cursor traversal and loss accounting;
- publication ordering and native notification generation;
- process-local `RingEventFd` readiness bridges.

`events` owns:

- the dedicated event ring kind and `RingSpec`;
- topic, payload, and timestamp framing;
- subscriber cursor and poll result types;
- topic filtering and event-level lag reporting;
- event-specific size and I/O errors.

The embedder owns typed codecs, handler registration, lifecycle dispatch,
retry, acknowledgement, and durability policy.

## Wire Contract

All topics share one `PerNode` ring:

```text
RingSpec::per_node(1024, 512)
```

Each frame payload is:

```text
u16 topic_len
u16 payload_len
u64 timestamp_ms
[topic bytes]
[opaque payload bytes]
```

The frame kind identifies an event record inside the event ring. The
`NetId64` identifies the producing node and physical ring counter. No total
order is promised across node lanes.

The topic must be valid UTF-8 at the API boundary. Decoding is tolerant of
invalid stored topic bytes and exposes them lossily; application payload bytes
remain opaque.

## Cursor and Loss Contract

Every subscriber owns its `FleetEventCursor`. `poll` advances all lane cursors
to the observed heads and returns every decodable retained event. Missing,
overwritten, malformed, or otherwise unavailable counters increase `lagged`.

`poll_topic` still consumes non-matching topics. Independent consumers must use
independent cursors.

The ring is a bounded runtime stream, not a durable log. Lag cannot be repaired
by this crate.

## Notification Contract

On Linux, FreeBSD and macOS 14.4 or later, `event_fd` creates a readiness
handle local to the subscribing process. Publishers first commit the ring frame
and then increment the shared notification generation. Wakeups can coalesce;
consumers must drain the fd and poll the ring.

Other targets and older macOS have the same event and cursor semantics but require periodic or
caller-triggered polling.

## Reset Contract

`reset_ring` is only for owner-controlled boot cleanup while the event ring is
quiescent. It is not a coordinated runtime clear operation.

## Invariants

- All event topics use one shared ring, not one ring per topic or Rust type.
- Payloads are bounded and opaque; the crate never chooses a serializer.
- Subscriber progress is caller-owned and never global.
- Readiness is a wake signal, not delivery or acknowledgement.
- Ring retention does not imply durability.
- Application lifecycle and typed dispatch remain outside this crate.

# orbit-events

`orbit-events` provides a bounded topic and byte-payload stream for processes
in the same Orbit fleet. Applications normally use it through
`orbitive::events`; direct package access remains available.

```rust
use std::sync::Arc;

use orbitive::events::FleetEventBus;
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
let bus = FleetEventBus::new(fleet);
let mut cursor = bus.cursor_at_head();

bus.publish("worker.ready", b"worker-1")?;

let poll = bus.poll(&mut cursor);
assert_eq!(poll.events[0].topic, "worker.ready");
assert_eq!(poll.events[0].payload, b"worker-1");

# Ok::<(), Box<dyn std::error::Error>>(())
```

All topics share one per-node ring, and every subscriber owns an independent
cursor. Polling advances the cursor across every observed frame. A topic filter
changes which events are returned, not which frames are consumed. If the
cursor falls behind the retained window, `FleetEventPoll::lagged` reports the
loss.

The bus is generic over an `EventLayout`: the ring kind and its geometry.
`FleetEventBus::new` uses the default layout, kind 220 sized by
`ORBIT_EVENT_RING_CAPACITY` and `ORBIT_EVENT_RING_PAYLOAD_CAPACITY`. A second
layout names another ring with its own frame size and retained window, for
an embedding runtime that wants small service events and large application
events to stop competing for one ring:

```rust
use orbitive::events::{EventLayout, FleetEventBus};
use orbitive::RingSpec;

struct ApplicationEvents;

impl EventLayout for ApplicationEvents {
    const RING_KIND: u8 = 221;
    const RING_SPEC: RingSpec = RingSpec::per_node(256, 8_192);
}

# let fleet = std::sync::Arc::new(orbitive::Fleet::join("example-layout", 1)?);
let bus = FleetEventBus::<ApplicationEvents>::with_layout(fleet)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

On Linux, FreeBSD, and macOS 14.4 or later, an SHM-backed bus can create a
process-local
`RingEventFd`. Consumers drain that readiness signal and then poll the ring;
the fd does not carry event data, and wakeups may coalesce.

The crate does not provide acknowledgement, durable replay, consumer groups,
typed serialization, handler dispatch, or network transport.

# orbit-core

`orbit-core` is the low-level Orbit runtime beneath the `orbitive` facade. It
provides fleets, bounded typed rings, cursor traversal, POSIX shared-memory
backing, and native readiness primitives.

Most applications should depend on `orbitive`. Direct `orbit-core` access is
available for semantic crates and integrations that deliberately need the
complete low-level surface.

## Core model

`Fleet` is a process-local handle to a named runtime fleet. A fleet can use
ordinary memory or shared memory visible to sibling processes.

```rust
use orbitive::{Fleet, OrbitTyped, RingSpec};

struct WorkerLoad;

impl OrbitTyped for WorkerLoad {
    const KIND: u8 = 12;
    const RING_SPEC: RingSpec = RingSpec::per_node(1_024, 8);
}

let fleet = Fleet::join("example", 1)?;
fleet.publish::<WorkerLoad>(0, b"snapshot")?;

# Ok::<(), Box<dyn std::error::Error>>(())
```

An `OrbitTyped` implementation assigns a stable kind and layout to one ring
family. Every peer in a shared fleet must use the same kind, capacity, payload
limit, and topology. Frames are bounded and may be overwritten after the ring
wraps.

## External read-only observation

An independent Unix process can inspect an existing SHM ring through
`FleetObserver` without becoming a fleet member or reproducing the writer's
compile-time geometry:

```rust,no_run
use orbit_core::FleetObserver;
use orbit_core::ring::cursor::{RingCursor, poll_ring};

let observer = FleetObserver::attach_existing("example")?;
let ring = observer.ring(12)?;

for lane_index in 0..ring.metadata().lane_count {
    let lane = ring.lane(lane_index)?;
    let mut cursor = RingCursor::from_counter(lane.retained_range().start);
    for frame in poll_ring(&lane, &mut cursor).frames {
        println!("{lane_index}: {frame:?}");
    }
}

# Ok::<(), Box<dyn std::error::Error>>(())
```

`FleetObserver` is a namespace handle, not a read-only `Fleet`: it has no node
id, joins no membership, owns no lane, and cannot publish, reset, create, or
unlink. Each `ring(kind)` call opens one exact existing uid-scoped POSIX object
with a read-only mapping. Dropping the observer or `ShmRingView` only unmaps
local memory and does not change the observed fleet's lifetime.

The persisted header supplies raw geometry and is validated before frame
access. `typed_ring::<T>()` additionally verifies a linked `OrbitTyped`
contract. Core intentionally does not decode semantic payloads or decide
whether a frame is fresh, healthy, or application-successful; the crate that
owns the kind retains those responsibilities. Observation is snapshot/poll
oriented and does not install a native readiness subscription.

## What it provides

- process-local and POSIX SHM-backed fleets;
- shared, per-node, and globally ordered ring topologies;
- stable frame identifiers through `netid64::NetId64`;
- cursor polling with explicit overwritten and unavailable counts;
- external read-only SHM ring observation without fleet membership;
- shared sequence allocation and batch publication;
- process-local readiness bridges backed by futex on Linux, umtx on
  FreeBSD, and shared address waits on macOS 14.4 or later;
- reusable SHM mapping and locking primitives for current-state tables.

`orbit-core` does not choose a serializer, implement application lifecycle,
provide durable storage, or define cache, event, lock, metrics, or TLS policy.
Those semantics live in separate `orbit-*` crates.

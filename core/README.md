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

## What it provides

- process-local and POSIX SHM-backed fleets;
- shared, per-node, and globally ordered ring topologies;
- stable frame identifiers through `netid64::NetId64`;
- cursor polling with explicit overwritten and unavailable counts;
- shared sequence allocation and batch publication;
- process-local readiness bridges backed by futex on Linux and umtx on
  FreeBSD;
- reusable SHM mapping and locking primitives for current-state tables.

`orbit-core` does not choose a serializer, implement application lifecycle,
provide durable storage, or define cache, event, lock, metrics, or TLS policy.
Those semantics live in separate `orbit-*` crates.

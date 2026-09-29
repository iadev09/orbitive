# orbit-core

`orbit-core` is the low-level Orbit runtime beneath the
[Orbitive](https://github.com/iadev09/orbitive) facade. It provides fleets,
bounded typed rings, cursor traversal, POSIX shared-memory backing, and native
readiness primitives.

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

## Per-ring SHM access policy

`orbitive::shm::ShmAccessPolicy` (also `orbit_core::shm::ShmAccessPolicy`)
selects OS access independently of `RingSpec` and the persisted ring layout:

| Policy | Maximum permissions |
| --- | --- |
| `OwnerOnly` (default) | `0600`, owner read/write |
| `GroupRead { gid }` | `0640`, owner read/write and selected group read |
| `GroupReadWrite { gid }` | `0660`, owner and selected group read/write |

Existing constructors retain their signatures and select `OwnerOnly`. The new
`ShmRegion::open_or_create_with_policy` and
`ShmRegion::open_or_create_locked_with_policy` accept an explicit policy.
`ShmRing` exposes `open_or_create_with_policy` and
`open_or_create_for_fleet_with_policy` for standalone ring handles.

For a fleet, supply per-kind overrides before any ring is opened:

```rust,no_run
use orbit_core::{Fleet, NodeId, OrbitTyped};
use orbit_core::shm::ShmAccessPolicy;

fn join<T: OrbitTyped>(name: &str, observer_gid: u32) -> orbit_core::Result<Fleet> {
    Fleet::join_shm_as_with_policies(
        name,
        1,
        NodeId::ZERO,
        [(T::KIND, ShmAccessPolicy::GroupRead { gid: observer_gid })],
    )
}
```

Unspecified kinds remain owner-only. Policies are immutable for that fleet
handle and apply to its typed rings, not separate semantic state tables or
membership/companion locks. Group permissions do not introduce cross-uid fleet
joining: writer names and coordination locks still belong to their effective uid.
External group readers use `FleetObserver::attach_existing_for_uid` followed by
`ring_with_policy` or `typed_ring_with_policy`; `ShmRingView` also provides
`attach_existing_with_policy` and `attach_existing_for_uid_with_policy`.

Every new open validates the descriptor's actual owner uid, selected group gid
and permission bits before mapping. Group policies require the exact gid;
`OwnerOnly` does not constrain an ineffective group owner. Permissions may be
narrower than the selected maximum, but execute, special, other-user and excess
group bits are rejected. OS access checks still apply. Validation also runs for
read-only views and `ShmRegion::validate_existing_with_policy`.

**Compatibility:** no ring wire/layout change or source migration is required
for existing owner-only users. Previously accepted objects with the wrong owner
or broader permissions now fail with `PermissionDenied`. Rejection never repairs,
resizes, unlinks or recreates an existing object. Peers must agree on access policy;
changing policy does not revoke already-open mappings or cached ring handles.

Platform behavior is selected with `cfg`:

- On macOS, new objects receive the requested mode directly through `shm_open`.
  The selected group must match the creator's effective gid; another gid on a
  missing object returns `InvalidInput`. Orbit does not attempt unsupported
  SHM `fchmod`/`fchown` calls. Existing objects with the requested gid can still
  be attached, subject to OS permissions and expected-owner validation.
- On other Unix targets, a new object starts owner-only. For explicit group
  sharing, Orbit sets its gid before applying the exact `0640`/`0660` mode;
  inability to set that gid fails creation. This explicit group mode overrides
  the group bits of `umask`. Existing objects are never chmoded/chowned.
- Default creation remains `shm_open(..., 0600)` with native platform
  creation-mask semantics. Orbit never changes the process's `umask` or credentials.

These are OS user/group boundaries, not application identities or encryption.
Trust every permitted writer; same-uid processes are not isolated. Sensitive
payload encryption and authentication belong to the owning semantic layer.

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

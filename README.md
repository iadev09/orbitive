<p align="center">
  <img src="https://raw.githubusercontent.com/iadev09/orbitive/main/assets/logo.svg" alt="Orbitive" width="480">
</p>

<p align="center">
  <a href="https://crates.io/crates/orbitive"><img src="https://img.shields.io/crates/v/orbitive.svg" alt="crates.io"></a>
  <a href="https://docs.rs/orbitive"><img src="https://img.shields.io/docsrs/orbitive" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="license">
</p>

> **Bounded same-host runtime state for Rust process fleets**

Orbitive is a Rust facade for bounded, same-host runtime state shared by a
fleet of sibling processes.

It provides two storage shapes:

- rings for recent, append-only history;
- fixed-capacity tables for current keyed state.

Both shapes can run in process memory or over POSIX shared memory. They are
intended for ephemeral runtime coordination where a database or network broker
would be unnecessary. They do not provide durability or network replication.

The facade is published as `orbitive`. Its runtime model and public type names
use the established Orbit vocabulary, such as `Fleet`, `OrbitTyped`, and
`OrbitEpoch`.

## Add the facade

```toml
[dependencies]
orbitive = { version = "0.3.13", features = ["events", "lock"] }
```

Core types used by most integrations are available at the crate root. The full
low-level surface remains under `orbitive::core`.

```rust
use std::sync::Arc;

use orbitive::events::FleetEventBus;
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
let bus = FleetEventBus::new(fleet);
bus.publish("worker.ready", b"worker-1")?;

# Ok::<(), Box<dyn std::error::Error>>(())
```

## Feature modules

| Feature | Module | Purpose |
| --- | --- | --- |
| always | `orbitive::fleet` | fleet membership and node identity |
| always | `orbitive::ring` | typed rings, cursors, and readiness primitives |
| Unix | `orbitive::shm` | POSIX shared-memory regions and naming |
| always | `orbitive::core` | the complete low-level core surface |
| `cache` | `orbitive::cache` | process-local L1 caches with fleet-wide mutation propagation |
| `counter` | `orbitive::counter` | keyed signed counters shared across the fleet |
| `cell` | `orbitive::cell` | typed atomic cells addressed by id, shared across the fleet |
| `events` | `orbitive::events` | raw topic and byte-payload event streams |
| `invoke` | `orbitive::invoke` | bounded invocation requests with operation routing |
| `lock` | `orbitive::lock` | keyed leases with ownership checks and fencing tokens |
| `metrics` | `orbitive::metrics` | current metrics snapshots across workers |
| `pool` | `orbitive::pool` | fleet-wide resource leases, reservations, and creation claims |
| `stream` / `stream-tokio` | `orbitive::stream` | bounded byte streams between two fleet members, in memory or shared memory |
| `rustls` / `rustls_0_24` | `orbitive::rustls` | fleet-shared rustls 0.23/0.24 server-session storage |

The implementation crates are also published separately as
[`orbit-core`](core/README.md), [`orbit-cache`](cache/README.md),
[`orbit-arena`](arena/README.md), [`orbit-cell`](cell/README.md), [`orbit-counter`](counter/README.md), [`orbit-events`](events/README.md),
[`orbit-invoke`](invoke/README.md), [`orbit-lock`](lock/README.md),
[`orbit-metrics`](metrics/README.md), and
[`orbit-rustls`](rustls/README.md). Direct dependencies are supported when an
integration needs a single narrow layer; applications can otherwise use the
facade and enable only the modules they need.

## Compile-time geometry

The default shared-memory geometry can be selected once for an entire
downstream Cargo build graph. Put the desired values in the application's
`.cargo/config.toml`; ordinary `cargo build`, `cargo test`, and `cargo run`
commands then compile every Orbit dependency with the same values:

```toml
[env]
ORBIT_EVENT_RING_CAPACITY = { value = "1024", force = true }
ORBIT_EVENT_RING_PAYLOAD_CAPACITY = { value = "512", force = true }

ORBIT_COUNTER_CAPACITY = { value = "1024", force = true }
ORBIT_COUNTER_KEY_MAX = { value = "240", force = true }

ORBIT_INVOKE_RING_CAPACITY = { value = "256", force = true }
ORBIT_INVOKE_RING_PAYLOAD_CAPACITY = { value = "8192", force = true }

ORBIT_CACHE_MUTATION_RING_CAPACITY = { value = "1024", force = true }
ORBIT_CACHE_MUTATION_RING_PAYLOAD_CAPACITY = { value = "1024", force = true }
ORBIT_CACHE_PAYLOAD_RING_CAPACITY = { value = "1024", force = true }
ORBIT_CACHE_PAYLOAD_RING_PAYLOAD_CAPACITY = { value = "4096", force = true }

ORBIT_LOCK_EVENT_RING_CAPACITY = { value = "1024", force = true }
ORBIT_LOCK_EVENT_RING_PAYLOAD_CAPACITY = { value = "1024", force = true }
ORBIT_LOCK_STATE_CAPACITY = { value = "256", force = true }
ORBIT_LOCK_STATE_PAYLOAD_CAPACITY = { value = "960", force = true }

ORBIT_METRIC_SNAPSHOT_RING_CAPACITY = { value = "1024", force = true }
ORBIT_KEYED_METRIC_RING_CAPACITY = { value = "4096", force = true }

ORBIT_RUSTLS_SESSION_SET_COUNT = { value = "256", force = true }
ORBIT_RUSTLS_SESSION_WAYS = { value = "8", force = true }
ORBIT_RUSTLS_SESSION_DOMAIN_CAPACITY = { value = "64", force = true }
ORBIT_RUSTLS_SESSION_KEY_CAPACITY = { value = "64", force = true }
ORBIT_RUSTLS_SESSION_VALUE_CAPACITY = { value = "16384", force = true }
```

Every key is optional and retains the documented default when absent. Values
are decimal byte or entry counts and may contain `_` separators. Invalid
values fail compilation. These settings are part of the fleet wire contract:
all peers that open the same fleet must be built with the same geometry, and
existing SHM objects must be cleared before a geometry change takes effect.

## Command-line inspection

The separately published `orbitive-cli` package installs the `orbit` command.
It can inspect Orbit POSIX shared-memory objects without joining the fleet:

```sh
cargo install orbitive-cli
orbit list example
orbit list example --kind 231
```

It can also remove one object or every object belonging to a stopped fleet:

```sh
orbit clear example --kind 231
orbit clear example --all --yes
```

Do not clear a running fleet. Existing POSIX mappings survive unlink while a
later process can create a different object under the same name.

## External read-only observation (no fleet join)

On Unix, an independent Rust process can inspect an existing SHM ring without
joining as a writer or reproducing the producer's fleet geometry:

```rust,no_run
use orbitive::FleetObserver;
use orbitive::ring::cursor::{RingCursor, poll_ring};

let observer = FleetObserver::attach_existing("example")?;
let ring = observer.ring(221)?;

for lane_index in 0..ring.metadata().lane_count {
    let lane = ring.lane(lane_index)?;
    let mut cursor = RingCursor::from_counter(lane.retained_range().start);
    for frame in poll_ring(&lane, &mut cursor).frames {
        println!("{lane_index}: {frame:?}");
    }
}

# Ok::<(), Box<dyn std::error::Error>>(())
```

The observer opens exact existing objects read-only. It cannot create, reset,
publish, or unlink, and dropping it only unmaps local memory. Raw frame access
belongs to core; the crate that owns a kind remains responsible for payload
decoding and freshness or liveness judgments. A consumer linked to the same
typed contract can use `observer.typed_ring::<T>()` for an additional
`OrbitTyped::RING_SPEC` check.

This is intentionally observation rather than another form of `Fleet::join`:
the external process receives no node id, holds no fleet membership, owns no
writer lane, and does not keep the fleet alive. `ring(kind)` opens only that
already-existing uid-scoped object. Use `attach_existing_for_uid` when an
operator process is permitted to inspect a different user's namespace.

## Runtime contract

Every process joining the same shared fleet must agree on the immutable fleet
capacity, node ownership, stable kind numbers, and ring or table layouts.
Capacity reserves physical node lanes; it does not assert current membership
or liveness. Storage is
bounded: old ring frames may be overwritten, caches may miss, and session
entries may be evicted. Callers must treat those outcomes according to the
semantic module they use.

Shared-memory operation currently targets Unix. Native readiness is available
on Linux, FreeBSD, and macOS 14.4 or later. On older macOS, readiness creation
returns `io::ErrorKind::Unsupported`; callers can use polling where the
semantic module permits it.

Repository: <https://github.com/iadev09/orbitive>

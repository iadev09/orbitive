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
orbitive = { version = "0.3.3", features = ["events", "lock"] }
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
| `events` | `orbitive::events` | raw topic and byte-payload event streams |
| `lock` | `orbitive::lock` | keyed leases with ownership checks and fencing tokens |
| `metrics` | `orbitive::metrics` | current metrics snapshots across workers |
| `rustls` / `rustls_0_24` | `orbitive::rustls` | fleet-shared rustls 0.23/0.24 server-session storage |

The implementation crates are also published separately as
[`orbit-core`](core/README.md), [`orbit-cache`](cache/README.md),
[`orbit-events`](events/README.md), [`orbit-lock`](lock/README.md),
[`orbit-metrics`](metrics/README.md), and
[`orbit-rustls`](rustls/README.md). Direct dependencies are supported when an
integration needs a single narrow layer; applications can otherwise use the
facade and enable only the modules they need.

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

## Runtime contract

Every process joining the same shared fleet must agree on the immutable fleet
capacity, node ownership, stable kind numbers, and ring or table layouts.
Capacity reserves physical node lanes; it does not assert current membership
or liveness. Storage is
bounded: old ring frames may be overwritten, caches may miss, and session
entries may be evicted. Callers must treat those outcomes according to the
semantic module they use.

Shared-memory operation currently targets Unix. Native readiness is available
on Linux and FreeBSD; other supported targets use caller-driven polling where
the semantic module permits it.

Repository: <https://github.com/iadev09/orbitive>

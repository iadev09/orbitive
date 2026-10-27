<p align="center">
  <img src="https://raw.githubusercontent.com/iadev09/orbitive/main/assets/logo.svg" alt="Orbitive" width="480">
</p>

<p align="center">
  <a href="https://crates.io/crates/orbitive"><img src="https://img.shields.io/crates/v/orbitive.svg" alt="crates.io"></a>
  <a href="https://docs.rs/orbitive"><img src="https://img.shields.io/docsrs/orbitive" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="license">
</p>

**Shared state and bounded communication between Rust processes on the same machine.**

Orbitive lets processes share events, counters, caches, locks and byte streams
through POSIX shared memory. It runs inside your application, with no separate
broker to deploy. Processes find shared resources by name; they do not need to
inherit memory mappings from a common parent.

Use it when several local workers need to exchange data or coordinate access
to resources within a fixed memory budget. Choose the modules you need through
Cargo features. The core API does not require an async runtime; Tokio
integration is available for asynchronous communication.

Shared memory is runtime state. Orbitive does not provide durable storage,
cross-machine replication or a network protocol. Applications own their message
formats, process lifecycle and recovery policy.

[API documentation](https://docs.rs/orbitive) ·
[Crate](https://crates.io/crates/orbitive) ·
[Source](https://github.com/iadev09/orbitive)

## Quick start: send an event between processes

This Unix example publishes an event in one process and reads it from another.
It uses the `events` feature, which provides topics and byte payloads over a
bounded shared ring. See [platform support](#platform-support) for release-specific
requirements.

Create a project:

```sh
cargo new orbitive-demo
cd orbitive-demo
```

Add to `Cargo.toml`:

```toml
[dependencies]
orbitive = { version = "0.5.9", features = ["events"] }
```

Replace `src/main.rs` with:

```rust
use std::sync::Arc;

use orbitive::events::FleetEventBus;
use orbitive::{Fleet, NodeId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let action = std::env::args().nth(1).unwrap_or_default();
    let node = match action.as_str() {
        "publish" => NodeId::new(0),
        "read" => NodeId::new(1),
        _ => return Err("usage: orbitive-demo <publish|read>".into()),
    };

    // Same name and capacity; a different node id for each process role.
    let fleet = Arc::new(Fleet::join_shm_as("demo", 2, node)?);
    let bus = FleetEventBus::new(fleet);

    if action == "publish" {
        bus.publish("worker.ready", b"hello from another process")?;
        println!("Published worker.ready");
    } else {
        let mut cursor = bus.cursor_from_start();
        let batch = bus.poll(&mut cursor);
        for event in batch.events {
            println!("{}: {}", event.topic, String::from_utf8_lossy(&event.payload));
        }
        if batch.lagged != 0 {
            eprintln!("{} events were unavailable", batch.lagged);
        }
    }
    Ok(())
}
```

Run the publisher, then the reader, as the same OS user:

```sh
cargo run -- publish
cargo run -- read
```

On a fresh `demo` fleet, the output from the two commands is:

```text
Published worker.ready
worker.ready: hello from another process
```

These are separate process invocations. The publisher can exit before the
reader starts because the named SHM object survives the publisher. This is not
disk persistence: the data is not a durable log and should not be expected to
survive a reboot.

The reader takes one snapshot of retained events and exits. Each run starts a
new cursor, so repeated runs can read earlier retained messages again. In a
long-lived consumer, keep the cursor between reads; use `cursor_at_head()` to
start after existing events. For event-driven waiting, the event bus exposes
`event_fd()`; drain readiness and then poll the ring.

Only run one publisher for node 0 and one reader for node 1 at a time in this
example. Use a different fleet name if `demo` already belongs to an application.
When both programs have exited, remove the example's event ring:

```sh
cargo install orbitive-cli
orbit list demo
orbit clear demo --kind 220
```

Kind `220` is the default event bus ring. The CLI is a separate package; it is
not required to use the library. See [resource lifecycle](#resource-lifecycle)
before clearing resources used by a running application.

## The model

A **fleet** is a named group of cooperating processes on one machine. Each
member uses a **node id** to address its part of the shared storage. Fleet
capacity reserves space for those nodes; it is not a live process count.

A **kind** identifies a ring or table within the fleet. A **layout** specifies
its physical capacity and payload limits. Peers opening the same resource must
agree on its kind, layout and fleet capacity.

Storage has different semantics depending on the module:

| Shape | Use | When capacity is reached |
| --- | --- | --- |
| Ring | Retained events and recent history | Older frames may be overwritten; cursors report loss |
| Current-state table | Counters, leases, sessions and keyed state | The module defines refusal or eviction behavior |
| Inbox and stream | Addressed work and bounded byte flow | Admission or writes report capacity limits; callers handle backpressure |

The published entry point is `orbitive`. Public names such as `Fleet`,
`OrbitTyped` and `OrbitEpoch` use **Orbit**, the runtime model's name.

### Local memory versus shared memory

Choose the backing explicitly:

| Constructor | Storage | Intended use |
| --- | --- | --- |
| `Fleet::join(name, capacity)` | Process-local memory, node 0 | In-process coordination and tests |
| `Fleet::join_as(name, capacity, node)` | Process-local memory, explicit node | In-process use with explicit identity |
| `Fleet::join_shm(name, capacity)` | Named POSIX SHM, node 0 | A single shared-memory member |
| `Fleet::join_shm_as(name, capacity, node)` | Named POSIX SHM, explicit node | Multiple cooperating processes |

**Matching names do not connect separate `Fleet::join` handles.** Share a
handle or its clone within one process. Use SHM constructors for communication
between processes. For per-node rings, assign each active process a distinct
node id below the fleet capacity; the core does not allocate unique ids for you.

## Choose a module

Core types are available without optional features. Enable only the modules
your application uses; each linked guide describes its API and limits.

| Need | Cargo feature | Guide |
| --- | --- | --- |
| Typed rings, cursors, SHM mappings and read-only observers | None | [Core](core/README.md) |
| Topic and byte-payload events | `events` | [Events](events/README.md) |
| Keyed shared counters | `counter` | [Counters](counter/README.md) |
| Bounded GCRA quota admission | `quota` | [Quota](quota/README.md) |
| Typed atomic cells | `cell` | [Cells](cell/README.md) |
| Per-process caches with shared invalidation | `cache` | [Cache](cache/README.md) |
| Shared byte storage with pinned reads | `arena` | [Arena](arena/README.md) |
| Keyed leases with fencing tokens | `lock` | [Locks](lock/README.md) |
| Worker metrics snapshots | `metrics` | [Metrics](metrics/README.md) |
| Bounded invocation requests | `invoke`, `invoke-tokio` | [Invoke](invoke/README.md) |
| Named dispatch and reusable duplex sessions | `link`, `link-tokio` | [Link](link/README.md) |
| Duplex byte streams and paired exchanges | `stream`, `stream-tokio` | [Stream](stream/README.md) |
| Fleet-wide resource leases and creation claims | `pool` | [Pool](pool/README.md) |
| Resource leases bound to stream exchanges | `pool-stream` | [Pool](pool/README.md) |
| Process-local outbound client pools | `client-pool` | [Client pool](client-pool/README.md) |
| Client pools with remote-owner execution within the fleet | `client-pool-fleet` | [Client pool](client-pool/README.md) |
| Encrypted tokens and shared session, refresh and replay state | `auth` | [Auth](auth/README.md) |
| Shared rustls server sessions | `rustls` or `rustls_0_23` | [rustls](rustls/README.md) |
| Shared rustls 0.24 server and client sessions | `rustls_0_24` | [rustls](rustls/README.md) |

Feature names generally match module paths: `events` enables
`orbitive::events`, and `link-tokio` enables the async API in `orbitive::link`.
The `*-tokio` features also enable their base modules. Client pools require
Tokio. The `rustls_0_24` feature currently selects a rustls 0.24 prerelease;
choose the feature matching your application's rustls dependency.

`full` is a convenience bundle containing `auth`, `arena`, `cache`, `cell`,
`events`, `counter`, `invoke`, `link`, `metrics`, `lock`, `rustls`, `stream`,
`stream-tokio` and `pool`. It does **not** enable every optional integration:
`link-tokio`, `invoke-tokio`, `pool-stream`, client pools and `rustls_0_24`
remain explicit choices.

The `orbit-*` implementation crates are also available directly. Most
applications can depend on `orbitive` and use its feature modules; libraries
building a narrow integration can depend on the underlying crate. Packages
are versioned independently, so their version numbers need not match.

### Events, streams or links?

Use **events** when readers need to observe recent publications and can handle
a bounded retention window. Each reader has an independent cursor; there is
no acknowledgement or durable replay.

Use **stream** for bounded duplex byte flow or paired exchanges. Your
application supplies the protocol and decides how to handle backpressure.

Use **link** when callers need to find a named local worker and dispatch work
to its inbox, optionally carrying a ticket for a duplex stream. A full inbox
refuses admission rather than overwriting accepted frames. Applications still
choose message encoding and routing policy.

For Tokio-backed links and streams, application data stays in SHM. Local
Unix-datagram sockets provide wake notifications to the runtime; they carry no
application payload. A wake tells the receiver to check shared state.

## Capacity and compatibility

Orbitive bounds storage explicitly. Ring capacity, payload limits, table slots
and stream windows are part of a resource's layout. More capacity reserves more
memory; it does not make events durable or consumers lossless.

Core rings use `OrbitTyped::KIND` and `OrbitTyped::RING_SPEC`. Links and streams
accept explicit specifications. Several higher-level modules also let an
application select default geometry at compile time in `.cargo/config.toml`:

```toml
[env]
ORBIT_EVENT_RING_CAPACITY = { value = "1024", force = true }
ORBIT_EVENT_RING_PAYLOAD_CAPACITY = { value = "512", force = true }
```

These values apply to every dependency in that application's Cargo build.
Omitted settings retain the module's defaults; invalid values fail compilation.
See the individual module guides for available settings and capacity behavior.
For events, the payload budget includes the topic and frame header as well as
the application's bytes.

All peers sharing a resource must use compatible layouts. An existing SHM
object is not resized to accommodate a new binary. To change a layout, stop
its users and remove the old object, or assign a new kind so old and new layouts
can coexist during migration. Check the module's assigned kinds before choosing
one for your own data.

## Resource lifecycle

Named SHM objects outlive the handles and processes that created them.
Dropping a handle releases its mapping, not the shared resource's name.
Applications must decide who initializes state, reclaims dead owners and
removes retired resources. Recovery behavior belongs to the selected module;
there is no universal process supervisor built into `Fleet`.

Use the `orbit` CLI to inspect objects without joining the fleet:

```sh
orbit list example
orbit list example --kind 231
```

After stopping all users of the selected resources:

```sh
orbit clear example --kind 231
orbit clear example --all --yes
```

Unlinking an object does not invalidate mappings already held by running
processes. Clearing a live resource can leave old processes using one object
while new processes create another under the same name.

Fleet resources are scoped by OS user id. The default SHM policy grants owner
read/write access. Processes sharing those credentials are within the same
trust boundary. For external observation, `FleetObserver` opens existing rings
read-only without allocating a writer node or changing their lifecycle; see
[Core](core/README.md#external-read-only-observation).

## Platform support

Shared-memory APIs target Unix. Native ring readiness is implemented for
Linux, FreeBSD and macOS 14.4 or later. On older macOS, constructing a native
ring readiness bridge returns `Unsupported`; polling remains available where
the module permits it. SHM support does not imply support for every async or
platform-specific integration.

## License

Licensed under either the MIT license or the Apache License, Version 2.0,
at your option.

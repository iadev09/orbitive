<p align="center">
  <img src="https://raw.githubusercontent.com/iadev09/orbitive/main/assets/logo.svg" alt="Orbitive" width="480">
</p>

<p align="center">
  <a href="https://crates.io/crates/orbitive"><img src="https://img.shields.io/crates/v/orbitive.svg" alt="crates.io"></a>
  <a href="https://docs.rs/orbitive"><img src="https://img.shields.io/docsrs/orbitive" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="license">
</p>

**Share live state between the processes of one machine as you would between threads.**

A multi-worker web or application server, a pool of job workers, an
application split into services with different privileges: each process has
its own memory, so a counter, a flag, a cached value or a lock in one of them
is invisible to the others. The usual answer is a broker such as Redis, or
sockets and a protocol of your own.

Orbitive is a library that keeps that state in shared memory instead. Every
process of the group reads and writes the same value directly: no server to
run, no network round trip, no serialization for a number. Memory use is fixed
when a resource is created.

[API documentation](https://docs.rs/orbitive) ·
[Crate](https://crates.io/crates/orbitive) ·
[Source](https://github.com/iadev09/orbitive)

## What you can do with it

**A value every process sees.** `std::sync::atomic`, between processes. Declare
it once; any process opens it by type and uses it as an atomic.

```rust
orbital! {
    /// Requests served by the whole group.
    pub Hits: u64 = 0, "hits"
}

let hits = Hits::open(&orbitals)?;
hits.fetch_add(1);                  // one atomic instruction, no lookup
```

**Wait for another process, without polling.** A write in one process wakes a
waiter parked in another.

```rust
let mut left = jobs_left.load();
while left != 0 {
    left = jobs_left.wait(left)?;   // parks until the value changes
}
```

**One cache for all workers.** A value stored by one worker is a hit in the
others, and one call invalidates everything carrying a tag, everywhere.

```rust
store.put_tagged(b"user:42", b"profile", None, &["users", "team:7"])?;
store.invalidate_tags(&["team:7"])?;          // a miss in every process
```

**Count by key.** Counters named at run time, shared by the group.

```rust
let served = counter.increment("tenant:42:requests", 1)?;
```

**Tell the others something happened.** Publish to a topic; each reader keeps
its own position in a bounded history.

```rust
bus.publish("worker.ready", b"hello from another process")?;
```

**Split an application into processes that work as one.** A web server and a
firewall service can be two programs, each under its own OS user, with its own
privileges and its own restarts. Over a [Link](link/README.md) one finds the
other by name and calls it as if it were a module in the same process: the
request, the reply and any stream between them travel through shared memory.
The two share that one link through an OS group and nothing else; neither can
reach the other's files or its other state.

And with the same model:

| You want to | Use |
| --- | --- |
| Let one process at a time hold something, with a fencing token | [Locks](lock/README.md) |
| Limit a rate across all processes | [Quota](quota/README.md) |
| Hand work to another process and get its answer | [Invoke](invoke/README.md), [Link](link/README.md) |
| Move bytes between two processes with backpressure | [Stream](stream/README.md) |
| Share a pool of connections or other resources | [Pool](pool/README.md), [Client pool](client-pool/README.md) |
| Keep sessions and tokens valid in every worker | [Auth](auth/README.md) |
| Resume TLS sessions whichever worker takes the connection | [rustls](rustls/README.md) |
| Read every worker's metrics from one place | [Metrics](metrics/README.md) |

[Claviron](https://github.com/claviron/claviron) is a fleet-aware application
server built on these modules together with
[Continuo](https://github.com/iadev09/continuo); its repository shows them
working together in one system.

Each is a Cargo feature; you compile only what you use. The core needs no
async runtime, and Tokio integration is available where waiting is
asynchronous.

## What it is not

Shared memory is runtime state. Orbitive does not store data durably, does not
replicate between machines and is not a network protocol. It serves
cooperating processes on one machine: by default those of one OS user, and
across users only for a link they are explicitly given through a shared
group. It is not a sandbox between hostile processes. Applications own their
message formats, their process lifecycle and their recovery policy.

## Try it: two processes, one value

One process watches a value; another changes it. This uses the `atomic`
feature and runs on Linux, FreeBSD and macOS 14.4 or later.

```sh
cargo new orbitive-demo
cd orbitive-demo
```

Add to `Cargo.toml`:

```toml
[dependencies]
orbitive = { version = "0.6.19", features = ["atomic"] }
```

Replace `src/main.rs` with:

```rust
use std::sync::Arc;

use orbitive::atomic::Orbitals;
use orbitive::{Fleet, NodeId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let action = std::env::args().nth(1).unwrap_or_default();
    let node = match action.as_str() {
        "watch" => NodeId::new(0),
        "hit" => NodeId::new(1),
        _ => return Err("usage: orbitive-demo <watch|hit>".into()),
    };

    // Same fleet name in both processes; a different node id for each.
    let fleet = Arc::new(Fleet::join_shm_as("demo", 2, node)?);
    // Both processes declare the same name and get the same value.
    let hits = Orbitals::new(fleet)?.declare("hits", 0_u64)?;

    if action == "hit" {
        println!("hits = {}", hits.fetch_add(1) + 1);
        return Ok(());
    }

    let mut seen = hits.load();
    loop {
        println!("hits = {seen}");
        // Parks until another process changes the value. No polling.
        seen = hits.wait(seen)?;
    }
}
```

Start the watcher in one terminal:

```sh
cargo run -- watch
```

and run this a few times in another:

```sh
cargo run -- hit
```

Each `hit` prints the new count, and the watcher prints it at the same moment:

```text
hits = 0
hits = 1
hits = 2
```

Stop the watcher and run `hit` again: the count continues. The value belongs
to the shared memory, not to either process. It is not on disk and does not
survive a reboot.

Use a different fleet name if `demo` already belongs to an application. When
both programs have exited, remove the example's shared memory:

```sh
cargo install orbitive-cli
orbit list demo
orbit clear demo --all --yes
```

The CLI is a separate package; it is not required to use the library. See
[resource lifecycle](#resource-lifecycle) before clearing resources used by a
running application.

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
The rings, cursors and mappings underneath every module are in
[Core](core/README.md); most applications never use them directly.

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

Enable only the modules your application uses; each linked guide describes
its API and limits.

| Need | Cargo feature | Guide |
| --- | --- | --- |
| Atomics shared between processes, declared by name, with wait and notify | `atomic` | [Atomics](atomic/README.md) |
| Topic and byte-payload events | `events` | [Events](events/README.md) |
| Keyed shared counters | `counter` | [Counters](counter/README.md) |
| Bounded GCRA quota admission | `quota` | [Quota](quota/README.md) |
| Fleet caches with shared invalidation by key, store or tag | `cache` | [Cache](cache/README.md) |
| Shared byte storage with pinned reads | `arena` | [Arena](arena/README.md) |
| Keyed leases with fencing tokens | `lock` | [Locks](lock/README.md) |
| Worker metrics snapshots | `metrics` | [Metrics](metrics/README.md) |
| Typed rings, cursors, SHM mappings and read-only observers | None | [Core](core/README.md) |
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

`full` is a convenience bundle containing `auth`, `arena`, `atomic`, `cache`,
`events`, `counter`, `invoke`, `link`, `metrics`, `lock`, `rustls`,
`stream`, `stream-tokio` and `pool`. It does **not** enable every optional
integration:
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

## Benchmarks

Measurements currently live with each crate: its `benches/` directory and,
where one exists, the record beside it, such as
[Link's](link/docs/benches/README.md). A page that gathers them per module is
in preparation.

## Platform support

Shared-memory APIs target Unix. Native ring readiness is implemented for
Linux, FreeBSD and macOS 14.4 or later. On older macOS, constructing a native
ring readiness bridge returns `Unsupported`; polling remains available where
the module permits it. SHM support does not imply support for every async or
platform-specific integration.

## License

Licensed under either the MIT license or the Apache License, Version 2.0,
at your option.

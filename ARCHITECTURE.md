# Orbitive architecture

How state is shared between the processes of one machine, and what each part
of the workspace is responsible for. The [README](README.md) shows what the
library is for; this document describes how it is built. Each module has its
own `ARCHITECTURE.md` with its layout and invariants; this one stops where
they begin.

## Names

**Orbitive** is the repository, the published facade crate and its Cargo
features. **Orbit** is the runtime model, and the name its public types carry:
`Fleet`, `OrbitTyped`, `OrbitEpoch`.

The facade exposes a small set of core types at its root (`Fleet`, `NodeId`,
`OrbitTyped`, `RingSpec`), the whole of `orbit-core` as `orbitive::core`, and
each optional module under its own name, such as `orbitive::events` or
`orbitive::atomic`. The `orbit-*` crates are published separately and versioned
independently; a library that extends one narrow part can depend on that crate
alone.

## The model

Four terms carry everything else.

- A **fleet** is a named group of cooperating processes on one machine. The
  name is one to eight ASCII letters, digits, hyphens or underscores, kept
  inline without allocation; it is short because macOS limits the length of a
  POSIX shared-memory name.
- A **node id** addresses one member's part of a shared resource, such as its
  writer lane in a ring. The fleet's **capacity** fixes how many lanes exist.
  It is geometry, not a count of processes that are present or healthy.
- A **kind** is an 8-bit identity for one ring or table within the fleet. Each
  module owns the kinds it declares, and they are stable: peers that share a
  resource must agree on its kind.
- A **layout** is the physical shape of a resource: capacities, slot and
  payload sizes. It is written into the resource's header when it is created,
  and a process that opens it with another layout is refused.

A `Fleet` is a process-local handle: a name, a node id, a capacity and a
registry of the resources this process has opened. Orbit has no notion of
master and worker. Any process given the same fleet name and compatible
layouts can join; which process initializes or resets a resource is the
embedding application's decision.

### Backing

A fleet is backed either by process-local memory or by named POSIX shared
memory. The API is the same over both. The in-memory backing serves tests and
single-process use; sharing between processes uses shared memory.

A shared-memory object is named

```text
/orbit-{fleet}-{kind}-{uid}
```

so one fleet's resources are separate objects, found by name without a common
parent process, and scoped to one OS user. The exception is a Link opened with
explicit group access, described under [Link](#link-and-streams).

### Geometry is a compile-time profile

A module reads its capacities from documented `ORBIT_*` variables with
`option_env!`, and the application declares them once under `[env]` in its
`.cargo/config.toml`. Cargo applies one profile to the whole dependency graph;
there is no runtime configuration channel. A missing value keeps the module's
default and an invalid one fails to compile. Kinds are not configurable.

Every process of a fleet must be built with the same profile. An existing
object is never resized: changing a layout means stopping its users and
removing the object, or declaring a new kind so the old and new shapes can
coexist while processes are replaced.

## Storage shapes

A module chooses the shape that matches its data. History and current state
are different things, and current state is never reconstructed from retained
history.

### Rings: bounded history

One `OrbitTyped` kind maps to one ring, with a `RingSpec { capacity,
payload_capacity }` that fixes its retention window and its inline payload
size. A frame is

```text
NetId64 id | frame kind | version | payload bytes
```

The id's kind says which value family the frame belongs to; the frame kind is
an opcode chosen by the module that owns the ring. A write reserves a
monotonically increasing counter and publishes into `counter % capacity`, so
old frames are overwritten and a reader that asks for one gets a miss, by
design. Payloads are bounded; a larger value is stored elsewhere and
referenced.

Each member writes in its own lane. Lane counters locate frames; a separate
per-ring version allocator, shared by all lanes, gives a module one
fleet-wide order when it needs one.

Readers walk a ring with a **cursor**, which holds the next counter to attempt.
Polling advances it to the head and reports loss in two buckets: *overwritten*,
when the cursor fell behind the window, and *unavailable*, when a slot in the
window was empty, torn or reused. The cursor does not decode payloads,
acknowledge frames or provide durability. Every module that reads a ring uses
this one traversal.

### Tables: current state

Mutable truth that is looked up by key or by slot lives in a fixed-capacity
table: counters, shared atomics, leases, sessions, the shared cache index.
A table is one shared object, never one per key. What happens when it is full
is the module's contract: refusal, or eviction under stated rules.

### Arenas: bytes

Values larger than a frame live once in a byte arena and are addressed by slot
and generation. A reader pins what it reads and gets the bytes without a copy;
a slot that was reused is a detectable miss, never another value's bytes.
Pins are held through kernel locks, so a process that is killed releases them
without a supervisor or a timeout.

### Inboxes and streams: addressed flow

Work addressed to one receiver goes to that receiver's bounded inbox, and
bytes between two ends flow through a bounded duplex stream. Neither
overwrites: a full inbox refuses admission and a full stream applies
backpressure to its writer.

## Waiting and waking

A wait parks on a word in shared memory and a write wakes it. The primitive is
the platform's wait-on-address: futex on Linux, umtx on FreeBSD,
`os_sync_wait_on_address` on macOS 14.4 and later, all in their shared
(cross-process) mode. There is no polling interval anywhere in the library.

For a ring, the publisher commits the frame and then advances a notification
generation in the ring's header. Each subscribing process turns that shared
wake into readiness on a file descriptor of its own (an eventfd, or a
nonblocking pipe on macOS), which an event loop can register. The descriptor
carries readiness only: no event identity and no payload. Several publishes
may coalesce into one wake without losing frames that are still in the ring.

A wake on every publish would cost a system call per frame, so the publisher
wakes only a reader that is actually parked. A reader's driver counts itself
in the ring's header before it waits and re-reads the generation; the
publisher advances the generation and then reads that count. Each side's
second step is sequentially consistent, so one of them always sees the other.
While a reader is busy draining, a publish is a slot write and an atomic
operation, with no system call. The driver signals once per drain, so a
reader clears its reactor's readiness before draining, not after.

Tokio-backed links and streams use a Unix-datagram socket per lane as the
wake. It carries no application data: the receiver checks shared memory before
parking and again after arming.

On older macOS, constructing a native readiness bridge returns `Unsupported`;
publication still works and readers poll.

## Lifetime and recovery

A named shared-memory object outlives the handles and the processes that
created it. Dropping a handle unmaps it; nothing is removed. This is what lets
a restarted worker find the state its predecessor left, and it is why
initialization, reset and removal are explicit.

- **Reset** is an owner-side operation for a quiescent resource, typically at
  boot before peers attach. It is not a concurrent mutation.
- **Recovery of a dead owner** uses kernel-held locks rather than time. A lane,
  a pin or a structural lock held by a process that died is released by the
  kernel, and the next process that needs it can prove the holder is gone.
- **Removal** is done by the application or by an operator with the CLI, after
  the resource's users have stopped. Unlinking does not invalidate mappings
  that running processes still hold.

Orbit includes no process supervisor. Which process owns initialization, when
a fleet is quiescent and what a restart preserves are decided by the
application.

## Modules

| Module | Shape | What it keeps |
| --- | --- | --- |
| [atomic](atomic/ARCHITECTURE.md) | Table and name directory | 64-bit values used as atomics between processes, reached by id or by declared name, with wait and notify |
| [counter](counter/ARCHITECTURE.md) | Table | Signed counters under keys chosen at run time |
| [events](events/ARCHITECTURE.md) | Ring | Topic and payload frames; every frame matters until it is overwritten |
| [cache](cache/ARCHITECTURE.md) | Ring and arena, or table and arena | A per-process cache kept coherent by a mutation ring, and a shared cache every process reads directly; invalidation by key, store or tag |
| arena | Arena | The keyed byte store the cache and other modules build on |
| [lock](lock/ARCHITECTURE.md) | Table and ring | Keyed leases with an owner, a deadline and a fencing token; transitions are published for waiters |
| [quota](quota/ARCHITECTURE.md) | Table | GCRA admission with explicit geometry, bounded probes and immediate refusal instead of waiting |
| [metrics](metrics/ARCHITECTURE.md) | Ring | Snapshot families; the newest frame per node wins |
| [invoke](invoke/ARCHITECTURE.md) | Ring | Bounded invocation requests between members |
| link | Inbox, tables and streams | Named dispatch to a receiver and reusable duplex sessions |
| [stream](stream/ARCHITECTURE.md) | Streams and arena | Duplex byte streams and paired exchanges |
| [pool](pool/ARCHITECTURE.md) | Table | Fleet-wide leases, reservations and creation claims for shared resources |
| [client-pool](client-pool/ARCHITECTURE.md) | Process-local, optionally over pool and stream | Ownership of outbound clients, with fleet-wide admission as an option |
| [auth](auth/ARCHITECTURE.md) | Table | Encrypted tokens, and session and replay state every process agrees on |
| [rustls](rustls/ARCHITECTURE.md) | Tables | TLS session state shared by the processes that accept connections |

Three differences between ring users are worth stating once, because they
decide which module fits: in the replicated cache the newest frame for a key
wins, in metrics the newest frame per node wins, and in events every frame
matters.

### Link and streams

A link gives each participant one bounded inbox lane. A caller publishes a
reference to an exchange in the chosen receiver's lane; the exchange table
holds two independent directions for request and response bytes. A reader
borrows an immutable view and returns the arena credit when the last view is
dropped.

Admission is owned by the receiver. Each receiver has a declared window of
work it will hold, of which it runs a stated number at once and queues the
rest. A sender is never made to wait for receiver capacity: when every window
is full it is refused, and it can read each receiver's load to prefer one that
will start the work now. A reservation is a recoverable claim: recovery of a
dead member cancels that member's reservations and leaves work a live receiver
has accepted untouched.

By default a link, like every other resource, belongs to one OS user. With
explicit group access, processes of different users share one link through an
OS group: the owner creates the mappings and lock files, and group members
attach to them. That sharing covers the link and nothing else.

### Atomics

Values live in one table per fleet; a handle points at a slot and its
operations are the processor's atomic instructions on that word. A value is
reached by its id, carried from the process that allocated it, or by a
declared name, which a separate directory resolves once, at declaration. The
value path never reads the directory. A slot is not returned while the table's
epoch lasts; a reset starts a new epoch, ends every earlier id and carries
over only the values declared as kept.

## Observation and the CLI

`FleetObserver` lets a diagnostic tool read an existing ring without joining
the fleet. It has no node id and no writer lane, maps the ring read-only, and
derives the layout from the persisted header. It can walk frames through the
ordinary cursor, with the same loss accounting. It cannot create, publish,
reset or unlink, it does not interpret payloads or infer liveness, and dropping
it changes nothing for the fleet.

`orbitive-cli` installs the `orbit` executable. It is a separate package, not
a facade feature. It finds objects by constructing exact names over the 8-bit
kind space and opening them, which works on systems without a `/dev/shm` view;
it never matches by prefix. It lists fleets, lists and sizes a fleet's
objects, and removes them on an explicit command for a stopped fleet. Its
cache inspection reads retained mutation history and selected values through
read-only mappings and the arena's ordinary pin protocol, without becoming a
member.

## Invariants

- No global singleton and no runtime provider: every handle is constructed
  from a `Fleet` the application owns.
- No serializer is chosen for the application's values; payloads are bytes.
- No network transport and no durability. Shared memory is runtime state.
- Every capacity is fixed when a resource is created. Nothing grows.
- A layout is validated on open and never changed in place.
- A wait parks on a shared word and ends by a wake. No operation sleeps or
  retries on an interval.
- A dead process is detected through a lock the kernel released, not through
  elapsed time.
- An observer never becomes a member or an owner of a resource's lifecycle.
- A reset is valid only while the resource is quiescent.
- Kinds are stable for peers that share a resource.

## What the library leaves to the application

Process roles, boot order and supervision; the encoding of values and
messages; routing policy; what a restart preserves; and anything that reaches
another machine. A bridge from fleet events into an application's own event
system, or from a link into HTTP, belongs to the application or framework
that has those types.

## Measuring

A measurement of Orbit is mostly a measurement of the host's wake cost, so the
first figure to take on any machine is the kernel's own.

| Bench | What it measures | Run |
| --- | --- | --- |
| `core/benches/wake.rs` | One cross-thread wake through the calls every park uses, with no ring, table or runtime: the floor under every wait | `cargo bench -p orbit-core --bench wake -- [rounds]` |
| `core/benches/notify.rs` | What a ring publish costs with and without a wake, with no reader and with a busy one | `cargo bench -p orbit-core --bench notify` |

`wake` prints the round trip (two wakes), one wake, and how many waits really
parked. macOS does not keep that last counter and reports zero.

Reference figures for one wake, September 2026: 1.24 µs on bare-metal Linux
(x86-64, 40 cores), 2.03 µs on macOS (Apple silicon), 22.5 µs in a Linux guest
under a hypervisor. A guest's wake is slow enough to reverse a comparison
between shared memory and a socket, so transport comparisons are decided on
bare metal.

Other modules keep their benches in their own `benches/` directories.

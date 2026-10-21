# orbit-link

`orbit-link` provides named, bounded dispatch and reusable duplex sessions
between members of one Orbit fleet. Applications normally use it through the
[Orbitive](https://github.com/iadev09/orbitive) facade as `orbitive::link`.

It owns the layer immediately above `orbit-stream`:

```text
caller: choose an advertised lane by depth -> create side A
caller: put an application frame containing the ticket in that lane's inbox
target: read the frame -> open side B -> exchange application bytes
caller: side B releases -> rearm the retained slot -> dispatch it again
```

Each fleet member owns one inbox lane. A full lane refuses rather than
overwriting, and an unowned lane refuses rather than accepting work nobody can
answer. The directory exposes service identity, role, incarnation and current
depth; selection policy remains with the caller. A kernel-held lane lock is
the death signal, so a new join can reclaim the exact incarnation and end its
streams without a timeout.

The fleet name and lane are the rendezvous. No descriptor is inherited or
passed, and all application bytes remain in shared memory. With `tokio`, both
the inbox and the attached stream use named Unix-datagram bells to wake the
runtime directly. A bell carries one meaningless byte, never an application
frame or stream payload; SHM remains authoritative and wakes may coalesce.

`LinkSegment::receiver(lane)` binds the one async receiver that owns a lane.
Its `recv` checks SHM before arming the bell and checks the lane generation
again before awaiting readiness, so a write that crosses either boundary is
not lost. Cancelling the future consumes no frame: the next call checks SHM
first. Blocking integrations may continue to use the lane's wake word.

`LinkSegment::watch()` gives a process that holds no lane a
`DirectoryWatcher`, woken by the same claims and releases. Being woken is not
being connected: the watcher takes a slot of its own, serves nothing and never
appears in the directory. `DirectoryWatcher::next()` reports a changed
directory generation; the caller reads the directory to determine which
services are currently available.

The crate deliberately defines no application frame. HTTP heads, invocation
envelopes, resource leases and routing policy belong to adapters. It also does
not own an upstream connection pool: reoffering retains bounded SHM capacity,
not a TCP connection or its TLS/protocol state.

Every link requires a `LinkSpec`: fleet capacity, inbox geometry and kind, and
the stream table's `StreamSpec`. The two kinds must differ. This crate reserves
no default kinds because kind ownership and incompatible geometry migrations
belong to the deployment composing the tables. A changed physical layout uses
a new kind while old and new processes may coexist.

Inbox depth is sampled consumer-first (`read`, then `reserve`). Both counters
only move forward, so this ordering may conservatively overstate a concurrent
producer but cannot pair an old reservation with a newer read and underflow
into a false `Full`. Admission and write reservation use the same ordering.

## Dedicated cross-user Links

`LinkSegment::open` remains owner-only. To connect trusted processes running
as different OS users, explicitly select `GroupAccess` for one dedicated Link:

```rust,no_run
use orbit_link::{GroupAccess, LinkSegment};
# fn example(spec: orbit_link::LinkSpec) -> orbit_link::Result<()> {
// Operator-provisioned parent, explicit resource owner and shared OS group.
let access = GroupAccess::new(1001, 2000, "/run/example/ipc")?;
let link = LinkSegment::open_group("example", spec, access)?;
let joined = link.join("example-service", "worker", 1)?;
let bodies = link.bodies(joined.lane, 1)?;
# let _ = bodies;
# Ok(())
# }
```

The owner starts first and creates the inbox, duplex stream table, lock files
and wake directories. Other users attach existing resources only; a missing
owner resource is an error, not permission to create it under another UID.
Use `link.bodies(...)` so body streams and crash recovery inherit the same
access scope; the owner-only `LinkBodies::for_name` is not interchangeable.
No HTTP or event schema is required. Event bridges can retain private event
rings in their respective fleets while sharing only this Link.

This policy does **not** alter Fleet constructors, unrelated SHM kinds, account
credentials, process umask or existing resource permissions. The SHM namespace
includes the configured owner, group, canonical parent/directory, fleet name
and kind. Different IPC directories cannot supply competing lane locks for
the same group-shared segment. These names differ from owner-only names.

Deployment requirements:

- Provision the parent directory on a trusted path, traversable by every peer
  but not replaceable by untrusted users. All peers use the same path and GID.
- The resource owner must be allowed to assign the selected group. On macOS
  it must be the owner's effective group because POSIX SHM permissions cannot
  be repaired with `fchown`/`fchmod` after creation.
- Run participants with a creation mask preserving group write, for example
  `0007`. The library never changes it. Inadequate permissions are rejected.
- The owner creates a `0750` IPC directory containing `0660` lock files and a
  separate setgid `2770` `bells` directory. Group members can lock but cannot
  replace lock inodes. Wake sockets use that directory on every Unix platform,
  including Linux; they carry readiness only, not application data.
- Keep lock files for the lifetime of the deployment. Do not remove or replace
  them on restart: death proof depends on all peers locking the same inode.
  Only the owner removes SHM names, after every participant has stopped.

Every group writer is trusted with this Link's shared state. Role/service
names are routing declarations, not authenticated user identities. Group
sharing is not a sandbox between malicious peers and does not grant kernel
capabilities, access to other fleets or access to a consumer's private files.
Applications must still validate every requested operation.

The test suite covers group-scoped inbox/stream wakes, owner-only isolation,
permissions, symlink refusal, scope separation and killed-owner reclamation.
`cross_uid_group_transport` is an explicitly ignored privileged test: on an
authorized isolated host, run it as root with the test-name filter and
`--ignored`. It adopts numeric UIDs/GIDs in child processes only, creates no
accounts and changes no host services. This test must pass on a target OS
before declaring cross-user deployment validated.

## Benchmark

`cargo bench -p orbit-link --bench dispatch` compares a complete small
request/reply dispatch with `UnixStream::pair`: lane lookup, ticket encoding,
inbox publication, ticket parsing, stream opening and the duplex bytes are in
the Orbit measurement. The UDS case has no separate routing envelope because
the connected pair is already its address. Both fresh-slot and retained-slot
paths are reported, so allocation and rearming are not conflated.

The benchmark uses representative production geometry: 128 inbox frames of
17 KiB, 128 duplex slots per node and a 64 KiB window in each direction. It is
still a same-process, sequential protocol-cost benchmark. Its reader calls
`Inbox::read` directly, so it does not measure a Tokio reactor wake, process
scheduling, an application protocol or worker routing.

`orbit-stream`'s `transport` benchmark answers a different question. It
compares the exchange payload arena with sockets across body size, application
chunk size and concurrency. `orbit-link` uses `StreamSpec` duplex byte rings,
not that exchange arena. Those results inform stream and chunk geometry; they
are not a substitute for a downstream named-link integration test.

Recorded results, environments and measurement limits are in
[BENCHMARKS.md](BENCHMARKS.md).

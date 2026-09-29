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

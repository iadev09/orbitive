# orbit-link

`orbit-link` provides named, bounded dispatch and reusable duplex sessions
between members of one Orbit fleet. Applications normally use it through
`orbitive::link`.

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
passed, and all application bytes remain in shared memory. With `tokio`, the
stream's named socket doorbell wakes the runtime; the socket carries no
application data.

The crate deliberately defines no application frame. HTTP heads, invocation
envelopes, resource leases and routing policy belong to adapters. It also does
not own an upstream connection pool: reoffering retains bounded SHM capacity,
not a TCP connection or its TLS/protocol state.

Every link requires a `LinkSpec`: fleet capacity, inbox geometry and kind, and
the stream table's `StreamSpec`. The two kinds must differ. This crate reserves
no default kinds because kind ownership and incompatible geometry migrations
belong to the deployment composing the tables. A changed physical layout uses
a new kind while old and new processes may coexist.

## Benchmark

`cargo bench -p orbit-link --bench dispatch` compares a complete small
request/reply dispatch with `UnixStream::pair`: lane lookup, ticket encoding,
inbox publication, ticket parsing, stream opening and the duplex bytes are in
the Orbit measurement. The UDS case has no separate routing envelope because
the connected pair is already its address. Both fresh-slot and retained-slot
paths are reported, so allocation and rearming are not conflated.

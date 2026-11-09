# orbit-rustls

`orbit-rustls` provides same-host, fleet-shared runtime state for rustls over
Orbit shared memory. Applications normally use it through the
[Orbitive](https://github.com/iadev09/orbitive) facade as `orbitive::rustls`;
direct package access remains available. The session-store
surface requires a Unix target. Server-session storage supports rustls 0.23
and 0.24; client-session storage requires rustls 0.24.

rustls 0.23 is the default:

```toml
orbitive = { version = "0.6.6", features = ["rustls"] }
```

Select rustls 0.24 explicitly when the consuming TLS stack uses that line:

```toml
orbitive = { version = "0.6.6", default-features = false, features = ["rustls_0_24"] }
```

The version features are additive. Enabling both is supported for workspaces
that contain consumers on both rustls lines; `OrbitSessionStorage` implements
each enabled `StoresServerSessions` trait.

`FleetServerSessions` creates domain-isolated implementations of
`rustls::server::StoresServerSessions`. rustls generates the keys, encodes the
values, and validates values read back from the store. This crate treats both
as opaque, sensitive bytes.

The store provides:

- bounded fixed-capacity storage;
- configurable TTL up to rustls' stateful ticket lifetime;
- isolation between caller-defined session domains;
- atomic single-use `take` across fleet processes;
- explicit reset and SHM unlink operations for owner-controlled lifecycle.

With `rustls_0_24`, `FleetClientSessions` creates an implementation of
`rustls::client::ClientSessionStore`. The client table is physically separate
from the server table. It uses rustls' `ClientSessionKey` security-context hash,
stores key-exchange hints and TLS 1.2 sessions, and keeps a bounded queue of
TLS 1.3 tickets per server. Taking a TLS 1.3 ticket is atomic across fleet
processes, so a ticket is returned at most once.

The client adapter accepts the same `CryptoProvider` used by the client
configuration. rustls owns the session encoding and validates decoded values;
an incompatible or evicted value is a cache miss and causes a full handshake.
The client key capacity is fixed to the largest identity rustls accepts, so a
server name is never truncated or replaced with a probabilistic digest.
rustls 0.23 does not expose a supported client-session persistence codec, so
this crate deliberately remains server-only for that dependency line.

The stored representation is private to rustls, not a TLS wire format. A
deployment preserving SHM across incompatible binary or rustls upgrades must
change its domain compatibility epoch or remove the old segment.

Each session store is a current-state table, not a ring. It has no cursor,
notification fd, or background task. A miss simply causes a full TLS
handshake.

The server table has no table-wide lock. An operation works on the one set
its key belongs to and does not wait for another operation using that set at
the same moment: the lookup is a miss, or the session is not stored, and the
handshake is a full one. Each open table holds one of a fixed number of owner
registrations, 256 by default and set at build time with
`ORBIT_RUSTLS_SESSION_OWNERS`; opening fails when all of them are held. A
registration is released by the kernel when its process ends, and what that
process left unfinished is erased by the next store that meets it. Open the
table in the process that uses it. A table inherited across `fork` is not
checked for on each operation: it stays consistent, but what the child leaves
unfinished is not erased while the parent lives.

## Upgrading to orbit-rustls 0.5.6

The server table has a new shared layout and therefore a new SHM kind:
`SERVER_SESSION_STATE_KIND` is 185, where 0.5.5 and earlier used 231. Read the
kind from that constant rather than writing the number: whatever names the
segment for validation, reset or removal must name the one this crate opens.
The earlier segment is neither opened nor removed by this version; remove it
once nothing built against the earlier layout is running.

Processes built against the two layouts use two tables. During a rolling
upgrade a session stored by one is not found by the other, which costs full
handshakes for that time and nothing else. The client table is unchanged.

This is not a browser, authentication, or general application-session store.
Those uses normally require reusable reads, durability, larger values, and
different eviction guarantees.

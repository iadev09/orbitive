# orbit-rustls

`orbit-rustls` provides same-host, fleet-shared runtime state for rustls over
Orbit shared memory. Applications normally use it through
`orbitive::rustls`; direct package access remains available. The session-store
surface requires a Unix target and supports rustls 0.23 and 0.24.

rustls 0.23 is the default:

```toml
orbitive = { version = "0.2.2", features = ["rustls"] }
```

Select rustls 0.24 explicitly when the consuming TLS stack uses that line:

```toml
orbitive = { version = "0.2.2", default-features = false, features = ["rustls_0_24"] }
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

The stored representation is private to rustls, not a TLS wire format. A
deployment preserving SHM across incompatible binary or rustls upgrades must
change its domain compatibility epoch or remove the old segment.

The session store is a current-state table, not a ring. It has no cursor,
notification fd, or background task. A miss simply causes a full TLS
handshake.

This is not a browser, authentication, or general application-session store.
Those uses normally require reusable reads, durability, larger values, and
different eviction guarantees.

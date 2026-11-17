# orbit-rustls - Architecture

`rustls` adapts rustls runtime state to Orbit's same-host shared-memory
substrate. It is a semantic integration crate: `orbit-core` owns fleet identity,
SHM mapping, locking, and naming, while rustls owns TLS protocol state and its
opaque encodings.

The current crate surface requires `std` and a Unix target. A future resolver
or credential format may choose a narrower runtime contract independently; it
must not weaken the session table's cross-process exclusion guarantees.

The dependency has two layers. `session/store.rs` is the version-neutral
primitive: it imports no rustls types and stores opaque key/value bytes without
interpreting them. `session/v23.rs` and `session/v24.rs` are thin typed
server adapters over that primitive. The 0.23 adapter receives byte keys
directly; the 0.24 adapter unwraps `ServerSessionKey` through `AsRef<[u8]>`.
`session/client_v24.rs` is the client adapter over a physically separate table.

rustls compatibility is selected by additive Cargo features. `rustls_0_23` is
the package default, while `rustls_0_24` opts into the typed 0.24 adapter. Both
may be enabled in one dependency graph: the same concrete
`OrbitSessionStorage` implements both versioned traits, so workspace
`--all-features` remains valid. These are real dependency and adapter features,
not no-op compatibility labels.

## Initial Surface

The first surface is a fleet-shared implementation of
`rustls::server::StoresServerSessions`:

- rustls generates opaque, uniformly random keys;
- rustls encodes and validates the highly sensitive values;
- this crate provides bounded storage, TTL, domain isolation, and atomic
  single-use `take()` across processes;
- the embedding runtime owns boot, reload, cleanup, and transport policy.

Session values live directly in one fixed-capacity current-state SHM table.
There is no ring, cursor, notification fd, or background runnable because a TLS
handshake performs synchronous keyed access.

## Exclusion

`store.rs` owns the slot: its layout, its three states, matching, and erasure.
What keeps two operations apart is a separate choice, `Exclusion`, and with
it the shared layout.

**`Exclusion::Set`, layout version 2, the server table (kind 185).**
`store/claim.rs`. An operation claims the one set its key hashes to with a
single compare-and-swap, uses the set's slots as their only user, and releases
the claim. Nothing waits and no system call is made: a set found claimed is a
miss or a refused store. Nothing of a slot, not even its hash, is read before
the claim. Two stores of one key meet in the same set, so they cannot both
install it; that and the claim around every `take` are what keep a ticket
single use.

A claim names its owner as a registration index and generation. Each open
handle registers once: it takes the first registration whose lock file
(`<segment>.lane<index>`, `orbit-core`'s lane hold) nobody holds, moves its
generation forward, and keeps that exclusive lock for its lifetime. The kernel
releases it when the process is gone. Fleet membership is retained beside it
for as long as the table is open.

A claim left by an owner that is gone is taken back in two places, both by
holding that owner's lock: a store that meets the claim, and the next handle
to take the same registration, which sweeps the sets at opening. The observed
claim word is exchanged for the taker's own while the lock is held, then the
whole set is erased. A taker that dies while erasing leaves a claim of its
own, recovered the same way, so recovery restarts rather than resumes and
never trusts a half-erased set. A read never asks the kernel: it misses until
a store has taken the set back.

Each operation checks that the handle still owns its registration: one load
of the registration's generation. It does not ask which process it is in. A
version that did (`getpid` on every operation, refusing an inherited handle in
a fork child) measured about 2.9 µs a connection on a 40-CPU Linux host, 40%
of what was left of the table, for a fact the embedder settles by opening the
table in the process that uses it. Without it an inherited handle still
operates exclusively, because parent and child claim under one identity, and
no claim is taken from a live process, because the lock's description is
shared and stays held until both are gone; the cost of the misuse is a set
that is not taken back while the parent lives. The process is still compared
when a handle is dropped, so a child dropping its copy does not unlock.

`reset` claims every set in turn and erases it, and refuses when a live owner
holds one. `unlink` removes the segment and its lock files as before.

**`Exclusion::Table`, layout version 1, the client table (kind 254).** A
process-local mutex plus the region's `flock`, taken for every operation. The
server table used this layout under kind 231 until version 2; this crate no
longer opens that kind.

Erasure is the same in both. A committed entry is erased over the extents it
wrote, with volatile stores; a slot whose write was never committed, or whose
lengths are implausible, is erased whole. Every byte outside a committed
entry's extents is therefore already clear.

## Client Sessions

rustls 0.24 exposes stable `encode` and `from_slice` operations for client
sessions and supplies `ClientSessionKey`, whose configuration hash isolates
different verifier and client-credential policies. `FleetClientSessions`
therefore implements `ClientSessionStore` for that version.

The client table uses kind 254 and is separate from the server table at kind
185. Its key capacity accepts the complete rustls server identity rather than
truncating or probabilistically hashing it. Record type and rustls'
configuration hash form the table domain. TLS 1.3 permits multiple independent
tickets per server, so insertion retains a bounded set and `take` consumes one
entry atomically across processes.

rustls 0.23 client session values contain private runtime references and expose
no supported persistence codec. The crate does not inspect private layout,
forge those references, or claim cross-process support for that line. Its
server adapter remains supported unchanged.

## Compatibility Boundary

The key/value byte format is opaque and belongs to rustls. It is not a TLS RFC
wire format and is not promised to remain compatible across rustls releases.
Domains must isolate server configurations with different authentication or
identity policy, and embedders preserving SHM across binary upgrades must use a
new domain epoch when rustls' stored representation or policy changes.

No rustls type is persisted by layout or reconstructed with an unsafe cast.
The SHM ABI contains only this crate's fixed-width header, slot metadata, and
byte arrays. A rustls upgrade can invalidate stored bytes but cannot silently
change the SHM layout through Rust type-layout drift.

## Not Application Sessions

This crate must not be used for browser, PHP, authentication, or application
sessions. rustls explicitly permits a session store to be lossy: a miss falls
back to a full TLS handshake. Application sessions normally require reusable
reads, explicit durability, larger values, and non-lossy eviction semantics.

## Future Scope

Future rustls-specific shared-memory integrations, such as encrypted credential
material or resolver snapshots, may live in this crate when their security and
versioning contracts are explicit. Generic ring and SHM mechanics remain in
`orbit-core`; embedding-runtime lifecycle and operator configuration remain
outside.

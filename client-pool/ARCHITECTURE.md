# orbit-client-pool — Architecture

`orbit-client-pool` owns reusable outbound clients for asynchronous
applications. It is protocol-neutral: HTTP, database, mail and application
protocols supply a `ClientManager`; this crate owns bounded admission,
connection lifetime and reuse.

## Boundaries

`ClientPool` is the process-local physical owner. A client object never lives
in shared memory and never moves to another process. Construction is lazy:
creating a pool performs no network IO. `acquire()` may create on demand, and
`run()` is the explicit runtime phase that maintains `min_idle`.

## Construction

The library API is code-first and Tokio-based. `PoolOptions` and
`FleetPoolOptions` declare
capacity and optional durations directly; they do not deserialize files and
carry no serde dependency. A Claviron application that owns operator config
maps its typed config into these options when registering its provider. This
keeps configuration reload and schema policy in the application instead of
making a protocol-neutral pool pretend to own them.

## Capacity

`max_live` bounds physically owned clients. `max_concurrency` independently
bounds admitted requests; a request creating a client counts until its lease
returns. `max_idle` bounds retained idle clients and `min_idle` is maintained
only while `run()` is active. `max_waiting` bounds acquisitions that have not
been admitted. `acquire_timeout` is an operator declaration and may be absent
for an indefinite wait; no internal fallback duration is invented.

Admission has four visible states: `Open`, `Paused`, `Draining`, and `Closed`.
An application policy may classify any domain outcome into a
`PoolDirective::Pause`; the triggering operation still returns its own domain
result and is never retried by this crate. Later acquisitions join the same
bounded wait queue and remain parked until explicit resume, caller
cancellation, pool shutdown, or the declared acquire timeout. The crate knows
nothing about HTTP statuses, headers, authentication, or response decoding.

Idle and maximum-lifetime retirement are deadline driven. The maintainer parks
until the next declared deadline, a pool state change, or cancellation. It
does not poll. A creation failure ends the maintainer and is returned to its
owner; restart/backoff policy belongs to that owner.

An acquire checks only the client it is about to reuse. Returned clients go to
the back of the idle queue, so the front is the oldest: the acquire retires
expired clients ahead of the one it takes and leaves the rest to the
maintainer. `live` and `stats()` therefore count an expired idle client until
an acquire or the maintainer reaches it, and an acquire costs one check rather
than one per idle client under the pool's mutex.

## Lifecycle

`close()` rejects new work and drops retained idle clients. Existing leases
remain owned by their callers. `drain()` transitions through `Draining` and
waits without an implicit deadline until those leases return. Cancellation
passed to `run()` drains the pool, preserving
`GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION`.

`acquire_cancellable()` adds caller-owned cancellation to admission, creation,
and recycle work. Closing or draining cancels those not-yet-active operations
so their reservations are returned.

An acquired lease is different: it represents an active request owned by the
caller. Every acquisition must therefore declare an `ActiveRequestPolicy`.
`Preserve` lets a transaction finish naturally. `CancelOnDrain(None)` signals
its lease token as soon as drain begins; `CancelOnDrain(Some(grace))` waits the
declared grace period first. Requests using the same pool may choose different
policies. The duration belongs to the request owner and is never invented by
the pool.

This signal is cooperative. The pool does not abort a foreign future or close a
protocol transaction itself, and drain continues waiting until the lease is
returned. A runtime may still call `cancel_active()` after admission has ended
to signal every remaining lease explicitly. Capacity waiters and unfinished
create/recycle operations are not active transactions: closing or draining
wakes and rejects them immediately regardless of active-request policy.

## Reuse

`pool::reservation::Pending` owns unfinished manager work across suspension
points. Its entry precedes its accounting guard in field order: cancellation
drops the physical client before releasing `live`/`in_flight` and notifying
waiters, including during unwinding. A creation also owns `creating`; warm
local creation does not own request admission. Success constructs the lease
before disarming the guard, or transfers the client to `finish_warm_create`.
Error branches do not perform duplicate manual cleanup. The warm interruption
branch drops its reservation before awaiting drain, so it cannot wait on its
own unfinished creation. No detached cleanup task or duration is involved.

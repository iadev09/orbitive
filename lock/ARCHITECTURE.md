# orbit-lock - Architecture

`lock` is a framework-neutral keyed lock primitive for a same-host Orbit
fleet. It owns lock mechanics only; authentication, permissions, retries,
blocking policy, language bindings, and framework compatibility belong above
it.

## State and notification

An active lock is current state, not retained history. The authoritative form
is therefore one fleet-shared open-addressed SHM table:

```text
LockKey -> LockOwner | acquired_at | expires_at | fence | state_revision
```

Every state-table mutation is serialized with a process-local mutex plus a
kernel-owned `flock`. If a process exits inside the critical section, the
kernel releases the lock. A slot is tombstoned before rewrite and becomes
occupied only after all bytes are complete.

Successful acquire, renew, and release transitions are also appended to a
dedicated per-node ring. Linux, FreeBSD, and macOS 14.4 or later expose that
ring through Orbit's process-local readiness bridge (`eventfd`, or a pipe on
macOS). The fd carries no lock identity or payload; a consumer drains it and
then advances its own ring cursor.

The transition ring is advisory. It wakes observers and can feed a local
view, but it never decides ownership. If history wraps, authoritative lookup
still reads the current-state table in expected O(1) time.

## Identity

`LockKey` is a readable byte namespace plus the resource label being mutually
excluded. `LockType::NAMESPACE` gives typed callers a stable domain such as
`runtime.tasks.pool`; dynamic callers may provide the same namespace bytes
directly. This avoids a second global numeric-kind registry unrelated to SHM
ring layout. `LockOwner` is an opaque compare token for one caller-chosen
tenure. None of these bytes is interpreted by Orbit.

Only `LockKey` determines contention. Combining key and owner as the table key
would permit two owners to hold the same resource and is therefore invalid.
Release and renewal compare both the requested key and the currently stored
owner in one atomic state-table operation. `force_release` is a deliberately
named administrative escape hatch for adapter contracts that require
ownerless removal; it is not used by the normal release path.

Lock keys and cache keys are distinct namespaces. The lock crate neither
depends on `cache` nor observes cache `Put`, `Delete`, or `Reset`.

## TTL and fencing

Every acquisition has a positive TTL. The table stores an absolute deadline
from the host monotonic clock. Expired entries are reclaimable by later table
operations. Expiry does not need a ring event; a caller implementing waiting
must also honor the observed deadline rather than depend solely on
notifications.

Each successful acquisition receives a strictly increasing fencing token.
Renew keeps the same token; a later owner receives a larger one. The token can
be carried to a protected external resource when stale-holder rejection is
needed. Every successful state change also increments `state_revision`, which
orders transition events from independent writer lanes.

## Layout

The default layout reserves kind `228` for current state and kind `229` for
the transition ring. Applications may supply another `LockLayout`, but all
fleet peers must agree.

The state table contains 256 slots. This bounds simultaneous resident lock
keys, not historical transitions. Namespace plus key plus owner may occupy at
most 960 bytes. The transition ring retains 1,024 frames per writer lane with
1,024 bytes per frame. A full table rejects a new distinct live key; it never
evicts one.

Every operation is one immediate attempt. This crate has no sleep, retry,
blocking, callback, or timeout loop. Higher layers may combine `try_acquire`,
authoritative lookup, the observed expiry deadline, and transition readiness
according to their own policy.

## Lifecycle

`Lock::new` subscribes after current transition heads. `Lock::replay_retained`
is for diagnostics and tests. `Lock::reset_transport` clears state and ring
only during owner-controlled quiescent boot. Runtime cache clearing has no
relationship to this operation.

## Invariants

- One active entry at most exists for a `LockKey`.
- Owner is an opaque compare token, not an authorization mechanism.
- Only an exact current owner can renew or release a live entry.
- Normal release and renewal never act without an owner or exact lease.
- Ring overwrite cannot release, acquire, or hide authoritative current state.
- Event publication precedes the table commit while the state lock is held;
  readers re-check the table, so a false edge after a publisher crash is safe.
- There is one state object and one transition ring per layout, never one SHM
  object per key.

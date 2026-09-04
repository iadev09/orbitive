# orbit-lock

`orbit-lock` provides bounded keyed leases for processes in the same Orbit
fleet. Applications normally use it through `orbitive::lock`; direct package
access remains available.

Each active key stores its current owner, deadline, fencing token, and state
revision in a shared current-state table:

```text
LockKey -> LockOwner + deadline + fencing token + revision
```

Acquisition, owner-matched renewal, and owner-matched release are atomic across
sibling processes. Every method performs one immediate attempt. Waiting,
retry, backoff, and authorization belong to the caller.

A separate bounded ring records successful transitions and provides readiness
on Linux and FreeBSD, but that history is advisory: the current-state table
alone decides ownership.

The table never evicts a live lock to admit another key. Expired entries may be
reclaimed, and a full table rejects a new distinct lock. Callers protecting an
external resource can use the monotonically increasing fencing token to reject
stale holders.

Lock keys and cache keys are separate namespaces. Cache reset or eviction has
no effect on lock ownership.

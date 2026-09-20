# orbit-pool

Who has a free one, and may I have it: fleet-wide leases over Orbit shared
memory.

One worker holds a warm connection and another needs it. The second asks
the pool, which knows every connection registered under that key across
the fleet, how loaded each is and who owns it; it reserves one unit of the
chosen connection's capacity with a single compare-and-swap and gets a
lease. It brings the lease to the owner, the owner accepts it, does the
work, and gives the unit back when the work is over. Nothing moves between
processes but numbers: the connection stays where it is. Applications
normally use it through `orbitive::pool`.

Bringing the lease to its owner is the one step every consumer shares and
every consumer can get wrong, so the optional `pool-stream` feature owns it:
`open_session` reaches the owner over an `orbit-stream` stream you supply,
`accept_session` reads the lease and accepts it exactly once before a byte
of your payload is trusted, and what crosses afterwards is yours alone.

Without `pool-stream`, this crate still owns the complete fleet-wide pool
ledger: resource census, capacity, leases, creation claims, generations and
waiting. The feature adds only the reusable ceremony that binds a remote
lease to a paired byte exchange; it does not change pool policy or make
streaming mandatory.

```rust
use std::sync::Arc;
use std::time::Duration;

use orbitive::pool::{Incarnation, Key, Limits, LocalFirst, Plan, Pool};
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
// One value per process life, from whoever supervises processes.
let pool = Pool::new(fleet, Incarnation::new(1))?;

// The caller's digest of "what this is usable for": an origin plus
// everything that forbids reuse across contexts.
let key = Key::new(0x1234_5678);
let limits = Limits { max_live: 4, attempts: 4 };

match pool.acquire(key, &limits, &LocalFirst)? {
    Plan::LocalReuse(lease) => {
        // Ours: accept, use the thing, complete. No stream, no hop.
        let execution = pool.accept(lease)?;
        execution.complete();
    }
    Plan::RemoteReuse(lease) => {
        // Theirs: send `lease` to the owner (over an orbit-stream, say);
        // the owner accepts and completes.
    }
    Plan::Create(permit) => {
        // Budget claimed: make the resource, then register it.
        let id = pool.register(key, 1)?;      // capacity 1: exclusive
        permit.finish();
        let _ = id;
    }
    Plan::Wait(version) => {
        // Nothing usable yet; park until the key changes, then ask again.
        pool.wait_capacity(key, version)?;
    }
    Plan::Reject(reason) => { let _ = reason; }
}

# Ok::<(), Box<dyn std::error::Error>>(())
```

## What the pool is

A **resource** is something one process owns and others may use through
it. Its owner registers it under a **key** with a **capacity**: one for an
exclusive thing such as an HTTP/1 connection, more for one that admits
several users at once such as HTTP/2 streams. The table then shows it to
the fleet as a **candidate**: owner, capacity, how many units are reserved
and how many are being executed, whether it is live or draining.

A **lease** is one unit of that capacity, reserved by any process with one
compare-and-swap; a snapshot that showed room is not a lease. The lease is
plain data: the resource's address, a fence that grows with every
reservation on that resource, and who holds it. The owner **accepts** it,
which takes that fence out of the resource's pending reservations exactly
once and turns the unit into an active one; the returned `Execution` guard
gives the unit back when dropped or completed. A lease accepted twice, or
after the owner aged it out, is refused.

That guard is the only way accepted capacity returns. A caller that gives up,
times out or dies after delivery frees nothing, because only the owner knows
when the resource is idle again; a reservation whose delivery is uncertain is
aged out by the owner's `reconcile`, with a grace it chooses, and never by a
caller's clock. The session helper has one narrower guarantee: when it proves
that the exchange offer was never published, it cancels that exact pending
fence immediately. This is transport rollback before ownership transfer, not
request cancellation after delivery.

A **creation claim** keeps a fleet from overshooting: `claim_create`
counts live resources and claims in progress together against the
caller's `max_live`, so forty workers that all see an empty key do not
each open a connection. Dropping the permit returns the claim.

A consumer that needs one fleet-wide idle policy opens a distinct
`PoolSpec::with_fleet_availability()` kind. That contract adds an atomic free
unit count per key: `claim_warm` claims only the shared `min_idle` deficit,
and `Execution::complete_idle(max_idle)` either retains the unit within the
shared ceiling or tells the owner to retire the resource. The default spec
does not pay for or reinterpret this policy.

`max_live`, `min_idle`, and `max_idle` are caller policy inputs, not a policy
document stored in the pool. The shared table enforces each atomic claim, but
all nodes using one key must currently be configured with the same values.
A future fleet-managed policy can make that configuration canonical without
changing the pool's ownership counters.

`acquire` runs the loop for one request: snapshot, ask the **policy**,
reserve or claim what it chose, retry a lost race with a fresh snapshot,
and hand back a committed `Plan`: `LocalReuse`, `RemoteReuse`, `Create`,
`Wait` or `Reject`. `LocalFirst` and `LocalOnly` are the policies shipped;
a `Policy` is a trait, and no cost figure from any benchmark is built in.
Whether reusing a remote resource beats creating a local one is a
measurement the embedder makes and expresses in its own policy.

## What it is not

The pool decides and counts. It does not execute anything, and it carries
no bytes: the input and output of a remote use travel over an
`orbit-stream` stream that the caller opens to the owner, with the lease
in its first bytes or in any setup message the application prefers. A
local use goes nowhere: no stream, no shared-memory hop. HTTP, health,
load-balancing weights and retry rules live above it.

The optional `pool-stream` feature is experimental. It is an adapter for the
case where a remotely owned resource must execute over a paired exchange;
neither the pool nor the stream transport depends on that composition.

## Waiting and death

`wait_capacity(key, version)` parks a thread until the key changes: a
completion, an unregister, a closed resource, a returned claim.
`poll_capacity` does the same for a task through one driver thread per
process, as `orbit-stream` does. Nothing here knows whether a process is
alive: the embedder reports a confirmed death with `node_dead(node,
incarnation)`, and every resource that life of the process owned is
closed and its creation claims returned; leases it held on other owners'
resources are those owners' to complete, which they do when the stream
that carried them ends.

Geometry is compile-time: `ORBIT_POOL_KEY_CAPACITY` (keys per epoch,
default 256) and `ORBIT_POOL_RESOURCE_LANE_CAPACITY` (resources per node,
default 256). `reset_all` empties the table during quiescent boot and
starts a new epoch.

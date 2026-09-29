# orbit-client-pool

`orbit-client-pool` is the outbound-client pooling crate in
[Orbitive](https://github.com/iadev09/orbitive). It is published as a separate
crate and can be used directly. HTTP, database, mail and application protocols
provide a `ClientManager`; the pool owns bounded admission, physical client
lifetime, reuse and cooperative drain.

The crate is Tokio-based. Constructing a pool performs no network IO. Clients
are created lazily by `acquire()`, while the explicit `run()` lifecycle keeps a
declared warm floor and drains on cancellation.

The process-local pool needs no shared-memory runtime. The optional `fleet`
feature uses Orbitive's `orbit-pool` and `orbit-stream` primitives to compose
that local pool across a process fleet. Physical client objects remain inside
their owner process. Fleet peers share concurrency and resource admission;
when policy selects a remote owner, request bytes travel through a paired
exchange instead of moving the client object through shared memory.

```toml
[dependencies]
orbitive = { version = "0.5.2", features = ["client-pool"] }
```

```rust
use orbitive::client_pool::{ClientManager, ClientPool, PoolOptions};
```

Enable the fleet composition only when the application supplies compatible
Orbit pool and exchange surfaces:

```toml
[dependencies]
orbitive = { version = "0.5.2", features = ["client-pool-fleet"] }
```

The separately published `orbit-client-pool` package remains available for
consumers that intentionally want the component crate instead of the Orbitive
facade.

## Capacity options

The pool is code-first. `PoolOptions` declares the complete operating envelope
for one process-local pool:

```rust
let options = PoolOptions::new(
    min_idle,
    max_idle,
    max_live,
    max_concurrency,
    max_waiting,
)
.acquire_timeout(acquire_timeout)
.idle_timeout(idle_timeout)
.max_lifetime(max_lifetime);
```

| Option | Meaning |
| --- | --- |
| `min_idle` | Idle floor maintained by `run()` while the pool is open. |
| `max_idle` | Maximum number of returned clients retained for reuse. |
| `max_live` | Maximum number of physical clients, including clients being created. |
| `max_concurrency` | Maximum number of requests admitted at once. This is independent of physical client count. |
| `max_waiting` | Maximum number of acquisitions waiting for admission or capacity. |
| `acquire_timeout` | Optional caller wait bound. `None` means no pool-owned deadline. |
| `idle_timeout` | Optional retirement age for an unused client. |
| `max_lifetime` | Optional total lifetime for a physical client. |

Capacity must satisfy `min_idle <= max_idle <= max_live`; `max_live` and
`max_concurrency` must be non-zero. Optional durations must be non-zero when
present. The pool supplies no implicit duration.

### Fleet scope

`FleetPoolOptions` declares the same policy for one fleet profile and adds
`attempts`, the bounded budget for retrying a lost shared-selection race:

```rust
let options = FleetPoolOptions::new(
    min_idle,
    max_idle,
    max_live,
    max_concurrency,
    max_waiting,
    attempts,
)
.acquire_timeout(acquire_timeout)
.idle_timeout(idle_timeout)
.max_lifetime(max_lifetime);
```

For a shared pool key, `min_idle`, `max_idle`, `max_live`, and
`max_concurrency` are fleet-wide values. They are not multiplied by the worker
count: all members maintain one idle floor and ceiling, share one physical
client budget, and consume one request-admission budget. A physical client
still lives only in its owner process; a peer selected by fleet policy reaches
that owner through an exchange.

`max_waiting` bounds each caller process's local waiters rather than creating a
second shared queue. `acquire_timeout` belongs to each acquisition, while
`idle_timeout` and `max_lifetime` are enforced by the process that owns the
physical client. These values apply the same declared policy at every member,
but they are not additional fleet counters.

## Protocol policy

Protocol-specific admission is extensible without making the pool depend on a
protocol. A protocol adapter implements `ClientPolicy<O>` and maps any outcome
it understands—such as an HTTP status or rate-limit header, an authentication
failure, a database error, or an application payload—to one of two admission
directives:

- `PoolDirective::Continue` leaves admission unchanged.
- `PoolDirective::Pause { reason }` pauses later acquisitions until the owner
  explicitly calls `resume()`.

This keeps the pool generic while allowing each adapter to enforce its real
protocol and application rules without changes to the pool itself.

The directive cannot rewrite, retry, delay or cancel the operation that
produced the outcome. That operation keeps its protocol result. Applying a
directive through `ClientLease::observe()` changes only the pool's admission
gate; retained idle clients and requests that already hold a lease remain
owned by their callers.

A paused acquisition remains in the bounded waiting queue until the adapter
resumes the pool, the caller cancels it, the pool begins shutdown, or a
configured acquire timeout expires. The pool never invents a retry delay or a
resume deadline.

With the `fleet` feature, concurrency and resource admission are fleet-wide,
but protocol policy is currently process-local. `apply_local()` and
`resume_local()` deliberately do not claim to pause sibling processes; a
fleet-wide policy gate requires its own managed shared resource.

## Cancellation and drain

Waiting for admission and executing with an acquired client are separate
lifecycle states:

- `close()` or `drain()` stops new admission, wakes queued acquisitions and
  retires idle clients.
- A request that already holds a `ClientLease` remains caller-owned. The pool
  does not abort its future or close its protocol connection implicitly.
- Every acquisition declares an `ActiveRequestPolicy`: `Preserve` lets the
  request finish naturally, `CancelOnDrain(None)` signals it when drain starts,
  and `CancelOnDrain(Some(grace))` signals it after that explicitly declared
  grace period.
- The protocol adapter must observe `ClientLease::cancellation_token()` and
  decide how to stop its own operation safely. Signalling is cooperative; it is
  not an implicit transport abort.

`drain()` waits until all active leases have returned. `cancel_active()` is a
separate, explicit escalation available only after admission has stopped; a
normal graceful drain does not indiscriminately cancel preserved requests.

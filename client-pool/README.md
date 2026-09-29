# orbit-client-pool

`orbit-client-pool` owns reusable outbound clients for asynchronous Rust
applications. It is protocol-neutral: HTTP, database, mail and application
protocols provide a `ClientManager`; the pool owns bounded admission, physical
client lifetime, reuse and cooperative drain.

The crate is Tokio-based. Constructing a pool performs no network IO. Clients
are created lazily by `acquire()`, while the explicit `run()` lifecycle keeps a
declared warm floor and drains on cancellation.

The optional `fleet` feature composes the local pool with `orbit-pool` and
`orbit-stream`. Physical client objects remain inside their owner process.
Fleet peers share concurrency and resource admission; when policy selects a
remote owner, request bytes travel through a paired exchange instead of moving
the client object through shared memory.

```toml
[dependencies]
orbit-client-pool = "0.5.0"
```

Enable `fleet` only when the application supplies compatible Orbit pool and
exchange surfaces:

```toml
[dependencies]
orbit-client-pool = { version = "0.5.0", features = ["fleet"] }
```

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

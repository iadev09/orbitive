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

The pool does not interpret HTTP statuses, rate-limit headers, authentication,
database errors or application payloads. Protocol adapters classify their own
outcomes and decide when a paused profile may resume.

# Orbit exchange protocol

`orbit-stream::exchange` is a bounded, same-host transport between two process
lifetimes. It defines transport state and ownership, not HTTP, FastCGI, RPC, or
task semantics.

## Physical resources

Every `Exchanges` table has one duplex control table plus either:

- one payload arena shared by both directions; or
- two directional payload arenas with independent geometry and credit.

`ExchangeSpec::new(control, payload)` selects the shared layout.
`ExchangeSpec::directional(control, a_to_b, b_to_a)` selects the directional
layout. A protocol chooses the latter when the directions need different slot
sizes, capacities, or isolation. The choice changes physical SHM geometry but
does not change the endpoint API.

An arena is fleet-wide. Each node allocates only from its own lane. A chunk is
one immutable publication spanning one or more contiguous slots in that lane;
its descriptor carries the arena kind, owner node, first slot, slot count,
allocation generation, and exact byte length. Returning a received chunk gives
the whole extent back and wakes only its owning producer node.

Slot payload size and lane width are physical geometry. Chunk width is runtime
policy: for a selected slot count `n`, full publication capacity is exactly
`n * slot_payload_bytes`. `Sender::chunk_geometry()` exposes the geometry of
that sender's actual direction, so request and response policy remain
independent when their arenas differ. The helper rejects zero and slot counts
outside the local lane; an adapter may choose `n` per invocation without
changing the SHM contract.

## Endpoint and direction

`Exchanges::create()` creates side A and an `ExchangeTicket`.
`Exchanges::open_peer(ticket)` opens side B.

Both sides expose the same capabilities:

```text
side A: Sender(A -> B), Receiver(B -> A)
side B: Sender(B -> A), Receiver(A -> B)
```

Sender and receiver are per-exchange capabilities, not permanent process or
worker roles. One worker may simultaneously hold side A of one exchange, side
B of another, and many senders and receivers from unrelated exchanges.

Applications may name A-to-B "request" and B-to-A "response", but those names
are not part of the transport ABI. Another application may assign the opposite
meaning or use both directions symmetrically.

## Direction state machine

Each direction independently follows:

```text
New -> START -> DATA* -> FIN
                    \-> RESET
```

- `START` occurs exactly once and may carry optional metadata.
- `DATA` carries one payload descriptor per application chunk.
- `FIN` is clean completion for that direction.
- `RESET(code)` is terminal failure for that direction.

The two state machines are independent. Either side may send `START` first; one
direction may produce data or finish while the other remains open. This is what
permits early response, uploads concurrent with responses, and fully duplex
protocols without teaching Orbit their sequencing rules.

Control publication is the visibility boundary. A producer reserves and fills
payload slots privately, then `commit()` publishes exactly one complete
descriptor. Dropping an uncommitted reservation publishes nothing and returns
its credit. If control publication fails, the payload reservation is returned
and the whole chunk may be retried.

## Backpressure

Payload credit is acquired before reading or producing the next external
chunk. When the local node lane has no suitable extent, `poll_ready` returns
`Pending` and blocking readiness parks; neither path polls. Readiness tokens may
coalesce, so a wake means "inspect current credit/state", not "one chunk became
available".

An adapter must not read unbounded data from its origin and queue it outside
the arena. The intended order is:

```text
reserve SHM extent -> read/encode into it -> commit descriptor
```

This makes the configured arena credit the application backpressure boundary.
Kernel, TLS, and protocol-library buffers still exist and must be included in
end-to-end buffering claims.

## Pool session binding

With the optional `orbit-pool` integration, a remote resource use is bound to
an exchange as follows:

```text
caller: Pool::acquire
  LocalReuse -> accept locally and use the object directly
  RemoteReuse -> open_exchange_session(lease, exchanges, metadata)

owner: poll_accept_exchange_session
  open ticket -> decode lease -> validate exact resource generation and fence
  -> return Execution + side B + application metadata
```

The lease frame precedes application metadata inside side A's `START` payload.
The owner accepts it before later application bytes are exposed. Dropping the
returned `Execution` releases resource capacity. A caller cannot release an
unaccepted reservation because it cannot prove the owner stopped using the
resource; the owner ages abandoned reservations through `Pool::reconcile`.

The pool and stream generations remain deliberately distinct:

- resource generation identifies one physical resource lifetime;
- exchange/stream generation identifies one transient session lifetime;
- payload allocation generation identifies one slot-extent publication.

No replacement process, new master, or new exchange implies that an older
resource or SHM table is quiescent.

## Adapter obligations

A protocol adapter owns:

- the application metadata codec;
- the meaning of A-to-B and B-to-A;
- conversion between protocol frames and payload chunks;
- mapping a validated `ResourceId` to the actual local object;
- cancellation, timeout, and clean/dirty reuse policy for that object;
- the owner's `reconcile` cadence for abandoned reservations.

It does not implement SHM allocation, stream generations, credit wakeups,
leases, or fencing. HTTP and FastCGI adapters may share orchestration helpers,
but neither protocol is encoded into `orbit-stream` or `orbit-pool`.

## Lifecycle rules

`node_dead(node, incarnation)` is valid only after that exact process lifetime
is confirmed dead. `reset_all` is quiescent-owner maintenance. Attaching a new
master or worker is not proof of quiescence. Unlink removes a name, not existing
mappings, and is not an ordinary shutdown operation.

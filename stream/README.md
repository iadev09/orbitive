# orbit-stream

Bounded process-to-process flows in shared memory.

The normative paired-exchange state, ownership, pool-binding and backpressure
contract is documented in [PROTOCOL.md](PROTOCOL.md).

The crate has two layers. `Streams` is the original private duplex byte ring:
two endpoints, ordered bytes, independent directions and no message
boundaries. `exchange` is the typed invocation layer: one paired exchange,
one lossless control channel, and either one fleet-wide payload arena or two
directional arenas. Applications normally use both through
`orbitive::stream`.

An exchange is not an HTTP implementation and is not specific to upstream
connections. It is the common shape for two runtimes handing each other an
invocation while preserving bounded memory and backpressure. Side A creates
the exchange and side B opens its ticket. Both receive the same endpoint
shape: an outbound `Sender` and an inbound `Receiver`. A worker may hold either
side of many exchanges at once; producer and consumer are capabilities, never
fixed worker roles.

## Paired exchanges

`Exchanges` opens either two or three resources under distinct SHM kinds:

- a control `Streams` table carrying fixed-size `Start`, `Data`, `Fin` and
  `Reset` events;
- one shared payload arena whose per-node lanes serve both directions; or
- separate A-to-B and B-to-A payload arenas when an adapter needs independent
  memory and credit budgets for the two flows.

Payload slots are fixed physical memory units. A chunk is a publication
decision, not a slot: one `ChunkDescriptor` identifies an exchange, flow,
chunk sequence, allocation generation, first slot, slot count and actual
payload length. A 777-byte chunk in a 256-byte-slot arena therefore occupies
four slots and produces one descriptor. A later chunk gets another extent
and another descriptor; chunks are never pointer chains.

Adapters do not repeat slot arithmetic. `Sender::chunk_geometry()` reports the
actual arena used by that direction, including its payload bytes per slot and
its per-node lane width. Runtime policy may choose a different slot count `n`
for every request and response; `ChunkGeometry::chunk_bytes(n)` yields exactly
`n * slot_payload_bytes` and rejects zero or a count outside the lane.
`chunk_bytes_for_target` converts a runtime byte target into that same aligned,
lane-bounded capacity. This leaves future network-aware policy above the
transport while keeping physical SHM geometry compile-time stable.

For known payload lengths, `ChunkGeometry::plan(data_bytes, n)` reports chunk
count, full-chunk bytes/slots and the exact final chunk. The existing
`ChunkPlan::new`, `PayloadArenaSpec::plan_chunks` and `Sender::plan_chunks`
remain benchmark-backed 64 KiB conveniences; `plan_chunks_with` is the
runtime-slot-count form. A final short chunk keeps its exact byte length.

`start(Some(bytes))` and `data(bytes)` copy existing bytes into an extent.
When the application is producing or encoding the bytes itself,
`reserve_start(len)` and `reserve_data(len)` return the writable extent
directly. Filling that guard is still private producer work: `commit()` makes
the complete extent immutable and publishes exactly one descriptor. Dropping
an uncommitted guard publishes no event and returns all of its slots. If the
control ring cannot accept the descriptor, the failed commit returns those
slots too, so readiness can be awaited and the whole chunk retried.

Payload arenas are fleet-wide rather than one SHM object per exchange. Every
sender allocates from its worker node's exclusive lane; a descriptor carries
the arena kind, owner node and extent coordinates, so the peer finds the same
bytes without request/response-specific addressing. The shared layout is the
compact generic default. A directional layout is for adapters such as an HTTP
or FastCGI relay where an upload must not spend response credit and the two
directions deliberately use different slot and chunk budgets. Dropping a
received `PayloadChunk` returns its complete extent. A sender whose lane is
full parks on its producer node's credit generation or returns `Pending`; it
does not poll. Returning a chunk wakes only the node that owns that extent,
not every producer sharing the arena.
Notifications are coalesced: publication does not require an unconditional
kernel wake per payload slot or per chunk.

Both directions enforce `Start -> Data* -> (Fin | Reset)`, but their state
machines are independent. Either endpoint may start sending first. The
transport enforces ordering, ownership, credit and terminal rules only; an
adapter decides whether a direction means request, response, invocation input
or something else.

`orbit-pool`'s `open_exchange_session` writes the lease and application
metadata directly into side A's reserved start extent.
`accept_exchange_session` validates that lease before exposing later data.
This connects a fleet-owned resource to the exchange without teaching the
transport what that resource is.

`node_dead` is explicit and incarnation-scoped. Merely attaching a
replacement does not reset either arena. Once a process is confirmed dead,
its unpublished or unread sender allocations and its held receiver chunks
return to the arena; chunks already held by a surviving receiver remain that
receiver's. `reset_all` remains quiescent-owner maintenance, never an
implicit boot action.

The matched benchmark is:

```console
cargo bench -p orbit-stream --features tokio --bench transport
```

It compares the paired SHM exchange with a Unix socket under the same body,
concurrency and application chunk decisions. Slot size and chunk size are
reported separately in benchmark IDs.

## Transport scope and measured cost

`orbit-stream` is a stable transport primitive. It guarantees bounded memory,
duplex progress, backpressure, terminal signaling and explicit owner-death
recovery. It deliberately does not decide whether relaying bytes through
another process is preferable to opening a socket directly.

That choice belongs to the adapter and must be measured at the same boundary
as its real workload. The included benchmarks report slot size, application
chunk size, body size and concurrency separately. A zero-copy receive result
does not include application parsing or copying, and a same-process benchmark
does not by itself prove a production routing benefit.

The optional `orbit-pool` session binding is experimental. It is useful only
when an application has already decided to execute against a remotely owned
resource; it is not part of the stream transport contract.


```rust
use std::sync::Arc;

use orbitive::stream::{Incarnation, Streams, Ticket};
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
// The incarnation is whatever tells this life of the process apart from
// the next one under the same node id: a start stamp, a supervisor's
// generation. Orbit does not mint it, and it is one value per process
// life, never one per request. `1` here is a stand-in.
let streams = Streams::new(fleet, Incarnation::new(1))?;

// One end creates the stream and hands the ticket to the other end,
// through an event, an invocation, a cache entry: any channel it likes.
let (mut a, ticket) = streams.create()?;
let ticket_text = ticket.to_string();

// The other end. This example opens it from the same handle in the same
// process to stay short; in another process, B joins the same fleet,
// makes its own `Streams`, and opens the ticket it was handed. Nothing of
// A's, no `Arc`, no handle, crosses over: only the ticket's text.
let b = streams.open(ticket_text.parse::<Ticket>()?)?;

// Write then read on one thread works because five bytes fit the ring.
// A body larger than the ring needs the reader running at the same time:
// the writer parks when the ring is full and only the reader frees it.
a.blocking_write_all(b"hello")?;
a.finish()?;                                  // FIN: nothing more this way
assert_eq!(b.blocking_read_chunk(16)?.as_ref(), b"hello");
assert!(b.blocking_read_chunk(16)?.is_empty()); // clean end of stream

# Ok::<(), Box<dyn std::error::Error>>(())
```

The two-process shape, with the peer on Tokio:

```rust,ignore
// Process A (node 0): create, hand the ticket over, stream.
let (a, ticket) = streams.create()?;
streams.offer(ticket, NodeId::new(1))?;      // or publish `ticket.to_string()` anywhere
let (mut a_read, mut a_write) = a.split();
tokio::spawn(async move { a_write.write_all(&body).await?; a_write.shutdown().await });
a_read.read_to_end(&mut reply).await?;       // the reader runs while the writer waits

// Process B (node 1): its own fleet handle, its own `Streams`.
let streams = Streams::new(fleet, supervisor_incarnation)?;
let ticket = streams.blocking_take_offer()?;  // or parse the text it was sent
let b = streams.open(ticket)?;
```

## What a stream is

A stream has two ends and two directions. Side A is whoever called
`create`; side B is whoever opens the ticket. Each direction is a bounded
ring of `STREAM_BUFFER_BYTES` (default 64 KiB) that one side writes and the
other reads, in order, without loss: a full ring makes the writer wait, an
empty one makes the reader wait. The two directions are independent, so a
request body can still be going one way while the response starts the
other.

A read returns what is there, up to what was asked: `blocking_read_chunk(16)`
means at most sixteen bytes, not a frame, and a five-byte write may well
arrive in two reads once both ends are running at full speed. The bytes
and their order are the contract; message boundaries belong to the
protocol above, which accumulates or reads exact lengths. A writer that is
done calls `finish`; the reader drains what is buffered and then sees a
clean end (`Ok(0)`, or an empty chunk). A writer that gives
up calls `reset`, or is dropped without finishing, and the reader gets an
error instead. A reader that is dropped tells the writer, whose next write
fails. Each side is held once: an `Endpoint` can be split into a `ReadHalf`
and a `WriteHalf` for two tasks, and the side is released when both are
gone.

What travels between processes is the address. A `StreamId` is a `NetId64`
whose kind is the stream segment's, whose node is the creator's lane and
whose counter is the slot and the slot's generation; a `Ticket` is that
address plus the side plus the table's epoch, and prints as
`<address>/b/<epoch>`. Two things keep a ticket from reaching the wrong
memory: the generation separates reuses of a slot, and the epoch, which
every quiescent `reset_all` advances, separates before and after a reset.
Either mismatch answers `Error::Stale`. A slot's generation never wraps: one
that runs out sits idle until the next epoch. An address is not a
permission: whoever hands out a ticket decides who may have it.

The ticket can travel any way the application likes. For the common case
where the creator knows which node should take the other end, `offer`
puts the stream in that node's offer list inside the segment and rings it;
`take_offer`, `blocking_take_offer` and `poll_take_offer` on that node hand
the ticket out once. Taking an offer is discovery only; opening it is the
claim, and each side is claimed exactly once.

## Waiting

The blocking calls (`blocking_read`, `blocking_write`, `blocking_write_all`,
`blocking_read_chunk`, `wait_readable`, `wait_writable`) park the thread on
the direction's own change word through the platform's shared address wait
(Linux futex, FreeBSD umtx, macOS `os_sync_wait_on_address` from 14.4;
earlier releases poll). A writer pays one extra load to see whether anyone
is parked, and a wake only when someone is. They are for a thread that is
free to sleep: a dedicated PHP thread, a synchronous Rust consumer. Inside
a Tokio task use the halves' `AsyncRead` / `AsyncWrite` instead; the
difference is that a poll with nothing to do registers a wake and returns
`Pending` rather than parking a runtime thread.

With the `tokio` feature, `ReadHalf` implements `AsyncRead`, `WriteHalf`
implements `AsyncWrite`, and `Endpoint` implements both. A poll that finds
nothing registers its waker and returns `Pending`; nothing blocks a runtime
thread. In a fleet, one thread per process waits on that process's doorbell
in the segment and wakes the tasks whose streams have news, so the cost is
one thread and no descriptor however many streams are open. Standalone
there is no thread at all: the other end wakes the task directly.
`tokio_util::io::ReaderStream` turns a `ReadHalf` into a stream of `Bytes`
chunks; write boundaries are not preserved, only the bytes and their order.

## Geometry and lifetime

Geometry is compile-time, like the other Orbit tables:
`ORBIT_STREAM_LANE_CAPACITY` (streams each fleet node can hold open at
once, a power of two, default 256) and `ORBIT_STREAM_BUFFER_BYTES` (a power
of two, default 65 536) in the application's `.cargo/config.toml`. Every
process that opens the fleet must be built with the same values; the
segment refuses a peer built differently. Each node allocates in its own
lane, so creating a stream takes no cross-process lock.

The table is current state, not history. The fleet owner clears it with
`reset_all` during quiescent boot, and a stream ends when both sides have
released it. Nothing in this crate knows whether a process is alive. The
embedding runtime, which does know, reports a confirmed death with
`node_dead(node, incarnation)`: every side that life of the process held,
in any lane, is finished, so a peer parked on it wakes with an error and
nobody can claim it; the peer keeps the slot until it drops its own
handles, and only then is the slot reused. A report about another
incarnation of the same node touches nothing, and a timeout is not a
death. Shared memory is backed as it is touched: creating the segment
writes the header only, so a fleet pays for the buffers its streams use,
not for the geometry; measure it rather than assume it.

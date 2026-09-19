# orbit-stream

A byte stream between two processes, in shared memory.

One worker accepts a request and another holds the connection it needs. The
first writes the request body into a stream, the second reads it as it
arrives and writes the response back the same way, and neither process
copies more than the bytes themselves: no socket, no descriptor passing, no
frame protocol between them. When the reader is slow the writer waits; when
the writer is done the reader drains to a clean end. That is what
`orbit-stream` is for, and it works the same when both ends are in one
process. Applications normally use it through `orbitive::stream`.

## Experimental — the decision, not the code

The implementation is finished, and saying "experimental" about it would be
the wrong warning. Every test passes on Linux, FreeBSD and macOS; the lost
wakeup this crate was written around is found, fixed, and pinned by a
regression that fails by timeout without the fix; the transport moves bytes
at about 0.25 µs/KiB, which is a socket's order of magnitude.

What is experimental is **whether a byte relay between processes belongs in
your problem at all**. The cost model is narrow, and it is measured rather
than guessed — figures below from a 40-core bare-metal Xeon, `BENCHMARKS.local.md`:

| | |
|---|---|
| transport, blocking | ~0.25 µs/KiB, 4–8 GB/s, flat across chunk sizes |
| transport, through the async adapters | three to five times that — each drain wakes a driver, then a task, then a worker |
| one borrow: reserve, session, a round trip | ~50 µs, of which roughly three quarters is the rendezvous and one quarter the bytes |
| against dialling your own connection | a borrow pays only where a dial is expensive: about fifty requests' worth against a TLS handshake, and never against a Unix socket, where a dial costs 1.4 µs and a relayed request 50 |

Read that as one sentence: **a socket is the right answer far more often than
it looks**, and the cases left over are cold starts, bursts, and origins whose
connection budget is genuinely scarce.

The session API is expected to move as well. A release does not yet carry a
verdict — the absence of one should mean *dirty* and today means nothing —
and there is no error for an owner that has gone away.


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

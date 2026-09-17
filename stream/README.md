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

```rust
use std::sync::Arc;

use orbitive::stream::{Incarnation, Streams, Ticket};
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
// The incarnation is whatever tells this life of the process apart from
// the next one under the same node id: a start stamp, a supervisor's
// generation. Orbit does not mint it.
let streams = Streams::new(fleet, Incarnation::new(1))?;

// One end creates the stream and hands the ticket to the other end,
// through an event, an invocation, a cache entry: any channel it likes.
let (mut a, ticket) = streams.create()?;
let ticket_text = ticket.to_string();

// The other end, in any process of the fleet:
let b = streams.open(ticket_text.parse::<Ticket>()?)?;

a.blocking_write_all(b"hello")?;
a.finish()?;                                  // FIN: nothing more this way
assert_eq!(b.blocking_read_chunk(16)?.as_ref(), b"hello");
assert!(b.blocking_read_chunk(16)?.is_empty()); // clean end of stream

# Ok::<(), Box<dyn std::error::Error>>(())
```

## What a stream is

A stream has two ends and two directions. Side A is whoever called
`create`; side B is whoever opens the ticket. Each direction is a bounded
ring of `STREAM_BUFFER_BYTES` (default 64 KiB) that one side writes and the
other reads, in order, without loss: a full ring makes the writer wait, an
empty one makes the reader wait. The two directions are independent, so a
request body can still be going one way while the response starts the
other.

A writer that is done calls `finish`; the reader drains what is buffered
and then sees a clean end (`Ok(0)`, or an empty chunk). A writer that gives
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
is parked, and a wake only when someone is.

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

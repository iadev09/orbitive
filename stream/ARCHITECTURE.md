# orbit-stream - Architecture

One segment per fleet (kind `STREAM_KIND` = 246): a header with the epoch,
one doorbell and two bitmaps (pending, offers) per fleet node,
`fleet_capacity × STREAM_LANE_CAPACITY` slots of 128 bytes, then two
`STREAM_BUFFER_BYTES` rings per slot. Creation writes the header only, so
the kernel's zero pages back the rest until a stream touches them. The same
layout is allocated on the heap for a process-local fleet. Every field a
peer can touch is atomic; the mapping is never borrowed `&mut`.

## Address

`StreamId(NetId64)`: kind 246, node = the creator's lane, counter =
`generation (24 bits) << 16 | slot (16 bits)`: a packed allocation
identity, not a sequence number. The slot stores its generation; `install`
bumps it, so a released address never matches again, and it never wraps: at
`GENERATION_LIMIT` the slot is `EXHAUSTED` until the next epoch (unit tests
lower the limit to 4). A `Ticket` adds the side and the header's epoch;
`reset_all` advances the epoch (`max(now_ms, previous + 1)`, so a recreated
segment never repeats one), and `open` refuses a ticket from another epoch.
The address names the object; the ticket is what a claim needs.

## Slot

`state` (EMPTY / LIVE / EXHAUSTED), `generation`, `claimed[2]` (free /
claimed / released / dead), `node[2]` and `incarnation[2]` (which life of
which node's process holds each side), and two
`Direction`s: `head` (writer-owned, bytes committed), `tail` (reader-owned,
bytes consumed), `flags` (FIN, RESET, READER_GONE), `changes`, `waiters`.
Positions are monotonic `u64`; the ring index is the position masked.

Creation happens in the creator's own lane under a process-local mutex
(one live process per node id is already the per-node ring rule). Side B
is claimed with a CAS on `claimed[1]`, re-checked against the generation so
a claim that lands on a reinstalled slot is given back. A side is released
when both of its halves are dropped; the slot returns to EMPTY when no side
holds it (B may never have claimed). Old handles then answer `Stale`.

## Bytes

Writer: copy into `[head, head+n)` (wrapping), then `head.store(Release)`.
Reader: `head.load(Acquire)`, copy `[tail, min(head, tail+n))`, then
`tail.store(Release)`. That pair is the whole memory contract; it is the
same one the arena and the cells rely on across processes. FIN and RESET
are flags, so a full ring never blocks shutdown; a dropped `WriteHalf`
without `finish` is a RESET, a dropped `ReadHalf` sets READER_GONE.

### What a stream cannot carry

Bytes, and only bytes. A file descriptor is a per-process index into the
kernel's file table, not data, so a connection cannot be handed to another
process through here — that needs `SCM_RIGHTS` over a Unix socket. The
distinction has a name upstream of this crate: carrying the bytes while
their owner keeps the socket is a *relay*, and this is the transport for
it; handing the socket over is a *migration*, and it is not.

### What the ring holds when nobody is reading it

A socket's payload is transient. A ring is **residue**: what crossed stays
in the segment until other traffic overwrites that slot, and the segment
outlives every process that used it (which is why the workspace has a rule
and a tool for clearing kinds). Any process that can map the segment can
read the last bytes that crossed it, whether or not it was ever a party to
them.

The boundary is enforced rather than assumed: segments are
`shm_open(..., O_CREAT|O_EXCL, 0o600)` named `/orbit-{fleet}-{kind}-{uid}`,
and the init lock refuses a directory others can write to
(`orbit_core::shm`). So: **same host, same uid.**

That is a real boundary for a fleet whose processes are one service, and a
weaker one than a socket where it is not — not because of access control,
which is stricter here than a filesystem socket's, but because of the
residue.

A consumer carrying credentials over a stream is making a deployment
decision and should be made to say so. Note what is *not* one of its
options: a byte stream carries whatever is written to it, and the head of a
message is the first thing written. Keeping the credential-bearing part out
of the segment while relaying the rest would require something else to
construct the message, which is a different design (a proxy) and not this
one. So the choices are to accept the residue in writing, or not to carry
that traffic over a stream at all. The crate does not zero rings on
release; doing so is a memset per release and wants a measurement before it
is offered as a third.

## Death

Orbit has no liveness. `node_dead(node, incarnation)` is the embedder's
confirmed report: every LIVE slot with a CLAIMED side stamped with that
node and incarnation has that side moved to DEAD, its write direction
RESET, its read direction READER_GONE, both notified. A slot with no
CLAIMED side left empties; one whose other side is still claimed stays
LIVE until that holder drops (deferred reclamation). The dead node's
doorbell stops counting as listening only if its recorded incarnation
matches, so a replacement process under the same node id is untouched. A
side that `Handle::drop` finds already DEAD is not released twice.

## Offers

Discovery inside the segment: `offer` sets the slot's bit in the target
node's offer bitmap and rings its doorbell; `take_offer` clears one bit
with `fetch_and` and rebuilds the ticket from the slot, skipping one that
ended; the driver wakes the process's single offer waker when any bit is
set; a blocking taker parks on the doorbell generation and counts as
listening while it does. Taking is not claiming: `open` is, once per side.

## Waking

Three mechanisms, all hints over authoritative state:

- **Blocking waiters** park on the direction's `changes` word with
  `orbit_core::sync::wait_word`, announcing themselves in `waiters` first
  (the cell idiom, SeqCst on both sides). Every commit, consume and flag
  change bumps `changes` and wakes only when `waiters > 0`.

  Bounded forms take the same path with a deadline —
  `wait_readable_timeout`, `wait_writable_timeout`, `take_offer_timeout`
  — and answer `false`/`None` for the timeout and nothing else, after one
  last look. They are for a caller whose deadline is its own: a worker
  with a shutdown to observe, a request that must answer rather than
  queue. A caller waiting for several things at once uses the descriptor
  instead, since one `poll` can hold both.

  There is no polling fallback anywhere below this. Where the platform
  cannot wait on a shared word — macOS before 14.4 — the table refuses to
  open (`ErrorKind::Unsupported`) rather than degrade to a sleep loop
  wearing the shape of a wait.
- **Async tasks** register a `Waker` per (slot, direction, role) in a
  process-local registry. After a change that a parked task could be
  waiting for (a commit onto an empty ring, a consume from a full ring,
  any flag or claim change) the writer sets the slot's bit in the pending
  bitmap of each node holding a side, bumps that node's doorbell
  generation, and wakes it only when its `listening` count is nonzero. A
  commit onto a ring the reader has not drained, or a consume from one the
  writer never found full, rings nobody: under look/register/look-again a
  reader parks only on empty and a writer only on full.

  **That test is taken after the change, never before it.** A writer reads
  the tail again after storing its new head, a reader reads the head again
  after storing its new tail, each with a `SeqCst` fence between its own
  store and the other's load. The two fences are what make the pair safe:
  either the reader sees the new head and does not park, or the writer
  sees the drained tail and rings. Judged from the tail read before the
  copy — as it was until 2026-09-18 — a reader that drains the ring and
  parks while the writer is copying into it is left asleep on bytes that
  are already there, and only an unrelated poll finds them. Two writes in
  a row, a small header and then a body, is all it takes; it is pinned by
  `tokio_stream::a_body_behind_a_header_wakes_a_reader_that_parked_between_the_two`
  and was found by `benches/fleet.rs` across four processes.

  One
  driver thread per process per (segment, node) parks on the doorbell,
  swaps the bitmap words out and wakes the registered tasks. Bit before
  bump, drain before compare: either the driver sees the bump or the park
  returns at once. In memory the writer wakes the registry directly and no
  thread exists.

- **A descriptor**, for a runtime that parks on descriptors rather than
  on wakers or on a word: `Streams::readiness` hands out one `eventfd`
  (a pipe on macOS) per table, and the driver signals it at the end of
  every drain — *after* it has taken the pending bits, so a consumer that
  drains the descriptor and re-tries its streams cannot miss what that
  pass made ready. Edge-triggered and coalescing: it says something may
  have changed, not what. One holder per table; a second is refused
  rather than handed a descriptor whose signals the first would drain. On
  a memory table there is no driver, so `notify` and `offer` signal it
  directly. This is what an embedded runtime needs — libuv, asyncio, any
  foreign loop — and it costs one atomic load per notify when nobody has
  asked for one. The pair itself is `orbit_core::readiness`, shared with
  the ring bridge and with `orbit-pool`: one descriptor implementation,
  three things that signal it.

The driver starts lazily on the first registration in a process, or when
a readiness descriptor is taken, and is stopped and joined when the table
drops, before the mapping goes away; a
forked child that inherited the table skips the join because the thread is
not there. Create tables after fork, as with ring readiness fds.

## Specs: one fleet, several tables

A `StreamSpec` names the segment a `Streams` table opens — its kind, its
lane capacity and its ring size — and `Streams::new` is
`with_spec(StreamSpec::DEFAULT)`, the kind 246 table built from the
compile-time geometry. Independent specs are independent tables:
separate segments, lanes, rings and epochs.

This is what the measurements below ask for. One ring size cannot serve
both a relay carrying megabyte bodies (which wants a ring near the body,
or it pays a wake per 64 KiB) and a control channel carrying short frames
(which is faster with a small one, because concurrency times two times
the ring is a working set). With one compile-time constant a process had
to choose; with a spec each table chooses, and `StreamSpec::lane_bytes`
is the ceiling that choice commits to.

A kind is a fleet-wide identity, not a local choice: every process
opening it passes the same geometry, the header refuses a peer that does
not, and a process that opens one kind twice under two geometries is
refused rather than handed a table that is not the one it asked for.
Geometry is validated when the table is opened, where a compile-time
assert used to stand. Each kind still owes the deployment an index row of
its own.

## Geometry for a deployment (Linux is production)

Measured 2026-09-17 on the same M3 Max, macOS host and a Debian aarch64
guest (`xd01`); logs in `BENCHMARKS.local.md`:

| | macOS host | Linux guest |
|---|---|---|
| wake latency (one park and wake through the doorbell driver) | ~12 µs | ~55 µs |
| stream one way, 64 KiB ring, bodies ≥ 64 KiB | ~5 GiB/s | ~1.1 GiB/s |
| Unix socket one way, 1 MiB body | ~1 GiB/s | ~4 GiB/s |
| round trip 128 B, SHM vs socket | 11.7 vs 12.2 µs | 54 vs 62 µs |

The stream moves at most one ring per wake cycle, so its bulk
throughput is

```text
throughput ≈ STREAM_BUFFER_BYTES / wake latency
```

and both rows above obey it (64 KiB / 12 µs ≈ 5 GiB/s, 64 KiB / 55 µs ≈
1.15 GiB/s). The socket's advantage on Linux is a larger kernel buffer
and no user-space wake hop, not a faster copy. Consequences:

- **The ring size is a per-deployment number.** The sweep was run end to
  end on `xd01` on 2026-09-18 (`benches/fleet.rs`, four processes) and
  the formula holds to the wake count: a 1 MiB body one at a time goes
  2.24 → 7.68 → 21.7 GiB/s as the ring goes 64 KiB → 256 KiB → 1 MiB,
  with 62.6 → 18.2 → 6.05 voluntary context switches per request. At
  ring = body the stream passes the Unix socket (12.1 GiB/s) for two
  thirds of its CPU; at 64 KiB it is a fifth of it. The Linux deficit
  was geometry.

  It has a second bound, which the same sweep found: a 64 KiB body at 64
  in flight runs 28.4 GiB/s with a 64 KiB ring and 13.6 GiB/s with a
  1 MiB one. Concurrent streams × 2 × ring is a working set, and past
  the last-level cache the copy itself slows down. So:

  ```text
  ring ≈ the body actually carried,
         bounded by concurrency × 2 × ring staying near the LLC
  ```

  For 64 KiB–1 MiB bodies at 8–64 in flight on that guest, 256 KiB was
  the best single choice. The deployment default in
  `claviron-full/.cargo/config.toml` is **256 KiB per direction on
  Linux**. The crate default stays 64 KiB: it is the host-agnostic floor
  and what the tests and the macOS numbers were taken with.
- Address space is `fleet × lanes × 2 × ring`, backed only as touched:
  17 × 512 × 2 × 256 KiB is 4.25 GiB of sparse mapping and costs what
  live streams use. On Linux the segment lives on `/dev/shm` (tmpfs);
  its size limit applies to touched pages, but a container's `/dev/shm`
  must still admit the mapping.
- **Write and read in large pieces.** A byte-by-byte reader rings the
  peer only on transitions now, but each `poll` still costs a budget
  check and each wake ~55 µs; a consumer that frames a header should
  read it with one `read_buf` into a local buffer, not one byte per
  call.
- **Round trips are the guest's, not ours.** Every transport pays the
  same ~50 µs there; the stream is no worse than a socket and no
  better. Anything latency-shaped (a lease handshake, a small RPC)
  should count its hops: each stream setup and each direction reversal
  is one wake.
- The driver-thread hop (writer → futex → driver → waker → runtime) is
  one of those wakes on both hosts. Parking the runtime directly on a
  per-process fd fed by the doorbell would remove it; it stays on the
  list until a consumer needs the microseconds and measures them.

## Invariants

- Bytes are never overwritten before they are read; there is no lag.
- No thread and no descriptor per stream.
- Nothing blocks inside a `poll_*`.
- No liveness: `node_dead` is the embedder's confirmed report, per
  incarnation; a timeout is not a death; supervision adoption is not a
  death.
- A ticket carries the epoch; slot generations never wrap.
- Geometry and the address split are wire ABI; a change is a new version
  of the header and a new segment.

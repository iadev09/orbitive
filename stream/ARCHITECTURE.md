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

Two independent mechanisms, both hints over authoritative state:

- **Blocking waiters** park on the direction's `changes` word with
  `orbit_core::sync::wait_word`, announcing themselves in `waiters` first
  (the cell idiom, SeqCst on both sides). Every commit, consume and flag
  change bumps `changes` and wakes only when `waiters > 0`.
- **Async tasks** register a `Waker` per (slot, direction, role) in a
  process-local registry. After a change that a parked task could be
  waiting for (a commit onto an empty ring, a consume from a full ring,
  any flag or claim change) the writer sets the slot's bit in the pending
  bitmap of each node holding a side, bumps that node's doorbell
  generation, and wakes it only when its `listening` count is nonzero. A
  commit onto a non-empty ring or a consume from a non-full one rings
  nobody: under look/register/look-again a reader parks only on empty and
  a writer only on full. One
  driver thread per process per (segment, node) parks on the doorbell,
  swaps the bitmap words out and wakes the registered tasks. Bit before
  bump, drain before compare: either the driver sees the bump or the park
  returns at once. In memory the writer wakes the registry directly and no
  thread exists.

The driver starts lazily on the first registration in a process and is
stopped and joined when the table drops, before the mapping goes away; a
forked child that inherited the table skips the join because the thread is
not there. Create tables after fork, as with ring readiness fds.

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

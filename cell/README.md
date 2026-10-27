# orbit-cell

A variable that several processes hold at once.

Two processes, one value: a task on one worker adds to a cell twenty million
times as fast as it can; an application on another worker sleeps on the
same cell, is woken by the writes, and pushes what it sees to a browser at
fifty frames a second. Nothing is copied, nothing is sent between the two,
and the writer never learns it was watched. That is the whole idea, and the
rest of this file is how it holds.

`orbit-cell` keeps typed atomic cells in one fleet-shared table in shared
memory, addressed by id rather than by name. A cell is a place: allocate it
once, hand its id to any process in the fleet, and every handle reads and
updates the same 64 bits atomically, without a lock, without a copy and
without publishing a frame. The id is the only thing that ever travels: in a
message, in a cache, in a web page. Whoever opens it is looking at the same
memory. `orbit-counter` is the keyed sibling; this is memory rather than a
dictionary. Applications normally use it through the
[Orbitive](https://github.com/iadev09/orbitive) facade as `orbitive::cell`.

```rust
use std::sync::Arc;

use orbitive::cell::{CellId, Cells};
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
let cells = Cells::new(fleet)?;

let hits = cells.allocate(0_i64)?;          // the type is stamped on the cell
let id = hits.id().to_string();             // "cell:<slot>:<generation>": what travels

let same = cells.open::<i64>(id.parse::<CellId>()?)?;
same.fetch_add(1)?;
assert_eq!(hits.load()?, 1);

hits.release()?;                            // every handle to it is now stale
assert!(same.load().is_err());

# Ok::<(), Box<dyn std::error::Error>>(())
```

## What a cell is, and is not

A cell is the shared-memory form of a variable: one value, one place, read
and written in place by every process that holds its id. Three things it
deliberately is not:

- **Not a counter by key.** `orbit-counter` names values; a cell names a
  place. There is no lookup, no string, no hashing on the hot path, and two
  handles to one id are the same memory, not two entries that agree.
- **Not an event.** Nobody is told that a cell changed, and no change is
  retained. A reader that wants to know reads; a reader that wants to wait
  waits (below). Writes that happen while nobody looks leave only their
  result behind. That is the gauge contract: the latest value wins, and the
  history was never the point.
- **Not a queue.** Nothing is delivered, acknowledged or replayed. If every
  change matters, that is a ring of events, and `orbit-events` is next door.

Four types fit a cell: `i64`, `u64`, `f64` and `bool`. Integers get checked
`fetch_add` / `fetch_sub`, floats a compare-and-swap `fetch_add`, flags
`set` / `clear`; every type has `load`, `store`, `swap` and
`compare_exchange`. Opening an id as the wrong type is refused.

Text has its own arena: `allocate_text` / `open_text` give a `Text` handle
holding up to `CELL_TEXT_MAX` bytes of UTF-8 (default 240, sized by
`ORBIT_CELL_TEXT_CAPACITY` and `ORBIT_CELL_TEXT_MAX`). `load` copies the
current text, `store` replaces it and `append` extends it atomically with
respect to every other writer; a write that would not fit is refused whole,
never cut. Readers go through a seqlock and never observe a torn string. A
`TextId` prints as `text:<slot>:<generation>`.

## Waiting for a change

A cell is state, not a message: nobody is told when it changes, and a reader
that wants the latest value reads it. Between the two sits one more thing a
cell can do, which neither an atomic nor an event can: park a reader until
the cell has been written.

```rust
# use std::sync::Arc;
# use orbitive::cell::Cells;
# use orbitive::Fleet;
# let cells = Cells::new(Arc::new(Fleet::join("example-wait", 1)?))?;
let progress = cells.allocate(0_i64)?;

// Elsewhere, in any process: a loop that adds as fast as it likes.
let writer = progress.clone();
std::thread::spawn(move || {
    for _ in 0..1_000_000 {
        writer.fetch_add(1).unwrap();
    }
});

// Here: sleep until something happened, read what it is now, repeat.
let mut seen = progress.version()?;
loop {
    seen = progress.wait_changed(seen)?;    // parks; one wake for any number of writes
    let now = progress.load()?;
    if now >= 1_000_000 { break; }
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

`version` is the cell's write count; `wait_changed(since)` parks until the
count has moved past `since` and returns the count now. A million writes
while the reader is parked wake it once, and `load` gives the latest: this is
a gauge, not a queue, and nothing in between is retained. The same pair
exists on `Text`, where the seqlock's own sequence is the count.

The wait is the memory's, not a channel's. Linux futex, FreeBSD umtx and
macOS `os_sync_wait_on_address` (14.4 and later; earlier releases are refused) key
waiters by the physical location of the word, so a write in one process
wakes a reader parked in another with nothing carried between them: no
descriptor to share, no ring to drain, no subscription to keep. A writer
pays one extra load per write to see whether anyone is parked, and a wake
only when someone is, so an unwatched cell costs what an atomic costs.

Releasing a cell is a change too: whoever is parked on it wakes with
`Error::Stale` rather than sleeping on a slot somebody else may take next.

### Why nothing is lost

The native comparison word is a parking generation separate from the value
version. A reader announces itself (`waiters += 1`), snapshots that generation,
then rechecks the value version before parking. A writer publishes the value
version first; when readers are registered it advances the parking generation
before waking them. Both sides use sequentially consistent ordering: either
the reader sees the value change or the writer sees its registration, and a
late kernel entry cannot park on the earlier parking generation.

`wait_changed_until` also checks a caller-owned interrupt flag. Set that flag,
then call `wake_waiters`. This wake advances the parking generation even if no
thread is in the kernel yet. The reader either observes the interrupt in its
final check or the native comparison prevents its late park. It returns
`Ok(None)` for cancellation. `wake_waiters` changes neither payload nor value
version; an ordinary waiter keeps waiting until an actual write or release.
The parking generation never wraps: after `u32::MAX` notifications, a call
that would park returns `Error::WakeExhausted`. Value operations still work;
release and reallocate the cell to obtain a new parking generation.

A change made while the reader is *not* parked is not lost either. The
count moved, so the reader's next `wait_changed(seen)` returns without
sleeping. What the reader never sees are the intermediate values, and a
gauge has none to show.

### What a write costs

With nobody parked, a write is the atomic operation plus one atomic
increment and one atomic load: a few nanoseconds, no system call. With a
reader registered, the write also increments the parking generation and issues
one wake, which lifts every parked
reader at once. A reader is parked only between its own reads, so a hot
writer with one reader pays a wake at the reader's pace, not its own: twenty
million writes and a reader that holds twenty milliseconds between looks is
a few hundred wakes.

The pathological shape is a hot writer with readers parked at all times, so
that every write finds someone waiting and pays a system call. That is a
reader-side problem with a reader-side answer: park once per process and
fan out locally. One watcher parks on the cell; the sockets, tasks or
threads in that process subscribe to the watcher. The cell then sees as
many waiters as there are processes, whatever the number of readers.

## Lifetime and geometry

Every id carries the generation its slot was allocated in, and `release`
retires that generation: a handle kept past a release reports `Error::Stale`
instead of reaching whoever took the slot next. Cells have no lifetime of
their own; what allocates one is expected to release it, and the table
reports `Error::Full` rather than evicting.

Release is logical retirement, not immediate physical reuse. Each allocated or
opened handle pins its generation; clones share that pin. Operations that
already passed validation may finish on the retired generation, but a new
generation cannot occupy its slot until the last old handle drops. Keep handles
for repeated operations, and drop stale handles when finished. `reset_all`
retires generations by the same rule. A slot whose generation reaches
`u32::MAX` is never reused.

Value operations do not acquire pins, locks or owner holds. Opening a handle
uses the table's structural lock; the last clone's drop releases its pin.
In SHM, each process holds one kernel lock per table, with bounded owner rows
recording its pins. A full allocation scan can reclaim a dead owner's row only
while holding that owner's kernel lock. A live process's pins are preserved,
even after release. No PID probe, timeout or periodic recovery poll is used.
Owner death removes abandoned pins, not unreleased values. Open tables and
handles again after `fork`; inherited SHM handles must not be used in the child.

`ORBIT_CELL_OWNER_CAPACITY` selects the number of SHM owner rows; when omitted,
the fleet capacity is used. All processes attaching to a table must agree on
this value. Repeated opens of the same table within one process share its owner
row. Each row uses one `u32` counter per slot, rounded to a cache-line stride;
owner capacity exhaustion returns an IO error with `WouldBlock`.

The handle-pinned protocol with separate parking generations uses SHM kinds
`187` (scalar) and `188` (text), with version 4 headers. Scalar slots remain
64 bytes; text slots use `(20 + CELL_TEXT_MAX)` rounded up to a cache line
(320 bytes at the default text capacity). Earlier kinds `189`/`190` (v3)
and `233`/`227` (v2) are left untouched. Old and
new binaries do not share cell tables; applications must coordinate the change
of protocol. There is no automatic migration, reset or unlink of old tables.

Geometry is compile-time: `ORBIT_CELL_CAPACITY` (a power of two, default
4096) sizes the table for every binary that opens the fleet. The table is
current state, not history: the fleet owner clears it with `reset_all` during
quiescent boot, and nothing about it survives a fleet generation on purpose.

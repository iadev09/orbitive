# orbit-atomic invariants

Maintainer record. The public contract is in `README.md`.

## Lifetime: nothing is reused

A value lives as long as its table's shared memory. There is no release, no
slot reuse, no slot generation, no lease, no per-process owner row and no
kernel hold. A handle is a pointer to its slot, the id, and an `Arc` of the
table (the SHM mapping, or the in-memory slot array). That `Arc` is the only
thing that keeps the pointer valid, and it is enough because a slot never
changes meaning within an epoch.

Consequences that must stay true:

- The value path (`load`, `store`, `swap`, `compare_exchange`, `fetch_*`) has
  no backend match, no bounds check and no state or epoch load.
- Capacity counts declarations per table life, not live values.
- Only `reset_all` returns slots, and it is correct only when quiescent: it
  does not invalidate handles that already exist. A handle kept across a
  reset aliases whatever is allocated in its slot next. Nothing in the code
  detects this; the owner's boot order is the guarantee.
- Names do not change this. The directory is read only by `declare`,
  `find` and `reset_all`; `open` by id and the value path never touch it.

## Layout

SHM kind `174`, magic `ORBL`, version 1.

Header, 64 bytes: `magic u32`, `version u16`, `header_size u16`,
`capacity u32`, `slot_size u32`, `epoch AtomicU32`, `next AtomicU32`.
Geometry is validated at attach. A different slot or header layout takes a
new kind, never a new meaning for this one.

Slot, 64 bytes: `state AtomicU8` (empty, occupied), `kind AtomicU8`,
`value AtomicU64`, `wake.sequence AtomicU32`, `wake.waiters AtomicU32`. All
zeroes is an empty slot, which is what a created segment and a reset leave.

Id: `kind:8 | slot:24 | epoch:32`. The epoch is the table's. It starts at 1,
so no id with epoch 0 is ever current.

## Allocate, open, reset

**Allocate** takes no lock. One `fetch_add` on `next` decides who owns an
index; the owner writes kind and value and publishes `state` with `Release`.
A load of `next` precedes the `fetch_add` and returns `Full` without writing,
so `next` passes the capacity by at most the number of callers racing in one
window and cannot wrap back to a used index.

**Open** only reads: bounds, table epoch (`Acquire`), the id's kind against
`T`, then `state` (`Acquire`, which publishes the install) and the slot's own
kind. A foreign kind in the id is `KindMismatch`; everything else is `Stale`,
including a well-formed id whose kind disagrees with its slot.

**Reset** holds the process-local reset mutex and the region's exclusive lock,
advances `epoch` (`EpochExhausted` rather than a wrap), clears every slot
including its wake words, and zeroes `next`. The locks serialize resets with
each other and with attach; they do not exclude allocation or value access,
which is why reset is quiescent-only.

## Names

The directory is a segment of its own, SHM kind `170`, magic `ORBN`,
version 1, not part of kind `174`. Kind `174` was already published and
bound by Claviron; growing it would stop a new master from booting beside
workers that hold the old segment. A fleet that never declares or looks up
a name never creates the directory.

Header, 64 bytes: `magic u32`, `version u16`, `header_size u16`,
`capacity u32`, `entry_size u32`, `name_max u32`, `epoch AtomicU32`. Geometry,
`name_max` included, is validated at attach.

Entry, `(16 + ATOMIC_NAME_MAX)` rounded up to 64 bytes: `state AtomicU8`,
`len AtomicU8`, `lifetime AtomicU8` (reset or kept), a reserved byte,
`hash AtomicU32` (FNV-1a), `id AtomicU64` (the id's eight bytes), then the
name. There is one entry per table slot and every entry names a distinct
slot of its epoch, so the directory cannot fill before the table. Probing
is linear from the hash.

**The directory epoch.** The header records the table epoch its entries
belong to; zero means none. A directory in any other epoch than the table's
holds no names: `declare` empties it under the lock and adopts the table's
epoch. That covers a reset by a binary that knows nothing of names, and a
reset cut short.

**Find** takes no lock: it compares the directory epoch with the table's,
then probes. An entry is written once and published by `state` with
`Release`, and nothing clears it within its epoch, so an occupied entry
seen with `Acquire` is complete and stays true, and probing chains only
grow. A miss says the name was not installed when the probe passed. That
is the answer for `Orbitals::find`, ordered before any install still in
flight; `declare` asks again under the lock before installing.

`find` attaches the directory, creating it if no process has, rather than
attaching only an existing one. Attaching only takes the initialization
lock and an `shm_open` on every miss until some process declares a name;
creating once leaves every later lookup in the process lock-free. An
empty directory has epoch zero and resolves nothing, so creating it early
changes no answer. Counters use `find_kept` for `get`: a key read is not
installed, so reads of keys nobody counts do not use up slots.

**Install** holds the process-local mutex and the directory region's
exclusive lock (flock, released by the kernel when the holder dies). It
probes again, allocates a slot through the ordinary lock-free path, and
writes the entry with `state` last. An installer that dies before that
leaves an empty entry and at most one slot used without a name.

**Reset** holds the table's locks, then the directory's; every path that
takes both takes them in that order. It copies out the kept names of the
current epoch with their kinds and values, invalidates and empties the
directory, advances the table epoch, clears every slot, writes the kept
values into slots `0..k` in their former slot order, sets `next` to `k`,
records the kept names with ids of the new epoch, and only then publishes
the directory epoch. A reset cut short loses the kept values; it never
leaves a name that resolves to another value. Kept values are compacted to
the front rather than left in place: allocation only moves forward, so a
kept value left high in the table would hold every slot below it out of
use across all later epochs. Ids change at a reset anyway, so moving a
value costs nobody anything.

**Creating the table** removes any directory segment left under the same
fleet name, while the table's initialization lock keeps every peer out. A
new table starts again at epoch 1, so an old directory would otherwise
resolve names to slots of the new table. A binary of `orbit-atomic` 0.1,
which knows nothing of names, does not do this. So whatever clears kind
`174` must clear `170` with it. `Orbitals::unlink` removes both.

## Orderings

A watch must not miss a write. The waiter stores its registration and then
loads the value; the writer stores the value and then loads the registration
count. That is the store-then-load pair of Dekker's algorithm and holds only
when all four accesses are `SeqCst`. So every write is `SeqCst` on the value
word followed by a `SeqCst` load of `wake.waiters`, and the caller cannot
choose otherwise. `load` is `Acquire`. A relaxed write would be a separately
named operation that documents it wakes nobody; none exists.

`i64` operations view the value word as `AtomicI64` through `from_ptr`, so
`fetch_max` and `fetch_min` compare signed in one read-modify-write. `bool`
is stored as 0 or 1 and every bool write keeps it so. `f64` is its bit
pattern throughout.

## Waiting

`Wake` is a `u32` parking sequence and a waiter count, separate from the
value. A waiter increments the count, snapshots the sequence, rechecks
"value still equals `current` and interrupt not set", and parks on the
sequence with the snapshot as the expected word. A write that finds a waiter
advances the sequence and wakes. `notify` always advances the sequence, even
with nobody in the kernel, so a canceller that sets its flag and then
notifies is either seen by the recheck or changes the word the late park
compares. No clock, retry or poll anywhere.

The sequence wraps. A waiter parks on a stale sequence only if exactly 2^32
notifies land between its recheck and its park; it is still registered, so
the next write wakes it. A saturating sequence was the alternative and was
refused: with no per-slot reinstall it would end waiting on a long-watched
value until the table's reset.

Known costs of having no per-slot reinstall:

- A process killed while parked leaves its registration in `wake.waiters`
  until the next reset. Writes to that value then pay a sequence advance and
  a wake call each.

## Registries and fork

In-memory tables are keyed by the fleet's `Arc` address and hold the fleet so
the address cannot be reused. SHM tables are keyed by segment name, one
mapping per name in a process. The directory hangs off the table in a
`OnceLock`, so it is mapped once per process too, on first use. The SHM
registry captures the process owner; a fork child gets a registry of its
own, because it cannot take a mutex inherited from its parent, and maps the
table and its directory again. `unlink` drops the name
from the registry so a later `Orbitals::new` in the same process maps the new
object rather than the removed one.

Value operations on a handle inherited across `fork` reach the same shared
memory. `reset_all` through an inherited table would share the parent's lock
descriptor, so a child opens its own.

## Tests

The before-park hook in `wake.rs` (`cfg(test)`, thread-local) is how a test
places a write or an interrupt after the waiter's last check without
sleeping. Tests run in parallel; each in-memory test joins its own fleet.

# orbit-atomic

`std::sync::atomic` between processes instead of between threads.

`orbit-atomic` keeps 64-bit values in one fleet-shared table in shared
memory. A process declares a value and gets an `Orbital<T>`, a handle with the
operations of an atomic: `load`, `store`, `swap`, `compare_exchange` and the
`fetch_*` family. The handle's id is eight bytes. Hand it to any other process
of the fleet, in a message or a page, and whoever opens it holds the same
value: nothing is copied and nothing is sent between them. A handle can also
park until the value changes, as C++ `atomic::wait` does, and a write in one
process wakes a waiter in another.

Applications normally use it through the
[Orbitive](https://github.com/iadev09/orbitive) facade as `orbitive::atomic`,
with the `atomic` feature.

```rust
use std::sync::Arc;

use orbitive::Fleet;
use orbitive::atomic::{OrbitalId, Orbitals};

let fleet = Arc::new(Fleet::join("example", 1)?);
let orbitals = Orbitals::new(fleet)?;

let hits = orbitals.allocate(0_i64)?;      // declared once, kept
let id = hits.id().to_string();            // "i64:<slot>:<epoch>": what travels

// In any process of the fleet:
let same = orbitals.open::<i64>(id.parse::<OrbitalId>()?)?;
same.fetch_add(1);
assert_eq!(hits.load(), 1);

# Ok::<(), Box<dyn std::error::Error>>(())
```

## Types and operations

`T` is `i64`, `u64`, `f64` or `bool`. Opening an id as another type is
refused with `Error::KindMismatch`.

| Type | Operations beyond `load`, `store`, `swap`, `compare_exchange` |
| --- | --- |
| `i64`, `u64` | `fetch_add`, `fetch_sub`, `fetch_and`, `fetch_or`, `fetch_xor`, `fetch_max`, `fetch_min` |
| `f64` | `fetch_add` |
| `bool` | `fetch_and`, `fetch_or`, `fetch_xor` |

They behave as their `std::sync::atomic` namesakes:

- Value operations cannot fail and return `T`, not a `Result`.
- Every `fetch_*` returns the previous value.
- Integer arithmetic wraps on overflow.
- `f64` is compared by its bits in `compare_exchange` and `wait`: `0.0` is not
  `-0.0`, and a NaN equals only the same NaN.

There is no `Ordering` parameter. Every write is sequentially consistent,
which is what lets a waiter never miss one; `load` is an acquire load.

## Waiting for a change

```rust
# use std::sync::Arc;
# use orbitive::Fleet;
# use orbitive::atomic::Orbitals;
# let orbitals = Orbitals::new(Arc::new(Fleet::join("example", 1)?))?;
let progress = orbitals.allocate(0_u64)?;

// Elsewhere, in any process: writes as fast as it likes.
let writer = progress.clone();
std::thread::spawn(move || {
    for _ in 0..1_000 {
        writer.fetch_add(1);
    }
});

// Here: park until it is no longer what was last seen.
let mut seen = 0;
while seen < 1_000 {
    seen = progress.wait(seen)?;
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

`wait(current)` parks until the value differs from `current` and returns what
it is then. It returns at once when the value already differs, so a change
made while the caller was not waiting is not lost. Any number of writes while
parked are one wake: the caller sees the latest value, not each one in
between. A value that left `current` and returned to it before the waiter
looked is no change.

The wait is the memory's, not a channel's. Linux futex, FreeBSD umtx and macOS
`os_sync_wait_on_address` key waiters by the physical location of a word, so a
write in one process wakes a waiter parked in another. macOS needs 14.4 or
later; on a platform that cannot wait on a shared word, `Orbitals::new`
returns an error instead of falling back to polling.

`wait_until(current, &interrupt)` is the same wait, which a second word can
end. The canceller stores `true` in the `AtomicBool` and then calls `notify()`
on any handle to the same value, in that order; the waiter returns `Ok(None)`.
It is not a timeout, and nothing here measures time. `notify()` on its own
wakes waiters without writing; one whose value is unchanged and whose
interrupt is not set parks again.

A write with nobody waiting costs the atomic operation and one load on the
same cache line. Only when a waiter is registered does a write also issue a
wake. A hot writer with a waiter parked at all times therefore pays a system
call per write; park once per process and fan out locally when many readers
in one process watch one value.

## Lifetime

A value lives as long as its table's shared memory. There is no way to
release one value and no slot is reused, so:

- **Declare, do not allocate per request.** The table holds
  `ORBIT_ATOMIC_CAPACITY` values (default 4,096) between two resets. When
  they are used up, `allocate` returns `Error::Full`.
- **Dropping a handle changes nothing.** The value stays, and its id opens it
  again.
- **`reset_all` is how values end.** It empties the table and starts a new
  epoch, after which every earlier id is `Error::Stale`. It is for the
  process that owns the fleet's lifecycle, at a boot where no other process
  is attached and no handle is in use: a handle kept across a reset still
  points at its slot, which the table may hand out again.

An id is valid on the host and in the table that minted it, until that table
is reset or its shared memory is removed. It survives the restart of a
process that held it. It belongs in messages and short-lived caches, never in
a database.

`OrbitalId` is `kind:8 | slot:24 | epoch:32`. `to_bytes` gives eight
big-endian bytes and `from_bytes` refuses an unknown kind byte; the text form
is `<kind>:<slot>:<epoch>`, such as `i64:12:3`.

## Geometry and platforms

`ORBIT_ATOMIC_CAPACITY`, set in the `[env]` table of the application's
`.cargo/config.toml`, sizes the table at compile time. It must be a power of
two no larger than 16,777,216. Every binary that opens one fleet's table must
be built with the same value; a different one is refused at attach.

The table uses Orbit SHM kind `174` (`ATOMIC_KIND`), one 64-byte header and
one 64-byte slot per value, so two hot values never share a cache line. An
in-memory fleet gets the same table on the heap, shared by everything that
holds the same `Fleet`.

After `fork`, call `Orbitals::new` again in the child; it maps the table for
that process.

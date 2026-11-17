# orbit-atomic

`std::sync::atomic` between processes instead of between threads.

`orbit-atomic` keeps 64-bit values in one fleet-shared table in shared
memory. A process gets an `Orbital<T>`, a handle with the operations of an
atomic: `load`, `store`, `swap`, `compare_exchange` and the `fetch_*` family.
It reaches a value in one of two ways:

- **By name.** Every process that declares the same name holds the same
  value. This is the way for a value with a fixed identity, such as a
  counter, a flag or a gauge of the whole fleet.
- **By id.** `allocate` returns a handle whose id is eight bytes. Hand the id
  to any other process of the fleet, in a message or a page, and whoever
  opens it holds the same value.

Either way nothing is copied and nothing is sent between processes. A handle
can also park until the value changes, as C++ `atomic::wait` does, and a
write in one process wakes a waiter in another.

Applications normally use it through the
[Orbitive](https://github.com/iadev09/orbitive) facade as `orbitive::atomic`,
with the `atomic` feature.

```rust
use std::sync::Arc;

use orbitive::Fleet;
use orbitive::atomic::{Orbitals, orbital};

orbital! {
    /// The fleet's hit counter.
    pub Hits: i64 = 0, "hits"
}

let fleet = Arc::new(Fleet::join("example", 1)?);
let orbitals = Orbitals::new(fleet)?;

// In any process of the fleet; the first one installs the initial value.
let hits = Hits::open(&orbitals)?;
hits.fetch_add(1);                         // the Orbital itself: no lookup
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Declaring a value by name

`Orbitals::declare(name, initial)` is get-or-create. The first caller in the
table's epoch allocates a slot holding `initial`; every later caller, in any
process, gets a handle to the same slot, and its `initial` is ignored.

```rust
# use std::sync::Arc;
# use orbitive::Fleet;
# use orbitive::atomic::Orbitals;
# let orbitals = Orbitals::new(Arc::new(Fleet::join("example", 1)?))?;
let ready = orbitals.declare("ready", false)?;
let again = orbitals.declare("ready", true)?;   // the same value, still false
assert_eq!(again.id(), ready.id());
# Ok::<(), Box<dyn std::error::Error>>(())
```

- The name is hashed and looked up only in `declare`. Hold the handle, not
  the name: the value operations never see it.
- A name that is already installed is found without a lock. Installing takes
  a short structural lock, which the kernel releases if its holder dies.
- Declaring a name again allocates nothing. A worker that restarts and
  declares its values again consumes no slots.
- Declaring a name with another type is `Error::KindMismatch`. A name is at
  most `ATOMIC_NAME_MAX` bytes (default 112); an empty or longer name is
  `Error::NameEmpty` or `Error::NameTooLong`.

`Orbitals::find(name)` looks a name up without declaring it: `Ok(None)` when
no process has declared it in the table's epoch, and nothing is installed.
It takes no lock and refuses another type or lifetime as `declare` does;
`find_kept` finds a kept name. It is for names chosen at run time, where a
read must not use up a slot, as [`orbit-counter`](https://docs.rs/orbit-counter)
does with its keys.

The `orbital!` macro gives a declared value a type of its own: a newtype with
the doc comment and visibility written, `Clone`, `Deref<Target = Orbital<T>>`,
and an inherent `open`. Several declarations can share one macro call. A name
the directory cannot hold does not compile.

```rust
use orbitive::atomic::orbital;

orbital! {
    /// Requests served since the fleet booted.
    pub Requests: u64 = 0, "requests"
    /// Survives the owner's reset, see below.
    pub Visitors: u64 = 0, "visitors", kept
    pub(crate) Draining: bool = false, "draining"
}
```

Each generated type implements the `Declared` trait, which carries
`type Value`, `const NAME`, `const INITIAL`, `const LIFETIME` and
`fn open(&Orbitals)`. A host that registers or injects declared values
generically works over that trait.

### Kept values

A value declared with `, kept` in the macro, or with
`Orbitals::declare_kept`, survives `reset_all`. It keeps its value and is
found again by declaring its name after the reset, under a new id. Every
other value returns to empty: a plain name is forgotten and starts again from
its initial value when it is next declared. Declaring an existing name with
the other lifetime is `Error::LifetimeMismatch`.

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

A value lives as long as the table. There is no release: nothing gives one
value back, and dropping a handle changes nothing. A slot allocated with
`allocate` and passed by id is never returned within an epoch, so:

- **Declare, do not allocate per request.** The table holds
  `ORBIT_ATOMIC_CAPACITY` values (default 4,096) between two resets. When
  they are used up, `allocate` and `declare` return `Error::Full`.
- **Names last exactly as long as the epoch.** When workers are restarted
  and the table is reused, names and their slots stay, and declaring again
  returns the same value.
- **`reset_all` is how values end.** It starts a new epoch, after which
  every earlier id is `Error::Stale`. It empties every slot and forgets every
  name except the kept ones. It is for the process that owns the fleet's
  lifecycle, at a boot where no other process is attached and no handle is
  in use: a handle kept across a reset still points at its slot, which the
  table may hand out again.

Not designed yet: values that should live only as long as one job, created
for the job, passed through an invocation and gone when it ends. Who owns
such a value and when its slot comes back is undecided, and is tracked in
issue 19 of the repository. Until then, `allocate` per job uses up the table
for good. Declarations are the stable path for values with a fixed identity.

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

`ORBIT_ATOMIC_NAME_MAX` sets the longest name the same way: at most 255, and
the same in every binary of a fleet.

The table uses Orbit SHM kind `174` (`ATOMIC_KIND`), one 64-byte header and
one 64-byte slot per value, so two hot values never share a cache line. Names
live in a directory of their own, Orbit SHM kind `170` (`ATOMIC_NAMES_KIND`),
created by the first declaration or lookup: one entry per slot the table
can hold, 128 bytes each at the default name length. A fleet that never
declares or looks up a name never creates it. `Orbitals::unlink` removes both, and so must anything else
that clears the table's shared memory. An in-memory fleet gets the same
table and directory on the heap, shared by everything that holds the same
`Fleet`.

After `fork`, call `Orbitals::new` again in the child; it maps the table for
that process.

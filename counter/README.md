# orbit-counter

`orbit-counter` provides keyed, signed 64-bit counters shared by every process
in the same Orbit fleet. Applications normally use it through the
[Orbitive](https://github.com/iadev09/orbitive) facade as `orbitive::counter`,
with the `counter` feature, which also enables `atomic`; direct package access
remains available.

A counter is an [`orbit-atomic`](https://docs.rs/orbit-atomic) value: a kept `i64`
Orbital whose name is the counter's key. The counter keeps no table of its
own. It adds what keys chosen at run time need: a lookup that does not
install, and checked arithmetic.

```rust
use std::sync::Arc;

use orbitive::Fleet;
use orbitive::atomic::Orbitals;
use orbitive::counter::Counter;

let orbitals = Orbitals::new(Arc::new(Fleet::join("example", 1)?))?;
let counter = Counter::new(orbitals);

assert_eq!(counter.get("tenant:42:requests")?, None);     // not installed
assert_eq!(counter.increment("tenant:42:requests", 1)?, 1);

// Resolve the key once; the handle's operations do no lookup.
let requests = counter.open("tenant:42:requests")?;
assert_eq!(requests.increment(2)?, 3);
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Operations

- `get` on a missing key returns `None` and installs nothing.
- `increment`, `decrement`, `reset` and `open` install a missing key at zero.
- `increment` and `decrement` return the value after the update. A negative
  amount is `Error::NegativeAmount`, refused before the key is installed. An
  update that would leave the signed 64-bit range is `Error::Overflow` and
  the value is kept.
- `reset` sets the counter to zero.

The `Counter` methods take the key and resolve it on every call: they check
and hash it and look it up in the name directory, without a lock once the
key exists. `Counter::open` resolves a key once and returns a
`CounterHandle` with the same operations and no key. A handle is cheap to
clone and derefs to its `Orbital<i64>`, so a caller can `wait` for the
counter to change or `notify` its waiters. Writing through the Orbital
bypasses the checks: `fetch_add` wraps.

## One namespace

A counter key and an Orbital name are the same thing. A value declared
elsewhere as

```rust
use orbitive::atomic::orbital;

orbital! {
    pub Hits: i64 = 0, "hits", kept
}
```

is the counter `"hits"`: an increment through either is seen by the other.
A key already declared with another type, or as an `i64` that is not kept,
is `Error::NotCounter`, which holds the table's own error and with it the
value's id.

## Bounds and lifetime

- A key is at most `ATOMIC_NAME_MAX` bytes (default 112). An application
  that needs longer keys sets `ORBIT_ATOMIC_NAME_MAX`, as `orbit-atomic`
  describes; it is the same for every declared name in the fleet. An empty
  key is `Error::KeyEmpty`, a longer one `Error::KeyTooLarge`.
- Counters share the Orbitals table's capacity, `ORBIT_ATOMIC_CAPACITY`
  (default 4,096), with every other value of the fleet. A full table is
  `Error::Full`, never an eviction. Each distinct key uses one slot until
  the table is reset, so keys belong to a bounded set, not one per request.
- Counters are declared kept: the table owner's `Orbitals::reset_all`
  keeps their values. A `CounterHandle` belongs to the table's epoch like
  any Orbital handle; after a reset, open the key again.

There is no operation that clears every counter: the table holds other
values too. Set each counter to zero with `reset`.

An in-memory fleet gets the same counters on the heap, shared by everything
that holds the same `Fleet`.

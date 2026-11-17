# orbit-counter - Architecture

`counter` is a framework-neutral keyed counter primitive for a same-host
Orbit fleet. It owns checked counter arithmetic over keys chosen at run
time; storage, naming, capacity and lifecycle are `orbit-atomic`'s, and
naming policy, metrics export and language bindings belong above it.

## A counter is a kept `i64` Orbital

There is no counter table. A key is an Orbital name, resolved by the
Orbitals name directory (kind `170`) to a slot of the Orbitals table (kind
`174`). The former counter segment, kind `230`, is retired: nothing opens
it, and a fleet that ran an earlier build keeps it until it is cleared.

- One namespace, no prefix. A typed `orbital!` declaration and a key of the
  same name are one value, which is how a language binding that counts by
  key and a Rust declaration meet. The cost is that a key can collide with
  a value of another type or lifetime; that is refused as `NotCounter`,
  never coerced.
- Kept. Counters survive the owner's `Orbitals::reset_all`, as the earlier
  counter table did across restarts when its owner reused it.

## Paths

- `Counter::get` is `Orbitals::find_kept`: no install, no lock after the
  process has attached the directory. A miss is ordered before any
  declaration still in flight.
- `increment`, `decrement`, `reset` and `open` go through
  `Orbitals::declare_kept`: lock-free when the key exists, the directory's
  structural lock when it is installed. The amount is checked first, so a
  refused amount installs nothing.
- `CounterHandle` holds the `Orbital<i64>`. Its operations do no hashing,
  no lookup and no validation of the key. Checked updates are a
  compare-exchange loop: load, `checked_add`/`checked_sub`, exchange. A
  refused update writes nothing and wakes no waiter. The exchange is the
  Orbital's, so it is sequentially consistent and wakes a parked `wait`.

## What was removed and why

`reset_all` and `unlink` are gone. With one shared table there is no set of
"all counters" to clear without also clearing every other value of the
fleet, and the table's reset and unlink belong to whoever owns the
Orbitals lifecycle. `COUNTER_CAPACITY`, `COUNTER_KEY_MAX`,
`COUNTER_STATE_KIND` and the `ORBIT_COUNTER_*` geometry went with the
table.

`Counter::new` takes `Orbitals`, not a `Fleet`. Every `Orbitals::new` of
one fleet in a process maps the same table, so a fleet constructor would be
harmless in effect, but it would hide that counters live in, and are reset
with, a table someone else owns.

# orbit-cache

`orbit-cache` keeps a bounded L1 metadata cache in each process and propagates
mutations between processes in the same Orbit fleet. Applications normally use
it through the [Orbitive](https://github.com/iadev09/orbitive) facade as
`orbitive::cache`; direct package access remains available.

Each logical store has its own process-local metadata L1. One mutation ring
carries `Put`, `Delete`, and `Reset` between peers. Encoded values live once in
the fleet's existing arena; a hit pins that exact body without copying it.

```text
worker 0: models L1 + responses L1 --\
worker 1: models L1 + responses L1 ---- mutation ring -> shared arena
worker 2: models L1 + responses L1 --/
```

The mutation ring is bounded coherence transport; the arena is a bounded byte
heap, not an authoritative source.
A process starts with an empty L1 and must poll the connection to apply remote
mutations. A normal miss may fall through to another store. Detected mutation
loss places the L1 in `ResyncRequired` until the caller performs explicit
recovery.

```rust
use std::sync::Arc;

use orbitive::cache::{Cache, CacheRead};
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
let cache = Cache::new(fleet)?;
let store = cache.open_default_store()?;

store.put(b"user:42", b"encoded value", None)?;

match store.read(b"user:42") {
    CacheRead::Hit(entry) => assert_eq!(&entry.value[..], b"encoded value"),
    CacheRead::Miss => { /* consult an authoritative source */ }
    CacheRead::ResyncRequired => { /* restore coherence */ }
}

# Ok::<(), Box<dyn std::error::Error>>(())
```

The cache provides named stores, bounded LRU state, TTL expiry, pinned arena
values, fleet-wide last-write-wins ordering, and explicit payload-overwrite and
lag reporting. On Linux, FreeBSD, and macOS 14.4 or later, `Cache::event_fd`
supplies a readiness
signal; consumers drain it and then call `Cache::poll`.

It does not provide persistence, serialization, an async runtime, concrete
backing-store drivers, increment/decrement, or lock semantics.

Value and record capacity come from the existing arena geometry (16 MiB per
record with its 64 MiB default). Idle L1 metadata does not hold pins. A returned
`entry.value` owns its pin until the last `Bytes` clone drops; it remains valid
across invalidation. Kernel-held reader ownership allows SIGKILL recovery
without timers or clearing another live process' pins.

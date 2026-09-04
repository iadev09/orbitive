# orbit-cache

`orbit-cache` keeps a bounded L1 byte cache in each process and propagates
mutations between processes in the same Orbit fleet. Applications normally use
it through `orbitive::cache`; direct package access remains available.

Each logical store has its own process-local L1. One shared mutation ring and
one shared payload ring carry `Put`, `Delete`, and `Reset` operations between
peers.

```text
worker 0: models L1 + responses L1 --\
worker 1: models L1 + responses L1 ---- mutation ring + payload ring
worker 2: models L1 + responses L1 --/
```

The rings are bounded transport, not the cache heap or an authoritative source.
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

The cache provides named stores, bounded LRU state, TTL expiry, multi-slot
values, fleet-wide last-write-wins ordering, and explicit payload-overwrite and
lag reporting. On Linux and FreeBSD, `Cache::event_fd` supplies a readiness
signal; consumers drain it and then call `Cache::poll`.

It does not provide persistence, serialization, an async runtime, concrete
backing-store drivers, increment/decrement, or lock semantics.

Default ring dimensions permit values up to 4 MiB, but a value that large
occupies its writer's complete payload retention window. Capacity should be
chosen for expected value size, write rate, reader latency, and fleet size.

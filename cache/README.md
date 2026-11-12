# orbit-cache

`orbit-cache` provides bounded caches for processes in the same Orbit fleet.
Applications use `orbitive::cache` through the
[Orbitive](https://github.com/iadev09/orbitive) facade.

| API | Where hits read | When peers see writes |
| --- | --- | --- |
| `Cache` | Each worker's local values | After applying the mutation |
| `SharedCache` | The fleet's shared index and value slots | As soon as the write completes |

## Replicated cache

Each logical store has its own LRU. The notified mutation ring carries Put,
Delete and Reset. A Put references an arena payload, which the receiving
worker copies into L1. Hits read local bytes and check TTL. Each process
starts empty at the current ring heads, and a driver applies later mutations.

A normal miss may fall through to a backing store. Detected mutation loss
places the connection in `ResyncRequired` until explicit recovery.

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

Each store has an entry limit. All stores on one connection share a resident
payload byte limit (64 MiB with the default arena geometry). Direct users can
set it with `Cache::with_byte_capacity`. A value returned to a caller remains
valid across eviction or invalidation.

On Linux, FreeBSD and macOS 14.4 or later, `Cache::event_fd` supplies a readiness
signal. Register it before the first drain, then drain coalesced signals and
call `Cache::poll`. Direct callers may drive polling explicitly. The cache
provides fleet-wide last-write-wins revisions, explicit payload-overwrite
and lag reporting, and store-scoped invalidation.

## Shared cache

Open `SharedCache::new(fleet)`, then `open_store(b"store-name")`. Reads use
the shared index and return pinned bytes. TTL, deletion, and clearing a named
store apply to the same records for all workers. The bounded arena can evict
unpinned values under pressure. Its records survive a worker restart while
the segment is retained. Shared stores do not need a mutation listener.

## Mutation diagnostics

On Unix, `observer::CacheObserver` maps existing mutation history without
joining a fleet or opening arena values. Its diagnostic snapshot reports
retained Put candidates after applying retained Delete/Reset mutations,
including loss and concurrent-change accounting. This is not an authoritative
list of worker L1 entries; missing retained history cannot prove a cache miss.
Use an existing arena reader capability only when selecting a value, and
handle exact-address reuse as unavailable rather than replacement data.

## Benchmarks

With `orbitive-cli` installed, run `cargo bench -p orbit-cache --bench get`
to compare warmed hits and misses across 16 independent processes on Unix.
Each phase performs five million reads per worker against the same 256 keys.
Setup and mutation draining happen before timing. Output reports fleet-wide
gets per second and summed worker CPU time per get. Each run removes its
temporary fleet after all workers exit.

Run `cargo bench -p orbit-cache --bench replicated_write` to measure the
replicated cache with concurrent writers on one key.

Run `cargo bench -p orbit-cache --bench fleet_write -- --rate 128` to
compare both modes at 128 puts per worker per second. Sixteen processes
write 128 distinct keys each, using 256-byte values without expiry. The
coordinator releases one write per worker per tick and rotates dispatch
order. Each mode runs three times.

Throughput uses the scheduled window, extended if puts complete late. CPU
per put includes all workers and the replicated readiness bridge. Receiver
probe CPU is measured separately and subtracted. Replicated visibility is
timed from put entry through the drain that made the value readable on the
last peer. SHM visibility uses put completion and verifies peer reads after
timing. The benchmark reports dispatch latency, ring loss, and writes
whose visibility could not be verified on every peer.

Pass `--hot-key` after `--rate 128` to make all writers update the same key.
Each value identifies its writer and round. After the last put, every worker
reads the key and reports its value and revision. The coordinator checks the
highest published revision for replicated mode and the last arena generation
for SHM mode. The run fails if any worker reads a different value or revision.

### Arena reservation

Run `cargo bench -p orbit-cache --bench fleet_write --features arena-timing -- --rate 128`
to include lock acquisition and slot scan timings.

When a record fits in the unwritten portion of the arena's first pass,
reservation advances the cursor directly. After wrap, reservation scans for
overlapping records and protects pinned bytes.

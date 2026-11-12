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

### Reference run

macOS arm64, 16 worker processes, three runs per mode. Each row reports the
median across the runs. The read phases performed 80 million gets each.

| Mode | Hit/s | CPU/hit | Miss/s | CPU/miss |
| --- | ---: | ---: | ---: | ---: |
| Replicated | 19.1 M | 767 ns | 28.3 M | 522 ns |
| SHM | 133.1 M | 105 ns | 202.9 M | 72 ns |

Each write phase completed 2,048 puts at an offered load of 2,048 puts/s.

| Keys | Mode | CPU/put | Put p99 | All-peer visibility p99 |
| --- | --- | ---: | ---: | ---: |
| Distinct | Replicated | 479 µs | 2.76 ms | 4.07 ms |
| Distinct | SHM | 185 µs | 1.86 ms | 1.86 ms |
| Hot | Replicated | 462 µs | 2.43 ms | |
| Hot | SHM | 197 µs | 1.58 ms | |

Every distinct-key write was visible on all 15 peers. Every hot-key run
converged on the final value and revision on all 16 workers. The replicated
runs recorded zero ring loss. Replicated miss throughput ranged from 15 M
to 74 M gets/s. Write p99 also varied across runs.

### Linux verification

Linux x86_64, 16 worker processes, three runs per mode. Each row reports the
median across the runs. Every run completed 2,048 puts at 2,048 puts/s.

| Keys | Mode | CPU/put | Put p99 | All-peer visibility p99 |
| --- | --- | ---: | ---: | ---: |
| Distinct | Replicated | 289 µs | 418 µs | 589 µs |
| Distinct | SHM | 63 µs | 737 µs | 737 µs |
| Hot | Replicated | 255 µs | 413 µs | |
| Hot | SHM | 55 µs | 364 µs | |

With distinct keys, SHM used 78% less CPU per put. Its put p99 was 737 µs
versus 418 µs for replicated mode, and its all-peer visibility p99 was
737 µs versus 589 µs.

All 15 peers observed every distinct-key write. All 16 workers converged on
the final value and revision in every hot-key run. The replicated runs
recorded zero ring loss.

#### Arena first-pass reservation

Run `cargo bench -p orbit-cache --bench fleet_write --features arena-timing -- --rate 128`
to include lock acquisition and slot scan timings. Before the optimization,
`flock` accounted for a median 93.6% of put time among the slowest 1% of SHM
writes. The slot scan took a median 17.7 µs across SHM puts, within a
median 22.3 µs lock hold.

When a record fits in the unwritten portion of the arena's first pass,
reservation advances the cursor directly. After wrap, reservation scans for
overlapping records and protects pinned bytes.

The Linux x86_64 comparison used fresh arenas, distinct keys, 16 workers,
and 2,048 puts/s. The before and after builds used the same instrumentation.
Each p99 below is the median of six runs per build.

| Mode | Phase | Before | After |
| --- | --- | ---: | ---: |
| SHM | Put p99 | 651 µs | 167 µs |
| SHM | Flock p99 | 601 µs | 141 µs |
| SHM | Slot scan p99 | 34.4 µs | 0 µs |
| Replicated | Put p99 | 434 µs | 201 µs |
| Replicated | Flock p99 | 353 µs | 136 µs |
| Replicated | Slot scan p99 | 28.5 µs | 0 µs |

All runs sustained the target rate with zero ring loss. Every write was
visible on all 15 peers.

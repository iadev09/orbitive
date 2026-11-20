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

A normal miss may fall through to a backing store. Take `Store::fill_token`
before reading it and give the value to `Store::fill` with the token: the store
keeps it with its expiry unless a mutation it applied since is newer, and
publishes nothing (see [Fenced fills](#fenced-fills)).
Detected mutation loss places the connection in `ResyncRequired` until explicit
recovery.

```rust
use std::sync::Arc;

use orbitive::cache::{Cache, CacheRead};
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
let cache: Cache = Cache::new(fleet)?;
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
call `Cache::poll`; with an async reactor, clear its readiness before each
drain, because the fd signals once per drain. Direct callers may drive polling explicitly. The cache
provides fleet-wide last-write-wins revisions, explicit payload-overwrite
and lag reporting, and store-scoped invalidation.

## Shared cache

Open `SharedCache::new(fleet)`, then `open_store(b"store-name")`. Reads use
the shared index and return pinned bytes. TTL, deletion, and clearing a named
store apply to the same records for all workers. The bounded arena can evict
unpinned values under pressure. Its records survive a worker restart while
the segment is retained. Shared stores do not need a mutation listener.

## Tags

Both cache modes support tags. `put_tagged` stores a value with a set of
tags; `invalidate_tags` makes every entry carrying any of them a miss, as one
operation, for every process in the fleet. Untagged operations are unchanged
and are separate entry points: `put` never carries tags, and a read returns a
hit or a miss either way.

```rust
use std::sync::Arc;

use orbitive::cache::{Cache, CacheRead, SharedCache};
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);

let cache: Cache = Cache::new(fleet.clone())?;
let store = cache.open_default_store()?;
store.put_tagged(b"user:42", b"profile", None, &["users", "team:7"])?;
store.put(b"config", b"untagged", None)?;
store.invalidate_tags(&["team:7"])?;
assert_eq!(store.read(b"user:42"), CacheRead::Miss);
assert!(matches!(store.read(b"config"), CacheRead::Hit(_)));

let shared = SharedCache::new(fleet)?.open_store(b"sessions")?;
shared.put_tagged(b"session:1", b"state", None, &["users"])?;
shared.invalidate_tags(&["users"])?;
assert_eq!(shared.read(b"session:1"), CacheRead::Miss);

# Ok::<(), Box<dyn std::error::Error>>(())
```

- An entry carries at most `MAX_TAGS` (16) tags, and an invalidation names
  at most as many. A replicated put also needs room for its tags in the
  mutation frame beside the store name and key. A larger set is refused with
  `Error::TooManyTags` and the number that would fit; nothing is truncated.
  An invalidation with no tags is refused with `Error::TagsEmpty`.
- Tags are scoped to their store: the same tag name in two stores is two
  tags. They are compared as 64-bit keyed hashes, so two names that collide
  behave as one tag. That can only invalidate more, never less.
- An invalidation covers entries written before it. A put that completes
  after an invalidation is not affected by it, as a put after a delete is not.
- Replicated stores apply an invalidation to the caller's own view at once
  and to other workers when their listener drains it, like any mutation. A
  delayed older put carrying an invalidated tag is not admitted. Reading a
  tagged entry costs the same as reading an untagged one; an invalidation
  costs each worker the removal of the entries it covers.
- Shared stores check a tagged entry's tags against a shared table of tag
  invalidations when it is read; untagged reads do not consult it. The table
  is bounded: when it is full, an old invalidation is widened to the tags
  that share its slot rather than forgotten, which can cause extra misses for
  old tagged entries but never a stale hit. Invalidated entries stay in the
  shared arena, reading as misses, until they are overwritten or evicted.

## Fenced fills

A caller that reads an authoritative source, such as a database, on a miss
and caches the result under tags needs a fence: if a tag invalidation ran
between the start of its read and its store, the result may be stale, and
nothing would remove it. Both modes offer the same fence with the same names:
`fill_token` before the read, `fill_tagged` with the token after it, and a
typed outcome.

```rust
use std::sync::Arc;

use orbitive::cache::{Cache, Fill, SharedCache};
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
let cache: Cache = Cache::new(fleet.clone())?;
let replicated = cache.open_default_store()?;
let shared = SharedCache::new(fleet)?.open_store(b"queries")?;

// Reader, on a miss: the token first, then the authoritative read.
let token = shared.fill_token();
let rows = b"rows read from the source".to_vec();
match shared.fill_tagged(token, b"query:7", &rows, None, &["users"])? {
    Fill::Kept => {}
    // Not an error: serve `rows` to this caller and cache nothing.
    Fill::Invalidated | Fill::Superseded | Fill::Skipped => {}
}

// Writer: change the source first, then invalidate its tags.
shared.invalidate_tags(&["users"])?;

// The replicated store takes the same calls.
let token = replicated.fill_token();
assert_eq!(
    replicated.fill_tagged(token, b"query:7", &rows, None, &["users"])?,
    Fill::Kept
);

# Ok::<(), Box<dyn std::error::Error>>(())
```

- **The boundary.** A `FillToken` is a position on the store's invalidation
  clock: the mutation sequence for a replicated store, the tag table's clock
  for a shared one. It is the last position the clock had handed out when
  the token was read. An invalidation at or below the token advanced the
  clock before that read, so its writer had already changed the source and a
  read that starts after the token sees the change: the fill is kept. An
  invalidation of one of the fill's tags above the token refuses it. An
  invalidation concurrent with the token read falls on one side or the other
  by whether that read observed its advance of the clock; neither side admits
  a value the read could have missed.
- **What the caller must order.** The writer changes the source before it
  calls `invalidate_tags`. The reader takes the token before its read starts,
  including before any snapshot its read uses: a read inside a transaction
  that began before the token can miss a change the token says it saw.
- **The outcome.** A refusal is `Ok`, not an error: `Fill::Invalidated` (the
  fence), `Fill::Superseded` (a replicated view holds or fenced a newer write
  of the key) or `Fill::Skipped` (already expired, no room, or a view that
  needs resync). `Err` is kept for arguments the store refuses, as
  `put_tagged` refuses them. `invalidate_tags` returns the token at the
  position it took.
- **Shared stores.** A kept fill is a record every process reads at once.
  It is stamped with the token, so an invalidation above the token hides it
  whether it lands before or after the fill. The fence orders fills against
  tag invalidations only: like a put, a fill replaces whatever its key holds,
  and a concurrent `put` or `delete` of that key is not ordered against it.
  Writers that change the source invalidate a tag; they do not merely put.
- **Replicated stores.** A fill is local: it stays in the filling process and
  publishes nothing. Every process misses once and fills its own view from
  its own read. Invalidation still reaches all of them. A fill is refused
  when its view has already applied an invalidation above its token, and
  removed when the view applies one later; until then that process may
  return it, as it may return any value a mutation not yet drained replaces.
  A replicated fill is also ordered against writes of its key.
- **Tokens name their clock.** A token from the other mode, or from a shared
  tag table recreated since, is refused as `Fill::Invalidated`. One token is
  valid for every store on the same clock.
- **Untagged fills.** `Store::fill` keeps a backing value untagged, and no tag
  invalidation reaches it. A value the backing store holds with tags is
  filled with `fill_tagged` and the same tags; if the backing store cannot
  report a value's tags, do not fill keys that are written with tags. Shared
  stores have no untagged fill: with no tag there is nothing to fence, and it
  would be a put that no write is ordered against.
- **Cost.** Reading a token is one atomic load. A shared fill costs a tagged
  put plus one floor lookup per tag; a replicated fill is a local insert.
  Reads, `put` and `put_tagged` do not change.

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
reservation advances the cursor directly. After wrap, a byte-range index
finds overlapping records and reservation skips pinned bytes.

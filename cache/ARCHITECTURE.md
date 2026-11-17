# orbit-cache - Architecture

The cache exposes two storage strategies. Both return immutable owning bytes
and leave serialization and backing stores to the caller.

## Replicated stores

`Cache` owns one notified mutation ring, one cursor, and the existing payload
arena. Each named `Store` has a bounded LRU with worker-local values. A Put
writes a payload to the arena and publishes its exact slot and generation.
The writer installs the original bytes locally. Other workers copy the arena
payload once as their listener applies the mutation. A hit checks expiry and
clones the local `Bytes` handle.

Cold starts begin empty at the current mutation heads. Mutation identity is
`(store, key)`. A delayed older Put cannot resurrect an expired, deleted or
evicted entry, and no stale fill is kept.

### Coherence of a local view

The L1 holds only values. What it has forgotten is kept in two places: a
store-wide floor, raised by Reset and recovery, and fences, one per hash
bucket of keys (the store's entry capacity rounded up to a power of two),
raised to the revision of whatever leaves the store for any other reason:
a capacity or byte-budget eviction, a Delete, an expiry, a Put whose payload
is unavailable or whose value cannot be kept. Call every Put or Delete for a
key that the view has applied or refused *observed*. The invariant is:

1. A key with a slot holds a value at least as new as every observed mutation
   for it. A mutation's slot has a revision at or above theirs; a fill's slot
   has a token sequence at or above theirs, because the fill saw each of them
   in its backing read.
2. For a key without a slot, every observed mutation has a sequence at or
   below `max(floor, fence(key))`.

A mutation for a key is admitted when it is above the floor and newer than
the key's slot, or, with no slot, above the key's fence. A fill at token `t`
is admitted when `t` is at or above the floor and newer than the key's slot,
or, with no slot, at or above its fence. Whatever removes a slot first records
its revision in the key's fence, or under the floor, so 2 holds again; a
refused mutation is at or below the slot or fence that refused it. A read
therefore never returns a value older than a mutation the view has applied or
refused for that key.

A key's own slot is its fence, so forgetting one key neither hides nor refuses
another resident: a read consults no floor. Until 2026-11-17 one floor did
both jobs. Each eviction raised it to the evicted entry's revision, which hid
every resident at or below it however recently it was read. Fills carry the
newest sequence, so with no writes all of them share one, and the first
eviction hid the whole store: a read-through workload over 100,000 Zipf keys
at capacity 10,000 hit 0.9 % of reads where a plain LRU hits 72 %. The view
now matches that LRU. A floor raised by eviction cannot simply be split into
an admission floor that clears the key of each mutation it refuses: a resident
above that floor would be cleared by a delayed mutation below it, and the next
delayed mutation between the two would then be admitted.

A Delete removes the slot and fences the key; it inserts nothing, so a run of
Deletes for keys this view does not hold evicts no value. Sharing a bucket only
refuses more, which costs a miss, never a stale hit. Reset removes every slot
it covers at once and keeps those above it, which are newer; a Reset at or
below the floor has nothing left to remove. Resync and recovery empty the
view; transport reset also clears floor and fences, since the ring restarts
its sequences. A store has one lock: per-key shards were tried (2026-11-13)
and removed, since they cost every uncontended hit a second hash. Fences hash
the key again, but only on the mutation path and on expiry.

### Backing fills

A caller with a backing store fills a cold key from it without publishing.
`Store::fill_revision` reads the last allocated mutation sequence before the
backing read; `Store::fill` keeps the value at that revision, with the
mutation id at zero. A writer updates its backing store before it allocates,
so every mutation up to that sequence is already in what the read returns,
and any mutation allocated later is newer than the fill and replaces it.

A fill is refused when this view already holds the key at a revision at or
above it, or when the floor or the key's fence is above its sequence. A floor
or fence at exactly that sequence leaves it current: unlike a delayed
mutation, the fill has seen every write it covers. For the same reason a
mutation at the token's own sequence does not replace the fill. A value the
store could not hold, one already expired, or an incoherent view keeps
nothing. A fill charges the connection's byte budget and may reclaim from
other stores, so it holds the connection cursor like a put and a drain:
before 2026-11-17 it did not, and two concurrent fills could push the budget
past its limit (a debug build panicked in the reservation) or lock two stores
in opposite orders while reclaiming. Shared stores have no local view and are
not filled.

### Tags

A put may carry tags; `invalidate_tags` makes every entry carrying any of
them a miss in every view, as one mutation. Tagged and untagged operations
are separate entry points (`put`/`put_tagged`, `fill`/`fill_tagged`); reads
do not change.

**Hashes.** A tag travels and is kept as a `u64`: the first eight bytes of
BLAKE3 keyed by a key derived from the store name (`orbit cache tag v1`),
with zero mapped to one so the shared table can use zero as empty. A set is
sorted and deduplicated. Two tags that collide are one tag to this crate, so a
collision only invalidates more. At most `MAX_TAGS` (16) per entry and per
invalidation, and fewer when the frame has no room left (below); a larger
set is refused with `Error::TooManyTags`, never truncated.

**Invariant.** Facts 1 and 2 above stay as they are and gain two more. Call
an invalidation of tag `t` this view has applied *observed*.

3. Every resident slot carrying `t` is newer than every observed
   invalidation of `t`: a mutation slot's revision is above it, a fill slot's
   token is at or above it (the fill read the backing store after the
   invalidation reached it, below).
4. Every observed invalidation of `t` has a sequence at or below
   `fence(bucket(t))`, the same bucket fences that keys use, indexed by the
   tag's hash.

**Eager removal, not a read-time check.** Applying an invalidation at `R`
raises each tag's bucket fence to `R` and walks a per-store index of resident
keys by tag (`tag -> set of keys`, sharing the LRU's `Arc<[u8]>` key), removing
each slot older than `R` and fencing its key at the slot's own revision, as
any other removal does. That keeps 3. A put carrying tags is admitted by the
key rule first, exactly as an untagged one; only then is it checked against
its tags' bucket fences. A put at or below one is a mutation this view has
observed and that an observed invalidation covers, so the key is forgotten at
the put's sequence: its old slot is older than the put and goes, and fact 2
holds with the put's sequence in the key's fence. Checking the tag fence
before the key's own slot would reopen the hole described under *Coherence of a local view*: a refused
delayed put would clear a newer resident, and the next delayed put between
the two would be admitted. A fill whose tags' fences are above its token is
refused and changes nothing, like any refused fill. That keeps 4 and 3 for
everything admitted afterwards.

The alternative, checking each tag's floor when a tagged entry is read, was
rejected on three counts. Floors that decide what a resident is would have to
be exact per tag and kept as long as any resident they cover, which a bounded
table cannot promise without hiding residents wholesale when it overflows; an
invalidated resident would keep its bytes and LRU place until read or
evicted; and every tagged read would pay a lookup per tag. Eager removal
leaves the read path untouched for every entry, tagged or not, and the floors
it needs only refuse delayed writes, which buckets do safely: a collision
refuses a reordered put or a fill racing an invalidation of another tag,
which costs a miss, never a stale hit. Sharing the key fences means a store
that never sees a tag carries no tag state at all, and a tagged store carries
no second table. A tag fence that also fences some absent key refuses only a
mutation for that key already older than an applied invalidation.

**Cost.** A read is unchanged: the slot's tag list is not touched on a hit.
A tagged put inserts its key into one set per tag; a removal (eviction,
Delete, expiry, Reset, overwrite) of a tagged slot unlinks it from those sets,
and of an untagged slot does nothing new. An invalidation in each view costs
one bucket fence per tag plus the work of removing each resident it covers,
under the store's lock; no view walks its key space. The index holds at most
`capacity * MAX_TAGS` key references and drops a tag's set when it empties.

**Paths.**

- *Ring apply order.* A drain sorts its batch by revision, so within a batch
  an invalidation follows the puts it covers and removes them. A put published
  before the invalidation but drained in a later batch meets the fence.
- *A local tagged put ahead of the cursor.* The writer installs its put at
  its own revision while an older invalidation may still be in the ring; when
  that invalidation is applied the resident is newer and stays. An
  invalidation allocated after the put but applied first, locally by another
  thread, raised the fence above the put, so the install forgets the key.
- *Local invalidation.* `invalidate_tags` applies at once to the writer's
  view, like `reset`; its own frame arrives later and is idempotent.
- *Fill.* `fill_tagged` carries the tags the backing store holds for the
  value. The contract mirrors puts: a writer applies an invalidation to its
  backing store before it calls `invalidate_tags`, so a fill whose token is
  at or above the invalidation's sequence read a backing store the
  invalidation had already reached. A token below a tag's fence is refused;
  an invalidation applied after a fill removes it when the token is below the
  invalidation (the same `precedes` rule as a mutation at the token's own
  sequence). A value filled without tags is not reached by a later
  invalidation: the cache cannot know tags the caller did not give it. A
  backing store that does not record tags cannot fill a tagged entry
  faithfully, and its caller must not fill keys it writes with tags; that is
  a contract on the caller, stated in the README, not something the view can
  check.
- *Delete, expiry, eviction, overwrite.* Each removes the slot through one
  path that unlinks its tags and fences its key, as before.
- *Reset.* Raises the floor and removes the slots it covers, unlinking them.
  An invalidation at or below the floor has nothing left to remove: every
  slot it could cover is gone, and the floor refuses every put it covers.
- *Resync and recovery.* The view and its index empty; the floor at the
  recovery cut refuses every put an invalidation before the cut covered, and
  a fill at the cut read a backing store every such invalidation had reached.
  Transport reset clears fences and index with the slots, as sequences
  restart.

## Shared stores

`SharedCache` opens one keyed arena dedicated to current cache state. Named
`SharedStore` handles derive a keyed digest from the store name and key. The
arena index locates the current slot and a pin keeps its bytes valid after
invalidation or eviction. A hit reads the indexed record directly. A successful
Put or Delete is visible to subsequent readers in other processes without
notification or polling.

Reset retires only records carrying the store's namespace tag under the
arena writer lock. Arena byte and index pressure apply across all shared
stores. The arena is retained across worker exits and is reopened by new
workers. Its pins use the same kernel-held reader rows and death recovery as
the payload arena. The shared table has no cursor, listener, or replay phase.

### Tags in the shared table

The shared table has no per-process index to walk, so a tag's invalidation is
checked when a tagged record is read. The arena record layout is unchanged:
a tagged record carries its tags in the record's second span (the arena's
`encoded` span, tag byte `1`), and an untagged record has none, so the arena
already tells the read which kind it holds from a field the pin loaded.

**Clock and floors.** A separate table of tag floors (kind 171) holds a
monotonic clock in its header and, per tag, the clock value of its last
invalidation. `invalidate_tags` takes the table's writer lock (a process
mutex, then the segment's `flock`), advances the clock once to `v`, and raises
each tag's floor to `v`. A tagged put reads the clock before it writes and
stores it in the span with the tags: `[instance][clock][tag]...`. A tagged
record is live while its clock is at or above each of its tags' floors. The
clock read is the put's place in the order: a put that read the clock before
an invalidation advanced it is covered by it, even if its record lands later,
and one that read after is newer, as a put after a Delete is.

**Bounds.** Floors live in sets of seven ways, 128 bytes each: an overflow
floor and seven tag hashes in the first 64, seven floors in the second. A tag hashes to one set
(`hash & (sets - 1)`). An invalidation raises the tag's way, claims an empty
one, or, in a full set, evicts the way with the lowest floor after raising
the set's overflow floor to it. A tag's effective floor is the larger of the
set's overflow floor and its own way's, if it has one, so an evicted floor is
never forgotten: it is widened to the other tags of its set that have no way,
which can only invalidate more. Sets default to `ARENA_SLOTS / 4` (1,024 sets,
7,168 exact floors, 128 KiB), `ORBIT_CACHE_TAG_SETS` overrides; peers built
otherwise are refused at open.

**Ordering.** The writer, under its lock, raises the overflow floor before it
writes a way's floor and that way's tag last; a reader loads a set's tags,
then the matched way's floor, then the overflow floor. A reader that still
sees an evicted tag reads either its old floor or the newer one written over
it, and the overflow floor raised before either; a reader that sees the new
tag sees the floor written before it. A concurrent read may miss an
invalidation in progress, never one that completed before it began.

**Lifecycle.** The floors and the records must not part: a table recreated
under retained records would forget their invalidations. The table's header
holds a random instance id written at creation, and every tagged record
carries it; a tagged record of another instance reads as a miss. A recreated
arena takes its records with it, so nothing further is needed in that
direction. `SharedCache::unlink` removes both segments. Arena `reset` and
store `reset` do not touch the floors.

**Cost.** An untagged read is the arena read it was: the span check is a
field of the pinned entry. A tagged read loads the instance id it compares
against (fixed for the handle's life, kept in the handle) and, per tag, one
set's tag words, one floor and the overflow floor, all lock-free. An
invalidation costs one lock and one set update per tag; dead records stay in
the arena, read as misses, until the cursor or an overwrite reaches them, as
expired ones do.

Both strategies check TTL at read time. At `now >= expires_at_ms` the result is
a miss. An already returned buffer remains valid until its last owner drops.

## Revisions and loss

The notified mutation ring owns a shared AtomicU64 semantic-version allocator.
A writer allocates immediately before publishing its mutation. `CacheRevision`
orders first by that sequence, then mutation NetId64 as a deterministic tie
breaker. Per-node ring counters and wall-clock time do not define mutation order.

Put commits bytes before its descriptor. Arena pressure may evict unpinned
bytes even while their mutation remains retained; polling reports the exact
`(store, key)` in `payload_unavailable`. No payload history is cursor-consumed.

Lost/malformed mutation frames disable all registered L1s as
`ResyncRequired`: the missing store may be unknowable. Explicit
`recover_from_backing` moves the cursor to current heads, empties the local values,
records the current shared sequence as the floor, and restores coherence.
Without an authoritative recovery source, the caller must not silently
accept an incomplete view. Unknown-store mutations are reported, not applied
to another namespace.

## ABI and bounds

Kind 173 carries protocol v4 mutation descriptors (tags). Kind 193 is the
shared payload arena. Kind 172 uses the same arena ABI for the keyed cache
table, whose tagged records carry a tag span; kind 171 holds its tag floors.
Retired: 194 (protocol v3, no tags), 180 (shared table whose records an older
reader would serve without their tags' floors), and earlier 200/201 and 242.
None is reopened, resized or automatically removed. Every fleet peer must use
the same compiled geometry and protocol. Old/new binaries on different kinds
do not exchange cache mutations; deploy them as a coherent generation. The
ring's frame shape changed without its segment geometry changing, so kind 194
would have opened; it is retired anyway so that a v3 peer and a v4 peer never
read each other's frames as loss and force each other into resync.

`CacheLayout` configures only the mutation ring kind/spec. Its default is
1,024 per-node frames of 1,024 bytes. A v4 header is 34 bytes:

```text
version u8 = 4 | store_len u16 | key_len u16 | tag_count u8 | expires_at u64
| arena slot u32 | arena generation u64 | value_len u64
| store | key | tag_count x u64
```

Put carries 0..=16 tags, Delete and Reset none, InvalidateTags (frame kind 4)
an empty key and 1..=16 tags with every other field zero. Store name, key and
tags share the remaining 990 bytes; a tagged put whose key leaves no room for
its tags, or an invalidation whose store name leaves none, is refused with
`TooManyTags` and the number that would fit. Both names are nonempty.

Values use `ARENA_RECORD_MAX`, one quarter of `ORBIT_ARENA_BYTES` (16 MiB with
the existing 64 MiB default). `ORBIT_ARENA_SLOTS` defaults to 4,096. Reader-row
count is the fleet's declared capacity, validated at attach; the arena shares
one row per process per arena. Kind 172 has its own arena byte and slot budget
with the same compile-time geometry. Replicated values have a separate resident
byte budget per connection.

Mutation capacity bounds reader lag, not resident values. The arena's byte
and index pressure may cause honest cache misses; pins are short-lived read
ownership, not a guarantee of residency.

## Lifecycle

`Cache::new` subscribes to future mutations. `replay_retained` is diagnostic,
not a complete snapshot. `reset_transport` resets only the mutation ring
under caller-owned quiescence, then realigns local values; it must not reset the
arena because that heap may also hold non-cache bodies.

Normal writes use notified publication on supported Unix platforms.
`Cache::event_fd` exposes the mutation readiness bridge; notifications may
coalesce. Consumers drain the fd and call `poll`. No arena read or pin drop
creates cache coherence traffic.

Serialization, configuration, concrete backing drivers, persistent storage,
atomic counters, locks and authorization remain outside this crate.

## Diagnostic inspection

`observer::CacheObserver` reads one bounded retained window per node from an
existing mutation ring. It reports loss, malformed frames, pending counters
and changes during the scan, then applies Put/Delete/Reset in revision order.
The observer describes replicated transport only. It cannot infer L1 residency
or inspect the shared cache table. Inspection decodes the same wire protocol without
the producer's compiled slot-count bound; selected addresses are checked by
the arena reader against the persisted geometry. The normal runtime decoder's
capacity check remains unchanged. No observer writes mutations or pins bodies.

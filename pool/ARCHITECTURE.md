# orbit-pool - Architecture

One segment per fleet (kind `POOL_KIND` = 247): header with epoch;
per-node doorbells, pending and interest bitmaps over key slots; per-node
creation-claim counters; the key table; the resource table (per-node
lanes). Same discipline as the stream segment: atomics only, compile-time
geometry, header refused on mismatch, memory twin with the same layout.

## Tables

`KeySlot` (64 B + `member_words × 8`): the caller's 128-bit key, `counts =
live << 32 | creating`, `changes`/`waiters`, then a bitmap of the resource
slots registered under it. Keys are found by open addressing on the
caller's digest and installed under the process lock; they are never
removed within an epoch.

`ResourceSlot` (384 B): state (EMPTY / LIVE / DRAINING / CLOSED /
EXHAUSTED), generation, owner node and incarnation, key, capacity,
`key_index`, `units = reserved << 32 | active`, `fence`,
`last_reserve_ms`, the pending set. `ResourceId(NetId64)`: kind 247, node = the owner's
lane, counter = `generation (24) | slot (16)`, allocated in the owner's
lane under a process-local mutex; CLOSED slots are reused, EXHAUSTED ones
wait for the next epoch.

## Units

Capacity applies to `reserved + active`. `reserve` is one CAS that adds a
reserved unit, mints a fence and files it in the slot's pending set (16
entries of fence and time; a resource whose owner is that far behind
refuses further reservations as busy). `accept` (owner only, live only)
takes the fence out of the pending set with one CAS, which succeeds
exactly once, then moves a unit from reserved to active and returns an
`Execution`; a second accept, one after the owner aged the entry out, or
one on an ended generation is `NotReserved`. Several leases on one
resource are told apart by their fences, so HTTP/2-style capacity works
without a "latest fence" rule. Dropping the `Execution` subtracts an
active unit and wakes the key. Nothing a caller does after
reserving changes the counts: the resource is busy until the owner says it
is not. A reservation that never reached the owner is aged out, entry by entry,
by the owner's `reconcile(id, active, grace)`, which also makes the
owner's active count the table's. The ordering keeps every error on the safe side:
the table may under-admit until the owner reconciles, never over-admit.

## Creation claims

`claim_create(key, max_live)` is one CAS on the key's `counts` requiring
`live + creating < max_live`; the node's claim is also counted in the
per-node claim table so a death report can return it. `register` adds to
`live`; the permit's drop subtracts from `creating`, so the overlap while
both are counted is conservative.

## Decisions

`acquire` snapshots the candidates and budget, calls the `Policy`, then
reserves or claims. Busy, stale and draining are lost races and cost one
attempt each; after `limits.attempts` the answer is `Wait(version)`. The
pool holds no cost model; `LocalFirst` prefers a local resource with room,
then the least loaded remote one, then creation within budget, then wait.

## Waking

Blocking waiters park on the key's `changes` word (the cell idiom). Tasks
register a waker per key in the process; the node marks interest in the
key in its interest bitmap; a `key_changed` sets the pending bit and rings
the doorbell of every interested node, and the node's driver thread wakes
the registered tasks and clears its interest. One driver per process,
started on first use, joined on drop, skipped after fork.

## Death

`node_dead(node, incarnation)` closes every LIVE or DRAINING resource that
incarnation owned (CLOSED, taken out of its key's members and `live`,
key woken), subtracts the claims it held, and stops its doorbell counting
as listening if the recorded incarnation matches. Leases the dead process
held on others' resources are not in the table; their owners complete
them when the streams that carried them end.

## Invariants

- Capacity is never over-admitted; under-admission lasts until the owner
  reconciles.
- Only the owner accepts, drains, unregisters and reconciles; only the
  owner's completion returns a unit.
- No transport, no execution, no cost figures in this crate.
- No liveness: death is the embedder's confirmed report per incarnation.

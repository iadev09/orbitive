# orbit-pool - Architecture

One segment per fleet (kind `POOL_KIND` = 247): header with epoch;
per-node doorbells, pending and interest bitmaps over key slots; per-node
creation-claim counters; the key table; the resource table (per-node
lanes). Same discipline as the stream segment: atomics only, compile-time
geometry, header refused on mismatch, memory twin with the same layout.

## Specs: one fleet, several pools

A `PoolSpec` names the segment a `Pool` opens — its kind, its two capacities,
and whether fleet availability is tracked — and `Pool::new` is
`with_spec(PoolSpec::DEFAULT)`, the kind
247 table built from the compile-time geometry. Independent specs are
independent pools: separate segments, separate key spaces, separate
creation budgets, separate epochs, and a `reset_all` on one leaves the
others untouched. A fleet's h1 origin connections, its FastCGI sockets
and a worker pool have neither the same shape nor the same life, so each
names its own kind and sizes itself.

A kind is a fleet-wide identity, not a local choice: every process
opening it passes the same capacities, the header refuses a peer that
does not, and a process that opens one kind twice under two geometries is
refused rather than handed a table that is not the one it asked for.
Capacities are validated when the table is opened, where a compile-time
assert used to stand. Each kind still owes the deployment an index row of
its own.

## Tables

`KeySlot` (64 B + `member_words × 8`): the caller's 128-bit key, `counts =
live << 32 | creating`, optional fleet-wide `available`, `changes`/`waiters`,
then a bitmap of the resource
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

For a spec with fleet availability, `claim_warm(key, max_live, min_idle)`
first takes the same creation claim and keeps it only while
`available + creating <= min_idle`. Concurrent workers therefore claim one
shared deficit rather than repeating the floor. `complete_idle(max_idle)`
claims one available unit with a CAS before releasing the active unit; if the
ceiling is full it leaves the resource active and returns `false`, so the owner
can unregister it without ever publishing an uncounted free candidate.

## Decisions

`acquire` snapshots the candidates and budget, calls the `Policy`, then
reserves or claims. Busy, stale and draining are lost races and cost one
attempt each; after `limits.attempts` the answer is `Wait(version)`. The
pool holds no cost model; `LocalFirst` prefers a local resource with room,
then the least loaded remote one, then creation within budget, then wait.

## Waking

Blocking waiters park on the key's `changes` word (the cell idiom), with
a bounded form — `wait_capacity_timeout` — that answers `None` for the
timeout and nothing else, after one last look. That is the admission
window a consumer has when it must answer busy rather than queue. There
is no polling fallback below it: where the platform cannot wait on a
shared word (macOS before 14.4), the table refuses to open. Tasks
register a waker per key in the process; the node marks interest in the
key in its interest bitmap; a `key_changed` sets the pending bit and rings
the doorbell of every interested node, and the node's driver thread wakes
the registered tasks and clears its interest. One driver per process,
started on first use, joined on drop, skipped after fork.

A third form, for a runtime that parks on descriptors: `Pool::readiness`
hands out one `orbit_core::readiness` pair per table and the driver
signals it at the end of a pass that drained anything — after the bits are
taken, so a consumer that drains and looks again cannot miss that pass.
`Pool::watch(key)` is how a descriptor consumer says which keys it wants,
since interest here is per key rather than implied by holding a side;
interest is taken when it is delivered, so it is re-armed before each
wait, exactly as a waker is.

The timeout such a consumer needs is its own `poll`, not an API here:
descriptors compose, so a worker waiting for its next request and for
capacity puts both in one call and gives *that* the deadline. A bounded
wait on a single word (a timed futex) would force it to choose which one
to block on, which is why there is none.

## Death

`node_dead(node, incarnation)` closes every LIVE or DRAINING resource that
incarnation owned (CLOSED, taken out of its key's members and `live`,
key woken), subtracts the claims it held, and stops its doorbell counting
as listening if the recorded incarnation matches. Leases the dead process
held on others' resources are not in the table; their owners complete
them when the streams that carried them end.

## What remote reuse costs (measured)

Load test, 8 tasks on one resource of capacity 4, both nodes in one
process (`BENCHMARKS.local.md`):

| | macOS host | Linux guest |
|---|---|---|
| local one-shot (`claim_create`) | ~1.0–1.3 M ops/s, ~2 µs CPU | 1.7 M ops/s, 1.9 µs CPU |
| local reuse (`reserve`/`accept`/`complete`) | ~1.0–1.7 M ops/s, 2–3 µs CPU | 1.9 M ops/s, 1.9 µs CPU |
| remote reuse over a stream, 128 B payload | 68 k ops/s, p50 70 µs, 40 µs CPU | 6.2 k ops/s, p50 197 µs, 54 µs CPU, 1.85 parks |

A remote request pays three to four wakes (offer, header, payload,
reply) at the host's wake latency. On the Linux guest that is ~200 µs
before any work is done, so remote reuse only beats a fresh local
connection whose connect costs more than that: a TLS handshake to a far
origin, yes; a Unix-socket connect to a local FastCGI, no. This is why
`LocalFirst` and `LocalOnly` are the shipped policies and why no cost
figure is built in: the embedder measures its connect cost on its host
and writes the comparison into its own `Policy`. Fairness across
contending tasks was exact on both hosts.

## Above the ledger: two transports, and this crate chooses neither

The table says who owns a resource and how much of it is in use. Reaching
*at* someone else's resource is a transport question, and there are two of
them with different costs and different limits. Naming them apart matters
because one of them is not this crate family's to provide:

- **Relay.** The borrower's bytes cross an `orbit-stream` session
  (`open_session` / `accept_session`); the owner keeps the socket and
  drives it. The cost is per request and permanent: measured on Linux,
  ~31 µs of CPU and ~1.6 parks against ~1.2 µs for a local resource. It is
  the only shape available to a connection that cannot move — a client
  holding read-ahead, a TLS session's keys and sequence numbers, an H2
  connection's HPACK table and flow-control windows.
- **Migrate.** The descriptor crosses once, over a Unix socket with
  `SCM_RIGHTS`, and afterwards there is no ongoing cost at all: the
  connection is simply local to whoever holds it. Measured by a consumer at
  845 ns against 1365 ns for a fresh local `connect()`, so moving one is
  cheaper than dialling one. **`orbit-stream` cannot carry a descriptor** —
  it is bytes in shared memory — so this needs a channel neither crate
  provides, and it is only available to a connection that carries no
  userspace state at rest.

Neither is a knob. The protocol decides: a connection that cannot move
relays, one that can may migrate, and an H2 connection can do neither,
because two client state machines cannot share one TCP stream at byte
level. Sharing H2 requires the owner to speak H2 and take requests rather
than bytes, which is a proxy and not this primitive.

What the ledger owes either transport is the same and no more: who owns
what, how much is in use, a fence that says which reservation this is, and
a death report that returns the units.

## Invariants

- Capacity is never over-admitted; under-admission lasts until the owner
  reconciles.
- Only the owner accepts, drains, unregisters and reconciles; only the
  owner's completion returns a unit.
- No execution and no cost figures in this crate. No transport either,
  with one bounded exception under the `stream` feature: `open_session` /
  `accept_session` carry the lease itself to its owner over a stream the
  caller supplies, in one 32-byte frame written before the offer, and
  accept it exactly once before a byte of the consumer's payload is read.
  The bytes after that frame are never looked at here, the stream table is
  the caller's choice, and readiness stays with the caller — the pool is
  handed a ticket, it does not wait for one.
- No liveness: death is the embedder's confirmed report per incarnation.

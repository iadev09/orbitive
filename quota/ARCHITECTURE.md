# orbit-quota contract

Status: common integer GCRA and memory/SHM backend sources are present.
Memory validation and measurements are recorded under `benches/README.md`.
Native FreeBSD validation passes for memory and SHM, including clock-domain,
cross-process debt sharing and crash recovery. Initial multi-process SHM
measurements are recorded under `benches/README.md`.

## Boundary

Quota is an authoritative protection primitive, not an observational counter.
It admits a positive cost against an opaque, namespaced key. HTTP, users, IPs,
API credentials, JA3/JA4, routing, response bodies and authentication belong to
consumers. No raw credential should be used as a quota key.

`QuotaStore::try_consume` is the synchronous contract implemented by memory
and SHM. `MemoryQuota::new(geometry, policy, cas_budget)` creates independent
authority, without a Fleet, server identity or global registry. Clones retain
the same Arc-backed table. The last handle dropping destroys its state; a new
store is not recovery of an old one. The application owns this lifetime and
chooses which sites and capabilities share a store and key. Multiple policies
require multiple stores; no implicit per-site policy or quota is installed.

`ShmQuota::open(resource, geometry, policy, cas_budget, options)` uses an explicit
uid-scoped FleetName/kind resource. Matching opens share authority; clones share
one owner row. Owner capacity is persisted shape, not a live process count.
CAS and recovery attempt budgets are local per handle and may differ safely.
SHM shares within one host and OS access boundary. Same-UID
writers are trusted participants; quota does not defend against a process
that can arbitrarily overwrite its shared memory. Neither backing is durable.

## Acceptance and failure

### Explicit retention policies

`LocalGuard` wraps the process-local backend with guard retention. `FleetGuard`
opens a guard SHM resource. `FleetQuota` names the existing strict `ShmQuota`
type; `MemoryQuota` keeps its strict semantics for existing callers. All use the
same rate `Policy` and `QuotaStore` contract. Backend/retention choice is fixed
at construction, never changed by an admission failure. Guards are traffic
protection; exact per-key envelopes across replacement belong to strict stores.

On a miss, complete the existing bounded second lookup pass before installing
anything. Prefer the first empty or replenished slot. If none exists, guards
choose the least recently accessed candidate within that same probe range;
ties retain probe order. There is no global LRU, new scan, age threshold,
background sweeper or invented duration. One-slot geometry can replace a hot
key; a stream of new identities can reset old allowances. These are documented
guard semantics, not an exact quota guarantee or an attack-resistance claim.

Guard consumption refreshes the pinned bank's scaled monotonic last-access
word with bounded max-CAS before evaluating/committing GCRA. Limited attempts
also refresh recency but not TAT; a later failed attempt may leave recency
advanced without consuming cost. These CAS attempts spend the existing call
budget. There is no fallible recency update after a successful charge. Strict
consumption never reads or updates recency.

A live-debt candidate captures generation, TAT and last-access time. Exclusive
writer/freeze acquisition and pin exclusion precede revalidation of all three.
Changed state returns Busy, not a new search/retry. TAT validation also catches
a successful charge at the same clock tick. Last-access time is sampled, not
a strict access sequence; equal-time touches do not impose a global ordering.
No live pin, exhausted generation or inaccessible shard can be stolen.

Memory adds a fixed recency atomic to each slot. Guard SHM appends two aligned
recency words after the fixed key banks; `Layout::recency` is checked along with
stride and total size. The inactive word is initialized before descriptor
publication, so partial replacement cannot corrupt active recency. Guard
format identity is 2; strict format 1 and its physical geometry are unchanged.
Even equal-sized layouts reject mismatched retention before use. Opening never
converts/resets a committed resource. Ordinary timestamp updates are atomic;
crash recovery, owner holds and inactive-bank publication remain the same.

The 83-test quota suite passes on macOS and Linux. The macOS workspace tests,
warnings-denied all-target Clippy, and 20 repeated guard-test rounds pass.
The process benchmark smoke check covers all three policies on both hosts.
Historical validation and benchmark records below describe their stated
source/geometry; they are not new guard measurements or FreeBSD guard validation.

- `Allowed` means the entire requested cost was committed exactly once before
  return. The commit is the linearization point, not the return to the caller.
- `Limited` means insufficient budget. It consumes no units and reports the
  earliest retry delay for that cost under an otherwise idle key.
- A cost above burst capacity can never fit and returns `CostExceedsBurst`.
- `Busy` means contention, an inaccessible identity or the CAS budget prevented
  completion. It
  is not proof that the quota is exhausted and grants no permission.
- `CapacityFull` means a new key cannot be installed within the configured
  shard/probe budget. It may occur while other shards still have room.
- IO, corrupt state, clock failure and incompatible policy/layout are errors,
  never an empty bucket or a successful admission.

Consumers must explicitly handle errors without treating them as permission.
HTTP status selection, retries and overload responses remain adapter policy.
Repeated calls are separate attempts; this API is not an idempotent retry
protocol. A committed operation whose caller dies stays charged. A rate quota
is not a lease, and dropping a return value does not refund it.

One call concerns one policy/key. Several calls do not form an all-or-nothing
transaction across policies. A global quota and a subject quota may therefore
charge conservatively when a later check refuses work. No hidden refund or
cross-shard transaction is promised.

## Bounded work

### Optional admission snapshots

`QuotaStore::try_consume_with_snapshot` shares the same generic bounded admission
path as `try_consume`. Static result dispatch selects either Decision alone or
Captured (commit sample and committed TAT). It does not alter pin/generation,
retention, CAS budgets, failure paths or SHM layout. The capture is returned
only after commit, and resolved outside all storage holds/pins. Snapshot
resolution is infallible: evaluate proved TAT >= scaled now and
TAT - now <= capacity. Remaining units are floor((capacity - (TAT - now)) /
period_ns), bounded by burst. There is no post-commit store/clock read or CAS,
and no fallible operation may turn a reported charge into an error. Plain
Decision dispatch discards scalars and performs no snapshot division.

Admission snapshots describe the trusted sample used by a successful charge,
not a later observation or reservation. HTTP names, formatting, caching and
opt-in switches belong to adapters. All three policies preserve their scope
and retention semantics when reporting.

Geometry explicitly declares total slots, shards, maximum combined key bytes
and maximum probe count. No implicit capacities or operational defaults exist.
Slot capacity divides evenly across shards. Hashing, comparisons and probes
must be bounded; total table size alone is not a satisfactory request budget.

Memory uses one `RandomState` keyed hash per table, shared by clones. SHM must
use a resource-consistent seed, never separate per-process random seeds that
split identity across workers. A hash is an index hint, not
identity: compare the complete bounded namespace/key bytes to avoid granting a
different key's budget on collision. Slice hashing preserves component
boundaries; stored component lengths and full byte comparisons decide identity.
Two domain-separated hashes choose the shard and initial slot. Every slot has
fixed key backing; linear probing wraps within the shard for at most `probes`.
This is per pass: an existing-key lookup has one pass; a miss has a second
structurally serialized pass. There are at most `2 * probes` slot inspections,
and all passes share the same explicit admission CAS budget.
Lookups search for an existing key before committing any reusable slot, so an
earlier expired record cannot hide a later live matching identity.

Admission allocates no unbounded state, performs no DB/network operation,
emits no per-attempt ring record, invokes no user callback under a lock and
does not wait for another owner. No sleep, polling, backoff or unlimited CAS
retry is allowed. Miss-path installation and reclamation obey the same bounds.
Open/preparation may allocate the declared fixed backing, but is not admission.

Strict table pressure never evicts live quota debt. A strict key can be reclaimed only when
discarding its record is equivalent to a fully replenished bucket, under the
same policy, and no in-flight operation can mutate the replacement identity.
Denied traffic must not extend that horizon merely by being denied. New-key
pressure must not reset an existing key's budget. Full-table errors still
require upstream/coarse admission limits; bounded memory alone is not DDoS
protection or guaranteed availability for new legitimate keys.

## Memory synchronization and lifetime

`MemoryQuota::new(geometry, policy, cas_budget)` requires all settings explicitly.
The total CAS budget counts pin, watermark, writer-reservation and TAT attempts,
including successful operations. Loops spend budget before every CAS; reaching
zero returns Busy. No sleeps, yields, owner waits, fallback locks or unbounded
`fetch_update` loops exist. Mandatory unpin/publication cleanup is unconditional,
bounded by the number of held slots and never charged against remaining budget.

### Identity protection

Each fixed slot has a u64 gate (writer/poison/retired flags and bounded reader
count), non-atomic full identity/key bytes, and atomic u128 TAT. Pin acquisition
increments the gate with Acquire CAS after checking count/flag bounds. Identity
and optional expected generation are read only AFTER acquisition. Concurrent
pins allow same-key consumption, but a writer can reserve only exact gate zero.
Gate ABA is harmless: it may represent another generation by the time a pin
succeeds, but post-acquisition generation/full-key validation cannot authorize
the old identity. Reader count overflow returns Busy, never wraps into flags.

Identity is behind UnsafeCell; only pins expose immutable references and only
exclusive writer guards mutate it. References cannot outlive their guards.
Release unpins are RMWs: writer Acquire reservation of gate zero observes the
reader-release sequences. Release writer publication orders complete identity
and initial TAT before a later Acquire pin. Concurrent TAT accesses are atomic.
These are the invariants justifying Slot's manual Sync implementation, not an
optimistic seqlock over racing non-atomic bytes.

### Consumption and reuse

While pinned, consumption reads TAT, takes a fresh trusted clock sample,
evaluates exact GCRA and attempts a bounded AcqRel CAS. A lost/spurious CAS
restarts from state and time, spending the SAME remaining budget. A successful
TAT CAS commits the complete cost. Limited leaves TAT unchanged and resolves
the captured denied debt into Duration after unpinning. Generation cannot
change during any of these operations; a post-commit generation check is not
used as a substitute for protection.

Only installation/reuse takes a prepared structural mutex with try_lock. A
second complete probe pass prevents duplicate publication of the same key.
The candidate records its generation. After reader-free writer reservation,
the writer validates generation, re-reads TAT and samples time again: a charge
after candidate selection prevents reclaim. Strict reuse is allowed only for empty
or fully replenished records; guard live-debt reuse follows the rules above.
Generation increases without wrapping; u64::MAX
retires the fully replenished slot permanently rather than reusing its token.

Clean writer cancellation restores the old live identity. Mutation arms a
dirty guard; unwind before publication leaves the slot poisoned, not LIVE.
Structural mutex poisoning also refuses new calls. A reader unwinding releases
its pin but need not poison an unchanged record. An indefinitely suspended pin
can prevent that slot's reuse: no TTL, lock theft or guessed exact-death recovery
is introduced into memory storage. Other pins can still consume the same key.

Native lock-free u128 is checked before constructing atomic backing. The
portable-atomic dependency may compile emulation for unsupported targets, but
this backend refuses to open when is_lock_free is false; no operation is routed
through that emulation. Fixed key boxes and native structural mutex backing are
prepared during construction, never on first admission. This protocol is for
memory only, not a claim of process-crash-safe SHM recovery or formal wait-free
hardware execution.
The budget counts atomic API calls, not CPU instructions. In particular,
portable-atomic can use LL/SC instruction retry loops on some native lock-free
targets; is_lock_free does not imply wait-free. Admission has no application-level
unbounded retry or blocking fallback, but this is not a hard real-time bound.

## SHM pins and crash-safe commit

The mapping contains an immutable READY header, per-shard atomic clock
watermarks, owner generations, an owner-by-slot atomic pin matrix and fixed
two-bank slots. Every offset, stride and total size is checked before mapping.
Header/shard/owner/slot rows are 64-byte aligned; bank TAT is 16-byte aligned.
No pointer, Vec, Arc, Mutex, Duration or process-local address enters the ABI.

An independent open claims an owner row's kernel lifetime hold. Clones share
that row. Opening clears the preceding row's counters only under this hold,
then advances its non-wrapping generation. Exhausted owner rows are unavailable.
Guards borrow the mapping/owner lifetime. Inherited fork handles are refused;
a child must reopen, and must never unlock the parent's file description.

Pin acquisition samples the slot control, bounded-CAS increments its owner
counter, then validates the identical control and optional expected generation.
Only then may it read full key bytes. Drop performs unconditional SC decrement.
The slot control encodes a non-wrapping generation (low 62 bits), FROZEN and
RETIRED. Generation parity selects the active bank; generation zero is empty.
Identity stays immutable while pinned. TAT remains atomic and can be charged by
several pins concurrently using the same GCRA/CAS protocol as memory.

Installation takes one prepared per-shard kernel LOCK_NB hold. The prepared
lock also makes one local CAS to exclude threads sharing its file description;
each independent opener has a distinct description. Paths and descriptors are
prepared at opening. No admission path opens a file or formats a path.
After a complete serialized probe pass, the candidate control is CAS-frozen.
The writer scans at most the declared owner count. All pin-counter increments,
decrements, scans and descriptor operations are SeqCst: either the writer sees
an admitted pin or a delayed reader rejects the changed descriptor. A later
generation check is never used to justify a racing non-atomic key read.

A nonzero pin can be cleared only by taking that owner's kernel hold. A live
owner makes reuse Busy, never a timed takeover. Recovery consumes an explicit
per-call recovery budget and the local-lock CAS consumes the same CAS budget
as other admission operations. Clear only the selected counter; do not scan or
clear every slot in admission. Reclaiming a counter changes no TAT or identity.

Under freeze, re-read TAT and trusted time before discarding a candidate.
Strict stores replace only empty/fully replenished identities; guards may
replace an exclusively revalidated cold identity as described above. Fill the inactive
bank's full key, lengths and initial atomic TAT, then publish the next generation
with one SC descriptor store. This is the identity/first-charge commit point.
Arm the guard's committed flag before this infallible store; cleanup after a
committed publication can never restore an earlier descriptor. Before commit,
unwind restores the old descriptor and ignores all partial inactive bytes.
Generation exhaustion retires the replenished slot instead of wrapping.

SIGKILL may leave FROZEN. A successor may thaw the preceding descriptor only
after taking the exact shard kernel hold: the interrupted writer can no longer
write. Its partial inactive bank is not published. Existing-key TAT CAS is its
own complete charge commit; death after CAS, even before return, never refunds
it. No per-charge bank copy or global writer lock is needed.

The initial magic tags ownership before the INITIALIZING/READY protocol.
READY is published only after all rows and immutable metadata are initialized.
A tagged non-READY table can be completed under the initialization hold. An
untagged, undersized or incompatible object fails closed; it is not zeroed or
unlinked. An interruption before tagging can therefore need operator cleanup,
but can never have admitted work. Existing READY state is never reinitialized.
Opening may wait for initialization; admission never waits for an owner.

ABI v1 persists geometry, owner capacity, policy, hash protocol/seed and clock
domain. Native lock-free u128 is mandatory. macOS uses the kernel boot-session
UUID; Linux uses boot ID plus time-namespace identity. FreeBSD hashes its exact
16-byte kern.boot_id without jail identity; jails share the host clock domain.
Other Unix targets fail
ClockUnavailable until a verified provider exists. The monotonic clock and
watermark protocol is the same as memory. No wall-clock fallback is permitted.

No default kind is reserved: the application must select a non-conflicting
resource kind after auditing its current and retained owners. Geometry/layout
changes require a separate compatible resource, never resizing a live table.
Membership holds prevent stopped-fleet cleanup while handles remain. Restart
or adoption never clears committed debt. This is same-host process-crash
recovery, not durable storage or a defense against hostile same-uid writers.

## Algorithm and clock

GCRA uses exact integer scaled time: `now = monotonic_ns * units`,
`charge = period_ns * cost`, `capacity = period_ns * burst` and
`candidate = max(previous_tat, now) + charge`. Admission commits candidate
only when `candidate <= now + capacity`. There is no partial grant. Burst
means total immediate units, not extra tolerance. A denial returns
`ceil((candidate - now - capacity) / units)` nanoseconds and leaves TAT intact.
All multiplications, additions and Duration conversions are checked. This
engine has no storage dependency, clock read or mutation and is reusable by
the later SHM backend without defining a shared ABI now.

Memory reads Unix `CLOCK_MONOTONIC` while holding a pin or writer reservation.
The represented
unit is a nanosecond, not a claim of nanosecond hardware resolution. Suspend
accounting follows the host OS; a common cross-platform suspend guarantee is
not imposed. Non-Unix opening returns `ClockUnavailable`. No wall clock,
process-relative Instant or caller timestamp enters admission. Each shard has
an atomic unscaled u128 clock watermark. Read the watermark BEFORE sampling;
a sample below that observed floor fails closed. Publish its maximum with the
same bounded CAS budget. A newer peer publication after sampling is not clock
rollback, and never clamps the old sample forward. Every failed TAT CAS causes
a fresh state read followed by a fresh clock sample, not reuse of an old time
against newer state. Private fake clocks support deterministic overlap tests.
u128 scaled time avoids
interval rounding; range exhaustion or an unavailable clock is an error.
Construction validates policy products and the current burst horizon.
SHM validates a reboot/clock-domain identity before trusting persisted
timestamps. Policy and geometry never change within a memory store.

Concurrency admission is separate: a simultaneous connection/stream cap needs
owner-bound permits and exact-death recovery, not time-based rate tokens.
Reuse or adapt the existing pool contract separately rather than inventing
refund-on-drop for rate quota.

## Proposed hybrid admission and periodic key synchronization

Status: deferred design exploration, not one of the three selected policies.
No hybrid backend, synchronization driver or public API
is implemented. Existing `ShmQuota` reads and updates the authoritative SHM
table directly; it has no mutation ring or worker-local replica. Its exact
admission and crash-recovery contract above remains unchanged.

### Scope and consistency

Scope identifies whose consumption is coordinated; consistency identifies
what a decision guarantees. They are not aliases for memory versus SHM.

| Model | Decision state | Sharing | Meaning of Allowed |
| --- | --- | --- | --- |
| Local, exact | One process-local store | Clones only | Exact within that store |
| Fleet, exact | Current authoritative SHM table | Per-call shared commit | One fleet-wide linearization point |
| Fleet, approximate (proposed) | Local replica per participant | Periodic published contributions | Locally committed cost against the latest safely observed fleet state |

Approximate does not mean unsafe publication or unchecked memory access.
It relaxes fleet-wide decision freshness, not local all-or-nothing charging,
key identity, bounded work or detection of corrupt/incompatible state.
Consistency is an explicit store contract, never an automatic optimization or
fallback after an SHM failure. Peers must reject conflicting policy/mode/layout.

There is no `total / worker_count` budget split and no assumption about uniform
or nonuniform traffic placement. A shared key retains the same declared policy
on every participant. No rule requires a key to return to the same process.
Worker roles, reuseport, HTTP responses, authentication and dispatch remain
adapter concerns. Request admission can coexist with keep-alive: a charge
does not imply closing a connection or asking a client to synchronize.

Stores remain application-selected: several independent stores may coexist;
sharing one store/key is explicit. A participant is a producer identity, not
a hard-coded server worker number. Participant capacity is a storage bound,
not a divisor used to assign rate budgets.

### Data path and authority

```text
admission -> bounded local key table + dirty accounting
                 |
          configured periodic publisher
                 v
       per-producer SHM checkpoint -- coalesced ring notice
                 |
          bounded receiver/resync work
                 v
       peers' local key/debt replicas
```

Admission operates only on process-local memory and the trusted local clock.
It performs no SHM read/write, ring operation, notification syscall, kernel
ownership lock or synchronization wait. There is no shared authoritative
fleet-total quota record or cross-process total lock in this mode. Per-process
threads still require safe local atomic/synchronization operations; local-only
does not mean unsynchronized.

It checks bounded local coordination health, evaluates the local policy and
commits cost plus pending accounting together under local synchronization.
Failure to reserve dirty/pending capacity must precede any successful grant.
The same-key concurrency rules still apply within one process.

The receiver maintains a ready-to-use local decision view. An admission must
not iterate every peer, reconstruct a checkpoint or sum an entire history on
demand. Materialization belongs to bounded background work; admission reads
local state and updates its own contribution within the declared work budget.
No cached-view miss implicitly falls through to shared-table admission.

The ring is a notification mechanism, not the only copy of accepted history.
Published producer checkpoints retain the state needed to reconstruct known
contributions. Repeated notices may collapse to one "new generation ready".
A notice contains bounded resource/producer/generation identity, not an entire
HashMap. Receivers load complete published generations through bounded work.
They never hold every peer's entire table in a temporary allocation.

This is not `orbit-cache` as a guard backend. Its LRU/TTL and cross-writer
last-write-wins replacement do not preserve concurrent quota contributions.
Existing ring/readiness and safe snapshot mechanisms may be reused; their
notification, lifetime and loss semantics must remain explicit.

### Own contribution, imported contributions and decision view

Each process is the sole publisher of its own accepted consumption. It does
not publish the fleet total it observed, write another producer's contribution
or forward received consumption as newly owned work. Publishing derived totals
from several workers would repeatedly count the same remote charges.

The local representation has three logical parts:

- Own: this incarnation's time-aware consumption, including its unpublished
  tail. Its foreground accepts update it immediately.
- Imported: the latest verified contribution revisions of other producer
  incarnations, stored locally and allowed to be delayed within policy.
- Decision view: a locally materialized composition of Own and Imported,
  advanced against the current monotonic time for admission.

"Own + others" describes ownership, not an arithmetic sum of arbitrary quota
values. An others summary may accelerate lookup, but must retain enough bounded
per-origin revision/time information to replace or retire one contribution
without double charging the rest. A process's own checkpoint arriving through
the receiver must not charge it again. Receiving a newer peer checkpoint
replaces that peer revision; it does not increment consumption by the entire
checkpoint value.

Receiver publication of a decision view must preserve foreground charges
accepted during its construction. Local generation/pinning or another proven
protocol must prevent a background snapshot from overwriting newly accepted
Own data. This is process-local concurrency, not a shared fleet transaction.

### Time is part of a contribution

Rate consumption has a temporal effect, not just a count. The existing GCRA
backend does not retain a separate TTL for every request: one TAT summarizes
the ordered charge/refill history. In scaled units, an admitted cost advances
`TAT = max(previous_TAT, now) + period * cost`. Debt diminishes as monotonic
time advances; request execution duration and connection lifetime do not
define when that cost replenishes.

Two independently evolved TATs are not additive. Summing absolute timestamps
is meaningless, and taking the maximum can discard parallel consumption.
Neither applying one full policy refill independently to every contributor
nor resetting debt when a batch arrives defines the shared policy correctly.
The local composition needs one explicit temporal rule for all contributions.

Temporal sharing is possible, but periodic transport alone does not define
that rule. A candidate for approximate mode is a bounded sequence of aligned
time buckets, with cumulative weighted cost per producer/key/bucket and a
revision/coverage watermark. Peers can combine corresponding time positions
without transmitting one record per request. Batch cadence and accounting
resolution are independent explicit settings; no bucket width is chosen here.

This candidate is not a selected algorithm or an implementation claim.
Bucketed counts cannot silently replace GCRA with a fixed/sliding-window
limiter while claiming unchanged burst behavior. Specify time rounding, late
updates, temporal approximation error and local-view materialization first.
An alternative bounded temporal summary is acceptable if it meets those rules.

Use the shared host-monotonic clock domain and compatible resolution/epoch
interpretation, not wall-clock arrival times or process-relative Instant.
A receiver must age a contribution from its accounting time, not grant it a
new lifetime when received. An unchanged idle producer's known debt can decay
without a fresh packet per request; coordination freshness and debt age are
different facts.

History retirement must retain any residual debt or carry state that can still
affect admission. An old bucket's end is not automatically proof that its
contribution is replenished, especially after concurrent approximate accepts.
If retained temporal state or a late update cannot be reconciled safely,
return an explicit unavailable/resync outcome instead of assuming zero debt.
Temporal history count, resolution and permitted update age are bounded along
with key count; no per-request timestamp log or unbounded replay is introduced.

### Required bounds, with no numeric defaults

All capacities, work budgets and durations are declared by the caller. The
following are conceptual settings, not a proposed stable Rust/YAML API.

| Bound | Required meaning |
| --- | --- |
| Key bytes | Maximum combined namespace and subject length, checked before copying/hashing |
| Local keys | Fixed maximum distinct keys per local store, including remote-only debt |
| Local lookup/work | Bounded probes and local synchronization attempts; table size is not a work budget |
| Participants | Maximum producer/incarnation contributions that can be retained |
| Temporal state | Maximum temporal records/buckets per key and producer, explicit accounting resolution and permitted update age |
| Checkpoint capacity | Maximum retained keys and encoded bytes per producer/store, including old debt and retirement metadata |
| Dirty keys | Maximum distinct local keys awaiting publication; at most one dirty-list entry per key |
| Unpublished cost | Maximum pending cost per key and across the store, not merely a number of dirty keys |
| Publish interval | Explicit cadence for the participant/store publisher; no per-key timer |
| Reconciliation interval | Explicit missed-notice recovery cadence; it may deliberately share the publish cadence |
| Batch keys and bytes | Independent maximum record count and encoded byte count per batch |
| Batch cost/work | Maximum accounting/encoding/publication work in one driver turn |
| Receive/resync work | Maximum frames, records and bytes processed per turn, including rebuild pages |
| Notification capacity | Explicit ring geometry and bounded outstanding notices |
| Coordination age | Maximum time since a verified synchronized state may be used for admission |
| Failure behavior | Explicit errors for contention, capacity, stale state and failed reconstruction |

Charge cost is weighted: one request may consume many units. Count and byte
bounds therefore cannot substitute for an unpublished-cost bound. Arithmetic,
lengths and total backing sizes are checked. A maximum-sized key plus metadata
must fit an admissible checkpoint record and batch; reject impossible geometry
at construction rather than blocking publication later.

A HashMap is not a capacity policy. Backing must be prepared or explicitly
bounded, with no admission-time growth, rehashing or unbounded key copies.
If a representation stores temporal records per key per retained producer,
its bound includes key count times retained origins times temporal records,
plus bounded carry/retirement state, not just the number of local keys. Declare
local table, dirty metadata, checkpoint, batch buffers and receiver backing
separately; batching must not duplicate the complete table transiently.

Existing dirty keys may coalesce further consumption only within their pending
cost bounds. New keys must reserve both table and dirty capacity before grant.
Denied traffic creates no charge/dirty mutation. Remote mutations are applied
without being republished as new local consumption.

No LRU eviction or arbitrary TTL may turn live, pending, remotely uncertain or
unreconciled debt into a fresh budget. Reclamation requires a policy-derived
replenishment horizon and resolved ownership/synchronization state. Exhaustion
refuses work; it does not discard another key's charge.

### Periodic publication and batching

The publisher runs at the configured interval. Admission records pending/dirty
pressure in local memory for the driver; it does not issue a notification
syscall, perform a flush inline or wait for it. Lifecycle-driven flush requests
outside admission may also be supported explicitly. Once an unpublished bound
is reached, further affected grants are refused until capacity is safely
released. There is no invented emergency
sleep, retry interval or timer per key. Reuse native coalescing readiness for
notices and cancellation; an explicit periodic publisher is not a receiver
polling loop.

Repeated consumption of one key updates one bounded pending entry. High
cardinality is different: many unique keys do not compress into one key.
Every flush obeys both record and byte/work limits, carries excess dirty work
forward and leaves unacknowledged publication dirty. A successful local flush
means a complete recoverable SHM generation is published, not that every peer
has already consumed it. No per-peer synchronous acknowledgement gates a
normal request.

Snapshot extraction and publication must not hold local admission exclusion
across a whole-table scan, encoding or peer IO. Bounded extraction/merge turns
must leave request admission nonblocking; background work cannot become an
unbounded critical section on the supposedly local fast path.

Use per-producer, per-key revisions and cumulative contributions so skipped or
duplicate notifications do not replay consumption twice. Producer incarnation,
key generation, store/policy identity and clock domain distinguish reused
rows and retired state. Cross-producer last-write-wins and `max(TAT)` would
lose concurrent charges and are not valid merge rules.

The temporal representation described above remains a design gate: a scalar
lifetime cost total alone does not reconstruct GCRA burst/refill history, and
applying every old charge at receive time can manufacture fresh debt. Define and prove a
bounded temporal summary/merge rule before selecting a wire layout. Latest
checkpoint replacement is valid only when it preserves everything relevant
from that producer's preceding revisions. Receivers must distinguish superseded
state from missing contributions; batching is not permission to forget cost.

### Full rings, overload and lag

Publishing checkpoint state precedes publishing its notification. An incomplete
checkpoint never replaces a preceding complete generation. Readers require
safe ownership/pinning or another established publication protocol; checking
a version around racing non-atomic reads is not sufficient memory safety.
No new fleet-wide per-key CAS/consensus protocol is required for approximate
decisions, but published bytes must still be complete and coherent.

If a notice cannot be delivered, retain/coalesce a bounded notification-pending
state. Ring overwrite or a cursor gap requests checkpoint reconciliation;
the missing notice must not be interpreted as missing consumption. Existing
ring primitives may implement overwrite rather than producer backpressure:
do not assume they provide a full/no-overwrite result they do not expose.
Use explicit notification-gap handling and configured periodic reconciliation
to discover published generations even if the last notice was lost.

Rebuild is staged and bounded; an incomplete scan is not "synchronized".
Checkpoint overwrite, torn/incompatible state or unrecoverable gaps mark the
affected store unavailable for protected admission, not empty. Only continue
with a preceding replica while its explicitly permitted age/uncertainty bounds
remain valid. If checkpoint/dirty/pending capacity is exhausted, grants are
refused even when a ring notice could still be written.

The publish interval is a target cadence, not a maximum observation delay.
Drivers can fall behind or be descheduled. Local admission must detect expired
coordination health rather than continue indefinitely against stale peers.
Any receiver retry/reconciliation cadence or freshness duration must be explicit;
no hidden poll interval is introduced to make liveness appear reliable.

### Worker death, restart and joining

Graceful shutdown stops new grants, attempts bounded final publication and
hands off complete published state. If it cannot finish, report the unfinished
state; do not claim a clean synchronization. Existing lifecycle policy owns
the time allowed for shutdown; quota invents no termination deadline.

On abrupt death, complete published contributions remain charge-bearing.
In-progress checkpoints are ignored without erasing the last complete one.
The local unpublished tail can be lost; approximate mode must disclose and
bound that exposure explicitly. Per-key/store pending-cost bounds limit one
incarnation's unpublished tail, not all possible repeated crashes.

If the unpublished key list is lost, uncertainty cannot be restricted to keys
already present in the checkpoint. Recovery must account for the affected
store, including keys first seen after the last publication.

A restarted process is a new producer incarnation and cannot overwrite an
old incarnation's unresolved contribution or silently acquire a fresh fleet
budget. Exact-death evidence can release ownership, not refund quota. Retain
retired contributions until policy-safe reclamation. Bound the retained-origin
table; when full, refuse a new origin rather than dropping old debt. Repeated
crashes must not reopen the same unpublished allowance indefinitely.

Before implementation, choose how unresolved crash exposure is handled:
retain conservative uncertainty until safely reconciled/replenished, or gate
the affected store pending explicit recovery. A configuration that merely
accepts lost unpublished work must label that weaker guarantee; it cannot
claim the exact backend's crash-preserved accounting.

A joining worker reconstructs a coherent checkpoint baseline and then catches
up with publication generations before entering ready admission. Empty local
memory is not evidence of zero fleet debt. Concurrent snapshot changes are
detected; restart/resync cannot use partially populated tables as a fresh quota.
Boot/clock-domain or policy incompatibility is an opening error, not a reset.

### Approximation and verification gates

This model does not enforce a strict fleet-wide burst at every instant.
Concurrent decisions can use incomplete remote consumption even when every
local decision is correct. No uniform-traffic assumption or worker-count
division is used to hide that exposure. A useful overshoot bound needs explicit
participant, cost, pending, receiver-age and crash semantics together; the
publish interval alone does not supply one. Select and test that contract
before exposing this as a protection mode.

The adapter may deliberately bypass a refusal, but that is outside the guard's
protection guarantee. The primitive itself does not turn Busy, full, stale or
resync-required states into permission. This mode is not appropriate where
exact global financial/accounting or strict fleet-wide admission is required.

Required tests/measurements before implementation is considered ready:

- Duplicate, delayed, reordered and coalesced contributions; no dropped parallel
  charges or double counting; temporal merge versus an independent model.
- Key byte/count pressure, weighted pending bounds, unique-key churn and batches
  cut by either key count or byte/work limits; no hot-path allocation growth.
- Ring overwrite, lost final notice, failed publication, stale receivers and
  multi-turn resync; no empty-budget fallback or unbounded receiver catch-up.
- Death before/during/after checkpoint publication, a lost local tail, repeated
  restart, exhausted producer history and joining during concurrent publication.
- Sustained repeated-key and unique-key traffic under different traffic
  placements, without treating any one placement as a correctness assumption.
- Per-request cost, ring frames/bytes/wakes, batching delay, accepted/refused
  rates, memory and unpublished/unobserved work under sustained overload.

Numeric settings, temporal merge encoding, coherent checkpoint mechanics and
the crash-uncertainty policy remain explicit design decisions. There is no
new implementation, API promise or implied production default in this section.

## Verification gates before use

Native validation on FreeBSD 15.1-RELEASE-p3 ARM64 with Rust 1.98.1:

- The quota suite passes all 62 tests under workspace/all-features selection.
- All 25 quota SHM tests pass in 50 consecutive rounds with normal parallelism.
- The full workspace test suite passes, as does workspace/all-features/all-targets
  Clippy with warnings denied.
- Separate processes produce the same native clock domain and share committed
  debt through the public SHM opening API.

Crash checkpoints are irreversible, distinct from resumable readiness barriers.
The parent retains the control pipe outside `Child` until reaping completes and
verifies SIGKILL termination. `Child::wait` otherwise closes stdin before death
is confirmed: treating EOF as readiness lets a supposedly stopped writer publish
or a reclaimer consume another unit. Regression tests close the pipe explicitly
and send unexpected input; neither may resume publication or recovery. Each
checkpoint names its exact fault boundary, and the parent keeps one buffered
stdout reader across the readiness/recovery exchange. These are test-harness
constraints; the backend's commit and recovery protocol is unchanged.

The SHM benchmark uses independent process opens and reports shared-key,
separate-key, replenished-identity churn and live-debt/full-table outcomes.
Initial single-NUMA Linux results are in `benches/README.md`; single-shard churn
shows substantial Busy pressure. Neither the tests nor these measurements
claim formal verification, production attack resistance or performance parity
with memory.

1. Deterministic arithmetic: rate/burst/cost, boundary rounding, no partial
   grants, retry horizons, overflow and clock failure.
2. Reuse the algorithm suite for memory and SHM. Independent memory
   constructors remain isolated; clones share debt and table lifetime.
3. Concurrent acceptance never exceeds the policy envelope. Busy and full
   outcomes never grant permission; rejected attempts do not consume budget.
4. Adversarial key lengths/collisions/churn exercise explicit work bounds,
   full shards, safe replenished-record reclamation and policy isolation.
5. Fault injection at initialization, inactive-bank writes, commit and slot
   replacement; SIGKILL releases ownership without losing committed debt.
6. SHM restart/adoption and incompatible opens preserve accepted state;
   memory lifetime is explicitly the lifetime of its last handle.
7. Benchmark accepted, limited, busy and new-key paths under contention before
   claiming hot-path cost or selecting a more complex lock-free design.

No throughput, starvation-freedom, fairness, cross-host quota or DDoS immunity
is claimed by these contracts. The required result is bounded work and explicit
failure without quota overshoot.

# orbit-quota

Bounded, nonblocking GCRA rate admission. Callers supply opaque namespaced
keys; HTTP, authentication, sessions and response handling stay outside quota.

`MemoryQuota::new` creates an independent process-local store. `clone()` shares
its table and budget. Different stores never share debt, even for identical
keys. Within a store, identical keys share a budget and different keys have
separate budgets under the same immutable policy. Applications choose whether
sites share a store, a key, both or neither. Memory state lasts until its last
handle drops. SHM state outlives its handles and processes.

```rust
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;
use orbit_quota::{CasBudget, Decision, Geometry, Key, MemoryQuota, Policy, QuotaStore};

# fn example() -> Result<(), orbit_quota::Error> {
// Example policy: 10 units per second, total immediate burst of 20 units.
let geometry = Geometry::new(
    NonZeroUsize::new(4096).unwrap(), // total slots
    NonZeroUsize::new(16).unwrap(),   // shards
    NonZeroUsize::new(128).unwrap(),  // combined namespace + subject bytes
    NonZeroUsize::new(8).unwrap(),    // slots inspected per bounded lookup pass
)?;
let policy = Policy::new(
    NonZeroU64::new(10).unwrap(),
    Duration::from_secs(1),
    NonZeroU64::new(20).unwrap(),
)?;
// Explicit total CAS attempts per call, not a library default.
let attempts = CasBudget::new(NonZeroUsize::new(64).unwrap());
let quota = MemoryQuota::new(geometry, policy, attempts)?;
let shared = quota.clone();
let key = Key::new(b"example-site", b"opaque-subject")?;
match shared.try_consume(key, NonZeroU64::new(1).unwrap())? {
    Decision::Allowed => { /* perform the admitted work */ }
    Decision::Limited { retry_after: _ } => { /* adapter chooses a response */ }
}
# Ok(())
# }
```

## Guarantees and boundaries

- Each call consumes the complete positive cost or none of it. Denial does
  not prolong debt. Weighted cost above burst is an error, not a partial grant.
- The complete table and key backing are allocated during construction.
  Admission allocates nothing. Existing-key lookup uses at most one declared
  probe pass; a miss uses a second serialized pass before installation.
- Pin acquisition, clock watermark publication, writer reservation and TAT CAS
  share the explicit attempt budget, counting successes and failures. There
  is no sleep, owner wait or unbounded retry; exhaustion returns `Busy`.
  Cleanup always releases owned pins and is not skipped when the budget ends.
  Too small a budget can refuse even uncontended installation.
- Existing keys use pinned identity and atomic GCRA commits, not the shard
  mutex. Installation/reuse takes the structural mutex with `try_lock` only.
  Contention or an inaccessible identity returns `Busy`. `CapacityFull`
  can occur even while slots outside the probe range are free. Neither is
  permission to proceed. A poisoned memory shard remains unavailable.
- Hashes are keyed per store; full namespace and subject bytes decide identity.
  Pins prevent generation/key replacement while readers consume. Live debt is
  never evicted. A writer rechecks TAT after exclusive reservation before reuse.
  Generations never wrap; exhausted slots retire. Partial memory writes stay
  poisoned; unpublished SHM banks never replace the committed bank.
- Integer GCRA preserves fractional refill intervals; retry delays round up
  to nanoseconds. No background refill, timer or implicit duration is used.
- Time comes from the host's `CLOCK_MONOTONIC` on Unix. Clock failure,
  regression or arithmetic overflow grants no admission. Suspend accounting
  follows the operating system's clock. Non-Unix construction currently fails
  with `ClockUnavailable`.
- Native lock-free 128-bit atomics are required. Construction returns
  `UnsupportedAtomics` instead of silently using lock-based emulation.
  The attempt budget bounds atomic API calls, not wall time or internal
  architecture-specific instruction retries. Lock-free is not wait-free.

Quota is an explicit defense at an application-selected boundary, not an
automatic wrapper around each capability. A rate charge is not a session,
concurrency lease or authentication proof and is not refunded on caller exit.
Several policy checks do not form a transaction. Memory storage does not share
state across processes or survive restart.

## Shared memory

`ShmQuota` implements the same `QuotaStore` admission API. Open the same
uid-scoped resource with matching geometry and policy to share debt:

```rust,ignore
use orbit_quota::{ShmOptions, ShmQuota, ShmResource};

// All settings are supplied by the application; no kind or capacity default.
let resource = ShmResource::new("example".parse()?, kind);
let options = ShmOptions::new(owner_capacity, recovery_attempts);
let quota = ShmQuota::open(resource, geometry, policy, attempts, options)?;
```

Each independent open claims one owner row; clones share it. `owner_capacity`
is a nonzero, persisted bound on simultaneously open owners. Pin backing grows
with owner capacity times slot capacity. A full owner table is an explicit
error, never a live-owner takeover.

Owner rows are protected by lifetime kernel holds. Under reuse pressure, pins
can be cleared only after acquiring the corresponding hold without waiting.
There is no PID probe, heartbeat expiry or timeout-based reclaim.
`recovery_attempts` bounds additional kernel-lock attempts per admission;
zero disables opportunistic recovery. CAS attempts remain separately bounded
by `CasBudget`. Kernel lock descriptors are prepared at opening, not admission.

Existing-key consumption commits through native atomic u128 TAT CAS. Identity
replacement freezes the slot, excludes admitted pins, writes an inactive bank
and publishes its generation and initial charge with one atomic descriptor.
Death before publication preserves the old bank; death after publication
preserves the new charge. A dead writer's frozen descriptor can be recovered
under its shard's kernel lock. Committed charges are not refunded after death.

Opening validates ABI, geometry, policy, hash protocol and boot/clock domain.
The shared hash seed is generated once; it is not independently randomized by
each opener. Incompatible state is never reset. Tagged incomplete initialization
can resume before READY; an untagged or undersized object is refused, not erased.
Opening may serialize initialization; admission uses nonblocking acquisition.

The native clock-domain provider currently supports macOS boot-session UUIDs
and Linux boot IDs plus time-namespace identity. FreeBSD uses the kernel's
16-byte boot ID, shared across jails along with the monotonic clock. Isolation
belongs to resource identity and OS access permissions, not the clock domain.
Other Unix targets refuse
opening with `ClockUnavailable`. All users must share native atomic/layout
compatibility and the same clock domain. Open after fork; inherited handles
must not be used. Inherited holds may delay recovery until the child closes
them or exits/execs; they never justify taking ownership from a live holder.
No power-loss durability or same-uid hostile-writer isolation
is provided. Clearing/unlinking requires all users to have stopped.

On FreeBSD ARM64, the quota suite passes, including native clock-domain,
cross-process debt sharing and SIGKILL recovery tests. SHM performance has not
been measured.

Bounded memory and bounded probes do not promise fairness, availability for
new keys under adversarial churn, or protection against volumetric DDoS.
Consumers still need coarse admission and an explicit failure policy.

## Measurement targets

The `admission` benchmark separates existing-key and new-key acceptance,
limited, oversized-key and full-table paths, compares fixed probe budgets
across table sizes, and measures
hot-key contention with 2, 4 and 8 threads. Setup is excluded from serial
measurements. Contention measures batches including synchronization and join
overhead; successful and busy attempts are distinct outcomes, not equivalent
admitted throughput. The recorded baseline is a local measurement, not a
portable latency or throughput guarantee.

Acceptance targets are bounded key/probe work, no admission-time allocation,
no debt eviction and no over-admission under contention. Deterministic tests
cover an independent integer token-bucket model, retry boundaries, weighted
costs, clock failures, poisoning, capacity pressure, allocation counts and
repeated reclamation.
Benchmarks measure cost; they do not prove these correctness properties.

Commands, measured results and interpretation are in the
[benchmark notes](benches/README.md).

# Memory quota verification and benchmarks

## Scope and targets

Quota is an explicit admission defense, not part of the capability it protects.
These measurements cover quota alone: no HTTP, session lookup, authentication,
event delivery or SHM. They do not measure end-to-end request latency or
protection against volumetric DDoS.

Correctness gates:

- GCRA matches an independent integer token-bucket model, including fractional
  refill intervals, weighted cost and upward-rounded retry delays.
- Admission commits all requested units or none. Limited attempts do not extend
  debt; concurrent clones cannot consume more than the same available budget.
- New stores are independent. Clones keep shared debt alive after another
  handle drops.
- Key churn cannot evict live debt. Fully replenished records can be reclaimed;
  the probe/key budgets and allocated backing remain fixed.
- Busy, full, poisoned, clock-error and invalid-input outcomes grant no access.
- Admission allocates nothing, including the first insertion and later slot
  replacement. The allocation counter is thread-local, so other parallel tests
  do not contaminate its result. It observes Rust global-allocator allocation
  and reallocation calls, not every internal allocation an OS may perform.

Performance targets are bounded work and no hidden allocation or waiting.
Numbers below establish a local baseline, not portable latency SLOs. Adding
slots should not add a table scan when the key/probe budget is unchanged.

## Reproduce

Run from the repository root on the current host:

```sh
rust-check.sh
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo bench --workspace --all-features --bench admission -- --noplot
```

`rust-check.sh` currently delegates to the workspace/all-features test script.
Use the same workspace/feature selection rather than building a separate
package-specific feature set. Criterion owns warmup, sample count and
measurement duration through its defaults or explicit CLI options; the quota
primitive has no benchmark-driven timing constants.

Serial benchmarks exclude store construction. New-key insertion uses a fresh
store per input; construction and destruction are outside the timed operation.
Full-table tests cover all slots with the declared per-pass probe budget.
Fixed keys include hashing, time sampling, lookup and GCRA calculation. The
historical mutex runs include shard try-lock; current existing-key runs use
pin acquisition and atomic TAT commits instead.

Contention starts 2, 4 or 8 threads on one shared hot key. Reported time is
amortized wall time per **attempt**, including busy rejections, not latency or
throughput of accepted work. Thread creation is outside timing; barrier release
and joins are included. Each run prints allowed/limited/busy totals, elapsed
wall time, allowed/limited operations per second and per-thread outcome counts
(including warmup/calibration). A faster rejection-heavy run is not evidence of greater
useful capacity or fairness.

## Verified behavior

The initial allocation test caught one allocation on first admission on macOS.
The standard pthread mutex backend lazily allocates native backing on its first
use, including `try_lock`. Construction now prepares every shard mutex before
returning. The test was not warmed or relaxed to hide the allocation.

After the critical-section reduction, all 23 then-existing quota tests and the workspace
test suite passed, as did
workspace/all-features/all-targets Clippy with warnings denied. The allocation
test reported zero allocations across first insertion, existing-key acceptance
or refusal, full-table/invalid-input refusal, replacement and busy rejection.
An additional allocation test uses the public constructor and real host clock,
also reporting zero for first and subsequent acceptance.

The model test covers 77 rate/period combinations with 1,000 weighted operations
each. A frozen-clock concurrent test uses eight clones and checks an aggregate
burst of 64 units. Reclamation is exercised across 10,000 changing keys in one
fixed slot; pressure against live debt is tested separately.
Prepared-cost and denied-snapshot tests cover the split GCRA phases. Retry
Duration overflow is tested both in the engine and through memory admission:
the failed conversion cannot charge, reset debt or poison the shard.

These tests do not prove SHM crash safety, scheduler fairness, starvation
freedom, or exhaustive correctness for all u128 states. SHM has not been added.

## Baseline: 2026-10-20

Environment: Apple M3 Max, arm64, macOS 27.0.1; rustc 1.99.1
(`58cf8f9c3`, 2026-10-13), Cargo 1.99.1, Criterion 0.8.2. The workspace release
profile uses optimization level 3, thin LTO and one codegen unit. Criterion used
its defaults: 3 seconds warmup, approximately 5 seconds measurement and 100
samples per workload. No other workspace checks ran during measurement.

The reported estimates below are Criterion slope estimates and 95% confidence
intervals, in nanoseconds. They are not p95/p99 request latency.

### Serial admission

These hot-key runs use one shard, four probes and a 64-byte combined key
budget. Different table sizes keep the same hot key cached; this does not
measure a larger cold/random working set.

| Slots | Existing key allowed, ns | Existing key limited, ns |
| ---: | ---: | ---: |
| 64 | 62.31 [62.12, 62.53] | 72.03 [71.95, 72.12] |
| 4,096 | 62.80 [62.57, 63.11] | 73.51 [72.88, 74.21] |
| 65,536 | 62.20 [62.09, 62.32] | 72.58 [72.34, 72.84] |

| Operation | Estimate, ns | 95% interval, ns |
| --- | ---: | ---: |
| New key allowed, fresh 64-slot store / 4 probes | 162.12 | [155.22, 167.83] |
| Weighted allowed, cost 32 | 63.54 | [63.23, 63.92] |
| New key refused, full 1-slot table / 1 probe | 53.97 | [53.72, 54.26] |
| New key refused, full 4-slot table / 4 probes | 61.80 | [61.46, 62.17] |
| New key refused, full 16-slot table / 16 probes | 91.20 | [90.70, 91.70] |
| Oversized key refused before hashing/clock/locking | 1.96 | [1.95, 1.97] |

Capacity alone did not add a scan to these hot hits. Full-table rejection cost
increased with the explicit probe budget, as expected. The new-key workload
uses a freshly constructed store per input; it is not a reclamation/churn
throughput benchmark. Reclamation correctness is covered by deterministic
tests, not inferred from this number.

### One shared hot key under contention

All threads race the same shard. Busy means refusal without waiting, not a
successful quota charge. Counts include warmup/calibration and are recorded
separately from the measured time estimate.

| Threads | Amortized ns/attempt [95% interval] | Allowed | Busy | Busy share |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 53.40 [52.46, 54.51] | 79,151,813 | 85,967,449 | 52.06% |
| 4 | 34.87 [33.92, 35.97] | 53,257,218 | 221,249,506 | 80.60% |
| 8 | 24.76 [24.52, 25.01] | 18,483,136 | 321,774,584 | 94.57% |

This exposes a practical limit: saturated single-key contention rejects most
attempts at eight threads. Lower amortized time there is mostly faster refusal,
not improved useful throughput. More shards isolate unrelated keys but do not
split one key's authoritative budget. The adapter must handle Busy explicitly;
silently allowing it, spinning or sleeping would violate this contract.
This baseline does not establish fairness or performance for distributed-key
workloads. It motivates measuring those separately before selecting a more
complex synchronization design.

Raw local artifacts are `target/quota-tests.log`, `target/quota-bench.log` and
Criterion's `target/criterion/` data. They are generated output, not source or
part of the published package.

## Critical-section comparison

The comparison uses the same host, release profile, Criterion defaults and
instrumented harness on both implementations. Before changing the engine,
the harness was extended to record aggregate allowed/limited operations per
second and per-thread outcome counts. Saturated admission and already-limited
hot keys are separate workloads. No other workspace checks ran during timing.

The retained change prepares checked cost multiplication before `try_lock` and
resolves denied debt into Duration after releasing the shard. Sampling time,
clock-regression validation, full-key lookup, GCRA transition and accepted debt
commit remain under the same exclusion. No retries, waiting, CAS, SHM or
quota-envelope changes are introduced. The public API is unchanged.

An additional TAT-only existing-slot write was tried and rejected: fresh-key
insertion increased from 170.70 ns to 204.76 ns and repeated at 203.39 ns.
Restoring the original full slot write gave 170.66 ns in the isolated check.
The final implementation retains that original slot commit; the final
comparison below does not use the rejected variant.

Capture the first baseline on the original implementation and the second
after the critical-section change:

```sh
cargo bench --workspace --all-features --bench admission -- --noplot --save-baseline quota_before_critical
cargo bench --workspace --all-features --bench admission -- --noplot --save-baseline quota_final_critical
```

Rates are aggregate outcome count divided by the sum of timed batch wall
durations, including warmup/calibration. They are not obtained by multiplying
Criterion's slope by a Busy percentage. Per-thread shares divide each worker's
outcome count by the aggregate of that outcome. Thread indices refer to spawn
order across fresh batches, not persistent OS thread identities; aggregate
balance is not a starvation-freedom or short-window fairness guarantee. The
rate/share observations have no confidence intervals and are not portable SLOs.

### Final paired observations

Serial values are Criterion slope estimates in ns. Each run uses one hot key;
the fresh-key row is the separate fresh-store workload described above.

| Operation / slots | Before, ns | Final, ns | Change |
| --- | ---: | ---: | ---: |
| Existing allowed / 64 | 62.74 | 59.89 | -4.5% |
| Existing allowed / 4,096 | 61.99 | 62.34 | +0.6% |
| Existing allowed / 65,536 | 61.46 | 60.39 | -1.7% |
| Existing limited / 64 | 72.59 | 68.17 | -6.1% |
| Existing limited / 4,096 | 72.16 | 67.49 | -6.5% |
| Existing limited / 65,536 | 72.10 | 67.42 | -6.5% |
| Weighted allowed, cost 32 | 61.91 | 59.98 | -3.1% |
| Fresh key / 64 | 170.70 | 191.10 | +11.9% |

The final fresh-key 95% interval was [179.50, 201.39] ns. Its isolated repeat
after restoring the slot write was 170.66 ns, but the final full-run result is
retained here rather than selecting the better repeat. The variation needs a
more controlled insertion/working-set study; this refactor cannot be claimed
to improve the fresh-key path. Other full-table/oversized-key cases changed
between -0.1% and +2.3% in the paired run.

Rates below count completed admission decisions, not Busy attempts. The
already-limited workload has zero Allowed outcomes in both implementations.

| Hot-key workload | Threads | Outcome/s before, millions | Outcome/s final, millions | Busy before | Busy final |
| --- | ---: | ---: | ---: | ---: | ---: |
| Allowed | 2 | 9.282 | 9.448 | 52.20% | 51.63% |
| Allowed | 4 | 5.599 | 4.954 | 83.47% | 83.20% |
| Allowed | 8 | 2.045 | 1.896 | 94.94% | 94.92% |
| Limited | 2 | 7.405 | 8.460 | 50.14% | 51.43% |
| Limited | 4 | 5.375 | 5.291 | 84.16% | 82.70% |
| Limited | 8 | 1.988 | 1.822 | 95.00% | 95.19% |

Per-thread shares of all Allowed outcomes, in worker spawn-index order:

| Threads | Before, % | Final, % |
| ---: | --- | --- |
| 2 | 47.35, 52.65 | 47.73, 52.27 |
| 4 | 24.80, 24.95, 24.58, 25.67 | 24.86, 24.98, 24.77, 25.39 |
| 8 | 12.66, 13.65, 14.36, 13.46, 12.65, 10.57, 10.02, 12.64 | 13.45, 14.31, 12.61, 13.51, 11.77, 10.62, 10.54, 13.19 |

For Limited outcomes, the min/max per-thread shares were 49.81–50.19% →
47.81–52.19% with two threads, 24.62–25.62% → 24.89–25.11% with four, and
9.78–14.01% → 11.01–13.86% with eight. No thread received zero aggregate
outcomes, but these aggregates say nothing about bounded individual progress.

**Conclusion:** GCRA correctness, zero admission allocations and immediate
refusal are preserved, and serial Limited resolution became cheaper. This is
not a demonstrated fix for saturated single-key contention: eight-thread Busy
remains about 95%, while Allowed throughput fell about 7.3% in the final paired
observation. More shards cannot split that one key. A separate synchronization
design and repeated controlled trials are needed before claiming a contention
improvement; no lock-free/CAS or waiting fallback was introduced in this phase.

Comparison artifacts: `target/quota-before-critical.log`,
`target/quota-final-critical.log`, and the named `quota_before_critical` /
`quota_final_critical` directories under `target/criterion/`. The rejected
variant and isolated repeats remain separate local artifacts, not replacement
measurements for the final rows above.

## Bounded CAS comparison

The memory backend now keeps identity pinned while existing-key consumers
update TAT atomically. Only installation/reuse tries the structural mutex.
Every call has an explicit `CasBudget`; pin, clock watermark, writer
reservation and TAT attempts share it, including successful CAS operations.
Mandatory release is never skipped on budget exhaustion. There is no default
budget, waiting fallback or application-level unbounded retry.

The following run uses the same host, profile, Criterion defaults, policies and
instrumented workload as `quota_final_critical`. The CAS fixture explicitly
chooses 64 attempts. Extra eight-thread runs choose 16 and 32. These are
measurement inputs, not recommended production defaults. Native lock-free
u128 atomics are required; construction refuses lock-emulated targets.
The budget bounds atomic API calls, not scheduling time or architecture-level
instruction retries; this is not a formal wait-free or hard real-time claim.

```sh
cargo bench --workspace --all-features --bench admission -- --noplot --save-baseline quota_cas_64
```

### Serial cost

| Operation / slots | Mutex, ns | CAS / 64 attempts, ns | Change |
| --- | ---: | ---: | ---: |
| Existing allowed / 64 | 59.89 | 67.43 | +12.6% |
| Existing allowed / 4,096 | 62.34 | 67.20 | +7.8% |
| Existing allowed / 65,536 | 60.39 | 67.05 | +11.0% |
| Existing limited / 64 | 68.17 | 79.30 | +16.3% |
| Existing limited / 4,096 | 67.49 | 79.03 | +17.1% |
| Existing limited / 65,536 | 67.42 | 79.01 | +17.2% |
| Weighted allowed, cost 32 | 59.98 | 67.59 | +12.7% |
| Fresh key / 64 | 191.10 | 255.55 | +33.7% |
| New key refused / 1 probe | 53.64 | 68.70 | +28.1% |
| New key refused / 4 probes | 61.27 | 87.63 | +43.0% |
| New key refused / 16 probes | 90.29 | 153.06 | +69.5% |

These are slope point estimates, not tail latencies. Existing-key admission
pays for pin/unpin and atomic time/TAT coordination. A miss can inspect two
bounded passes and revalidate a candidate under exclusive ownership; its
higher cost is not hidden by the hot-key improvement. Oversized-key rejection
remains about 2 ns. Store construction is excluded as before; the new layout
and its cache effects are part of this implementation comparison.

### Useful throughput and Busy

Outcome rates below use measured batch totals including warmup/calibration,
as in the preceding comparison. Already-limited runs grant zero admissions.

| Hot-key workload | Threads | Mutex outcome/s, millions | CAS outcome/s, millions | Mutex Busy | CAS Busy |
| --- | ---: | ---: | ---: | ---: | ---: |
| Allowed | 2 | 9.448 | 9.435 | 51.63% | <0.001% |
| Allowed | 4 | 4.954 | 5.875 | 83.20% | 0.019% |
| Allowed | 8 | 1.896 | 3.956 | 94.92% | 0.255% |
| Limited | 2 | 8.460 | 11.784 | 51.43% | <0.001% |
| Limited | 4 | 5.291 | 9.094 | 82.70% | 0.007% |
| Limited | 8 | 1.822 | 4.886 | 95.19% | 0.026% |

CAS amortized time per attempt is higher, because attempts now usually
complete a decision instead of returning Busy immediately. At eight threads,
Allowed throughput is about 2.09 times the mutex observation, not ten times
faster despite the large Busy reduction. Bounded retries spend CPU; they do
not make one key scale linearly with thread count.

A separate eight-thread repeat with the same 64-attempt budget measured
3.112 million Allowed/s with 0.194% Busy, and 4.648 million Limited/s with
0.059% Busy. Thus observed Allowed improvement is 1.64–2.09 times the retained
mutex baseline across these two CAS runs, not a stable twofold guarantee.
No workspace checks ran concurrently with either measurement. The repeat
is retained as `target/quota-cas-repeat.log` and `quota_cas_repeat_64`:

```sh
cargo bench --workspace --all-features --bench admission -- --noplot --save-baseline quota_cas_repeat_64 '^(memory_hot_key_contention|memory_hot_key_limited_contention)/amortized_attempt/8$'
```

| CAS attempts / call | Threads | Allowed/s, millions | Busy | Per-thread Allowed share, min–max |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 8 | 3.394 | 8.39% | 12.37–12.74% |
| 32 | 8 | 3.136 | 2.12% | 12.48–12.53% |
| 64 | 8 | 3.956 | 0.255% | 12.49–12.51% |

Budget runs are sequential observations, not a monotonic tuning law or
confidence intervals for throughput. More attempts do not guarantee higher
throughput. With budget 64, Allowed shares at two and four threads are about
50% and 25%; eight-thread Limited shares are 12.499–12.501%. The harness gives
each thread the same number of attempts, so near-equal shares with few Busy
outcomes are expected. They do not establish scheduler fairness, starvation
freedom or a bound on any individual caller's progress.

### Safety verification

All 37 quota tests and the workspace test suite pass; workspace/all-features/
all-targets Clippy passes with warnings denied. Allocation tests still observe
zero admission allocations, including first use, busy refusal and replacement.
The new tests cover:

- Shared CAS exhaustion without partial charge or installation.
- Concurrent pins, reader-count exhaustion and release on unwind.
- Stale generations, non-wrapping retirement and replacement-key validation.
- Candidate revalidation after a racing charge; live debt cannot be reclaimed.
- Lost TAT CAS, fresh time/state on retry, weighted all-or-nothing consumption
  and racing first installation without duplicate identities.
- Partial writer panic poisoning, structural-lock-independent hot hits,
  overlapping clock samples and bounded concurrent key churn.

These are deterministic schedules, model checks and stress tests, not an
exhaustive memory-model proof. No Loom, Miri or sanitizer validation was run.
SHM and cross-process crash recovery remain unimplemented.

Artifacts: `target/quota-cas-tests.log`, `target/quota-cas-clippy.log`,
`target/quota-cas-bench.log` and the `quota_cas_64` Criterion baseline. The
preceding mutex artifacts are retained; raw output is not published source.

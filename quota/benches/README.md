# Memory quota benchmarks

## Scope

These benchmarks measure process-local quota admission: hashing, lookup,
pinning, clock sampling and atomic GCRA decisions. HTTP, authentication,
sessions, SHM and end-to-end request latency are outside their scope.

Admission uses fixed backing and an explicit CAS attempt budget. Existing-key
consumers pin identity and update TAT atomically; installation and reuse take
the structural mutex with `try_lock`. A miss inspects at most two bounded probe
passes. Native lock-free u128 atomics are required.

## Run

From the repository root:

```sh
cargo bench --workspace --all-features --bench admission -- --noplot
```

Serial measurements exclude store construction and destruction. New-key cases
use a fresh store per input. Hot-key cases use one shard, four probes and a
64-byte combined key limit. Table-size comparisons keep one key hot; they do
not measure a cold or random working set.

Contention cases give each thread the same number of attempts against one
shared key. Thread creation is outside timing; barrier release and joins are
included. Outcome rates are aggregate counts divided by measured batch wall
time, including warmup/calibration. Busy is a refusal, not admitted work.
Amortized time per attempt is not latency per successful admission.

## macOS measurements — 2026-10-20

Apple M3 Max, arm64, macOS 27.0.1; rustc 1.99.1, Cargo 1.99.1, Criterion 0.8.2.
Release profile: optimization level 3, thin LTO, one codegen unit. Criterion
defaults: 3-second warmup, approximately 5-second measurement, 100 samples.
No workspace checks ran concurrently with measurement.

The main fixture uses 64 CAS attempts per call. Budgets of 16 and 32 are
separate eight-thread cases. These are explicit benchmark settings, not
library defaults or recommended production values.

### Serial admission

Values are Criterion slope point estimates, not p95/p99 latency.

| Operation | Slots / probes | Time, ns |
| --- | ---: | ---: |
| Existing key allowed | 64 / 4 | 67.43 |
| Existing key allowed | 4,096 / 4 | 67.20 |
| Existing key allowed | 65,536 / 4 | 67.05 |
| Existing key limited | 64 / 4 | 79.30 |
| Existing key limited | 4,096 / 4 | 79.03 |
| Existing key limited | 65,536 / 4 | 79.01 |
| Weighted allowed, cost 32 | 64 / 4 | 67.59 |
| New key allowed | 64 / 4 | 255.55 |
| New key refused, full table | 1 / 1 | 68.70 |
| New key refused, full table | 4 / 4 | 87.63 |
| New key refused, full table | 16 / 16 | 153.06 |
| Oversized key refused | 64 / 4 | 2.00 |

### One shared hot key

The Limited workload has no remaining quota and grants zero admissions.

| Workload | Threads | Outcome/s, millions | Busy |
| --- | ---: | ---: | ---: |
| Allowed | 2 | 9.435 | <0.001% |
| Allowed | 4 | 5.875 | 0.019% |
| Allowed | 8 | 3.956 | 0.255% |
| Limited | 2 | 11.784 | <0.001% |
| Limited | 4 | 9.094 | 0.007% |
| Limited | 8 | 4.886 | 0.026% |

A repeat of the eight-thread cases measured 3.112 million Allowed/s with
0.194% Busy and 4.648 million Limited/s with 0.059% Busy. These observations
are not throughput guarantees.

### CAS budget

| Attempts / call | Threads | Allowed/s, millions | Busy | Per-thread Allowed share, min–max |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 8 | 3.394 | 8.39% | 12.37–12.74% |
| 32 | 8 | 3.136 | 2.12% | 12.48–12.53% |
| 64 | 8 | 3.956 | 0.255% | 12.49–12.51% |

These are sequential runs, not confidence intervals or a monotonic tuning
rule. More attempts do not guarantee higher throughput. Near-equal outcome
shares are expected when threads make equal attempts and Busy is rare;
they do not establish scheduler fairness or starvation freedom.

## Linux measurements — 2026-10-20

Two Intel Xeon E5-2680 v2 CPUs at 2.80 GHz, x86_64, 20 physical cores /
40 logical CPUs, two NUMA nodes; AlmaLinux 8.10, Linux
5.15.223-1.el8.elrepo.x86_64; rustc 1.99.1, Cargo 1.99.1, Criterion 0.8.2.
Measured source revision: `9579d01`.

The command, release profile, fixture settings and Criterion defaults above
were unchanged: 3-second warmup, approximately 5-second measurement and
100 samples per case. No workspace checks ran concurrently. Existing services
remained running; no CPU affinity or NUMA placement was set for this run.
These results describe this machine, not an isolated Linux-versus-macOS
comparison: CPU architecture, hardware and scheduling differ.

### Serial admission

Values are Criterion slope point estimates, not p95/p99 latency.

| Operation | Slots / probes | Time, ns |
| --- | ---: | ---: |
| Existing key allowed | 64 / 4 | 180.76 |
| Existing key allowed | 4,096 / 4 | 180.99 |
| Existing key allowed | 65,536 / 4 | 183.48 |
| Existing key limited | 64 / 4 | 188.15 |
| Existing key limited | 4,096 / 4 | 188.31 |
| Existing key limited | 65,536 / 4 | 188.04 |
| Weighted allowed, cost 32 | 64 / 4 | 185.40 |
| New key allowed | 64 / 4 | 390.00 |
| New key refused, full table | 1 / 1 | 182.19 |
| New key refused, full table | 4 / 4 | 284.15 |
| New key refused, full table | 16 / 16 | 676.90 |
| Oversized key refused | 64 / 4 | 3.80 |

### One shared hot key

Rates include warmup/calibration and the timed barrier release and joins, as
in the macOS run. The Limited workload grants zero admissions.

| Workload | Threads | Outcome/s, millions | Busy |
| --- | ---: | ---: | ---: |
| Allowed | 2 | 5.463 | <0.001% |
| Allowed | 4 | 3.517 | 0.015% |
| Allowed | 8 | 2.266 | 1.157% |
| Limited | 2 | 7.244 | 0.000% |
| Limited | 4 | 7.347 | 0.000% |
| Limited | 8 | 5.558 | 0.000% |

### CAS budget

| Attempts / call | Threads | Allowed/s, millions | Busy | Per-thread Allowed share, min–max |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 8 | 2.224 | 24.511% | 12.21–13.03% |
| 32 | 8 | 2.268 | 8.464% | 12.39–12.63% |
| 64 | 8 | 2.266 | 1.157% | 12.47–12.52% |

These are sequential cases from one run, not repeated-run confidence
intervals. The 16-attempt case refuses a larger fraction of work; its lower
amortized attempt time does not mean higher admitted throughput. Equal
attempt counts and per-thread outcome shares do not establish scheduler
fairness or starvation freedom.

The Linux quota suite also passed all 61 tests, including cross-process SHM
admission and SIGKILL recovery. This benchmark still measures process-local
memory admission only; it supplies no SHM performance measurement.

Raw Linux output: `target/quota-linux-bench.log`; Criterion estimates are
under `target/criterion/`. These generated artifacts are not published source.

## Verification and limits

Memory/common tests cover:

- Integer GCRA against an independent token-bucket model: fractional refill,
  weighted all-or-nothing cost, retry rounding and arithmetic overflow.
- Independent stores, shared clones, capacity pressure and no live-debt eviction.
- CAS exhaustion, concurrent pins, unwind cleanup and partial-write poisoning.
- Stale generations, retirement without wrap, candidate revalidation and safe
  slot reuse under concurrent key churn.
- Racing first installation, lost TAT CAS and fresh time/state on retry.
- Zero admission allocations, including first insertion, replacement and Busy.

Allocation tests count thread-local Rust global-allocator calls, not every
internal OS allocation. The CAS budget bounds atomic API calls, not scheduling
time or architecture-specific instruction retries. Lock-free is not wait-free.
Tests and benchmarks do not prove exhaustive memory-model correctness,
cross-process crash recovery or protection against volumetric DDoS. No Loom,
Miri or sanitizer validation is included. SHM validation and costs are outside
these recorded memory measurements.

Raw output: `target/quota-cas-tests.log`, `target/quota-cas-clippy.log`,
`target/quota-cas-bench.log`, `target/quota-cas-repeat.log` and Criterion data
under `target/criterion/`. Generated artifacts are not published source.

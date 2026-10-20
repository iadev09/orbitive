# Quota benchmarks

## Scope

Memory benchmarks measure process-local quota admission: hashing, lookup,
pinning, clock sampling and atomic GCRA decisions. The process suite below
supports independently opened fleet mappings and independent local guards.
Neither measures HTTP,
authentication, sessions or end-to-end request latency.

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

### Single-NUMA CPU affinity comparison

One additional run restricted the benchmark to CPUs 2–9: eight distinct
physical cores on NUMA node 0, excluding SMT siblings. Threads could migrate
within that CPU set; memory placement was not explicitly bound. Source code,
release profile and benchmark settings were unchanged; existing services
remained running.

| Measurement | Unrestricted | CPU set 2–9 |
| --- | ---: | ---: |
| Existing key allowed, 64 slots, ns | 180.76 | 185.80 |
| Allowed/s, 8 threads, millions | 2.266 | 2.470 |
| Busy, 8-thread Allowed workload | 1.157% | 0.815% |
| Limited/s, 8 threads, millions | 5.558 | 6.812 |

This is one run per placement, not a repeated-run confidence interval.
Eight-thread Allowed throughput increased by about 9% and Limited throughput
by 23%; serial admission did not improve. The comparison does not isolate
kernel effects or establish that NUMA placement alone caused the differences.

Raw affinity-run output: `target/quota-linux-numa0-bench.log`.

## SHM process benchmarks

The process harness selects `LocalGuard`, `FleetGuard` or `FleetQuota` while
keeping workload generation and rate/geometry settings in one implementation.
`LocalGuard` creates one independent store in each child, with its own full
capacity and budget; fleet stores share one resource. This compares policy
choices, not three equivalent fleet-wide limits or equal total memory budgets.
Each call uses the same `dyn QuotaStore` dispatch boundary.

```sh
cargo bench --workspace --all-features --bench shm_admission -- --noplot
# All churn geometries:
cargo bench --workspace --all-features --bench shm_admission -- churn_reuse --noplot
# 256 slots, 4/8/16 shards, fixed 16 probes:
cargo bench --workspace --all-features --bench shm_admission -- churn_reuse_slots_256 --noplot
# 1024 slots, 16/32/64 shards, fixed 16 probes:
cargo bench --workspace --all-features --bench shm_admission -- churn_reuse_slots_1024 --noplot
# Short protocol/workload check, without Criterion measurements:
cargo bench --workspace --all-features --bench shm_admission -- --quota-smoke
# One fixed-work CPU/throughput batch; arguments are explicit:
# STORE CASE PROCESSES ATTEMPTS SAMPLE_EVERY WARMUP_ATTEMPTS
cargo bench --workspace --all-features --bench shm_admission -- \
  --quota-profile fleet_guard shared_allowed 8 500000 0 4096
# Separate latency sampling pass (one call out of every 64):
cargo bench --workspace --all-features --bench shm_admission -- \
  --quota-profile fleet_guard shared_allowed 8 100000 64 4096
```

Each case uses 1, 2, 4 or 8 child processes independently opening one isolated
SHM resource. Process startup, opening, seeding and batch readiness are outside
timing. GO/reply pipe traffic, result collection and scheduling are included;
small batches can be dominated by that overhead. Criterion time is amortized
per attempt, not per-admission latency. Summaries include warmup/calibration
and report Allowed, Limited, Busy and CapacityFull separately, including
per-process counts. Busy and CapacityFull are not admitted work.

| Workload | Keys | Slots / probes | Policy |
| --- | --- | ---: | --- |
| `shared_allowed` | One preseeded shared key | 64 / 16 | Available budget |
| `shared_limited` | One pre-exhausted shared key | 64 / 16 | No refill during measurement |
| `per_process_allowed` | One preseeded key per process | 64 / 16 | Available budget |
| `churn_reuse` | Unique process/sequence keys | 64 / 16 | One-unit burst, 1 ns debt |
| `churn_reuse_shards_2`, `churn_reuse_shards_4` | Same churn, 2 or 4 shards | 64 / 16 | Same churn policy |
| `churn_reuse_slots_256_shards_4/8/16` | Same churn, 4, 8 or 16 shards | 256 / 16 | Same churn policy |
| `churn_reuse_slots_1024_shards_16/32/64` | Same churn, 16, 32 or 64 shards | 1,024 / 16 | Same churn policy |
| `churn_debt`, `churn_debt_slots_1024_shards_64` | Unique keys, starting empty; finite warmup | 64 / 16 or 1,024 / 16 | One unit per `Duration::MAX`, burst 1; guards may evict live debt |
| `churn_full` | Unique keys against 16 preoccupied slots | 16 / 16 | Strict quota preserves live debt; guards may replace it |

Base cases use one shard; named churn variants declare their shard count.
All cases use a 64-byte combined key bound, 64 CAS attempts and zero
opportunistic recovery attempts. Owner capacity is process count plus the parent.
Available budget is 1 billion units/s with a `u64::MAX` burst;
non-refilling fixtures use one unit per `Duration::MAX` with a one-unit burst.
Churn key generation is included in timing; keys never repeat across batches.
The separate-key control still shares the shard clock watermark. These are
synthetic workload settings, not recommended rate-limit policies.

Workers stay open across batches. The smoke check verifies two batches per
workload, outcome totals and workload-specific refusals. Cleanup stops only
benchmark children, releases mappings and takes the isolated fleet's exclusive
hold before unlinking its exact resource. No production fleet is accessed.
Crash recovery costs and hostile-process isolation are not measured here.

Every child reports its user/system CPU time from `getrusage(RUSAGE_SELF)`
around the attempt loop. CPU per attempt includes key construction, virtual
dispatch, outcome counting and optional sampling; it excludes opening, warmup,
parent pipe handling and result encoding. It is not elapsed latency or CPU per
successful admission. Criterion summaries include warmup/calibration; fixed-work
profiles discard their explicitly requested warmup batch.

`SAMPLE_EVERY=0` disables call timing. Nonzero values enable fixed-size log2
nanosecond histograms per outcome. Reported p50/p95/p99 are bucket upper bounds,
not exact quantiles; empty outcomes report no quantile. Timings cover the
`try_consume` call, including lock attempts, but exclude key construction and
outcome classification. Sampling adds clock/histogram work and can alter
contention. Keep sampled latency and unsampled throughput/CPU passes separate.
The clock-read cost is not subtracted. Batch wall time includes GO/reply pipes,
result parsing and scheduling; small batches can be dominated by harness work.

### Linux SHM measurements — 2026-10-20

The same Linux hardware/toolchain and Criterion defaults as above, with the
benchmark and its children restricted to CPUs 2–9 on NUMA node 0. No explicit
memory binding or per-process CPU assignment was applied. Initial system idle
was 99–100%; existing services remained running. Backend source is `9579d01`.
This is one run per case, not repeated-run confidence intervals or a capacity
guarantee. Rates below are batch outcome totals divided by elapsed wall time.

| Workload | Processes | Allowed/s, millions | Limited/s, millions | Busy | CapacityFull |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared allowed | 1 | 0.988 | 0 | 0% | 0% |
| Shared allowed | 2 | 1.723 | 0 | 0% | 0% |
| Shared allowed | 4 | 3.225 | 0 | 0% | 0% |
| Shared allowed | 8 | 3.108 | 0 | 0.349% | 0% |
| Shared limited | 1 | 0 | 0.992 | 0% | 0% |
| Shared limited | 2 | 0 | 1.788 | 0% | 0% |
| Shared limited | 4 | 0 | 3.286 | 0% | 0% |
| Shared limited | 8 | 0 | 6.387 | 0% | 0% |
| Per-process key | 1 | 1.001 | 0 | 0% | 0% |
| Per-process key | 2 | 1.828 | 0 | 0% | 0% |
| Per-process key | 4 | 3.395 | 0 | 0% | 0% |
| Per-process key | 8 | 6.593 | 0 | 0% | 0% |
| Churn reuse | 1 | 0.067 | 0 | 0% | 0% |
| Churn reuse | 2 | 0.082 | 0 | 48.809% | 0% |
| Churn reuse | 4 | 0.091 | 0 | 74.578% | 0% |
| Churn reuse | 8 | 0.094 | 0 | 87.789% | 0% |
| Churn full | 1 | 0 | 0 | 0% | 100% |
| Churn full | 2 | 0 | 0 | 47.881% | 52.119% |
| Churn full | 4 | 0 | 0 | 73.474% | 26.526% |
| Churn full | 8 | 0 | 0 | 86.161% | 13.839% |

One-process Criterion slope estimates were 1.010 µs for an existing shared key,
14.953 µs for churn/reuse and 15.012 µs for full-table refusal. The single-shard
installation/reuse path serializes structural changes; adding processes barely
increases churn admissions and mostly increases Busy refusals. Existing-key
consumption does not acquire that structural hold. Full-table churn grants
zero admissions, but refusing work still consumes CPU. These measurements do
not establish protection against a hostile traffic mix or volumetric DDoS.

Raw output: `target/quota-shm-linux-bench.log`; named Criterion baseline:
`shm-numa0`. Smoke checks pass on macOS and Linux; the macOS workspace tests
and workspace/all-features/all-targets Clippy with warnings denied also pass.

### Linux churn shard comparison — 2026-10-20

Same hardware, toolchain, CPU set 2–9 and measurement settings. The backend is
unchanged. Total slots (64), probes per pass (16), key bound, CAS/recovery
budgets and churn policy stay fixed. The 1/2/4-shard matrix includes a fresh
single-shard reference. Each case is one run; initial system idle was 99–100%
and services remained running.

| Processes | Shards | Allowed/s | Busy |
| ---: | ---: | ---: | ---: |
| 1 | 1 | 67,209 | 0% |
| 1 | 2 | 67,492 | 0% |
| 1 | 4 | 66,289 | 0% |
| 2 | 1 | 81,716 | 48.645% |
| 2 | 2 | 104,796 | 24.704% |
| 2 | 4 | 115,207 | 12.889% |
| 4 | 1 | 90,568 | 74.397% |
| 4 | 2 | 140,579 | 54.671% |
| 4 | 4 | 179,977 | 33.662% |
| 8 | 1 | 94,243 | 87.743% |
| 8 | 2 | 161,961 | 76.384% |
| 8 | 4 | 247,363 | 58.911% |

Limited and CapacityFull counts were zero throughout. At eight processes,
four shards admitted about 2.62 times as much work as one, but Busy remained
substantial. One-process throughput stayed near 67,000/s: partitioning reduces
contention, not the serial cost of installation/reuse. Shards own disjoint
slot ranges; four shards leave 16 slots each, the minimum compatible with the
fixed 16-probe budget. More shards would require changing another setting.

Raw output: `target/quota-shm-linux-shards-bench.log`; named Criterion baseline:
`shm-shards`. These results do not establish workload-independent scaling.

### Linux churn with 256 slots — 2026-10-20

Same hardware, toolchain, CPU set 2–9 and Criterion settings. This matrix fixes
total slots at 256 and compares 4, 8 and 16 shards. Probes remain 16, CAS
attempts 64 and recovery attempts zero; policy and key generation are unchanged.
Initial system idle was 99–100%; services stayed running. Each case is one run.

| Processes | Shards | Allowed/s | Busy |
| ---: | ---: | ---: | ---: |
| 1 | 4 | 67,574 | 0% |
| 1 | 8 | 67,306 | 0% |
| 1 | 16 | 65,495 | 0% |
| 2 | 4 | 114,833 | 12.153% |
| 2 | 8 | 120,344 | 6.225% |
| 2 | 16 | 122,835 | 3.222% |
| 4 | 4 | 186,338 | 31.904% |
| 4 | 8 | 208,630 | 17.444% |
| 4 | 16 | 219,742 | 9.384% |
| 8 | 4 | 269,337 | 55.857% |
| 8 | 8 | 342,885 | 35.314% |
| 8 | 16 | 389,221 | 20.399% |

Limited and CapacityFull counts were zero. At eight processes, 16 shards
increased admissions by about 45% over four shards, but roughly one in five
attempts still returned Busy. Serial throughput did not improve. Sixteen
shards leave 16 slots each, matching the fixed probe budget's minimum geometry.
Comparisons with the 64-slot matrix also change total capacity and are not a
shard-only comparison.

Raw output: `target/quota-shm-linux-256-bench.log`; named Criterion baseline:
`shm-slots-256`. No backend code changed for either shard matrix.

### Linux churn with 1,024 slots — 2026-10-20

Same hardware, toolchain, CPU set 2–9 and Criterion settings. Total slots stay
at 1,024 while shards vary between 16, 32 and 64. Probes remain 16, CAS
attempts 64 and recovery attempts zero; policy and key generation are unchanged.
Initial system idle was 99–100%; services stayed running. Each case is one run.

| Processes | Shards | Allowed/s | Busy |
| ---: | ---: | ---: | ---: |
| 1 | 16 | 65,770 | 0% |
| 1 | 32 | 67,221 | 0% |
| 1 | 64 | 66,058 | 0% |
| 2 | 16 | 121,558 | 3.083% |
| 2 | 32 | 123,657 | 1.549% |
| 2 | 64 | 123,115 | 0.809% |
| 4 | 16 | 221,065 | 8.921% |
| 4 | 32 | 227,222 | 4.613% |
| 4 | 64 | 230,062 | 2.406% |
| 8 | 16 | 400,946 | 19.395% |
| 8 | 32 | 416,550 | 10.300% |
| 8 | 64 | 418,629 | 5.430% |

Limited and CapacityFull counts were zero. At eight processes, Busy decreased
from 19.395% to 5.430%, while admissions increased by only about 4.4%.
The 32-to-64-shard admission difference is about 0.5%; one run does not establish
a meaningful throughput gain at that scale. Lower Busy does not imply a
proportional throughput improvement. Sixty-four shards leave 16 slots each,
the minimum geometry compatible with the unchanged probe budget.

Raw output: `target/quota-shm-linux-1024-bench.log`; named Criterion baseline:
`shm-slots-1024`. Backend code remains unchanged.

## Linux policy comparison — 2026-10-21

Same dual Xeon E5-2680 v2, AlmaLinux 8.10 and Linux 5.15.223 host as above,
Rust/Cargo 1.99.1; release opt-level 3, thin LTO, one codegen unit. Source is
the working tree with guard policies, not a published revision; file hashes
are retained alongside the raw log. Processes inherited CPU set 2–9 (eight
physical cores on NUMA node 0), with no individual pinning or memory binding.
Initial idle was 98–100%, with no swapping. Existing services remained running.

Three fixed-work trials used 500,000 attempts per process after 4,096 untimed
warmup attempts. Store order was reversed in the middle trial. Tables report
medians, not confidence intervals. A separate single pass used 100,000 attempts
per process with one latency sample per 64 calls. No workspace validation ran
concurrently with measurement. Historical Criterion numbers above use a
different harness; compare policies within this new run instead.

Hot-key geometry was 64 slots / 1 shard; churn was 1,024 slots / 64 shards.
All used 16 probes per pass, 64 CAS attempts, 64 key bytes and zero recovery
attempts. Churn-reuse debt replenishes in 1 ns; churn-debt uses one unit per
`Duration::MAX`, burst 1. The latter deliberately exhausts retained capacity
without refill, rather than modeling a production rate. Local capacity is per
process; fleet capacity and debt are shared. This distinction is intentional.

### Eight processes

CPU is aggregate worker user + system time per attempt, including refusals.
Latency is conditional on the indicated outcome in the separate sampled pass.
The p99 column is a histogram bucket upper edge, not a hard latency bound.

| Workload | Policy | Allowed/s | Busy | CapacityFull | CPU µs/attempt | Sampled p99 upper edge, µs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Same key, available | LocalGuard | 34,375,524 | 0% | 0% | 0.223 | 0.511 |
| Same key, available | FleetGuard | 2,321,808 | 2.515% | 0% | 3.235 | 16.383 |
| Same key, available | FleetQuota | 2,937,902 | 0.397% | 0% | 2.580 | 16.383 |
| Churn, replenished debt | LocalGuard | 7,284,968 | 0% | 0% | 1.083 | 2.047 |
| Churn, replenished debt | FleetGuard | 435,184 | 5.538% | 0% | 17.093 | 32.767 |
| Churn, replenished debt | FleetQuota | 430,991 | 5.509% | 0% | 17.090 | 32.767 |
| Churn, live debt | LocalGuard | 6,163,170 | 0% | 0% | 1.137 | 2.047 |
| Churn, live debt | FleetGuard | 436,261 | 5.580% | 0% | 17.150 | 32.767 |
| Churn, live debt | FleetQuota | 0 | 5.104% | 94.896% | 16.823 | 32.767 (Full) |

The exhausted same-key workload grants nothing in every policy. Limited/s
medians were 33,336,176 (LocalGuard), 6,198,724 (FleetGuard) and 6,362,478
(FleetQuota), all with zero Busy/Full. CPU per attempt was 0.235, 1.286 and
1.243 µs, respectively; sampled p99 upper edges were 0.511, 2.047 and 2.047 µs.

### One-process reference

| Policy | Same-key Allowed/s | Same-key CPU µs/attempt | Replenished-churn Allowed/s | Churn CPU µs/attempt |
| --- | ---: | ---: | ---: | ---: |
| LocalGuard | 4,856,388 | 0.206 | 1,044,023 | 0.956 |
| FleetGuard | 954,842 | 1.046 | 65,207 | 15.307 |
| FleetQuota | 985,428 | 1.013 | 64,609 | 15.446 |

The guard's benefit is retention policy, not a generally faster SHM algorithm.
At eight processes it sustains new-key admissions after the live-debt table
fills; strict quota intentionally refuses them. Both still pay for bounded
identity lookup, structural acquisition and safe publication. Refusing a full
table also costs CPU. Replenished-churn rates are similar; same-key FleetGuard
has about 21% fewer admissions and more Busy than FleetQuota in this run.
The measurements do not isolate recency CAS cost from layout and scheduling.

Same-key FleetGuard rates ranged from 2.252–2.335 million/s across three trials.
Live-debt LocalGuard churn ranged from 4.923–7.078 million/s; its median is not
a stable capacity guarantee. Independent local stores avoid fleet coordination
but do not enforce one fleet-wide limit. No HTTP, attack-resistance, scheduler
fairness or hard real-time guarantee follows from these results.

Raw data: `target/quota-guards-linux-comparison.log` (72 unsampled and 24 sampled
batches), `target/quota-guards-linux-source.sha256`, and the Linux test/smoke logs.
All 83 quota tests pass on macOS and Linux. All 22 guard tests pass in 20 local
repetitions; both platforms pass the process smoke matrix. Workspace tests and
warnings-denied all-target Clippy pass. No production service was restarted.

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
Miri or sanitizer validation is included. SHM process measurements include
coordination overhead and use different hashing, pinning and probe geometry;
they are not a direct backend-only comparison with the memory results.

Raw output: `target/quota-cas-tests.log`, `target/quota-cas-clippy.log`,
`target/quota-cas-bench.log`, `target/quota-cas-repeat.log` and Criterion data
under `target/criterion/`. Generated artifacts are not published source.

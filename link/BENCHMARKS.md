# orbit-link benchmark journal

Host-specific measurements are recorded here with their workload and limits. Keep the microbenchmark and downstream integration separate: they
answer different questions.

## Measurement layers

- `stream/benches/transport.rs` measures the exchange payload arena across
  body size, application chunk size and concurrency. `orbit-link` does not use
  that arena for its current body path.
- `link/benches/dispatch.rs` measures the synchronous named-dispatch protocol
  core: target lookup, ticket encode/decode, inbox publication, stream open and
  a tiny duplex request/reply. It does not await the Tokio reactor bell.
- `servers/edge-server/scripts/bench.sh` is the downstream test: an edge server
  implementing Orbit. It includes cross-process SHM, the async inbox receiver,
  four edge workers, the retained body slot, HTTP and one Gufi application
  process.

The layers are complementary, not numerically interchangeable.

The native Rust downstream harness lives in Claviron's
[bench-app example](https://github.com/claviron/claviron/tree/main/examples/bench-app).
It compares Orbit Link with pooled UDS/h2c and TCP/h2c through the same Edge
and application handlers. Measurements from that harness are recorded here.

## 2026-09-29 — synchronous link core

Host: macOS `bison`. Command:

```sh
cargo bench -p orbit-link --bench dispatch
```

Geometry: two lanes, 128 inbox frames of 17 KiB, 128 duplex slots per node,
64 KiB per direction. Criterion confidence intervals:

| case | time |
|---|---:|
| Orbit, fresh slot | 369.61–375.51 ns |
| Orbit, rearmed slot | 328.42–330.07 ns |
| fresh `UnixStream::pair` | 2.4017–2.4456 us |

This is protocol-core cost. The same task writes and reads the inbox, so these
numbers say nothing about descriptor wake or process scheduling.

## 2026-09-29 — downstream edge server implementing Orbit

Host: macOS `bison`. Four edge workers, one Gufi application process, release
binaries built from the local Orbitive and Claviron branches. Command shape:

```sh
ORDER="uds orbit" TIME=10s ./scripts/bench.sh
ORDER="orbit uds" TIME=10s ./scripts/bench.sh
```

Load: `wrk -t4 -c64 -d10s /_gufi/rust`. The route returns a small Rust
response and does not enter Python.

After the reactor-native inbox receiver and consumer-first depth sampling, the
final binaries were rebuilt from the finished source and run in both orders:

| order | transport | requests/s | average | CPU/request |
|---|---|---:|---:|---:|
| UDS then Orbit | UDS | 115,016 | 573.86 us | 94.0 us |
| UDS then Orbit | Orbit | 134,389 | 451.14 us | 80.5 us |
| Orbit then UDS | Orbit | 132,065 | 466.35 us | 80.7 us |
| Orbit then UDS | UDS | 115,923 | 567.70 us | 95.2 us |

A separate 30-second Orbit row was clean at 134,713 requests/s, 449.09 us and
80.8 us/request. In the final bidirectional runs Orbit carried 13.9–16.8% more
requests, with 17.9–21.4% lower average latency and 14.4–15.2% less CPU per
request. Before the async inbox change, reversed-order Orbit rows were about
62.3k requests/s and
1.02 ms against 114–117k and 561–581 us for UDS.

The 30-second run also exposed an independent false-full race. Producers read
`reserve` before `read`; a consumer advance between those loads could make the
subtraction wrap and refuse an empty lane. Diagnostics saw depth 0–2, not the
128-frame capacity. Admission, reservation and reported depth now sample
consumer-first (`read`, then `reserve`). The clean long run retained the same
128-frame inbox; increasing capacity was neither necessary nor a fix.


## 2026-10-11 — FreeBSD synchronous link core (background build active)

Environment: FreeBSD 15.1-RELEASE-p3, ARM64, an 8-vCPU virtual machine with
32 GiB RAM. Toolchain: rustc 1.98.1 and cargo 1.98.1. Source revision:
`5946bf48c8a84c56c833185c6821ec274e8c2261`.

The benchmark ran from a local-disk copy, not the shared p9 filesystem.
Rust sources and Cargo manifests were checked against that revision before
measurement. Command, from the workspace root:

```sh
cargo bench --workspace --all-features --bench dispatch --locked -- --noplot
```

Release profile: optimization level 3, thin LTO, one codegen unit. Criterion
used 100 samples per case, a 3-second warm-up and an approximately 5-second
measurement window. The existing benchmark was run without source changes:
two lanes, 128 inbox frames of 17 KiB, 128 duplex slots per node and 64 KiB
per direction. Request and response payloads were 18 and 19 bytes respectively.

All times below are **nanoseconds per complete benchmark iteration**; bounds
are Criterion's 95% confidence interval.

| Case | Lower bound | Central estimate | Upper bound |
| --- | ---: | ---: | ---: |
| Orbit, fresh slot | 349.32 ns | 350.05 ns | 350.83 ns |
| Orbit, rearmed slot | 269.05 ns | 269.67 ns | 270.40 ns |
| Fresh `UnixStream::pair` | 1699.1 ns | 1703.1 ns | 1708.2 ns |

The fresh UDS iteration took approximately 4.87 times the fresh Orbit slot
iteration and 6.32 times the rearmed Orbit slot iteration. These ratios apply
only to this protocol-core benchmark:

- Both endpoints execute synchronously in the same process. There is no
  cross-process scheduling or awaited Tokio doorbell in the measurement.
- Each UDS iteration creates and closes a new socket pair. An already-open,
  reused UDS connection is not measured. Orbit opens its named SHM resources
  outside the measured iterations; its fresh-slot row allocates a stream slot,
  not a new SHM segment.
- The result does not establish end-to-end HTTP throughput, production latency
  or the relative cost of asynchronous wakeups.

A separate compilation was active throughout the run with two parallel build
jobs. Short `vmstat` samples during the benchmark showed 46–49% aggregate CPU
idle time, versus 71–74% in samples before the timed benchmark. The first
`vmstat` row is a since-boot average and is excluded from those ranges. CPU
headroom does not rule out cache, memory-bandwidth or host scheduling effects;
this is a **preliminary measurement under background load**, not an idle-host
baseline. Cross-host differences from the macOS rows must not be attributed
solely to the operating system.

Criterion reported 7, 2 and 4 outliers among the 100 samples for fresh Orbit,
rearmed Orbit and fresh UDS respectively. Narrow intervals within this run do
not quantify interference from the background build or run-to-run variability.

## 2026-10-22 — Linux owner-only dispatch before and after group access

Environment: Linux x86-64, Intel Xeon E5-2680 v2 at 2.80 GHz, 40 logical
CPUs, kernel `5.15.223-1.el8.elrepo.x86_64`, rustc
`1.99.1 (58cf8f9c3 2026-10-13)`.

The comparison used the parent revision
`b32c5ab04b64ccb269050e3ec80678294d00e749` and the group-access revision
`db8e8932a2cb3a24d3eb6beabe155d2673ffdd51`. The unchanged
`link/benches/dispatch.rs` exercised the existing owner-only API without group
configuration, using the geometry and request/response sizes described above.
External dependency versions were identical between revisions.

Both revisions were compiled with:

```sh
cargo bench --workspace --all-features --bench dispatch --locked --no-run
```

The resulting benchmark executables ran with `--bench --noplot` and separate
saved baselines. Release settings were optimization level 3, thin LTO and one
codegen unit. Criterion used its defaults: 100 samples, a 3-second warm-up and
an approximately 5-second measurement window per case. Both builds completed
before measurement. Run order was **before, after, after, before**.

Each invocation used separate mount, IPC, PID and network namespaces with
private SHM and temporary directories. No production services or global Cargo
configuration were changed. The host still carried background production
workloads; this was not a controlled idle-host measurement.

All values below are Criterion's central estimates in **nanoseconds per
complete benchmark iteration**, not confidence-interval bounds:

| Case | Before run 1 | Before run 2 | After run 1 | After run 2 |
| --- | ---: | ---: | ---: | ---: |
| Orbit, fresh slot | 970.54 ns | 1006.64 ns | 1008.11 ns | 975.41 ns |
| Orbit, rearmed slot | 790.16 ns | 806.03 ns | 782.94 ns | 780.56 ns |
| Fresh `UnixStream::pair` | 8725.35 ns | 8810.35 ns | 8537.31 ns | 8592.00 ns |

The fresh-slot estimates span nearly the same range before and after the
change; these repetitions show no consistent slowdown. Rearmed-slot estimates
were modestly lower after the change, but the UDS control was also lower, so
these results do not establish a causal performance improvement. Within-run
confidence intervals do not capture variation between runs.

The historical macOS and FreeBSD rows above use different hardware, operating
systems, revisions and background workloads. Their lower absolute times are
not evidence of a regression from this commit; the same-host comparison here
addresses that question.

This synchronous benchmark measures target lookup, ticket encoding/decoding,
inbox publication, stream opening and a small duplex exchange. It does not
measure awaited async wakeups, cross-process scheduling or HTTP. Separately,
the isolated Orbitive test run passed 451 tests plus the privileged cross-UID
group transport test, covering owner-only behavior, Linux abstract sockets,
dead-lane recovery and group access checks. No downstream Claviron or edge
server benchmark was run for this comparison.

## 2026-10-22 — native Rust HTTP harness, preliminary inbox comparison

Environment: macOS ARM64. One standalone Edge and one Rust application,
both built with the development profile at optimization level 1. Each row
is one `wrk` run: two threads, ten connections, ten seconds, `/bench/plain`.
Order was Orbit then UDS for each build. UDS used pooled HTTP/2 cleartext
connections; Orbit used the cross-process Link path. CPU cost was not measured.

| Dependency source | Transport | Requests/s | Average latency | Non-2xx/3xx |
| --- | --- | ---: | ---: | ---: |
| Registry Orbitive 0.5.7 | Orbit | 76,805.60 | 125.45 us | 0 |
| Registry Orbitive 0.5.7 | UDS | 53,275.04 | 186.27 us | 0 |
| Local producer-claim inbox changes | Orbit | 19,838.42 | 55.40 us | 7 |
| Local producer-claim inbox changes | UDS | 53,242.70 | 184.93 us | 0 |

The local Orbit run is **not a clean performance result**: it returned seven
503 responses and the Edge did not finish graceful shutdown after the load
generator exited. The lower reported latency does not establish an improvement;
throughput and outstanding work must be investigated together. No cause has
been established.

The older inbox segment was removed before starting the local build because
its layout was incompatible. Other retained test-fleet state was not cleared
before this run. A subsequent full cleanup was refused because the draining
Edge still held its fleet locks; no live segment was removed.

These are single runs, not an alternating repeated comparison. The local
checkout also changed `lru` from 0.18.5 to 0.19.0, so this is not an isolated
inbox-only A/B measurement. Keep this record as a failed integration measurement,
not a release performance claim; repeat with clean stopped test fleets and
controlled dependency versions before attributing the difference.

### Repeat after clearing both test fleets

The previously stuck test Edge was explicitly terminated. With both processes
stopped, all retained resources belonging to the two test fleets were removed.
The same local binaries were restarted without rebuilding; both transport paths
returned `200` before measurement. Workload, profile and Orbit-then-UDS order
were unchanged.

| Transport | Requests/s | Average latency | Non-2xx/3xx |
| --- | ---: | ---: | ---: |
| Orbit | 21,747.65 | 55.23 us | 17 |
| UDS | 53,441.74 | 184.23 us | 0 |

The Orbit errors were again HTTP 503. Clearing retained SHM did not remove the
failure or restore the registry baseline throughput. This remains a failed
integration run, not an isolated measurement of claim overhead. The unchanged
UDS control remained close to its earlier throughput.
After graceful stop, the application exited but the new Edge again remained
alive with nine TCP connections in `CLOSE_WAIT`; its listener was closed.

### Registry control after full cleanup

Both examples were rebuilt against crates.io Orbitive 0.5.7 (the latest
published version checked at measurement time), resolving registry orbit-link
0.5.5. Both test fleets were cleared after explicitly terminating the stuck
test Edge. Both paths returned `200` before the same Orbit-then-UDS workload.

| Transport | Requests/s | Average latency | Non-2xx/3xx |
| --- | ---: | ---: | ---: |
| Orbit | 76,162.83 | 127.24 us | 0 |
| UDS | 51,775.93 | 192.47 us | 0 |

The registry control restored Orbit throughput close to its original baseline,
without HTTP errors. The local clean-fleet Orbit run delivered approximately
71.4% fewer requests/s than this registry control and returned 503 responses.
This strongly associates the regression with the local dependency changes, but
does not isolate which change caused it or measure pure per-request overhead.
Both registry-backed test processes also exited after graceful stop; the
shutdown stall observed with the local build did not recur in this control.

### Diagnosis: contention refusal and lost reply heads

A diagnostic local run enabled existing transport DEBUG messages. Request
publication was refused with one live consumer and reported queue depths of
0, 1 and 2, against a 128-frame inbox. The new `reserve_frame` performs one
claim attempt and returns `InboxFull` on contention, including after helping
another reservation; the registry implementation retries reservation races.
Thus `InboxFull` no longer means only capacity exhaustion or an absent owner.

With runner DEBUG logging enabled, nine distinct reply correlations logged
`reply was not delivered`. The runner returns after a refused reply-head
publication. The Edge's waiting map still retains the corresponding oneshot
senders, so its reply-head waits receive neither a head nor channel closure.
After the load generator exited, nine connections remained in `CLOSE_WAIT`.
A read-only SHM snapshot showed both routing lanes fully drained
(`reserve == read`, depth zero) and no producer record with WRITING set.

This establishes an integration progress failure, not merely additional atomic
instruction cost or a permanently reserved FIFO head. Lost replies reduce the
number of load-generator connections still making progress and obstruct drain.
The implementation also adds intent-record scans, atomic updates and intent
validation to ordinary traffic; their isolated overhead has not been measured.
The failed throughput rows must not be presented as that overhead measurement.
No production fix was applied during this diagnosis.

### Bounded progress and terminal reply failure correction

Claim contention now helps admission and may try a later position only after
observing cursor progress. There are at most `inbox.capacity` attempts; an
unchanged live claim is refused rather than polled. Full capacity and exhausted
producer-record capacity still refuse. Recovery ownership, full-width turns,
intent publication and crash checkpoints remain unchanged.

The downstream runner resets an exchange's response stream if its reply head
cannot be published, including after a buffered body published FIN. The caller
observes reset while waiting for the head and removes its correlation on failure
or cancellation. It does not wait for another inbox frame to announce the error.
The stream's reset observer uses the existing readiness driver and a separate
bounded process-local waker entry, so concurrent body reading cannot replace
its wake. Neither change introduces a timer or a new shared-memory field.

Both examples resolved local Orbitive. The final verification cleared both
stopped test fleets, rebuilt development-profile binaries at optimization
level 1, verified both paths returned `200`, then ran the original two-thread,
ten-connection, ten-second workload in Orbit-then-UDS order. No build or test
was running during this final pair; normal editor and VM activity remained.

| Transport | Requests/s | Average latency | Non-2xx/3xx |
| --- | ---: | ---: | ---: |
| Orbit | 75,103.65 | 129.00 us | 0 |
| UDS | 53,418.06 | 184.28 us | 0 |

Both processes exited after graceful stop, without forced termination. The
failed 21.7k Orbit run was therefore not a valid measure of per-request claim
overhead: throughput recovered near the 76.2k registry control once accepted
exchanges stopped losing their terminal outcomes. The final Orbit row is about
1.4% below that registry control; separate single runs with differing dependency
versions and background activity do not isolate an overhead of that size.

Two earlier correction checks also had no HTTP errors and normal shutdown:
75,312.01/53,253.31 and 75,289.39/53,658.50 requests/s for Orbit/UDS.
They preceded the final progress-only retry refinement and are not additional
repetitions of the exact final source.

Validation: 464 Orbitive tests passed, including native producer/recovery/
consumer crash tests and reset-observer wake tests; workspace Clippy with
warnings denied and formatting passed. Downstream client tests covered failed
reply heads, cancellation cleanup, buffered FIN and successful-head priority;
the runner's refused-publication test passed too. Downstream all-feature Clippy
completed with existing `collapsible_if` warnings elsewhere; its warnings-denied
gate is not clean. No unrelated warning cleanup was included.

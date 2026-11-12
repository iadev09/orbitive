# orbit-link benchmark journal

Host-specific measurements are recorded here with their workload and limits. Keep the microbenchmark and downstream integration separate: they
answer different questions.

## Measurement layers

- `stream/benches/transport.rs` measures the exchange payload arena across
  body size, application chunk size and concurrency. `orbit-link` does not use
  that arena for its current body path.
- `link/benches/transport.rs` compares Link with a Unix socket between two
  processes with no HTTP. For the transport, trust it over the downstream rows
  below; see `link/docs/benches/`.
- A downstream benchmark script is the downstream test: an edge server
  implementing Orbit. It includes cross-process SHM, the async inbox receiver,
  four edge workers, the retained body slot, HTTP and one Gufi application
  process.

The layers are complementary, not numerically interchangeable.

The native Rust downstream harness lives in Claviron's
[bench-app example](https://github.com/claviron/claviron/tree/main/examples/bench-app).
It compares Orbit Link with pooled UDS/h2c and TCP/h2c through the same Edge
and application handlers. Measurements from that harness are recorded here.

## 2026-09-29 — downstream edge server implementing Orbit

Host: macOS. Four edge workers, one Gufi application process, release
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

#### Ownership-based recovery, event-driven progress

Recovery follows ownership rather than a clock. Producer intents and
generation-checked claims protect publication without a new `posix-sync`
dependency or a pthread robust mutex around inbox writes. Kernel holds stay
at registration and recovery; ordinary writes add no ownership lock or syscall.
Existing readiness notifications drive progress, with no sleep loop,
timeout-based death inference or periodic recovery scan.

Crash recovery therefore does not turn the inbox into a timed retry service.
Claims still cost atomic work: the measurements below show recovered
throughput and graceful shutdown, not zero overhead.

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

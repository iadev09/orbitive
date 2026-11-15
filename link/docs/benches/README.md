# orbit-link benchmarks

## Transport

`link/benches/transport.rs` measures one request and its reply between two
processes on one machine, in nanoseconds, with no HTTP in the way. Each process
runs a current-thread Tokio runtime.

| Path | What it is |
| --- | --- |
| `uds` | A connected Unix socket. The address is the connection. |
| `stream` | One Orbit stream slot, held like that connection. Transport only. |
| `link` | A named call with a body: a frame announcing the exchange, a slot reused for its bytes and a reply head through the caller's inbox. |
| `inline` | A named call whose request and reply each fit a frame, sent as such a call is today: a slot is still taken for it and ended at once. |
| `frame` | The same two frames and no slot. |

`stream` against `uds` compares the transports. `link` against `stream` is what
naming and dispatch add. `inline` against `frame` is what a slot costs an
exchange that puts nothing in it. The two frame-only paths run for requests up
to 3 KiB.

```sh
cargo bench -p orbit-link --bench transport --features tokio,probe
cargo bench -p orbit-link --bench transport --features tokio,probe -- \
  --paths uds,inline,frame --sizes 64 --conns 1,8 --seconds 5
```

A row counts `--seconds` after one second of warm-up, and its CPU is both
processes' user and system time over that period. `conns` is how many calls are
outstanding at once.

Both processes read one clock, so a stamp from one can be compared with a stamp
from the other. Each stage is a median:

| Column | From | To |
| --- | --- | --- |
| send | the caller begins | the caller has sent the request |
| wake | the caller has sent it | the other side sees the first of it |
| read | the other side sees the first of it | it has all of it |
| reply wake | the other side begins answering | the caller sees the first of it |
| reply read | the caller sees the first of it | it has all of it |

Stage medians do not sum to the round trip. A negative `wake` means the other
side was already reading while the caller was still sending.

The last columns count what an exchange cost in wakes, both processes
together, over the counted time. The `probe` feature supplies the bell counts.

| Column | What it counts |
| --- | --- |
| bells/op | doorbell send attempts, including failures |
| ns/bell | mean elapsed time per send attempt |
| empty bell reads/op | actual doorbell receive calls that returned `WouldBlock` |
| parks/op | times a runtime thread ran out of work and went to wait |
| switches/op | voluntary context switches; macOS reports none |

A stream's own wake is outside the bell counters. Parks and context switches
cover the whole benchmark runtime.

The `frame` path also prints one lane snapshot and one syscall summary for
each process. The caller receives reply frames and the responder receives
request frames. A wake here is a successful `readable()` return that followed
at least one `Pending` poll. Its batch ends at the next socket readiness
attempt or receiver drop. The histogram buckets are 0, 1, 2 and 3+ data
frames. First-poll readiness, frames before any readiness return and control
notifications have separate counters. Snapshots include completed batches.

The syscall summaries count send and receive attempts separately, including
`WouldBlock` and other errors. The receive timer runs inside Tokio's
readiness gate, so a cached `WouldBlock` does not count as a syscall. The
send and receive timers measure elapsed call time. User and system CPU
counters cover the entire process over the snapshot window.

Results: [after the bell changes](transport-2026-11-12-bell.md), compared with
[the baseline of 12 November 2026](transport-2026-11-12.md), which has the
frame-only paths and the wake counts; and [November 2026](transport-2026-11.md),
taken before they existed.

## Other benchmarks

[`BENCHMARKS.md`](../../BENCHMARKS.md) records downstream HTTP measurements. Those
compare Link with pooled HTTP/2 over a Unix socket through a full server, so they
are not a transport comparison.

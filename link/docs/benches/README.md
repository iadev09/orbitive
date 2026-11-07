# orbit-link benchmarks

## Transport

`link/benches/transport.rs` measures one request and its reply between two
processes on one machine, in nanoseconds, with no HTTP in the way. Each process
runs a current-thread Tokio runtime.

| Path | What it is |
| --- | --- |
| `uds` | A connected Unix socket. The address is the connection. |
| `stream` | One Orbit stream slot, held like that connection. Transport only. |
| `link` | A named call: a frame announcing the exchange, a slot reused for it and a reply head through the caller's inbox. |

`stream` against `uds` compares the transports. `link` against `stream` is what
naming and dispatch add.

```sh
cargo bench -p orbit-link --bench transport --features tokio
cargo bench -p orbit-link --bench transport --features tokio -- \
  --paths uds,link --sizes 64,4096 --conns 1,8 --seconds 5
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

Results: [November 2026](transport-2026-11.md).

## Other benchmarks

`link/benches/dispatch.rs` measures the synchronous protocol core with no wake.
[`BENCHMARKS.md`](../../BENCHMARKS.md) records downstream HTTP measurements. Those
compare Link with pooled HTTP/2 over a Unix socket through a full server, so they
are not a transport comparison.

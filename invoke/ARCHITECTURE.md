# orbit-invoke - Architecture

`invoke` is a framework-neutral request transport for a same-host Orbit
fleet. It owns the envelope and its retention; execution, placement, claiming,
results and retry belong to the adapter above it.

## Request ring

One per-node ring (kind `INVOCATION_RING_KIND` = 241, `RingSpec::per_node`)
carries every operation. The frame is:

```text
u16 operation_len | u32 payload_len | operation | payload
```

The frame's `NetId64` becomes the `InvocationId`; its embedded node identifies
the submitter, not any executor. `submitted_at` is the frame version, stamped
at publication.

## What this ring is not

Cursors are per reader and fan out: every reader that polls sees every frame.
That is filtering by operation, not work claiming. Single delivery is a
placement decision of the consumer (one subscriber in the fleet) or a claim
protocol layered above, with its own authoritative state. Overwritten frames
are reported as lag; the ring never blocks a publisher.

## Readiness

`event_fd` exposes Orbit's process-local readiness bridge where the platform
has one. The optional `tokio` adapter drains it and re-polls the cursor; on
platforms without native readiness it sleeps `FALLBACK_POLL_INTERVAL` between
polls.

# orbit-metrics - Architecture

`metrics` is the framework-agnostic metrics snapshot layer on top
of `orbit-core`. Embedders define their own snapshot type, implement
`OrbitMetricSnapshot`, and choose a stable `OrbitTyped::KIND`.

## Role

Metrics are not cache entries and not events. A metrics family is a
periodic stream of snapshots where readers usually want:

- the latest sample per logical node,
- the latest sample per logical metric key for row-like families,
- optional freshness filtering,
- tolerance for missed or malformed old frames,
- no hot-path shared-memory writes.

Hot paths should update process-local atomics. A background publisher
captures a compact snapshot and writes it to an Orbit ring. A collector
walks the ring backwards and keeps the newest decodable sample for each
node.

## Public Shape

- `OrbitMetricSnapshot`: snapshot codec and identity trait.
- `OrbitMetricKeyedSnapshot`: optional key projection for row-like
  families where the logical key is not the producing process id.
- `OrbitMetricFamily<T>`: typed family wrapper around an `Arc<Fleet>`.
- `OrbitMetricPublisher<T>`: write-side handle.
- `OrbitMetricCollector<T>`: read-side handle.
- `OrbitMetricSample<T>`: decoded sample plus the `NetId64` that carried
  it.

## Data Flow

```text
hot path atomics
  -> periodic capture
  -> OrbitMetricPublisher<T>::publish(snapshot)
  -> orbit-core ring for T::KIND
  -> OrbitMetricCollector<T>::latest_by_node() / latest_by_key()
  -> application aggregation / rendering
```

The crate does not aggregate counters by itself. It only finds the
newest snapshot per `node_id` or per caller-defined metric key. The
application decides whether to sum, average, render Prometheus text, or
use the result for control loops.

## Freshness

`OrbitMetricSnapshot::captured_at_unix_secs()` is part of the trait
because metrics reads are time-sensitive. `fresh_by_node(now, max_age)`
filters samples whose capture time is too old for the caller's decision.

This is especially important for supervisor control. A missing or stale
sample should skip a rotation decision rather than kill a worker based on
old data.

## Payload Contract

Snapshot codecs are explicit. The crate does not use serde by default
and does not choose JSON, MessagePack, bincode, or any other format for
the caller. Compact fixed-width binary snapshots are preferred for
high-cadence health metrics because SHM ring payloads are bounded.

Every metric type declares its own `OrbitTyped::RING_SPEC`.
`OrbitMetricPublisher` checks that type-specific payload capacity and
returns an error before publishing an oversized sample. Scalar and keyed
families may therefore choose different retention and memory bounds
without imposing one global SHM slot size.

## Invariants

- No dependency on application runtimes, transport servers, supervisors,
  language bindings, or application crates.
- No cache semantics: no TTL, delete, get/put store API, key namespace,
  or reset.
- No per-request/event writes from hot paths; publish captured snapshots.
- Decoding errors in old or malformed frames are ignored by collectors.
- The newest valid sample per node wins.
- Aggregation semantics belong to the application snapshot type or the
  caller, not here.

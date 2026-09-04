# orbit-metrics

`orbit-metrics` publishes compact runtime snapshots into Orbit rings and reads
the newest valid sample per node or logical metric key. Applications normally
use it through `orbitive::metrics`; direct package access remains available.

Hot paths remain process-local. A periodic task captures counters or gauges
into an application-defined snapshot and publishes that bounded value:

```text
local counters
  -> OrbitMetricPublisher<T>
  -> Orbit ring
  -> OrbitMetricCollector<T>
  -> latest_by_node() / latest_by_key()
```

Snapshot types implement `OrbitMetricSnapshot`; row-like families may also
implement `OrbitMetricKeyedSnapshot`. The caller owns encoding, aggregation,
and rendering.

The crate provides:

- typed publishers and collectors;
- latest-sample lookup by node or logical key;
- optional freshness filtering;
- tolerance for malformed or overwritten old samples;
- reusable default capacities for scalar and keyed families.

It does not aggregate counters, render Prometheus output, choose a serializer,
or write shared memory from application hot paths. Ring retention is a bounded
observation window, not metrics durability.

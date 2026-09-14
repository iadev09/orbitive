# orbit-counter

`orbit-counter` provides keyed, signed 64-bit counters shared by every process
in the same Orbit fleet. Applications normally use it through
`orbitive::counter`; direct package access remains available.

```text
"requests:total" -> AtomicI64
```

A missing key starts at zero for `increment` and `decrement`; both return the
value after their atomic update. `reset` installs a missing key or returns an
existing one to zero. Reads and updates never publish a ring frame and never
take an OS lock: keys are installed once, under a short process-recoverable
structural lock, into immutable-address slots that every process maps.

Capacity is bounded (`COUNTER_CAPACITY` keys of at most `COUNTER_KEY_MAX`
bytes; compile-time geometry via `ORBIT_COUNTER_CAPACITY` and
`ORBIT_COUNTER_KEY_MAX`) and a full table is reported as `Error::StateFull`,
never silently evicted. Values that would leave the signed 64-bit range are refused with
`Error::Overflow` and the previous value is kept.

The owner of a fleet generation clears the table with `reset_all` during
quiescent boot. In-memory fleets get a process-local table with the same API.

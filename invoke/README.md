# orbit-invoke

`orbit-invoke` carries invocation requests between processes of the same Orbit
fleet over a dedicated bounded ring. Applications normally use it through
`orbitive::invoke`; direct package access remains available.

An invocation is an operation name, opaque payload bytes, and the identity the
ring assigned when the frame was committed:

```text
InvocationId (NetId64, node = submitter) | operation | payload | submitted_at
```

`InvocationBus::submit` commits a frame and returns its id; success means
publication, not execution. Consumers advance their own `InvocationCursor`
with `poll` or `poll_operation`, and overwritten or invalid frames are reported
as `lagged` rather than lost silently. `InvocationCodec` lets a runtime attach
its own typed encoding to an operation name; the ring itself selects no
serializer.

The ring is not a durable queue: it provides no acknowledgement, claim, retry,
response, or exactly-once guarantee, and which process executes an invocation
is the caller's placement decision. Ring geometry is compile-time
(`ORBIT_INVOKE_RING_CAPACITY`, `ORBIT_INVOKE_RING_PAYLOAD_CAPACITY`).

With the `tokio` feature, `InvocationBus::subscribe` returns an
`InvocationSubscription` whose `receive` waits on the ring's native readiness
fd on Linux, FreeBSD, and macOS 14.4 or later, and polls elsewhere.

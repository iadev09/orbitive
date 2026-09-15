# orbit-arena

`orbit-arena` is a fleet-shared byte store: one index over one ring of bytes
in Orbit shared memory, read by every process in the fleet without copying.
`orbit-cache` keeps a private L1 per process and carries mutations between
them; this keeps the bytes themselves in one place. Applications normally use
it through `orbitive::arena`.

```rust
use std::sync::Arc;

use orbitive::arena::{Arena, Key, Record};
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
let arena = Arena::new(fleet)?;

// The caller brings a 128-bit digest of its real key — a file path, say.
let key = Key::new(0x1234_5678);
arena.put(key, Record::body(b"<html>…").encoded(b"\x1b…", 1).stamp(1_700_000_000))?;

if let Some(entry) = arena.get(key) {            // pinned while `entry` lives
    assert_eq!(entry.stamp(), 1_700_000_000);
    let body: &[u8] = entry.body();               // the bytes, where they are
    let view = entry.into_body();                 // an owner for bytes::Bytes::from_owner
    assert_eq!(view.as_ref(), b"<html>…");
}

# Ok::<(), Box<dyn std::error::Error>>(())
```

A record is an identity body, an optional encoded body with an opaque tag,
and two validators the caller reads back. Nothing here knows about files,
MIME types or HTTP.

There is no expiry. The ring is the budget: a put writes at the cursor and
whatever the bytes land on is evicted. A record a reader is holding is pinned,
and the cursor moves past it instead, so a body being served is never torn. A
record larger than a quarter of the ring is refused whole. `reset` empties
the index and bumps a generation; readers holding an entry keep it.

Readers take no lock. A writer holds the region's process lock (`flock`,
released by the kernel when a process dies) for the duration of a put.
Lookups are open addressing over a power-of-two slot table, so an evicted
slot can shorten a probe chain: the cost is a miss for a key that was still
present, never a wrong hit.

Geometry is compile-time, like the other Orbit tables: `ORBIT_ARENA_SLOTS`
(default 4096) and `ORBIT_ARENA_BYTES` (default 64 MiB) in the application's
`.cargo/config.toml`. Peers built with different values are refused when they
open the segment.

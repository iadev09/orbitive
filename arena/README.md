# orbit-arena

`orbit-arena` is a fleet-shared byte store: one index over one ring of bytes
in Orbit shared memory, read by every process in the fleet without copying.
`orbit-cache` uses the same arena for values and keeps only L1 metadata per
process; mutations carry addresses, not body copies. Applications normally use
it through the [Orbitive](https://github.com/iadev09/orbitive) facade as
`orbitive::arena`.

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
Pins are counted per reader owner, not in one anonymous counter. Each process
holds one kernel lock for its reader row, shared by all its arena handles.
Under allocation pressure, a writer reclaims a dead reader's counts only after
winning that lock. SIGKILL therefore cannot leave an eternal pin; a live reader
is never reclaimed by a timeout. No syscall is needed on an ordinary read or
pin drop. `reclaim_dead_pins` exposes the same death-proven operation explicitly.

Lookup probe chains retain retired markers. `store(record)` also supports
immutable addressed records without a caller key, returning `Address` (slot and
u64 generation). `get_address` pins that exact generation; replacement is a miss,
never another body's bytes. `forget_address` withdraws only that generation.
Index pressure evicts unpinned records just as byte pressure does, including
zero-length bodies. Record generations are monotonically allocated across
the arena; index pressure reuses the oldest unpinned record, rather than a
recent value at the same probe position.

Geometry is compile-time, like the other Orbit tables: `ORBIT_ARENA_SLOTS`
(default 4096) and `ORBIT_ARENA_BYTES` (default 64 MiB) in the application's
`.cargo/config.toml`. Peers built with different values are refused when they
open the segment.

Reader-row count is the fleet's declared capacity, not a new independent
budget. One process shares its mapping and row across arena consumers. Reopen
after exec; inherited handles/entries cannot be used by a fork child.

The v2 arena uses SHM kind 193. Kind 242 belongs to the former anonymous-pin
layout; it is not reopened, resized or automatically unlinked. All peers must
rebuild for the same layout before sharing this arena.

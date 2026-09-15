# orbit-cell

`orbit-cell` provides typed atomic cells in one fleet-shared table, addressed
by id rather than by name. A cell is a place: allocate it once, pass its id to
any process in the fleet, and every handle reads and updates the same 64 bits
atomically, without a lock and without publishing a frame. `orbit-counter` is
the keyed sibling; this is memory rather than a dictionary. Applications
normally use it through `orbitive::cell`.

```rust
use std::sync::Arc;

use orbitive::cell::{CellId, Cells};
use orbitive::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
let cells = Cells::new(fleet)?;

let hits = cells.allocate(0_i64)?;          // the type is stamped on the cell
let id = hits.id().to_string();             // "cell:<slot>:<generation>": what travels

let same = cells.open::<i64>(id.parse::<CellId>()?)?;
same.fetch_add(1)?;
assert_eq!(hits.load()?, 1);

hits.release()?;                            // every handle to it is now stale
assert!(same.load().is_err());

# Ok::<(), Box<dyn std::error::Error>>(())
```

Four types fit a cell: `i64`, `u64`, `f64` and `bool`. Integers get checked
`fetch_add` / `fetch_sub`, floats a compare-and-swap `fetch_add`, flags
`set` / `clear`; every type has `load`, `store`, `swap` and
`compare_exchange`. Opening an id as the wrong type is refused.

Text has its own arena: `allocate_text` / `open_text` give a `Text` handle
holding up to `CELL_TEXT_MAX` bytes of UTF-8 (default 240, sized by
`ORBIT_CELL_TEXT_CAPACITY` and `ORBIT_CELL_TEXT_MAX`). `load` copies the
current text, `store` replaces it and `append` extends it atomically with
respect to every other writer; a write that would not fit is refused whole,
never cut. Readers go through a seqlock and never observe a torn string. A
`TextId` prints as `text:<slot>:<generation>`.

Every id carries the generation its slot was allocated in, and `release`
retires that generation: a handle kept past a release reports `Error::Stale`
instead of reaching whoever took the slot next. Cells have no lifetime of
their own; what allocates one is expected to release it, and the table
reports `Error::Full` rather than evicting.

Geometry is compile-time: `ORBIT_CELL_CAPACITY` (a power of two, default
4096) sizes the table for every binary that opens the fleet. The table is
current state, not history: the fleet owner clears it with `reset_all` during
quiescent boot, and nothing about it survives a fleet generation on purpose.

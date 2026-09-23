//! Typed atomic cells in one fleet-shared SHM table, addressed by id.
//!
//! A cell is a place, not a name. [`Cells::allocate`] takes a slot, stamps
//! its type and hands back an [`Orbital`] handle whose [`CellId`] is the
//! only thing that ever needs to travel: any process in the fleet that
//! [`Cells::open`]s the id reads and updates the same 64 bits, atomically,
//! without a lock and without publishing a frame. `orbit-counter` is the
//! keyed sibling; this is memory rather than a dictionary.
//!
//! Every id carries the slot's generation. [`Cells::release`] bumps it, so a
//! handle kept past a release answers [`Error::Stale`] instead of touching
//! whoever took the slot next. Cells have no lifetime of their own: what
//! allocates one is expected to release it.

use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};

use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};

mod text;

pub use text::{CELL_TEXT_CAPACITY, CELL_TEXT_KIND, CELL_TEXT_MAX, Text, TextId};

/// Reserved Orbit SHM kind for the cell table.
pub const CELL_STATE_KIND: u8 = 233;
/// Number of cells one fleet generation can hold at once.
///
/// Compile-time geometry: `ORBIT_CELL_CAPACITY` in the application's
/// `.cargo/config.toml` overrides the default. Must be a power of two. Peers
/// built with a different value are refused when they open the table.
pub const CELL_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_CELL_CAPACITY"), 4_096);

pub(crate) const SLOT_EMPTY: u8 = 0;
pub(crate) const SLOT_OCCUPIED: u8 = 1;

/// Whether this build can park on a shared word. A table refuses to open
/// where it cannot: a sleep loop wearing the shape of a wait is worse
/// than a clear no, and nothing above here should have to ask again.
#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
pub(crate) fn waits_supported() -> bool {
    orbit_core::sync::supported()
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
pub(crate) fn waits_supported() -> bool {
    false
}

/// Park until `word` no longer holds `expected`, through the platform's
/// shared address wait. There is no polling fallback: a table refuses to
/// open where the platform cannot wait, so by here it can.
#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
pub(crate) fn wait_on(
    word: &AtomicU32,
    expected: u32
) -> Result<()> {
    orbit_core::sync::wait_word(word, expected).map_err(Error::Io)
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
pub(crate) fn wait_on(
    _word: &AtomicU32,
    _expected: u32
) -> Result<()> {
    Err(Error::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "orbit-cell needs a platform that can wait on a shared word"
    )))
}

/// Wake everyone parked on `word`; nothing to do where nobody can park.
pub(crate) fn wake_on(word: &AtomicU32) {
    #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    let _ = orbit_core::sync::wake_word(word);
    #[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
    let _ = word;
}

#[cfg(unix)]
const STATE_MAGIC: u32 = 0x43_45_4C_4C; // "CELL"
#[cfg(unix)]
const STATE_VERSION: u16 = 2;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Additions are expected: a cause discovered later lands here rather
/// than in a new major version, so a caller matches what it handles and
/// leaves the rest to a catch-all.
#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    /// The id names a slot nothing occupies, or a generation that has been
    /// released since the id was minted.
    Stale(CellId),
    /// The same, for a text cell.
    StaleText(TextId),
    /// The text would not fit the cell; nothing was written.
    TooLong {
        len: usize,
        max: usize
    },
    /// A text cell held bytes that are not UTF-8: something wrote past the
    /// contract.
    Corrupt(String),
    /// The id names a live cell of another type.
    TypeMismatch {
        id: CellId,
        expected: &'static str,
        found: &'static str
    },
    /// The update would leave the integer range.
    Overflow,
    /// Every slot is occupied.
    Full {
        capacity: usize
    },
    /// The text is not a cell id.
    Malformed(String),
    Io(std::io::Error)
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        match self {
            Self::Stale(id) => write!(f, "cell {id} has been released"),
            Self::StaleText(id) => write!(f, "text cell {id} has been released"),
            Self::TooLong { len, max } => {
                write!(f, "text does not fit the cell: len={len} max={max}")
            }
            Self::Corrupt(id) => write!(f, "text cell {id} holds bytes that are not UTF-8"),
            Self::TypeMismatch { id, expected, found } => {
                write!(f, "cell {id} holds {found}, not {expected}")
            }
            Self::Overflow => f.write_str("cell value is outside the integer range"),
            Self::Full { capacity } => write!(f, "cell table is full: capacity={capacity}"),
            Self::Malformed(text) => write!(f, "not a cell id: {text:?}"),
            Self::Io(error) => write!(f, "Orbit cell io error: {error}")
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// The address of one cell: its slot and the generation it was allocated in.
///
/// Prints as `cell:<slot>:<generation>` and parses back, which is the form
/// meant to cross a serialization boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CellId {
    index: u32,
    generation: u32
}

impl CellId {
    pub const fn index(self) -> u32 {
        self.index
    }

    pub const fn generation(self) -> u32 {
        self.generation
    }

    /// The two halves in one integer, slot high, generation low.
    pub const fn to_bits(self) -> u64 {
        ((self.index as u64) << 32) | self.generation as u64
    }

    pub const fn from_bits(bits: u64) -> Self {
        Self { index: (bits >> 32) as u32, generation: bits as u32 }
    }
}

impl fmt::Display for CellId {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        write!(f, "cell:{}:{}", self.index, self.generation)
    }
}

impl FromStr for CellId {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        let malformed = || Error::Malformed(text.to_owned());
        let rest = text.strip_prefix("cell:").ok_or_else(malformed)?;
        let (index, generation) = rest.split_once(':').ok_or_else(malformed)?;
        Ok(Self {
            index: index.parse().map_err(|_| malformed())?,
            generation: generation.parse().map_err(|_| malformed())?
        })
    }
}

/// A value that fits one cell: 64 bits, and a tag that says which type they
/// spell.
pub trait CellType: Copy + Send + Sync + 'static {
    const TAG: u8;
    const NAME: &'static str;

    fn to_bits(self) -> u64;
    fn from_bits(bits: u64) -> Self;
}

impl CellType for i64 {
    const TAG: u8 = 1;
    const NAME: &'static str = "int";

    fn to_bits(self) -> u64 {
        self as u64
    }

    fn from_bits(bits: u64) -> Self {
        bits as i64
    }
}

impl CellType for u64 {
    const TAG: u8 = 2;
    const NAME: &'static str = "unsigned";

    fn to_bits(self) -> u64 {
        self
    }

    fn from_bits(bits: u64) -> Self {
        bits
    }
}

impl CellType for f64 {
    const TAG: u8 = 3;
    const NAME: &'static str = "float";

    fn to_bits(self) -> u64 {
        self.to_bits()
    }

    fn from_bits(bits: u64) -> Self {
        f64::from_bits(bits)
    }
}

impl CellType for bool {
    const TAG: u8 = 4;
    const NAME: &'static str = "bool";

    fn to_bits(self) -> u64 {
        u64::from(self)
    }

    fn from_bits(bits: u64) -> Self {
        bits != 0
    }
}

fn type_name(tag: u8) -> &'static str {
    match tag {
        1 => i64::NAME,
        2 => u64::NAME,
        3 => f64::NAME,
        4 => bool::NAME,
        _ => "unknown"
    }
}

/// The fleet's cell tables, scalar and text. Cheap to clone.
#[derive(Clone)]
pub struct Cells {
    backend: CellBackend,
    text: text::TextBackend
}

#[derive(Clone)]
enum CellBackend {
    InMemory(Arc<MemoryCellTable>),
    #[cfg(unix)]
    Shm(Arc<ShmCellTable>)
}

impl Cells {
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        if !crate::waits_supported() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "orbit-cell needs a platform that can wait on a shared word: Linux, FreeBSD, or macOS 14.4 or later"
            )));
        }
        let backend = if fleet.is_shm() {
            #[cfg(unix)]
            {
                CellBackend::Shm(Arc::new(ShmCellTable::open_or_create(&ring_segment_name(
                    fleet.name(),
                    CELL_STATE_KIND
                ))?))
            }
            #[cfg(not(unix))]
            unreachable!("non-Unix fleets cannot use POSIX SHM")
        } else {
            CellBackend::InMemory(memory_table(&fleet))
        };
        let text = text::TextBackend::new(&fleet)?;
        Ok(Self { backend, text })
    }

    /// Take a text cell holding `initial`; up to [`CELL_TEXT_MAX`] bytes.
    pub fn allocate_text(
        &self,
        initial: &str
    ) -> Result<Text> {
        self.text.allocate(initial)
    }

    /// A handle to a live text cell.
    pub fn open_text(
        &self,
        id: TextId
    ) -> Result<Text> {
        self.text.open(id)
    }

    pub fn release_text(
        &self,
        id: TextId
    ) -> Result<()> {
        self.text.release(id)
    }

    pub fn is_text_live(
        &self,
        id: TextId
    ) -> bool {
        self.text.is_live(id)
    }

    /// Take a free slot, stamp it with `T` and `initial`, and return the
    /// handle. The id inside it is what other processes open.
    pub fn allocate<T: CellType>(
        &self,
        initial: T
    ) -> Result<Orbital<T>> {
        let id = self.with_structure(|slots, hint| {
            let capacity = slots.len();
            let start = hint.load(Ordering::Relaxed) as usize;
            for offset in 0..capacity {
                let index = (start + offset) & (capacity - 1);
                let slot = &slots[index];
                if slot.state.load(Ordering::Acquire) == SLOT_EMPTY {
                    let generation = slot.install(T::TAG, initial.to_bits());
                    hint.store(((index + 1) & (capacity - 1)) as u32, Ordering::Relaxed);
                    return Ok(CellId { index: index as u32, generation });
                }
            }
            Err(Error::Full { capacity })
        })?;
        Ok(Orbital { cells: self.clone(), id, _type: PhantomData })
    }

    /// A handle to a live cell of type `T`.
    pub fn open<T: CellType>(
        &self,
        id: CellId
    ) -> Result<Orbital<T>> {
        let slot = self.slot(id)?;
        let found = slot.tag.load(Ordering::Relaxed);
        if found != T::TAG {
            return Err(Error::TypeMismatch { id, expected: T::NAME, found: type_name(found) });
        }
        Ok(Orbital { cells: self.clone(), id, _type: PhantomData })
    }

    /// Give the slot back. Every handle to this generation goes stale.
    pub fn release(
        &self,
        id: CellId
    ) -> Result<()> {
        self.with_structure(|slots, _| {
            let slot =
                slots.get(id.index as usize).filter(|slot| slot.is(id)).ok_or(Error::Stale(id))?;
            slot.state.store(SLOT_EMPTY, Ordering::Release);
            // Whoever is parked on it finds the slot gone and answers Stale.
            slot.changed();
            Ok(())
        })
    }

    /// Whether `id` names a live cell right now.
    pub fn is_live(
        &self,
        id: CellId
    ) -> bool {
        self.slot(id).is_ok()
    }

    /// Clear both tables during quiescent owner boot.
    pub fn reset_all(&self) -> Result<()> {
        self.with_structure(|slots, hint| {
            for slot in slots {
                slot.state.store(SLOT_EMPTY, Ordering::Release);
                slot.changed();
            }
            hint.store(0, Ordering::Relaxed);
            Ok(())
        })?;
        self.text.reset_all()
    }

    /// Remove the cell SHM object. Existing mappings remain valid until their
    /// processes release them.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        match &self.backend {
            CellBackend::InMemory(_) => self.reset_all()?,
            CellBackend::Shm(table) => table.region.unlink().map_err(Error::Io)?
        }
        self.text.unlink()
    }

    fn slots(&self) -> &[CellSlot] {
        match &self.backend {
            CellBackend::InMemory(table) => &table.slots,
            #[cfg(unix)]
            CellBackend::Shm(table) => table.slots()
        }
    }

    fn slot(
        &self,
        id: CellId
    ) -> Result<&CellSlot> {
        self.slots().get(id.index as usize).filter(|slot| slot.is(id)).ok_or(Error::Stale(id))
    }

    fn with_structure<T>(
        &self,
        operation: impl FnOnce(&[CellSlot], &AtomicU32) -> Result<T>
    ) -> Result<T> {
        match &self.backend {
            CellBackend::InMemory(table) => {
                let _local = lock_unpoisoned(&table.structural_lock);
                operation(&table.slots, &table.hint)
            }
            #[cfg(unix)]
            CellBackend::Shm(table) => table.with_structure(operation)
        }
    }
}

/// What [`Orbital::add_until`] did. A bounded add either takes some of what
/// is left or finds nothing left; there is no third answer, and no value
/// past the limit is ever written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Claim {
    /// `taken` can be less than asked for: the last caller gets the
    /// remainder rather than being refused, so no unit goes unclaimed.
    Took { taken: i64, after: i64 },
    /// The limit was already reached. Nothing was added, nothing is left.
    Exhausted
}

/// One cell, typed. Cheap to clone; every clone is the same place.
#[derive(Clone)]
pub struct Orbital<T: CellType> {
    cells: Cells,
    id: CellId,
    _type: PhantomData<T>
}

impl<T: CellType> Orbital<T> {
    pub fn id(&self) -> CellId {
        self.id
    }

    pub fn load(&self) -> Result<T> {
        Ok(T::from_bits(self.slot()?.value.load(Ordering::Acquire)))
    }

    pub fn store(
        &self,
        value: T
    ) -> Result<()> {
        let slot = self.slot()?;
        slot.value.store(value.to_bits(), Ordering::Release);
        slot.changed();
        Ok(())
    }

    /// Replace the value and return what it was.
    pub fn swap(
        &self,
        value: T
    ) -> Result<T> {
        let slot = self.slot()?;
        let previous = slot.value.swap(value.to_bits(), Ordering::AcqRel);
        slot.changed();
        Ok(T::from_bits(previous))
    }

    /// The write count, the thing [`Self::wait_changed`] waits on. Take it
    /// before reading the value, and the wait misses nothing in between.
    pub fn version(&self) -> Result<u32> {
        Ok(self.slot()?.changes.load(Ordering::Acquire))
    }

    /// Park until the cell has been written since `since`, then return the
    /// count now. Coalescing: ten writes while parked wake the caller once,
    /// and [`Self::load`] gives the latest. A released cell wakes every waiter
    /// with [`Error::Stale`]. Blocking; an async runtime wraps it.
    pub fn wait_changed(
        &self,
        since: u32
    ) -> Result<u32> {
        loop {
            let slot = self.slot()?;
            let now = slot.changes.load(Ordering::SeqCst);
            if now != since {
                return Ok(now);
            }
            slot.waiters.fetch_add(1, Ordering::SeqCst);
            let outcome = if slot.changes.load(Ordering::SeqCst) == since {
                wait_on(&slot.changes, since)
            } else {
                Ok(())
            };
            slot.waiters.fetch_sub(1, Ordering::SeqCst);
            outcome?;
        }
    }

    /// Store `new` only if the cell still holds `current`. `Ok(Ok(previous))`
    /// on success, `Ok(Err(actual))` when it held something else.
    pub fn compare_exchange(
        &self,
        current: T,
        new: T
    ) -> Result<std::result::Result<T, T>> {
        let slot = self.slot()?;
        let exchanged = slot.value.compare_exchange(
            current.to_bits(),
            new.to_bits(),
            Ordering::AcqRel,
            Ordering::Acquire
        );
        if exchanged.is_ok() {
            slot.changed();
        }
        Ok(exchanged.map(T::from_bits).map_err(T::from_bits))
    }

    /// [`Self::wait_changed`], but a second word can end it. The waiter
    /// still parks until something wakes it; `interrupt` is what tells it
    /// which kind of wake arrived. `Ok(Some(count))` is a write,
    /// `Ok(None)` is the interrupt.
    ///
    /// This is the shape a caller needs when the wait may outlive its
    /// reason — a process that has to shut down while a script waits on a
    /// cell nothing will write again. It is not a timeout: nothing here
    /// measures time, and a wait nobody interrupts is
    /// [`Self::wait_changed`] exactly.
    ///
    /// The canceller sets `interrupt` and then calls
    /// [`Self::wake_waiters`], in that order. Set-then-wake is what closes
    /// the window: a waiter that had already parked is released by the
    /// wake, and one that had not yet parked sees the flag instead.
    pub fn wait_changed_until(
        &self,
        since: u32,
        interrupt: &core::sync::atomic::AtomicBool
    ) -> Result<Option<u32>> {
        loop {
            if interrupt.load(Ordering::SeqCst) {
                return Ok(None);
            }

            let slot = self.slot()?;
            let now = slot.changes.load(Ordering::SeqCst);
            if now != since {
                return Ok(Some(now));
            }

            slot.waiters.fetch_add(1, Ordering::SeqCst);
            let outcome = if slot.changes.load(Ordering::SeqCst) == since
                && !interrupt.load(Ordering::SeqCst)
            {
                wait_on(&slot.changes, since)
            } else {
                Ok(())
            };
            slot.waiters.fetch_sub(1, Ordering::SeqCst);
            outcome?;
        }
    }

    /// Wake everyone parked on this cell without writing to it. On its own
    /// this changes nothing — a waiter re-checks the count, finds it where
    /// it was and parks again — so it is only useful after the word the
    /// waiter also watches has been set. See [`Self::wait_changed_until`].
    pub fn wake_waiters(&self) -> Result<()> {
        wake_on(&self.slot()?.changes);
        Ok(())
    }

    /// Give the cell back; this and every other handle to it go stale.
    pub fn release(self) -> Result<()> {
        self.cells.release(self.id)
    }

    fn slot(&self) -> Result<&CellSlot> {
        self.cells.slot(self.id)
    }
}

impl Orbital<i64> {
    /// Add `by` (negative to subtract) and return the value after.
    pub fn fetch_add(
        &self,
        by: i64
    ) -> Result<i64> {
        let slot = self.slot()?;
        slot.value
            .try_update(Ordering::AcqRel, Ordering::Acquire, |bits| {
                (bits as i64).checked_add(by).map(|value| value as u64)
            })
            .map(|previous| {
                slot.changed();
                (previous as i64) + by
            })
            .map_err(|_| Error::Overflow)
    }

    pub fn fetch_sub(
        &self,
        by: i64
    ) -> Result<i64> {
        self.fetch_add(by.checked_neg().ok_or(Error::Overflow)?)
    }

    /// Add up to `by` while the value stays at or under `limit`, in one
    /// compare-and-swap, and say what was taken.
    ///
    /// This is the operation a fleet needs to divide a fixed amount of work
    /// between processes that cannot see each other. `fetch_add` followed by
    /// a test cannot do it: the add lands before the caller learns it went
    /// too far, so N callers overshoot by up to N-1 between them. Here the
    /// bound is decided inside the swap, so the value **never** passes
    /// `limit` however many callers race, and the one that finds less than
    /// `by` left takes the remainder instead of being turned away.
    ///
    /// Taking more than one at a time is how the cost comes down: the
    /// contended word is touched once per claim rather than once per unit.
    /// `limit` is the ceiling for the total, never for a single call.
    ///
    /// Overflow needs no separate guard: a result that cannot pass `limit`
    /// cannot pass `i64::MAX` either.
    ///
    /// **A claim is not a lease.** What this hands back is gone from the
    /// total the moment the swap lands, and nothing gives it back: a caller
    /// that takes a hundred, does forty and then dies leaves sixty that
    /// nobody will ever do, while the count says all hundred were taken. So
    /// `by` is not only a speed dial, it is the exposure — the most one
    /// death can lose. It is the same trade a database sequence makes with
    /// its cache size, and it leaves the same kind of gap. Work that must
    /// not be lost wants a lease whose owner notices a death, which is
    /// `orbit-pool`, not a counter.
    pub fn add_until(
        &self,
        by: i64,
        limit: i64
    ) -> Result<Claim> {
        if by <= 0 {
            return Err(Error::Overflow);
        }
        let slot = self.slot()?;
        loop {
            let current = slot.value.load(Ordering::Acquire) as i64;
            if current >= limit {
                return Ok(Claim::Exhausted);
            }
            let taken = by.min(limit - current);
            let after = current + taken;
            if slot
                .value
                .compare_exchange(current as u64, after as u64, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                slot.changed();
                return Ok(Claim::Took { taken, after });
            }
            // Lost the race; somebody else moved the value. Read it again
            // rather than retry blind: what is left may now be less, or
            // nothing.
        }
    }
}

impl Orbital<u64> {
    pub fn fetch_add(
        &self,
        by: u64
    ) -> Result<u64> {
        let slot = self.slot()?;
        slot.value
            .try_update(Ordering::AcqRel, Ordering::Acquire, |bits| bits.checked_add(by))
            .map(|previous| {
                slot.changed();
                previous + by
            })
            .map_err(|_| Error::Overflow)
    }

    pub fn fetch_sub(
        &self,
        by: u64
    ) -> Result<u64> {
        let slot = self.slot()?;
        slot.value
            .try_update(Ordering::AcqRel, Ordering::Acquire, |bits| bits.checked_sub(by))
            .map(|previous| {
                slot.changed();
                previous - by
            })
            .map_err(|_| Error::Overflow)
    }
}

impl Orbital<f64> {
    /// Add `by` and return the value after. A compare-and-swap loop: floats
    /// have no fetch-add of their own.
    pub fn fetch_add(
        &self,
        by: f64
    ) -> Result<f64> {
        let slot = self.slot()?;
        let previous = slot
            .value
            .try_update(Ordering::AcqRel, Ordering::Acquire, |bits| {
                Some((f64::from_bits(bits) + by).to_bits())
            })
            .unwrap_or_else(|bits| bits);
        slot.changed();
        Ok(f64::from_bits(previous) + by)
    }
}

impl Orbital<bool> {
    /// Set the flag and return whether it was already set.
    pub fn set(&self) -> Result<bool> {
        self.swap(true)
    }

    /// Clear the flag and return whether it was set.
    pub fn clear(&self) -> Result<bool> {
        self.swap(false)
    }
}

#[repr(C, align(64))]
struct CellStateHeader {
    #[cfg(unix)]
    magic: u32,
    #[cfg(not(unix))]
    _magic: u32,
    #[cfg(unix)]
    version: u16,
    #[cfg(not(unix))]
    _version: u16,
    header_size: u16,
    capacity: u32,
    slot_size: u32,
    /// Where the next allocation starts looking; a hint, never a promise.
    hint: AtomicU32,
    _reserved: [u8; 44]
}

impl CellStateHeader {
    fn new() -> Self {
        Self {
            #[cfg(unix)]
            magic: STATE_MAGIC,
            #[cfg(not(unix))]
            _magic: 0,
            #[cfg(unix)]
            version: STATE_VERSION,
            #[cfg(not(unix))]
            _version: 0,
            header_size: size_of::<Self>() as u16,
            capacity: CELL_CAPACITY as u32,
            slot_size: size_of::<CellSlot>() as u32,
            hint: AtomicU32::new(0),
            _reserved: [0; 44]
        }
    }
}

/// One cell, laid out for shared memory: a cache line of its own, so two hot
/// cells never contend, and every field atomic, so the mapping is never
/// borrowed `&mut` while another process reads it.
#[repr(C, align(64))]
struct CellSlot {
    state: AtomicU8,
    tag: AtomicU8,
    _reserved: [u8; 2],
    generation: AtomicU32,
    value: AtomicU64,
    /// Bumped by every write and by release: the word a waiter parks on.
    /// Linux futex wants 32 bits, so the value itself cannot be that word.
    changes: AtomicU32,
    /// Waiters parked on `changes`; a writer wakes only when this is nonzero,
    /// so the write path costs one extra load when nobody is listening.
    waiters: AtomicU32,
    _padding: [u8; 40]
}

impl CellSlot {
    fn empty() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            tag: AtomicU8::new(0),
            _reserved: [0; 2],
            generation: AtomicU32::new(0),
            value: AtomicU64::new(0),
            changes: AtomicU32::new(0),
            waiters: AtomicU32::new(0),
            _padding: [0; 40]
        }
    }

    /// After a write: count it, and wake whoever is waiting for one. `SeqCst`
    /// on both sides of the exchange with [`Orbital::wait_changed`], so a
    /// waiter that announced itself just before this write is either seen
    /// here or sees the new count itself; never neither.
    fn changed(&self) {
        self.changes.fetch_add(1, Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) > 0 {
            wake_on(&self.changes);
        }
    }

    fn is(
        &self,
        id: CellId
    ) -> bool {
        // `Acquire` on `state` publishes the generation written by `install`.
        self.state.load(Ordering::Acquire) == SLOT_OCCUPIED
            && self.generation.load(Ordering::Relaxed) == id.generation
    }

    /// Under the structural lock. Returns the generation the new cell lives in;
    /// it is bumped on every install, so no released id ever matches again.
    fn install(
        &self,
        tag: u8,
        value: u64
    ) -> u32 {
        let generation = self.generation.load(Ordering::Relaxed).wrapping_add(1).max(1);
        self.tag.store(tag, Ordering::Relaxed);
        self.value.store(value, Ordering::Relaxed);
        self.changes.store(0, Ordering::Relaxed);
        self.waiters.store(0, Ordering::Relaxed);
        self.generation.store(generation, Ordering::Relaxed);
        self.state.store(SLOT_OCCUPIED, Ordering::Release);
        generation
    }
}

pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

struct MemoryCellTable {
    /// Held so the fleet's address, which keys the registry, cannot be
    /// reused by another fleet while this table lives.
    _fleet: Arc<Fleet>,
    slots: Vec<CellSlot>,
    hint: AtomicU32,
    structural_lock: Mutex<()>
}

impl MemoryCellTable {
    fn new(fleet: Arc<Fleet>) -> Self {
        Self {
            _fleet: fleet,
            slots: (0..CELL_CAPACITY).map(|_| CellSlot::empty()).collect(),
            hint: AtomicU32::new(0),
            structural_lock: Mutex::new(())
        }
    }
}

type MemoryRegistry = HashMap<usize, Weak<MemoryCellTable>>;

static MEMORY_TABLES: LazyLock<Mutex<MemoryRegistry>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn memory_table(fleet: &Arc<Fleet>) -> Arc<MemoryCellTable> {
    let fleet_identity = Arc::as_ptr(fleet) as usize;
    let mut tables = lock_unpoisoned(&MEMORY_TABLES);
    tables.retain(|_, table| table.strong_count() > 0);
    if let Some(table) = tables.get(&fleet_identity).and_then(Weak::upgrade) {
        return table;
    }
    let table = Arc::new(MemoryCellTable::new(Arc::clone(fleet)));
    tables.insert(fleet_identity, Arc::downgrade(&table));
    table
}

#[cfg(unix)]
struct ShmCellTable {
    region: ShmRegion,
    structural_lock: Mutex<()>
}

#[cfg(unix)]
impl ShmCellTable {
    fn open_or_create(name: &str) -> Result<Self> {
        use std::{io, ptr};

        let (region, _initialization_lock) =
            ShmRegion::open_or_create_locked(name, shm_segment_size())?;
        if region.created() {
            unsafe {
                ptr::write(region.as_ptr().cast::<CellStateHeader>(), CellStateHeader::new());
                let slots = region.as_ptr().add(size_of::<CellStateHeader>());
                ptr::write_bytes(slots, 0, CELL_CAPACITY * size_of::<CellSlot>());
            }
        } else {
            let header = unsafe { &*region.as_ptr().cast::<CellStateHeader>() };
            if header.magic != STATE_MAGIC {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {name} has wrong magic 0x{:08X} (expected 0x{STATE_MAGIC:08X})",
                        header.magic
                    )
                )));
            }
            if header.version != STATE_VERSION
                || usize::from(header.header_size) != size_of::<CellStateHeader>()
                || header.capacity as usize != CELL_CAPACITY
                || header.slot_size as usize != size_of::<CellSlot>()
            {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("SHM segment {name} has an incompatible cell-state layout")
                )));
            }
        }
        Ok(Self { region, structural_lock: Mutex::new(()) })
    }

    fn header(&self) -> &CellStateHeader {
        unsafe { &*self.region.as_ptr().cast::<CellStateHeader>() }
    }

    fn slots(&self) -> &[CellSlot] {
        unsafe {
            std::slice::from_raw_parts(
                self.region.as_ptr().add(size_of::<CellStateHeader>()).cast::<CellSlot>(),
                CELL_CAPACITY
            )
        }
    }

    fn with_structure<T>(
        &self,
        operation: impl FnOnce(&[CellSlot], &AtomicU32) -> Result<T>
    ) -> Result<T> {
        let _local = lock_unpoisoned(&self.structural_lock);
        let _process = self.region.lock_exclusive()?;
        operation(self.slots(), &self.header().hint)
    }
}

#[cfg(unix)]
fn shm_segment_size() -> usize {
    size_of::<CellStateHeader>() + CELL_CAPACITY * size_of::<CellSlot>()
}

const _: () = assert!(CELL_CAPACITY.is_power_of_two());
const _: () = assert!(CELL_CAPACITY <= u32::MAX as usize);
const _: () = assert!(size_of::<CellStateHeader>() == 64);
const _: () = assert!(size_of::<CellSlot>() == 64);

#[cfg(test)]
mod registry_tests {
    use std::sync::Arc;

    use orbit_core::Fleet;

    use super::Cells;

    /// The in-memory registry keys tables by the fleet's address. The
    /// table therefore keeps the fleet alive: otherwise a dropped fleet's
    /// address could be reused by a new one, which would then find and
    /// share the old table.
    #[test]
    fn a_memory_table_keeps_its_fleet_alive() {
        let fleet = Arc::new(Fleet::join("cell-registry-test", 1).unwrap());
        let cells = Cells::new(Arc::clone(&fleet)).unwrap();
        assert!(Arc::strong_count(&fleet) > 1);
        drop(cells);
        assert_eq!(Arc::strong_count(&fleet), 1);
    }
}

#[cfg(test)]
mod wait_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use orbit_core::Fleet;

    use super::{Cells, Error};

    fn cells() -> Cells {
        Cells::new(Arc::new(Fleet::join("cell-wait-test", 1).unwrap())).unwrap()
    }

    /// A waiter parks on the change count and a write from another thread
    /// wakes it; it then reads the value the write left. Ten writes while
    /// parked are one wake.
    #[test]
    fn a_write_wakes_a_waiter_once() {
        let cells = cells();
        let cell = cells.allocate(0_i64).unwrap();
        let since = cell.version().unwrap();
        let watcher = cell.clone();
        let waiter = std::thread::spawn(move || watcher.wait_changed(since));

        std::thread::sleep(Duration::from_millis(30));
        for _ in 0..10 {
            cell.fetch_add(1).unwrap();
        }
        let now = waiter.join().unwrap().unwrap();
        assert!(now > since);
        assert_eq!(cell.load().unwrap(), 10);
        // Nothing new since: a wait would park; the count says so.
        assert_eq!(cell.version().unwrap(), cell.version().unwrap());
    }

    /// Releasing the cell is a change too: a parked waiter comes back Stale
    /// instead of sleeping on a slot somebody else may take.
    #[test]
    fn release_wakes_a_waiter_stale() {
        let cells = cells();
        let cell = cells.allocate(7_i64).unwrap();
        let since = cell.version().unwrap();
        let watcher = cell.clone();
        let waiter = std::thread::spawn(move || watcher.wait_changed(since));

        std::thread::sleep(Duration::from_millis(30));
        cell.release().unwrap();
        assert!(matches!(waiter.join().unwrap(), Err(Error::Stale(_))));
    }
}

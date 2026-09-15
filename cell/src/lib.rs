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

/// Reserved Orbit SHM kind for the cell table.
pub const CELL_STATE_KIND: u8 = 233;
/// Number of cells one fleet generation can hold at once.
///
/// Compile-time geometry: `ORBIT_CELL_CAPACITY` in the application's
/// `.cargo/config.toml` overrides the default. Must be a power of two. Peers
/// built with a different value are refused when they open the table.
pub const CELL_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_CELL_CAPACITY"), 4_096);

const SLOT_EMPTY: u8 = 0;
const SLOT_OCCUPIED: u8 = 1;

#[cfg(unix)]
const STATE_MAGIC: u32 = 0x43_45_4C_4C; // "CELL"
#[cfg(unix)]
const STATE_VERSION: u16 = 1;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    /// The id names a slot nothing occupies, or a generation that has been
    /// released since the id was minted.
    Stale(CellId),
    /// The id names a live cell of another type.
    TypeMismatch {
        id: CellId,
        expected: &'static str,
        found: &'static str,
    },
    /// The update would leave the integer range.
    Overflow,
    /// Every slot is occupied.
    Full {
        capacity: usize,
    },
    /// The text is not a cell id.
    Malformed(String),
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stale(id) => write!(f, "cell {id} has been released"),
            Self::TypeMismatch {
                id,
                expected,
                found,
            } => write!(f, "cell {id} holds {found}, not {expected}"),
            Self::Overflow => f.write_str("cell value is outside the integer range"),
            Self::Full { capacity } => write!(f, "cell table is full: capacity={capacity}"),
            Self::Malformed(text) => write!(f, "not a cell id: {text:?}"),
            Self::Io(error) => write!(f, "Orbit cell io error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
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
    generation: u32,
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
        Self {
            index: (bits >> 32) as u32,
            generation: bits as u32,
        }
    }
}

impl fmt::Display for CellId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
            generation: generation.parse().map_err(|_| malformed())?,
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
        _ => "unknown",
    }
}

/// The fleet's cell table. Cheap to clone.
#[derive(Clone)]
pub struct Cells {
    backend: CellBackend,
}

#[derive(Clone)]
enum CellBackend {
    InMemory(Arc<MemoryCellTable>),
    #[cfg(unix)]
    Shm(Arc<ShmCellTable>),
}

impl Cells {
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        let backend = if fleet.is_shm() {
            #[cfg(unix)]
            {
                CellBackend::Shm(Arc::new(ShmCellTable::open_or_create(&ring_segment_name(
                    fleet.name(),
                    CELL_STATE_KIND,
                ))?))
            }
            #[cfg(not(unix))]
            unreachable!("non-Unix fleets cannot use POSIX SHM")
        } else {
            CellBackend::InMemory(memory_table(&fleet))
        };
        Ok(Self { backend })
    }

    /// Take a free slot, stamp it with `T` and `initial`, and return the
    /// handle. The id inside it is what other processes open.
    pub fn allocate<T: CellType>(&self, initial: T) -> Result<Orbital<T>> {
        let id = self.with_structure(|slots, hint| {
            let capacity = slots.len();
            let start = hint.load(Ordering::Relaxed) as usize;
            for offset in 0..capacity {
                let index = (start + offset) & (capacity - 1);
                let slot = &slots[index];
                if slot.state.load(Ordering::Acquire) == SLOT_EMPTY {
                    let generation = slot.install(T::TAG, initial.to_bits());
                    hint.store(((index + 1) & (capacity - 1)) as u32, Ordering::Relaxed);
                    return Ok(CellId {
                        index: index as u32,
                        generation,
                    });
                }
            }
            Err(Error::Full { capacity })
        })?;
        Ok(Orbital {
            cells: self.clone(),
            id,
            _type: PhantomData,
        })
    }

    /// A handle to a live cell of type `T`.
    pub fn open<T: CellType>(&self, id: CellId) -> Result<Orbital<T>> {
        let slot = self.slot(id)?;
        let found = slot.tag.load(Ordering::Relaxed);
        if found != T::TAG {
            return Err(Error::TypeMismatch {
                id,
                expected: T::NAME,
                found: type_name(found),
            });
        }
        Ok(Orbital {
            cells: self.clone(),
            id,
            _type: PhantomData,
        })
    }

    /// Give the slot back. Every handle to this generation goes stale.
    pub fn release(&self, id: CellId) -> Result<()> {
        self.with_structure(|slots, _| {
            let slot = slots
                .get(id.index as usize)
                .filter(|slot| slot.is(id))
                .ok_or(Error::Stale(id))?;
            slot.state.store(SLOT_EMPTY, Ordering::Release);
            Ok(())
        })
    }

    /// Whether `id` names a live cell right now.
    pub fn is_live(&self, id: CellId) -> bool {
        self.slot(id).is_ok()
    }

    /// Clear the complete table during quiescent owner boot.
    pub fn reset_all(&self) -> Result<()> {
        self.with_structure(|slots, hint| {
            for slot in slots {
                slot.state.store(SLOT_EMPTY, Ordering::Release);
            }
            hint.store(0, Ordering::Relaxed);
            Ok(())
        })
    }

    /// Remove the cell SHM object. Existing mappings remain valid until their
    /// processes release them.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        match &self.backend {
            CellBackend::InMemory(_) => self.reset_all(),
            CellBackend::Shm(table) => table.region.unlink().map_err(Error::Io),
        }
    }

    fn slots(&self) -> &[CellSlot] {
        match &self.backend {
            CellBackend::InMemory(table) => &table.slots,
            #[cfg(unix)]
            CellBackend::Shm(table) => table.slots(),
        }
    }

    fn slot(&self, id: CellId) -> Result<&CellSlot> {
        self.slots()
            .get(id.index as usize)
            .filter(|slot| slot.is(id))
            .ok_or(Error::Stale(id))
    }

    fn with_structure<T>(
        &self,
        operation: impl FnOnce(&[CellSlot], &AtomicU32) -> Result<T>,
    ) -> Result<T> {
        match &self.backend {
            CellBackend::InMemory(table) => {
                let _local = lock_unpoisoned(&table.structural_lock);
                operation(&table.slots, &table.hint)
            }
            #[cfg(unix)]
            CellBackend::Shm(table) => table.with_structure(operation),
        }
    }
}

/// One cell, typed. Cheap to clone; every clone is the same place.
#[derive(Clone)]
pub struct Orbital<T: CellType> {
    cells: Cells,
    id: CellId,
    _type: PhantomData<T>,
}

impl<T: CellType> Orbital<T> {
    pub fn id(&self) -> CellId {
        self.id
    }

    pub fn load(&self) -> Result<T> {
        Ok(T::from_bits(self.slot()?.value.load(Ordering::Acquire)))
    }

    pub fn store(&self, value: T) -> Result<()> {
        self.slot()?.value.store(value.to_bits(), Ordering::Release);
        Ok(())
    }

    /// Replace the value and return what it was.
    pub fn swap(&self, value: T) -> Result<T> {
        Ok(T::from_bits(
            self.slot()?.value.swap(value.to_bits(), Ordering::AcqRel),
        ))
    }

    /// Store `new` only if the cell still holds `current`. `Ok(Ok(previous))`
    /// on success, `Ok(Err(actual))` when it held something else.
    pub fn compare_exchange(&self, current: T, new: T) -> Result<std::result::Result<T, T>> {
        Ok(self
            .slot()?
            .value
            .compare_exchange(
                current.to_bits(),
                new.to_bits(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(T::from_bits)
            .map_err(T::from_bits))
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
    pub fn fetch_add(&self, by: i64) -> Result<i64> {
        let slot = self.slot()?;
        slot.value
            .try_update(Ordering::AcqRel, Ordering::Acquire, |bits| {
                (bits as i64).checked_add(by).map(|value| value as u64)
            })
            .map(|previous| (previous as i64) + by)
            .map_err(|_| Error::Overflow)
    }

    pub fn fetch_sub(&self, by: i64) -> Result<i64> {
        self.fetch_add(by.checked_neg().ok_or(Error::Overflow)?)
    }
}

impl Orbital<u64> {
    pub fn fetch_add(&self, by: u64) -> Result<u64> {
        let slot = self.slot()?;
        slot.value
            .try_update(Ordering::AcqRel, Ordering::Acquire, |bits| {
                bits.checked_add(by)
            })
            .map(|previous| previous + by)
            .map_err(|_| Error::Overflow)
    }

    pub fn fetch_sub(&self, by: u64) -> Result<u64> {
        let slot = self.slot()?;
        slot.value
            .try_update(Ordering::AcqRel, Ordering::Acquire, |bits| {
                bits.checked_sub(by)
            })
            .map(|previous| previous - by)
            .map_err(|_| Error::Overflow)
    }
}

impl Orbital<f64> {
    /// Add `by` and return the value after. A compare-and-swap loop: floats
    /// have no fetch-add of their own.
    pub fn fetch_add(&self, by: f64) -> Result<f64> {
        let slot = self.slot()?;
        let previous = slot
            .value
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bits| {
                Some((f64::from_bits(bits) + by).to_bits())
            })
            .unwrap_or_else(|bits| bits);
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
    _reserved: [u8; 44],
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
            _reserved: [0; 44],
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
    _padding: [u8; 48],
}

impl CellSlot {
    fn empty() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            tag: AtomicU8::new(0),
            _reserved: [0; 2],
            generation: AtomicU32::new(0),
            value: AtomicU64::new(0),
            _padding: [0; 48],
        }
    }

    fn is(&self, id: CellId) -> bool {
        // `Acquire` on `state` publishes the generation written by `install`.
        self.state.load(Ordering::Acquire) == SLOT_OCCUPIED
            && self.generation.load(Ordering::Relaxed) == id.generation
    }

    /// Under the structural lock. Returns the generation the new cell lives in;
    /// it is bumped on every install, so no released id ever matches again.
    fn install(&self, tag: u8, value: u64) -> u32 {
        let generation = self
            .generation
            .load(Ordering::Relaxed)
            .wrapping_add(1)
            .max(1);
        self.tag.store(tag, Ordering::Relaxed);
        self.value.store(value, Ordering::Relaxed);
        self.generation.store(generation, Ordering::Relaxed);
        self.state.store(SLOT_OCCUPIED, Ordering::Release);
        generation
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

struct MemoryCellTable {
    slots: Vec<CellSlot>,
    hint: AtomicU32,
    structural_lock: Mutex<()>,
}

impl MemoryCellTable {
    fn new() -> Self {
        Self {
            slots: (0..CELL_CAPACITY).map(|_| CellSlot::empty()).collect(),
            hint: AtomicU32::new(0),
            structural_lock: Mutex::new(()),
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
    let table = Arc::new(MemoryCellTable::new());
    tables.insert(fleet_identity, Arc::downgrade(&table));
    table
}

#[cfg(unix)]
struct ShmCellTable {
    region: ShmRegion,
    structural_lock: Mutex<()>,
}

#[cfg(unix)]
impl ShmCellTable {
    fn open_or_create(name: &str) -> Result<Self> {
        use std::{io, ptr};

        let (region, _initialization_lock) =
            ShmRegion::open_or_create_locked(name, shm_segment_size())?;
        if region.created() {
            unsafe {
                ptr::write(
                    region.as_ptr().cast::<CellStateHeader>(),
                    CellStateHeader::new(),
                );
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
                    ),
                )));
            }
            if header.version != STATE_VERSION
                || usize::from(header.header_size) != size_of::<CellStateHeader>()
                || header.capacity as usize != CELL_CAPACITY
                || header.slot_size as usize != size_of::<CellSlot>()
            {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("SHM segment {name} has an incompatible cell-state layout"),
                )));
            }
        }
        Ok(Self {
            region,
            structural_lock: Mutex::new(()),
        })
    }

    fn header(&self) -> &CellStateHeader {
        unsafe { &*self.region.as_ptr().cast::<CellStateHeader>() }
    }

    fn slots(&self) -> &[CellSlot] {
        unsafe {
            std::slice::from_raw_parts(
                self.region
                    .as_ptr()
                    .add(size_of::<CellStateHeader>())
                    .cast::<CellSlot>(),
                CELL_CAPACITY,
            )
        }
    }

    fn with_structure<T>(
        &self,
        operation: impl FnOnce(&[CellSlot], &AtomicU32) -> Result<T>,
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

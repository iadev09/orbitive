//! Fixed-width text cells: the string arena beside the scalar table.
//!
//! A text cell holds up to [`CELL_TEXT_MAX`] bytes of UTF-8 in shared memory.
//! Sixty-four bits can be swapped in one instruction; a string cannot, so a
//! cell is a seqlock: writers take the cell's own spin bit and bump a version
//! around their write, readers copy and accept the copy only if the version
//! held still. Readers never block writers and never see a torn string.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU8, AtomicU16, AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};

use crate::{Error, Result, SLOT_EMPTY, SLOT_OCCUPIED, lock_unpoisoned};

/// Reserved Orbit SHM kind for the text cell table.
pub const CELL_TEXT_KIND: u8 = 227;
/// Number of text cells one fleet generation can hold at once.
///
/// Compile-time geometry: `ORBIT_CELL_TEXT_CAPACITY`. Must be a power of two.
pub const CELL_TEXT_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_CELL_TEXT_CAPACITY"), 1_024);
/// Bytes one text cell can hold.
///
/// Compile-time geometry: `ORBIT_CELL_TEXT_MAX`. It sizes every slot, so it is
/// part of the same wire contract as the capacity.
pub const CELL_TEXT_MAX: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_CELL_TEXT_MAX"), 240);

#[cfg(unix)]
const TEXT_MAGIC: u32 = 0x43_54_58_54; // "CTXT"
#[cfg(unix)]
const TEXT_VERSION: u16 = 1;

/// The address of one text cell; prints as `text:<slot>:<generation>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TextId {
    index: u32,
    generation: u32,
}

impl TextId {
    pub const fn index(self) -> u32 {
        self.index
    }

    pub const fn generation(self) -> u32 {
        self.generation
    }

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

impl fmt::Display for TextId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "text:{}:{}", self.index, self.generation)
    }
}

impl FromStr for TextId {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        let malformed = || Error::Malformed(text.to_owned());
        let rest = text.strip_prefix("text:").ok_or_else(malformed)?;
        let (index, generation) = rest.split_once(':').ok_or_else(malformed)?;
        Ok(Self {
            index: index.parse().map_err(|_| malformed())?,
            generation: generation.parse().map_err(|_| malformed())?,
        })
    }
}

/// One text cell. Cheap to clone; every clone is the same place.
#[derive(Clone)]
pub struct Text {
    table: TextBackend,
    id: TextId,
}

impl Text {
    pub fn id(&self) -> TextId {
        self.id
    }

    /// A copy of the current text.
    pub fn load(&self) -> Result<String> {
        let slot = self.slot()?;
        let mut bytes = vec![0_u8; CELL_TEXT_MAX];
        loop {
            let before = slot.version.load(Ordering::Acquire);
            if before & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let len = usize::from(slot.len.load(Ordering::Relaxed)).min(CELL_TEXT_MAX);
            for (target, source) in bytes[..len].iter_mut().zip(&slot.bytes[..len]) {
                *target = source.load(Ordering::Relaxed);
            }
            std::sync::atomic::fence(Ordering::Acquire);
            if slot.version.load(Ordering::Relaxed) == before {
                bytes.truncate(len);
                return String::from_utf8(bytes).map_err(|_| Error::Corrupt(self.id.to_string()));
            }
        }
    }

    pub fn len(&self) -> Result<usize> {
        Ok(usize::from(self.slot()?.len.load(Ordering::Acquire)))
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Replace the text.
    pub fn store(&self, text: &str) -> Result<()> {
        if text.len() > CELL_TEXT_MAX {
            return Err(Error::TooLong {
                len: text.len(),
                max: CELL_TEXT_MAX,
            });
        }
        let slot = self.slot()?;
        let _write = slot.writer();
        slot.write(0, text.as_bytes());
        Ok(())
    }

    /// Append to the text, atomically with respect to every other writer;
    /// refused whole, never cut, when the result would not fit.
    pub fn append(&self, text: &str) -> Result<usize> {
        let slot = self.slot()?;
        let _write = slot.writer();
        let current = usize::from(slot.len.load(Ordering::Relaxed));
        let total = current + text.len();
        if total > CELL_TEXT_MAX {
            return Err(Error::TooLong {
                len: total,
                max: CELL_TEXT_MAX,
            });
        }
        slot.write(current, text.as_bytes());
        Ok(total)
    }

    /// Give the cell back; this and every other handle to it go stale.
    pub fn release(self) -> Result<()> {
        self.table.release(self.id)
    }

    fn slot(&self) -> Result<&TextSlot> {
        self.table.slot(self.id)
    }
}

#[derive(Clone)]
pub(crate) enum TextBackend {
    InMemory(Arc<MemoryTextTable>),
    #[cfg(unix)]
    Shm(Arc<ShmTextTable>),
}

impl TextBackend {
    pub(crate) fn new(fleet: &Arc<Fleet>) -> Result<Self> {
        Ok(if fleet.is_shm() {
            #[cfg(unix)]
            {
                Self::Shm(Arc::new(ShmTextTable::open_or_create(&ring_segment_name(
                    fleet.name(),
                    CELL_TEXT_KIND,
                ))?))
            }
            #[cfg(not(unix))]
            unreachable!("non-Unix fleets cannot use POSIX SHM")
        } else {
            Self::InMemory(memory_table(fleet))
        })
    }

    pub(crate) fn allocate(&self, text: &str) -> Result<Text> {
        if text.len() > CELL_TEXT_MAX {
            return Err(Error::TooLong {
                len: text.len(),
                max: CELL_TEXT_MAX,
            });
        }
        let id = self.with_structure(|slots, hint| {
            let capacity = slots.len();
            let start = hint.load(Ordering::Relaxed) as usize;
            for offset in 0..capacity {
                let index = (start + offset) & (capacity - 1);
                let slot = &slots[index];
                if slot.state.load(Ordering::Acquire) == SLOT_EMPTY {
                    let generation = slot.install(text.as_bytes());
                    hint.store(((index + 1) & (capacity - 1)) as u32, Ordering::Relaxed);
                    return Ok(TextId {
                        index: index as u32,
                        generation,
                    });
                }
            }
            Err(Error::Full { capacity })
        })?;
        Ok(Text {
            table: self.clone(),
            id,
        })
    }

    pub(crate) fn open(&self, id: TextId) -> Result<Text> {
        self.slot(id)?;
        Ok(Text {
            table: self.clone(),
            id,
        })
    }

    pub(crate) fn release(&self, id: TextId) -> Result<()> {
        self.with_structure(|slots, _| {
            let slot = slots
                .get(id.index as usize)
                .filter(|slot| slot.is(id))
                .ok_or(Error::StaleText(id))?;
            slot.state.store(SLOT_EMPTY, Ordering::Release);
            Ok(())
        })
    }

    pub(crate) fn is_live(&self, id: TextId) -> bool {
        self.slot(id).is_ok()
    }

    pub(crate) fn reset_all(&self) -> Result<()> {
        self.with_structure(|slots, hint| {
            for slot in slots {
                slot.state.store(SLOT_EMPTY, Ordering::Release);
            }
            hint.store(0, Ordering::Relaxed);
            Ok(())
        })
    }

    #[cfg(unix)]
    pub(crate) fn unlink(&self) -> Result<()> {
        match self {
            Self::InMemory(_) => self.reset_all(),
            Self::Shm(table) => table.region.unlink().map_err(Error::Io),
        }
    }

    fn slots(&self) -> &[TextSlot] {
        match self {
            Self::InMemory(table) => &table.slots,
            #[cfg(unix)]
            Self::Shm(table) => table.slots(),
        }
    }

    fn slot(&self, id: TextId) -> Result<&TextSlot> {
        self.slots()
            .get(id.index as usize)
            .filter(|slot| slot.is(id))
            .ok_or(Error::StaleText(id))
    }

    fn with_structure<T>(
        &self,
        operation: impl FnOnce(&[TextSlot], &AtomicU32) -> Result<T>,
    ) -> Result<T> {
        match self {
            Self::InMemory(table) => {
                let _local = lock_unpoisoned(&table.structural_lock);
                operation(&table.slots, &table.hint)
            }
            #[cfg(unix)]
            Self::Shm(table) => table.with_structure(operation),
        }
    }
}

#[repr(C, align(64))]
struct TextStateHeader {
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
    hint: AtomicU32,
    _reserved: [u8; 44],
}

impl TextStateHeader {
    fn new() -> Self {
        Self {
            #[cfg(unix)]
            magic: TEXT_MAGIC,
            #[cfg(not(unix))]
            _magic: 0,
            #[cfg(unix)]
            version: TEXT_VERSION,
            #[cfg(not(unix))]
            _version: 0,
            header_size: size_of::<Self>() as u16,
            capacity: CELL_TEXT_CAPACITY as u32,
            slot_size: size_of::<TextSlot>() as u32,
            hint: AtomicU32::new(0),
            _reserved: [0; 44],
        }
    }
}

/// One text cell in shared memory: a seqlock version, a writer bit, a
/// length and the bytes, padded to the cache line.
#[repr(C, align(64))]
struct TextSlot {
    state: AtomicU8,
    writer: AtomicU8,
    len: AtomicU16,
    generation: AtomicU32,
    version: AtomicU32,
    _reserved: [u8; 4],
    bytes: [AtomicU8; CELL_TEXT_MAX],
}

impl TextSlot {
    fn empty() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            writer: AtomicU8::new(0),
            len: AtomicU16::new(0),
            generation: AtomicU32::new(0),
            version: AtomicU32::new(0),
            _reserved: [0; 4],
            bytes: std::array::from_fn(|_| AtomicU8::new(0)),
        }
    }

    fn is(&self, id: TextId) -> bool {
        self.state.load(Ordering::Acquire) == SLOT_OCCUPIED
            && self.generation.load(Ordering::Relaxed) == id.generation
    }

    /// Under the structural lock.
    fn install(&self, text: &[u8]) -> u32 {
        let generation = self
            .generation
            .load(Ordering::Relaxed)
            .wrapping_add(1)
            .max(1);
        for (index, byte) in self.bytes.iter().enumerate() {
            byte.store(text.get(index).copied().unwrap_or(0), Ordering::Relaxed);
        }
        self.len.store(text.len() as u16, Ordering::Relaxed);
        self.version.store(0, Ordering::Relaxed);
        self.writer.store(0, Ordering::Relaxed);
        self.generation.store(generation, Ordering::Relaxed);
        self.state.store(SLOT_OCCUPIED, Ordering::Release);
        generation
    }

    /// The cell's writer bit, held for the duration of one write.
    fn writer(&self) -> WriterGuard<'_> {
        while self
            .writer
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        WriterGuard(self)
    }

    /// Write `bytes` at `offset` and publish the new length; the caller holds
    /// the writer bit and has checked the fit.
    fn write(&self, offset: usize, bytes: &[u8]) {
        debug_assert!(offset + bytes.len() <= CELL_TEXT_MAX);
        self.version.fetch_add(1, Ordering::AcqRel);
        for (target, source) in self.bytes[offset..offset + bytes.len()].iter().zip(bytes) {
            target.store(*source, Ordering::Relaxed);
        }
        self.len
            .store((offset + bytes.len()) as u16, Ordering::Relaxed);
        self.version.fetch_add(1, Ordering::AcqRel);
    }
}

struct WriterGuard<'a>(&'a TextSlot);

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        self.0.writer.store(0, Ordering::Release);
    }
}

pub(crate) struct MemoryTextTable {
    slots: Vec<TextSlot>,
    hint: AtomicU32,
    structural_lock: Mutex<()>,
}

impl MemoryTextTable {
    fn new() -> Self {
        Self {
            slots: (0..CELL_TEXT_CAPACITY).map(|_| TextSlot::empty()).collect(),
            hint: AtomicU32::new(0),
            structural_lock: Mutex::new(()),
        }
    }
}

type MemoryRegistry = HashMap<usize, Weak<MemoryTextTable>>;

static MEMORY_TABLES: LazyLock<Mutex<MemoryRegistry>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn memory_table(fleet: &Arc<Fleet>) -> Arc<MemoryTextTable> {
    let fleet_identity = Arc::as_ptr(fleet) as usize;
    let mut tables = lock_unpoisoned(&MEMORY_TABLES);
    tables.retain(|_, table| table.strong_count() > 0);
    if let Some(table) = tables.get(&fleet_identity).and_then(Weak::upgrade) {
        return table;
    }
    let table = Arc::new(MemoryTextTable::new());
    tables.insert(fleet_identity, Arc::downgrade(&table));
    table
}

#[cfg(unix)]
pub(crate) struct ShmTextTable {
    region: ShmRegion,
    structural_lock: Mutex<()>,
}

#[cfg(unix)]
impl ShmTextTable {
    fn open_or_create(name: &str) -> Result<Self> {
        use std::{io, ptr};

        let (region, _initialization_lock) =
            ShmRegion::open_or_create_locked(name, shm_segment_size())?;
        if region.created() {
            unsafe {
                ptr::write(
                    region.as_ptr().cast::<TextStateHeader>(),
                    TextStateHeader::new(),
                );
                let slots = region.as_ptr().add(size_of::<TextStateHeader>());
                ptr::write_bytes(slots, 0, CELL_TEXT_CAPACITY * size_of::<TextSlot>());
            }
        } else {
            let header = unsafe { &*region.as_ptr().cast::<TextStateHeader>() };
            if header.magic != TEXT_MAGIC {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {name} has wrong magic 0x{:08X} (expected 0x{TEXT_MAGIC:08X})",
                        header.magic
                    ),
                )));
            }
            if header.version != TEXT_VERSION
                || usize::from(header.header_size) != size_of::<TextStateHeader>()
                || header.capacity as usize != CELL_TEXT_CAPACITY
                || header.slot_size as usize != size_of::<TextSlot>()
            {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("SHM segment {name} has an incompatible text-cell layout"),
                )));
            }
        }
        Ok(Self {
            region,
            structural_lock: Mutex::new(()),
        })
    }

    fn header(&self) -> &TextStateHeader {
        unsafe { &*self.region.as_ptr().cast::<TextStateHeader>() }
    }

    fn slots(&self) -> &[TextSlot] {
        unsafe {
            std::slice::from_raw_parts(
                self.region
                    .as_ptr()
                    .add(size_of::<TextStateHeader>())
                    .cast::<TextSlot>(),
                CELL_TEXT_CAPACITY,
            )
        }
    }

    fn with_structure<T>(
        &self,
        operation: impl FnOnce(&[TextSlot], &AtomicU32) -> Result<T>,
    ) -> Result<T> {
        let _local = lock_unpoisoned(&self.structural_lock);
        let _process = self.region.lock_exclusive()?;
        operation(self.slots(), &self.header().hint)
    }
}

#[cfg(unix)]
fn shm_segment_size() -> usize {
    size_of::<TextStateHeader>() + CELL_TEXT_CAPACITY * size_of::<TextSlot>()
}

const _: () = assert!(CELL_TEXT_CAPACITY.is_power_of_two());
const _: () = assert!(CELL_TEXT_MAX > 0 && CELL_TEXT_MAX <= u16::MAX as usize);
const _: () = assert!(size_of::<TextStateHeader>() == 64);
// 16 bytes of fixed fields, then the text, padded to the cache line.
const _: () = assert!(size_of::<TextSlot>() == (16 + CELL_TEXT_MAX).next_multiple_of(64));

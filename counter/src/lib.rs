//! Keyed fleet counters backed by one authoritative SHM table.
//!
//! A counter is current state, not history: no ring frame is published and
//! no cursor exists. The owner of a fleet generation calls [`Counter::reset_all`]
//! during quiescent boot; everything else is lock-free atomics on
//! immutable-address slots.
//!
//! Keys are installed once per fleet generation under a short,
//! process-recoverable structural lock. Once installed, their values are
//! immutable-address, signed 64-bit atomics: reads and updates do not publish
//! ring frames and do not acquire an OS lock.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};

use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};

/// Reserved Orbit SHM kind for the keyed counter table.
pub const COUNTER_STATE_KIND: u8 = 230;
/// Maximum number of distinct keys installed during one fleet generation.
///
/// Compile-time geometry: `ORBIT_COUNTER_CAPACITY` in the application's
/// `.cargo/config.toml` overrides the default. Must be a power of two. Peers
/// built with a different value are refused when they open the table.
pub const COUNTER_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_COUNTER_CAPACITY"), 1_024);
/// Maximum counter key length in bytes.
///
/// Compile-time geometry: `ORBIT_COUNTER_KEY_MAX`. It sizes every slot, so it
/// is part of the same wire contract as the capacity.
pub const COUNTER_KEY_MAX: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_COUNTER_KEY_MAX"), 240);

const SLOT_EMPTY: u8 = 0;
const SLOT_OCCUPIED: u8 = 1;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[cfg(unix)]
const STATE_MAGIC: u32 = 0x43_43_4E_54; // "CCNT"
#[cfg(unix)]
const STATE_VERSION: u16 = 1;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    KeyEmpty,
    KeyTooLarge { len: usize, max: usize },
    NegativeAmount(i64),
    Overflow,
    StateFull { capacity: usize },
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyEmpty => f.write_str("counter key must not be empty"),
            Self::KeyTooLarge { len, max } => {
                write!(f, "counter key is too large: len={len} max={max}")
            }
            Self::NegativeAmount(value) => {
                write!(f, "counter amount must not be negative: {value}")
            }
            Self::Overflow => f.write_str("counter value is outside the signed 64-bit range"),
            Self::StateFull { capacity } => {
                write!(f, "counter state table is full: capacity={capacity}")
            }
            Self::Io(error) => write!(f, "Orbit counter io error: {error}"),
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

/// Fleet-wide keyed signed counters.
///
/// A missing key starts at zero for `increment` and `decrement`. Both methods
/// return the value after their atomic update. `reset` installs a missing key
/// or atomically returns an existing value to zero.
#[derive(Clone)]
pub struct Counter {
    backend: CounterBackend,
}

#[derive(Clone)]
enum CounterBackend {
    InMemory(Arc<MemoryCounterTable>),
    #[cfg(unix)]
    Shm(Arc<ShmCounterTable>),
}

impl Counter {
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        let backend = if fleet.is_shm() {
            #[cfg(unix)]
            {
                CounterBackend::Shm(Arc::new(ShmCounterTable::open_or_create(
                    &ring_segment_name(fleet.name(), COUNTER_STATE_KIND),
                )?))
            }
            #[cfg(not(unix))]
            unreachable!("non-Unix fleets cannot use POSIX SHM")
        } else {
            CounterBackend::InMemory(memory_table(&fleet))
        };
        Ok(Self { backend })
    }

    pub fn get(&self, key: &str) -> Result<Option<i64>> {
        validate_key(key)?;
        let hash = counter_hash(key.as_bytes());
        match &self.backend {
            CounterBackend::InMemory(table) => {
                let slots = lock_unpoisoned(&table.slots);
                Ok(find_slot(&slots, hash, key.as_bytes())
                    .map(|slot| slot.value.load(Ordering::Acquire)))
            }
            #[cfg(unix)]
            CounterBackend::Shm(table) => Ok(find_slot(table.slots(), hash, key.as_bytes())
                .map(|slot| slot.value.load(Ordering::Acquire))),
        }
    }

    pub fn increment(&self, key: &str, by: i64) -> Result<i64> {
        validate_amount(by)?;
        self.update(key, by)
    }

    pub fn decrement(&self, key: &str, by: i64) -> Result<i64> {
        validate_amount(by)?;
        self.update(key, -by)
    }

    /// Atomically set a key to zero, installing it when necessary.
    pub fn reset(&self, key: &str) -> Result<()> {
        validate_key(key)?;
        let hash = counter_hash(key.as_bytes());
        match &self.backend {
            CounterBackend::InMemory(table) => {
                let slots = lock_unpoisoned(&table.slots);
                reset_or_install(&slots, hash, key.as_bytes())
            }
            #[cfg(unix)]
            CounterBackend::Shm(table) => {
                if let Some(slot) = find_slot(table.slots(), hash, key.as_bytes()) {
                    slot.value.store(0, Ordering::Release);
                    return Ok(());
                }
                table.with_structure(|slots| reset_or_install(slots, hash, key.as_bytes()))
            }
        }
    }

    /// Clear the complete table during quiescent owner boot.
    pub fn reset_all(&self) -> Result<()> {
        match &self.backend {
            CounterBackend::InMemory(table) => {
                let slots = lock_unpoisoned(&table.slots);
                clear_slots(&slots);
                Ok(())
            }
            #[cfg(unix)]
            CounterBackend::Shm(table) => table.with_structure(|slots| {
                clear_slots(slots);
                Ok(())
            }),
        }
    }

    /// Remove the counter SHM object. Existing mappings remain valid until
    /// their processes release them.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        match &self.backend {
            CounterBackend::InMemory(_) => self.reset_all(),
            CounterBackend::Shm(table) => table.region.unlink().map_err(Error::Io),
        }
    }

    fn update(&self, key: &str, delta: i64) -> Result<i64> {
        validate_key(key)?;
        let hash = counter_hash(key.as_bytes());
        match &self.backend {
            CounterBackend::InMemory(table) => {
                let slots = lock_unpoisoned(&table.slots);
                update_or_install(&slots, hash, key.as_bytes(), delta)
            }
            #[cfg(unix)]
            CounterBackend::Shm(table) => {
                if let Some(slot) = find_slot(table.slots(), hash, key.as_bytes()) {
                    return update_value(slot, delta);
                }
                table.with_structure(|slots| update_or_install(slots, hash, key.as_bytes(), delta))
            }
        }
    }
}

fn update_or_install(slots: &[CounterSlot], hash: u64, key: &[u8], delta: i64) -> Result<i64> {
    for offset in 0..slots.len() {
        let index = probe_index(hash, offset, slots.len());
        let slot = &slots[index];
        match slot.state.load(Ordering::Acquire) {
            SLOT_EMPTY => {
                slot.install(hash, key, delta);
                return Ok(delta);
            }
            SLOT_OCCUPIED if slot.matches(hash, key) => return update_value(slot, delta),
            _ => {}
        }
    }
    Err(Error::StateFull {
        capacity: slots.len(),
    })
}

fn find_slot<'a>(slots: &'a [CounterSlot], hash: u64, key: &[u8]) -> Option<&'a CounterSlot> {
    for offset in 0..slots.len() {
        let slot = &slots[probe_index(hash, offset, slots.len())];
        match slot.state.load(Ordering::Acquire) {
            SLOT_EMPTY => return None,
            SLOT_OCCUPIED if slot.matches(hash, key) => return Some(slot),
            _ => {}
        }
    }
    None
}

fn reset_or_install(slots: &[CounterSlot], hash: u64, key: &[u8]) -> Result<()> {
    for offset in 0..slots.len() {
        let index = probe_index(hash, offset, slots.len());
        let slot = &slots[index];
        match slot.state.load(Ordering::Acquire) {
            SLOT_EMPTY => {
                slot.install(hash, key, 0);
                return Ok(());
            }
            SLOT_OCCUPIED if slot.matches(hash, key) => {
                slot.value.store(0, Ordering::Release);
                return Ok(());
            }
            _ => {}
        }
    }
    Err(Error::StateFull {
        capacity: slots.len(),
    })
}

fn clear_slots(slots: &[CounterSlot]) {
    for slot in slots {
        slot.value.store(0, Ordering::Relaxed);
        slot.state.store(SLOT_EMPTY, Ordering::Release);
    }
}

fn update_value(slot: &CounterSlot, delta: i64) -> Result<i64> {
    slot.value
        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(delta)
        })
        .map(|previous| previous + delta)
        .map_err(|_| Error::Overflow)
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::KeyEmpty);
    }
    if key.len() > COUNTER_KEY_MAX {
        return Err(Error::KeyTooLarge {
            len: key.len(),
            max: COUNTER_KEY_MAX,
        });
    }
    Ok(())
}

fn validate_amount(by: i64) -> Result<()> {
    if by < 0 {
        return Err(Error::NegativeAmount(by));
    }
    Ok(())
}

fn counter_hash(key: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn probe_index(hash: u64, offset: usize, capacity: usize) -> usize {
    debug_assert!(capacity.is_power_of_two());
    (hash as usize).wrapping_add(offset) & (capacity - 1)
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

struct MemoryCounterTable {
    /// Held so the fleet's address, which keys the registry, cannot be
    /// reused by another fleet while this table lives.
    _fleet: Arc<Fleet>,
    slots: Mutex<Vec<CounterSlot>>,
}

impl MemoryCounterTable {
    fn new(fleet: Arc<Fleet>) -> Self {
        Self {
            _fleet: fleet,
            slots: Mutex::new(
                (0..COUNTER_CAPACITY)
                    .map(|_| CounterSlot::empty())
                    .collect(),
            ),
        }
    }
}

type MemoryRegistry = HashMap<usize, Weak<MemoryCounterTable>>;
static MEMORY_TABLES: LazyLock<Mutex<MemoryRegistry>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn memory_table(fleet: &Arc<Fleet>) -> Arc<MemoryCounterTable> {
    let fleet_identity = Arc::as_ptr(fleet) as usize;
    let mut tables = lock_unpoisoned(&MEMORY_TABLES);
    tables.retain(|_, table| table.strong_count() > 0);
    if let Some(table) = tables.get(&fleet_identity).and_then(Weak::upgrade) {
        return table;
    }
    let table = Arc::new(MemoryCounterTable::new(Arc::clone(fleet)));
    tables.insert(fleet_identity, Arc::downgrade(&table));
    table
}

#[repr(C, align(64))]
struct CounterStateHeader {
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
    _reserved: [u8; 48],
}

impl CounterStateHeader {
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
            capacity: COUNTER_CAPACITY as u32,
            slot_size: size_of::<CounterSlot>() as u32,
            _reserved: [0; 48],
        }
    }
}

/// One keyed counter, laid out for shared memory.
///
/// Every field is atomic, including the ones only ever written under the
/// structural lock. The lock makes the writes mutually exclusive, but it does
/// not make a `&mut [CounterSlot]` legal while readers hold `&[CounterSlot]`
/// over the same mapping — and they do, on the lock-free read path. Atomics
/// remove the need for the `&mut` entirely, which is what makes the aliasing
/// question go away rather than merely become unlikely.
///
/// `AtomicU8`, `AtomicU16` and `AtomicU64` match the size and alignment of the
/// integers they replace, so the on-disk layout this struct describes — and the
/// `slot_size` the header validates — is unchanged.
#[repr(C, align(64))]
struct CounterSlot {
    state: AtomicU8,
    _reserved: [u8; 7],
    key_hash: AtomicU64,
    value: AtomicI64,
    key_len: AtomicU16,
    _padding: [u8; 6],
    key: [AtomicU8; COUNTER_KEY_MAX],
}

impl CounterSlot {
    fn empty() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            _reserved: [0; 7],
            key_hash: AtomicU64::new(0),
            value: AtomicI64::new(0),
            key_len: AtomicU16::new(0),
            _padding: [0; 6],
            key: std::array::from_fn(|_| AtomicU8::new(0)),
        }
    }

    fn matches(&self, hash: u64, key: &[u8]) -> bool {
        // The `Acquire` on `state` is what publishes the fields below: `install`
        // writes them and then releases `state`, so a slot seen as occupied has
        // its key fully visible. They are therefore read `Relaxed`.
        self.state.load(Ordering::Acquire) == SLOT_OCCUPIED
            && self.key_hash.load(Ordering::Relaxed) == hash
            && usize::from(self.key_len.load(Ordering::Relaxed)) == key.len()
            && self.key[..key.len()]
                .iter()
                .zip(key)
                .all(|(stored, expected)| stored.load(Ordering::Relaxed) == *expected)
    }

    /// Takes `&self`, not `&mut self`: callers hold the structural lock, which
    /// is what makes this exclusive, and the shared reference is what keeps the
    /// mapping free of `&mut` while readers are looking at it.
    fn install(&self, hash: u64, key: &[u8], value: i64) {
        debug_assert!(key.len() <= COUNTER_KEY_MAX);
        self.key_hash.store(hash, Ordering::Relaxed);
        self.key_len.store(key.len() as u16, Ordering::Relaxed);
        for (index, byte) in self.key.iter().enumerate() {
            byte.store(key.get(index).copied().unwrap_or(0), Ordering::Relaxed);
        }
        self.value.store(value, Ordering::Relaxed);
        // Releases everything above to any reader that sees the slot occupied.
        self.state.store(SLOT_OCCUPIED, Ordering::Release);
    }
}

#[cfg(unix)]
struct ShmCounterTable {
    region: ShmRegion,
    structural_lock: Mutex<()>,
}

#[cfg(unix)]
impl ShmCounterTable {
    fn open_or_create(name: &str) -> Result<Self> {
        use std::{io, ptr};

        let (region, _initialization_lock) =
            ShmRegion::open_or_create_locked(name, shm_segment_size())?;
        if region.created() {
            unsafe {
                ptr::write(
                    region.as_ptr().cast::<CounterStateHeader>(),
                    CounterStateHeader::new(),
                );
                let slots = region.as_ptr().add(size_of::<CounterStateHeader>());
                ptr::write_bytes(slots, 0, COUNTER_CAPACITY * size_of::<CounterSlot>());
            }
        } else {
            let header = unsafe { &*region.as_ptr().cast::<CounterStateHeader>() };
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
                || usize::from(header.header_size) != size_of::<CounterStateHeader>()
                || header.capacity as usize != COUNTER_CAPACITY
                || header.slot_size as usize != size_of::<CounterSlot>()
            {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("SHM segment {name} has an incompatible counter-state layout"),
                )));
            }
        }

        Ok(Self {
            region,
            structural_lock: Mutex::new(()),
        })
    }

    fn slots(&self) -> &[CounterSlot] {
        unsafe {
            std::slice::from_raw_parts(
                self.region
                    .as_ptr()
                    .add(size_of::<CounterStateHeader>())
                    .cast::<CounterSlot>(),
                COUNTER_CAPACITY,
            )
        }
    }

    fn with_structure<T>(&self, operation: impl FnOnce(&[CounterSlot]) -> Result<T>) -> Result<T> {
        let _local = lock_unpoisoned(&self.structural_lock);
        let _process = self.region.lock_exclusive()?;
        // Shared, never mutable: the locks above provide the exclusion, and
        // every field is atomic, so nothing here needs a `&mut` over memory
        // other processes are reading.
        operation(self.slots())
    }
}

#[cfg(unix)]
fn shm_segment_size() -> usize {
    size_of::<CounterStateHeader>() + COUNTER_CAPACITY * size_of::<CounterSlot>()
}

const _: () = assert!(COUNTER_CAPACITY.is_power_of_two());
const _: () = assert!(COUNTER_KEY_MAX > 0 && COUNTER_KEY_MAX <= u16::MAX as usize);
const _: () = assert!(size_of::<CounterStateHeader>() == 64);
// 32 bytes of fixed fields, then the key, padded to the 64-byte slot alignment.
const _: () = assert!(size_of::<CounterSlot>() == (32 + COUNTER_KEY_MAX).next_multiple_of(64));

//! A fleet-shared byte store: one index over one ring of bytes, in shared
//! memory, read by every process without copying.
//!
//! [`Arena::put`] writes a record's bytes into the ring at the write cursor
//! and publishes them under a 128-bit [`Key`] in the index; [`Arena::get`]
//! finds the key and hands back an [`Entry`] that borrows the bytes where
//! they are. There is no expiry: the ring is the budget, and what the cursor
//! reaches is gone. An entry a reader is holding is *pinned* and the cursor
//! skips it, so a body being served is never torn.
//!
//! What is stored is up to the caller: a record is an identity body, an
//! optional encoded body with an opaque tag, and two validators. Nothing here
//! knows about files, MIME types or HTTP.
//!
//! Readers take no lock. A writer holds the region's process lock (`flock`,
//! which the kernel releases when a process dies) for the duration of a put,
//! so a crashed writer leaves at most one unpublished record behind.
//! Reader pins are per-owner counters protected by a lifetime kernel hold.
//! Allocation reclaims only owners whose hold can be taken, including after
//! SIGKILL. Ordinary reads and drops use atomics, not kernel calls.

use std::cell::UnsafeCell;
use std::fmt;
#[cfg(unix)]
use std::mem::ManuallyDrop;
use std::mem::size_of;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

use orbit_core::Fleet;
#[cfg(unix)]
mod reader;
#[cfg(unix)]
use orbit_core::shm::{
    LaneHold, ShmRegion, ShmRegionLock, ring_segment_name, try_hold_lane,
};
#[cfg(unix)]
pub use reader::{ArenaReader, ReaderEntry};

/// Reserved Orbit SHM kind for the arena segment.
pub const ARENA_KIND: u8 = 193;
/// Index slots: how many records the arena can name at once. Power of two.
///
/// Compile-time geometry: `ORBIT_ARENA_SLOTS` in the application's
/// `.cargo/config.toml` overrides the default. Peers built with a different
/// value are refused when they open the segment.
pub const ARENA_SLOTS: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_ARENA_SLOTS"),
    4_096,
);
/// Bytes in the ring. `ORBIT_ARENA_BYTES` overrides the default of 64 MiB.
pub const ARENA_BYTES: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_ARENA_BYTES"),
    64 * 1024 * 1024,
);
/// A record larger than this is refused whole: it would pin the cursor to
/// itself and evict everything else on every wrap.
pub const ARENA_RECORD_MAX: usize = ARENA_BYTES / 4;

const SLOT_EMPTY: u8 = 0;
const SLOT_LIVE: u8 = 1;
/// Evicted or reset while a reader held it: unreachable to lookups, its
/// bytes untouched until the last pin drops.
const SLOT_RETIRED: u8 = 2;
// Addressed records have no caller key and cannot collide with keyed records.
const SLOT_ADDRESSED: u8 = 3;

const MAGIC: u32 = 0x41_52_45_4e; // "AREN"
const VERSION: u16 = 2;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    /// The record would not fit the ring's record limit; nothing was written.
    TooLarge {
        len: usize,
        max: usize,
    },
    /// Every byte the record needs is under a reader's pin; nothing was
    /// written. Transient: pins are held for the length of a response.
    Pinned,
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::TooLarge { len, max } => {
                write!(
                    formatter,
                    "record of {len} bytes exceeds the arena record limit of {max}"
                )
            }
            Self::Pinned => formatter
                .write_str("the arena has no unpinned room for the record"),
            Self::Io(error) => write!(formatter, "arena I/O: {error}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// What a record is filed under. The arena never hashes: the caller brings a
/// 128-bit digest of whatever the real key is, so collisions are the caller's
/// digest's problem and not a weak hash of this crate's choosing.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Key(u128);

/// A particular immutable record, not a reusable key or a byte offset.
/// A reused index slot has a different generation and is an honest miss.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Address {
    pub slot: u32,
    pub generation: u64,
}

impl Key {
    pub const fn new(digest: u128) -> Self {
        Self(digest)
    }

    pub const fn from_bytes(digest: [u8; 16]) -> Self {
        Self(u128::from_le_bytes(digest))
    }

    pub const fn get(self) -> u128 {
        self.0
    }

    fn lo(self) -> u64 {
        self.0 as u64
    }

    fn hi(self) -> u64 {
        (self.0 >> 64) as u64
    }
}

/// What goes in: the bytes and what the caller wants back beside them.
#[derive(Clone, Copy, Debug)]
pub struct Record<'a> {
    /// The body as stored, served when no encoding fits.
    pub body: &'a [u8],
    /// An encoded form of the same body, with the caller's tag for it.
    pub encoded: Option<(&'a [u8], u8)>,
    /// A validator the caller reads back — a modification time, say.
    pub stamp: u64,
    /// A second one — a length, a checksum.
    pub extra: u64,
}

impl<'a> Record<'a> {
    pub const fn body(body: &'a [u8]) -> Self {
        Self { body, encoded: None, stamp: 0, extra: 0 }
    }

    pub const fn encoded(
        mut self,
        encoded: &'a [u8],
        tag: u8,
    ) -> Self {
        self.encoded = Some((encoded, tag));
        self
    }

    pub const fn stamp(
        mut self,
        stamp: u64,
    ) -> Self {
        self.stamp = stamp;
        self
    }

    pub const fn extra(
        mut self,
        extra: u64,
    ) -> Self {
        self.extra = extra;
        self
    }

    fn total(&self) -> usize {
        self.body.len() + self.encoded.map_or(0, |(bytes, _)| bytes.len())
    }
}

#[repr(C, align(64))]
struct Header {
    magic: u32,
    version: u16,
    header_size: u16,
    slots: u32,
    slot_size: u32,
    bytes: u64,
    /// Monotonic write position; `cursor % bytes` is where the next record
    /// starts. Advanced only under the write lock.
    cursor: AtomicU64,
    /// Bumped by every reset, so a reader can tell "emptied since" apart
    /// from "never filled".
    generation: AtomicU32,
    readers: u32,
    /// Non-repeating record generation, also ordering index eviction.
    record_generation: AtomicU64,
    _reserved: [u8; 16],
}

impl Header {
    fn new(readers: u16) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            header_size: size_of::<Self>() as u16,
            slots: ARENA_SLOTS as u32,
            slot_size: size_of::<Slot>() as u32,
            bytes: ARENA_BYTES as u64,
            cursor: AtomicU64::new(0),
            generation: AtomicU32::new(1),
            readers: u32::from(readers),
            record_generation: AtomicU64::new(0),
            _reserved: [0; 16],
        }
    }

    fn compatible(
        &self,
        readers: u16,
    ) -> bool {
        self.magic == MAGIC
            && self.version == VERSION
            && usize::from(self.header_size) == size_of::<Self>()
            && self.slots as usize == ARENA_SLOTS
            && self.slot_size as usize == size_of::<Slot>()
            && self.bytes as usize == ARENA_BYTES
            && self.readers == u32::from(readers)
    }
}

/// One index slot: two cache lines, every field atomic, never borrowed `&mut`.
#[repr(C, align(64))]
struct Slot {
    state: AtomicU8,
    encoding: AtomicU8,
    _reserved: [u8; 2],
    _reserved_pin: u32,
    /// Bumped on every install; an [`Entry`] carries the one it pinned.
    generation: AtomicU64,
    key_lo: AtomicU64,
    key_hi: AtomicU64,
    body_offset: AtomicU64,
    body_len: AtomicU64,
    encoded_offset: AtomicU64,
    encoded_len: AtomicU64,
    stamp: AtomicU64,
    extra: AtomicU64,
    _padding: [u8; 48],
}

impl Slot {
    fn holds(
        &self,
        key: Key,
    ) -> bool {
        self.key_lo.load(Ordering::Relaxed) == key.lo()
            && self.key_hi.load(Ordering::Relaxed) == key.hi()
    }

    /// The byte range this slot's record occupies, both spans together;
    /// spans are allocated contiguously, body first.
    fn span(&self) -> (u64, u64) {
        let start = self.body_offset.load(Ordering::Relaxed);
        let end = start
            + self.body_len.load(Ordering::Relaxed)
            + self.encoded_len.load(Ordering::Relaxed);
        (start, end)
    }
}

const _: () = assert!(size_of::<Slot>() == 128);
const _: () = assert!(ARENA_SLOTS.is_power_of_two());
const _: () = assert!(ARENA_SLOTS <= u32::MAX as usize);
const _: () = assert!(ARENA_RECORD_MAX > 0);

const fn reader_stride() -> usize {
    // One cache-line owner marker, followed by its per-slot pin counters.
    64 + ARENA_SLOTS * size_of::<AtomicU32>()
}

const fn ring_offset(readers: usize) -> usize {
    size_of::<Header>()
        + ARENA_SLOTS * size_of::<Slot>()
        + readers * reader_stride()
}

const fn segment_size(readers: usize) -> usize {
    ring_offset(readers) + ARENA_BYTES
}

#[repr(C, align(64))]
#[derive(Clone)]
struct Block([u8; 64]);

enum Backing {
    #[cfg(unix)]
    Shm(ShmRegion),
    Memory(Box<[UnsafeCell<Block>]>),
}

struct Inner {
    backing: Backing,
    /// Serialises this process's writers; the cross-process lock is taken
    /// inside it, so threads never contend for a file lock they cannot share.
    write: Mutex<()>,
    reader: usize,
    pid: u32,
    claimed: bool,
    _fleet: Arc<Fleet>,
    #[cfg(unix)]
    name: Option<String>,
    #[cfg(unix)]
    _hold: Option<ManuallyDrop<LaneHold>>,
}

/// Held for a put: this process's writer mutex, and the region's process
/// lock when the arena is shared.
struct WriteGuard<'a> {
    _local: MutexGuard<'a, ()>,
    #[cfg(unix)]
    _shared: Option<ShmRegionLock>,
}

impl Inner {
    fn base(&self) -> *mut u8 {
        match &self.backing {
            #[cfg(unix)]
            Backing::Shm(region) => region.as_ptr(),
            Backing::Memory(bytes) => bytes.as_ptr().cast_mut().cast(),
        }
    }

    fn header(&self) -> &Header {
        // SAFETY: the segment starts with a Header written at creation and
        // validated at open; the mapping outlives `self`.
        unsafe { &*self.base().cast::<Header>() }
    }

    fn slots(&self) -> &[Slot] {
        // SAFETY: ARENA_SLOTS slots follow the header, zeroed at creation;
        // every field is atomic, so shared references are sound.
        unsafe {
            std::slice::from_raw_parts(
                self.base().add(size_of::<Header>()).cast::<Slot>(),
                ARENA_SLOTS,
            )
        }
    }

    fn ring(&self) -> *mut u8 {
        // SAFETY: the ring follows the slots inside the mapped segment.
        unsafe { self.base().add(ring_offset(self.header().readers as usize)) }
    }

    /// The bytes of one span. Only meaningful while the slot that names it is
    /// pinned, which is what [`Entry`] guarantees.
    fn bytes(
        &self,
        offset: u64,
        len: u64,
    ) -> &[u8] {
        // SAFETY: offsets come from a published slot and were bounds-checked
        // at allocation; a pinned span is never rewritten.
        unsafe {
            std::slice::from_raw_parts(
                self.ring().add(offset as usize),
                len as usize,
            )
        }
    }

    fn lock(&self) -> Result<WriteGuard<'_>> {
        if self.pid != std::process::id() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "an inherited arena handle cannot be used after fork",
            )));
        }
        let local =
            self.write.lock().unwrap_or_else(|error| error.into_inner());
        #[cfg(unix)]
        let shared = match &self.backing {
            Backing::Shm(region) => Some(region.lock_exclusive()?),
            Backing::Memory(_) => None,
        };
        Ok(WriteGuard {
            _local: local,
            #[cfg(unix)]
            _shared: shared,
        })
    }

    fn reader_active(
        &self,
        reader: usize,
    ) -> &AtomicU32 {
        // SAFETY: validated reader rows follow the index; all access is atomic.
        unsafe {
            &*self
                .base()
                .add(
                    size_of::<Header>()
                        + ARENA_SLOTS * size_of::<Slot>()
                        + reader * reader_stride(),
                )
                .cast()
        }
    }

    fn pin(
        &self,
        reader: usize,
        slot: usize,
    ) -> &AtomicU32 {
        // SAFETY: the row contains exactly ARENA_SLOTS atomic counters.
        unsafe {
            &*self
                .base()
                .add(
                    size_of::<Header>()
                        + ARENA_SLOTS * size_of::<Slot>()
                        + reader * reader_stride()
                        + 64
                        + slot * size_of::<AtomicU32>(),
                )
                .cast()
        }
    }

    fn pinned(
        &self,
        slot: usize,
    ) -> bool {
        (0..self.header().readers as usize).any(|reader| {
            self.reader_active(reader).load(Ordering::Acquire) != 0
                && self.pin(reader, slot).load(Ordering::SeqCst) != 0
        })
    }

    fn clear_reader(
        &self,
        reader: usize,
    ) -> u64 {
        let pins = (0..ARENA_SLOTS)
            .map(|slot| {
                u64::from(self.pin(reader, slot).swap(0, Ordering::SeqCst))
            })
            .sum();
        self.reader_active(reader).store(0, Ordering::Release);
        pins
    }

    // Called only under the writer lock. Taking the owner's kernel hold is
    // death proof and also excludes a replacement owner while its row clears.
    fn reclaim_locked(&self) -> Result<u64> {
        let mut reclaimed = 0;
        #[cfg(unix)]
        if let Some(name) = &self.name {
            for reader in 0..self.header().readers as usize {
                if reader == self.reader
                    || self.reader_active(reader).load(Ordering::Acquire) == 0
                {
                    continue;
                }
                if let Some(_hold) = try_hold_lane(name, reader)? {
                    reclaimed += self.clear_reader(reader);
                }
            }
        }
        Ok(reclaimed)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Entries own Inner too: the last handle cannot leave live pins.
        if self.claimed && self.pid == std::process::id() {
            self.reader_active(self.reader).store(0, Ordering::Release);
            #[cfg(unix)]
            if let Some(hold) = &mut self._hold {
                // SAFETY: this process releases its hold exactly once. A fork
                // child must never unlock the parent's shared file description;
                // its inherited descriptor closes only at process exit/exec.
                unsafe {
                    ManuallyDrop::drop(hold);
                }
            }
        }
    }
}

// Share one mapping and reader claim between all consumers in a process.
// Inherited handles are not usable in a fork child. Private fleets are
// distinguished by their shared inner state, not the caller's Arc wrapper.
static OPEN: OnceLock<Mutex<Vec<Weak<Inner>>>> = OnceLock::new();

/// The fleet's arena. Cheap to clone.
#[derive(Clone)]
pub struct Arena {
    inner: Arc<Inner>,
}

impl Arena {
    /// Open the fleet's segment, creating and zeroing it if this process is
    /// first. A fleet without shared memory gets a private in-process arena
    /// of the same geometry.
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        #[cfg(unix)]
        let name =
            fleet.is_shm().then(|| ring_segment_name(fleet.name(), ARENA_KIND));
        let mut open = OPEN
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(inner) =
            open.iter().filter_map(Weak::upgrade).find(|inner| {
                if inner.pid != std::process::id() {
                    return false;
                }
                if !fleet.is_shm() {
                    return inner._fleet.ptr_eq(&fleet);
                }
                #[cfg(unix)]
                {
                    inner.name == name
                }
                #[cfg(not(unix))]
                {
                    false
                }
            })
        {
            if inner.header().readers != u32::from(fleet.fleet_capacity()) {
                return Err(Error::Io(std::io::Error::other(
                    "incompatible arena reader capacity",
                )));
            }
            return Ok(Self { inner });
        }
        open.retain(|inner| inner.strong_count() != 0);
        let readers = fleet.fleet_capacity();
        let backing = if fleet.is_shm() {
            #[cfg(unix)]
            {
                Backing::Shm(open_or_create(
                    name.as_ref().expect("SHM name"),
                    readers,
                )?)
            }
            #[cfg(not(unix))]
            unreachable!("non-Unix fleets cannot use POSIX SHM")
        } else {
            let mut bytes =
                std::iter::repeat_with(|| UnsafeCell::new(Block([0; 64])))
                    .take(segment_size(readers as usize).div_ceil(64))
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
            // SAFETY: Block provides the header's actual 64-byte alignment.
            unsafe {
                std::ptr::write(
                    bytes.as_mut_ptr().cast::<Header>(),
                    Header::new(readers),
                )
            };
            Backing::Memory(bytes)
        };
        let mut inner = Inner {
            backing,
            write: Mutex::new(()),
            reader: 0,
            pid: std::process::id(),
            claimed: false,
            _fleet: fleet,
            #[cfg(unix)]
            name,
            #[cfg(unix)]
            _hold: None,
        };
        #[cfg(unix)]
        if let Some(name) = &inner.name {
            let _guard = inner.lock()?;
            let mut claim = None;
            for reader in 0..readers as usize {
                if let Some(hold) = try_hold_lane(name, reader)? {
                    inner.clear_reader(reader);
                    inner.reader_active(reader).store(1, Ordering::Release);
                    claim = Some((reader, hold));
                    break;
                }
            }
            drop(_guard);
            let (reader, hold) = claim.ok_or_else(|| {
                Error::Io(std::io::Error::other(
                    "arena reader capacity exhausted",
                ))
            })?;
            inner.reader = reader;
            inner._hold = Some(ManuallyDrop::new(hold));
        }
        inner.reader_active(inner.reader).store(1, Ordering::Release);
        inner.claimed = true;
        let inner = Arc::new(inner);
        open.push(Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    /// Borrow the record filed under `key`, pinning its bytes for as long as
    /// the [`Entry`] lives. `None` is a miss: never filed, evicted, or reset.
    pub fn get(
        &self,
        key: Key,
    ) -> Option<Entry> {
        if self.inner.pid != std::process::id() {
            return None;
        }
        let slots = self.inner.slots();
        let mask = ARENA_SLOTS - 1;
        let start = key.lo() as usize & mask;
        for probe in 0..ARENA_SLOTS {
            let index = (start + probe) & mask;
            let slot = &slots[index];
            match slot.state.load(Ordering::Acquire) {
                SLOT_EMPTY => return None,
                SLOT_LIVE if slot.holds(key) => {
                    // Pin, then look again: a writer that evicts marks the
                    // slot before it checks for pins, so one of us sees the
                    // other and a span is never both rewritten and read.
                    let generation = slot.generation.load(Ordering::Acquire);
                    return self.pin_entry(index, generation, Some(key));
                }
                _ => {}
            }
        }
        None
    }

    /// Borrow an exact addressed record. Reuse, eviction or reset is a miss.
    pub fn get_address(
        &self,
        address: Address,
    ) -> Option<Entry> {
        self.pin_entry(address.slot as usize, address.generation, None)
    }

    fn pin_entry(
        &self,
        index: usize,
        generation: u64,
        key: Option<Key>,
    ) -> Option<Entry> {
        if self.inner.pid != std::process::id() || index >= ARENA_SLOTS {
            return None;
        }
        let slot = &self.inner.slots()[index];
        let pins = self.inner.pin(self.inner.reader, index);
        pins.try_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
            count.checked_add(1)
        })
        .ok()?;
        let state = slot.state.load(Ordering::SeqCst);
        if slot.generation.load(Ordering::SeqCst) == generation
            && match key {
                Some(key) => state == SLOT_LIVE && slot.holds(key),
                None => state == SLOT_ADDRESSED,
            }
        {
            return Some(Entry::pinned(
                Arc::clone(&self.inner),
                index,
                generation,
            ));
        }
        pins.fetch_sub(1, Ordering::SeqCst);
        None
    }

    /// File `record` under `key`, replacing what was there. Bytes go to the
    /// ring at the cursor; whatever they land on is evicted, unless a reader
    /// holds it, in which case the cursor moves past it.
    pub fn put(
        &self,
        key: Key,
        record: Record<'_>,
    ) -> Result<()> {
        self.write_record(Some(key), record).map(|_| ())
    }

    /// Store an immutable body without assigning a caller key. The address
    /// can be carried in a mutation; later writes cannot alias this generation.
    pub fn store(
        &self,
        record: Record<'_>,
    ) -> Result<Address> {
        self.write_record(None, record)
    }

    fn write_record(
        &self,
        key: Option<Key>,
        record: Record<'_>,
    ) -> Result<Address> {
        let total = record.total();
        if total > ARENA_RECORD_MAX {
            return Err(Error::TooLarge { len: total, max: ARENA_RECORD_MAX });
        }
        let _guard = self.inner.lock()?;
        let slots = self.inner.slots();

        // Whatever this key named before is gone once the new record is
        // published; retiring it first keeps a lookup from finding two.
        if let Some(index) = key.and_then(|key| self.find_locked(key)) {
            evict(&slots[index]);
        }

        let offset = match self.allocate_locked(total as u64) {
            Ok(offset) => offset,
            Err(Error::Pinned) => {
                self.inner.reclaim_locked()?;
                self.allocate_locked(total as u64)?
            }
            Err(error) => return Err(error),
        };
        let index = match self
            .free_slot_locked(key.unwrap_or(Key::new(offset as u128)))
        {
            Ok(index) => index,
            Err(Error::Pinned) => {
                self.inner.reclaim_locked()?;
                self.free_slot_locked(key.unwrap_or(Key::new(offset as u128)))?
            }
            Err(error) => return Err(error),
        };
        let slot = &slots[index];
        let generation = self
            .inner
            .header()
            .record_generation
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                Error::Io(std::io::Error::other(
                    "arena record generation exhausted",
                ))
            })?
            + 1;
        // SAFETY: the span is inside the ring, and every slot that named any
        // byte of it is now EMPTY or RETIRED-and-pinned-elsewhere (skipped),
        // so no reader borrows it.
        unsafe {
            let destination = self.inner.ring().add(offset as usize);
            std::ptr::copy_nonoverlapping(
                record.body.as_ptr(),
                destination,
                record.body.len(),
            );
            if let Some((encoded, _)) = record.encoded {
                std::ptr::copy_nonoverlapping(
                    encoded.as_ptr(),
                    destination.add(record.body.len()),
                    encoded.len(),
                );
            }
        }

        slot.key_lo.store(key.map_or(0, Key::lo), Ordering::Relaxed);
        slot.key_hi.store(key.map_or(0, Key::hi), Ordering::Relaxed);
        slot.body_offset.store(offset, Ordering::Relaxed);
        slot.body_len.store(record.body.len() as u64, Ordering::Relaxed);
        let (encoded_len, tag) =
            record.encoded.map_or((0, 0), |(bytes, tag)| (bytes.len(), tag));
        slot.encoded_offset
            .store(offset + record.body.len() as u64, Ordering::Relaxed);
        slot.encoded_len.store(encoded_len as u64, Ordering::Relaxed);
        slot.encoding.store(tag, Ordering::Relaxed);
        slot.stamp.store(record.stamp, Ordering::Relaxed);
        slot.extra.store(record.extra, Ordering::Relaxed);
        slot.generation.store(generation, Ordering::Relaxed);
        slot.state.store(
            if key.is_some() { SLOT_LIVE } else { SLOT_ADDRESSED },
            Ordering::SeqCst,
        );
        Ok(Address { slot: index as u32, generation })
    }

    /// Withdraw only this exact record. Existing pins keep its bytes valid.
    pub fn forget_address(
        &self,
        address: Address,
    ) -> Result<bool> {
        let _guard = self.inner.lock()?;
        let Some(slot) = self.inner.slots().get(address.slot as usize) else {
            return Ok(false);
        };
        if slot.state.load(Ordering::Acquire) != SLOT_ADDRESSED
            || slot.generation.load(Ordering::Relaxed) != address.generation
        {
            return Ok(false);
        }
        evict(slot);
        Ok(true)
    }

    /// Reclaim pins only from rows whose kernel hold can be taken. Never
    /// uses a PID probe, heartbeat, timeout or live owner's counter reset.
    pub fn reclaim_dead_pins(&self) -> Result<u64> {
        let _guard = self.inner.lock()?;
        self.inner.reclaim_locked()
    }

    /// Drop the record under `key`. Its bytes stay in the ring until the
    /// cursor reaches them; a reader holding them keeps them until it is done.
    pub fn forget(
        &self,
        key: Key,
    ) -> Result<bool> {
        let _guard = self.inner.lock()?;
        let Some(index) = self.find_locked(key) else {
            return Ok(false);
        };
        evict(&self.inner.slots()[index]);
        Ok(true)
    }

    /// Empty the index: every lookup misses from here on. Readers holding an
    /// entry keep it. Returns how many records were live.
    pub fn reset(&self) -> Result<usize> {
        let _guard = self.inner.lock()?;
        let mut live = 0;
        for slot in self.inner.slots() {
            if matches!(
                slot.state.load(Ordering::Relaxed),
                SLOT_LIVE | SLOT_ADDRESSED
            ) {
                live += 1;
            }
            if slot.state.load(Ordering::Relaxed) != SLOT_EMPTY {
                evict(slot);
            }
        }
        self.inner.header().generation.fetch_add(1, Ordering::AcqRel);
        Ok(live)
    }

    /// Live records.
    pub fn len(&self) -> usize {
        self.inner
            .slots()
            .iter()
            .filter(|slot| {
                matches!(
                    slot.state.load(Ordering::Relaxed),
                    SLOT_LIVE | SLOT_ADDRESSED
                )
            })
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bumped by every [`Arena::reset`].
    pub fn generation(&self) -> u32 {
        self.inner.header().generation.load(Ordering::Acquire)
    }

    /// Remove the segment's name. Existing mappings stay valid until each
    /// process drops its arena. For fleet teardown and tests, not for reuse.
    pub fn unlink(&self) -> Result<()> {
        match &self.inner.backing {
            #[cfg(unix)]
            Backing::Shm(region) => region.unlink().map_err(Error::Io),
            Backing::Memory(_) => Ok(()),
        }
    }

    fn find_locked(
        &self,
        key: Key,
    ) -> Option<usize> {
        let slots = self.inner.slots();
        let mask = ARENA_SLOTS - 1;
        let start = key.lo() as usize & mask;
        for probe in 0..ARENA_SLOTS {
            let index = (start + probe) & mask;
            match slots[index].state.load(Ordering::Acquire) {
                SLOT_EMPTY => return None,
                SLOT_LIVE if slots[index].holds(key) => return Some(index),
                _ => {}
            }
        }
        None
    }

    /// The first slot in `key`'s probe chain that can take a record: empty,
    /// or retired with no reader left on it.
    fn free_slot_locked(
        &self,
        key: Key,
    ) -> Result<usize> {
        let slots = self.inner.slots();
        let mask = ARENA_SLOTS - 1;
        let start = key.lo() as usize & mask;
        for probe in 0..ARENA_SLOTS {
            let index = (start + probe) & mask;
            let slot = &slots[index];
            match slot.state.load(Ordering::Acquire) {
                SLOT_EMPTY => return Ok(index),
                SLOT_RETIRED if !self.inner.pinned(index) => {
                    return Ok(index);
                }
                _ => {}
            }
        }
        // Index pressure can precede byte pressure (including empty bodies).
        // Reuse the oldest unpinned record, not a recently published value
        // just because its offset hashes to the start of this probe chain.
        for _ in 0..ARENA_SLOTS {
            let Some(index) = (0..ARENA_SLOTS)
                .filter(|&index| !self.inner.pinned(index))
                .min_by_key(|&index| {
                    slots[index].generation.load(Ordering::Relaxed)
                })
            else {
                return Err(Error::Pinned);
            };
            evict(&slots[index]);
            if !self.inner.pinned(index) {
                return Ok(index);
            }
        }
        Err(Error::Pinned)
    }

    /// Reserve `len` contiguous bytes at the cursor. Every record whose span
    /// meets the reservation is evicted; one a reader is pinning is skipped,
    /// and the reservation restarts past it.
    fn allocate_locked(
        &self,
        len: u64,
    ) -> Result<u64> {
        let header = self.inner.header();
        let bytes = ARENA_BYTES as u64;
        let slots = self.inner.slots();
        let mut cursor = header.cursor.load(Ordering::Relaxed);
        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > ARENA_SLOTS + 2 {
                return Err(Error::Pinned);
            }
            let mut position = cursor % bytes;
            if position + len > bytes {
                // No wrap-around records: a body is one slice.
                cursor += bytes - position;
                position = 0;
            }
            let end = position + len;
            let mut blocked_until = None;
            for (index, slot) in slots.iter().enumerate() {
                if slot.state.load(Ordering::Acquire) == SLOT_EMPTY {
                    continue;
                }
                let (span_start, span_end) = slot.span();
                if span_end <= position || span_start >= end {
                    continue;
                }
                if self.inner.pinned(index) {
                    blocked_until = Some(
                        blocked_until
                            .map_or(span_end, |b: u64| b.max(span_end)),
                    );
                    continue;
                }
                evict(slot);
                if self.inner.pinned(index) {
                    // A reader got in between: its bytes stay.
                    blocked_until = Some(
                        blocked_until
                            .map_or(span_end, |b: u64| b.max(span_end)),
                    );
                }
            }
            if let Some(past) = blocked_until {
                cursor += past - position;
                continue;
            }
            header.cursor.store(cursor + len, Ordering::Relaxed);
            return Ok(position);
        }
    }
}

/// Withdraw before checking owner pins. Keep a retired probe-chain marker;
/// publishing a new generation is safe only after all owner pins are zero.
fn evict(slot: &Slot) {
    if slot.state.load(Ordering::Relaxed) == SLOT_EMPTY {
        return;
    }
    slot.state.store(SLOT_RETIRED, Ordering::SeqCst);
}

#[cfg(unix)]
fn open_or_create(
    name: &str,
    readers: u16,
) -> Result<ShmRegion> {
    let (region, _initialization_lock) =
        ShmRegion::open_or_create_locked(name, segment_size(readers as usize))?;
    if region.created() {
        // SAFETY: a fresh segment of segment_size() bytes; header first, then
        // zeroed slots (SLOT_EMPTY is 0), then the ring, which needs nothing.
        unsafe {
            std::ptr::write(
                region.as_ptr().cast::<Header>(),
                Header::new(readers),
            );
            std::ptr::write_bytes(
                region.as_ptr().add(size_of::<Header>()),
                0,
                ARENA_SLOTS * size_of::<Slot>()
                    + readers as usize * reader_stride(),
            );
        }
    } else {
        // SAFETY: an existing segment large enough for a Header, validated
        // before anything else is read.
        let header = unsafe { &*region.as_ptr().cast::<Header>() };
        if !header.compatible(readers) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("SHM segment {name} has an incompatible arena layout"),
            )));
        }
    }
    Ok(region)
}

/// A record borrowed from the arena. The slot stays pinned, and its bytes
/// unwritten, until this is dropped.
pub struct Entry {
    inner: Arc<Inner>,
    slot: usize,
    generation: u64,
    body: (u64, u64),
    encoded: Option<(u64, u64, u8)>,
    stamp: u64,
    extra: u64,
}

impl Entry {
    fn pinned(
        inner: Arc<Inner>,
        index: usize,
        generation: u64,
    ) -> Self {
        let slot = &inner.slots()[index];
        let body = (
            slot.body_offset.load(Ordering::Relaxed),
            slot.body_len.load(Ordering::Relaxed),
        );
        let encoded_len = slot.encoded_len.load(Ordering::Relaxed);
        let encoded = (encoded_len > 0).then(|| {
            (
                slot.encoded_offset.load(Ordering::Relaxed),
                encoded_len,
                slot.encoding.load(Ordering::Relaxed),
            )
        });
        let stamp = slot.stamp.load(Ordering::Relaxed);
        let extra = slot.extra.load(Ordering::Relaxed);
        Self { inner, slot: index, generation, body, encoded, stamp, extra }
    }

    pub fn body(&self) -> &[u8] {
        assert_eq!(
            self.inner.pid,
            std::process::id(),
            "arena entries cannot be inherited across fork"
        );
        self.inner.bytes(self.body.0, self.body.1)
    }

    /// The encoded body and its tag, when one was stored.
    pub fn encoded(&self) -> Option<(&[u8], u8)> {
        assert_eq!(
            self.inner.pid,
            std::process::id(),
            "arena entries cannot be inherited across fork"
        );
        self.encoded
            .map(|(offset, len, tag)| (self.inner.bytes(offset, len), tag))
    }

    pub fn stamp(&self) -> u64 {
        self.stamp
    }

    pub fn extra(&self) -> u64 {
        self.extra
    }

    /// The slot's generation when it was pinned; a later put or reset of the
    /// same key gives a different one.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The identity body as an owner: something a `bytes::Bytes` can be built
    /// over without copying, holding the pin for as long as it lives.
    pub fn into_body(self) -> View {
        View { entry: self, encoded: false }
    }

    /// The encoded body as an owner, or the entry back if there is none.
    pub fn into_encoded(self) -> std::result::Result<View, Self> {
        if self.encoded.is_some() {
            Ok(View { entry: self, encoded: true })
        } else {
            Err(self)
        }
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        if self.inner.pid == std::process::id() {
            self.inner
                .pin(self.inner.reader, self.slot)
                .fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl fmt::Debug for Entry {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter
            .debug_struct("Entry")
            .field("slot", &self.slot)
            .field("generation", &self.generation)
            .field("body_len", &self.body.1)
            .field("encoded", &self.encoded.map(|(_, len, tag)| (len, tag)))
            .finish()
    }
}

/// One body of a pinned entry, as a slice owner.
pub struct View {
    entry: Entry,
    encoded: bool,
}

impl View {
    pub fn entry(&self) -> &Entry {
        &self.entry
    }
}

impl AsRef<[u8]> for View {
    fn as_ref(&self) -> &[u8] {
        if self.encoded {
            self.entry.encoded().map_or(&[], |(bytes, _)| bytes)
        } else {
            self.entry.body()
        }
    }
}

// SAFETY: the mapping is shared memory read through atomics and pinned
// spans; nothing in an entry is thread-affine.
unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

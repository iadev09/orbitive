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

use std::fmt;
use std::mem::size_of;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ShmRegionLock, ring_segment_name};

/// Reserved Orbit SHM kind for the arena segment.
pub const ARENA_KIND: u8 = 242;
/// Index slots: how many records the arena can name at once. Power of two.
///
/// Compile-time geometry: `ORBIT_ARENA_SLOTS` in the application's
/// `.cargo/config.toml` overrides the default. Peers built with a different
/// value are refused when they open the segment.
pub const ARENA_SLOTS: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_ARENA_SLOTS"), 4_096);
/// Bytes in the ring. `ORBIT_ARENA_BYTES` overrides the default of 64 MiB.
pub const ARENA_BYTES: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_ARENA_BYTES"), 64 * 1024 * 1024);
/// A record larger than this is refused whole: it would pin the cursor to
/// itself and evict everything else on every wrap.
pub const ARENA_RECORD_MAX: usize = ARENA_BYTES / 4;

const SLOT_EMPTY: u8 = 0;
const SLOT_LIVE: u8 = 1;
/// Evicted or reset while a reader held it: unreachable to lookups, its
/// bytes untouched until the last pin drops.
const SLOT_RETIRED: u8 = 2;

const MAGIC: u32 = 0x41_52_45_4E; // "AREN"
const VERSION: u16 = 1;

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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { len, max } => {
                write!(
                    formatter,
                    "record of {len} bytes exceeds the arena record limit of {max}"
                )
            }
            Self::Pinned => formatter.write_str("the arena has no unpinned room for the record"),
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
        Self {
            body,
            encoded: None,
            stamp: 0,
            extra: 0,
        }
    }

    pub const fn encoded(mut self, encoded: &'a [u8], tag: u8) -> Self {
        self.encoded = Some((encoded, tag));
        self
    }

    pub const fn stamp(mut self, stamp: u64) -> Self {
        self.stamp = stamp;
        self
    }

    pub const fn extra(mut self, extra: u64) -> Self {
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
    _reserved: [u8; 28],
}

impl Header {
    fn new() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            header_size: size_of::<Self>() as u16,
            slots: ARENA_SLOTS as u32,
            slot_size: size_of::<Slot>() as u32,
            bytes: ARENA_BYTES as u64,
            cursor: AtomicU64::new(0),
            generation: AtomicU32::new(1),
            _reserved: [0; 28],
        }
    }

    fn compatible(&self) -> bool {
        self.magic == MAGIC
            && self.version == VERSION
            && usize::from(self.header_size) == size_of::<Self>()
            && self.slots as usize == ARENA_SLOTS
            && self.slot_size as usize == size_of::<Slot>()
            && self.bytes as usize == ARENA_BYTES
    }
}

/// One index slot: two cache lines, every field atomic, never borrowed `&mut`.
#[repr(C, align(64))]
struct Slot {
    state: AtomicU8,
    encoding: AtomicU8,
    _reserved: [u8; 2],
    /// Readers currently borrowing the bytes. A writer never overwrites a
    /// span whose slot is pinned.
    pins: AtomicU32,
    /// Bumped on every install; an [`Entry`] carries the one it pinned.
    generation: AtomicU32,
    _reserved2: u32,
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
    fn holds(&self, key: Key) -> bool {
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
const _: () = assert!(ARENA_RECORD_MAX > 0);

const fn segment_size() -> usize {
    size_of::<Header>() + ARENA_SLOTS * size_of::<Slot>() + ARENA_BYTES
}

enum Backing {
    #[cfg(unix)]
    Shm(ShmRegion),
    Memory(Box<[u8]>),
}

struct Inner {
    backing: Backing,
    /// Serialises this process's writers; the cross-process lock is taken
    /// inside it, so threads never contend for a file lock they cannot share.
    write: Mutex<()>,
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
            Backing::Memory(bytes) => bytes.as_ptr().cast_mut(),
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
        unsafe {
            self.base()
                .add(size_of::<Header>() + ARENA_SLOTS * size_of::<Slot>())
        }
    }

    /// The bytes of one span. Only meaningful while the slot that names it is
    /// pinned, which is what [`Entry`] guarantees.
    fn bytes(&self, offset: u64, len: u64) -> &[u8] {
        // SAFETY: offsets come from a published slot and were bounds-checked
        // at allocation; a pinned span is never rewritten.
        unsafe { std::slice::from_raw_parts(self.ring().add(offset as usize), len as usize) }
    }

    fn lock(&self) -> Result<WriteGuard<'_>> {
        let local = self.write.lock().unwrap_or_else(|error| error.into_inner());
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
}

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
        let backing = if fleet.is_shm() {
            #[cfg(unix)]
            {
                Backing::Shm(open_or_create(&ring_segment_name(
                    fleet.name(),
                    ARENA_KIND,
                ))?)
            }
            #[cfg(not(unix))]
            unreachable!("non-Unix fleets cannot use POSIX SHM")
        } else {
            let mut bytes = vec![0_u8; segment_size() + 64].into_boxed_slice();
            // Align the header on its cache line inside the allocation.
            let misalignment = bytes.as_ptr() as usize % 64;
            let start = if misalignment == 0 {
                0
            } else {
                64 - misalignment
            };
            // SAFETY: start + size_of::<Header>() lies inside the allocation.
            unsafe {
                std::ptr::write(
                    bytes.as_mut_ptr().add(start).cast::<Header>(),
                    Header::new(),
                )
            };
            Backing::Memory(if start == 0 {
                bytes
            } else {
                shift(bytes, start)
            })
        };
        Ok(Self {
            inner: Arc::new(Inner {
                backing,
                write: Mutex::new(()),
            }),
        })
    }

    /// Borrow the record filed under `key`, pinning its bytes for as long as
    /// the [`Entry`] lives. `None` is a miss: never filed, evicted, or reset.
    pub fn get(&self, key: Key) -> Option<Entry> {
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
                    slot.pins.fetch_add(1, Ordering::SeqCst);
                    let generation = slot.generation.load(Ordering::Relaxed);
                    if slot.state.load(Ordering::SeqCst) == SLOT_LIVE && slot.holds(key) {
                        return Some(Entry::pinned(Arc::clone(&self.inner), index, generation));
                    }
                    slot.pins.fetch_sub(1, Ordering::SeqCst);
                    return None;
                }
                _ => {}
            }
        }
        None
    }

    /// File `record` under `key`, replacing what was there. Bytes go to the
    /// ring at the cursor; whatever they land on is evicted, unless a reader
    /// holds it, in which case the cursor moves past it.
    pub fn put(&self, key: Key, record: Record<'_>) -> Result<()> {
        let total = record.total();
        if total > ARENA_RECORD_MAX {
            return Err(Error::TooLarge {
                len: total,
                max: ARENA_RECORD_MAX,
            });
        }
        let _guard = self.inner.lock()?;
        let slots = self.inner.slots();

        // Whatever this key named before is gone once the new record is
        // published; retiring it first keeps a lookup from finding two.
        if let Some(index) = self.find_locked(key) {
            evict(&slots[index]);
        }

        let offset = self.allocate_locked(total as u64)?;
        // SAFETY: the span is inside the ring, and every slot that named any
        // byte of it is now EMPTY or RETIRED-and-pinned-elsewhere (skipped),
        // so no reader borrows it.
        unsafe {
            let destination = self.inner.ring().add(offset as usize);
            std::ptr::copy_nonoverlapping(record.body.as_ptr(), destination, record.body.len());
            if let Some((encoded, _)) = record.encoded {
                std::ptr::copy_nonoverlapping(
                    encoded.as_ptr(),
                    destination.add(record.body.len()),
                    encoded.len(),
                );
            }
        }

        let index = self.free_slot_locked(key)?;
        let slot = &slots[index];
        let generation = slot
            .generation
            .load(Ordering::Relaxed)
            .wrapping_add(1)
            .max(1);
        slot.key_lo.store(key.lo(), Ordering::Relaxed);
        slot.key_hi.store(key.hi(), Ordering::Relaxed);
        slot.body_offset.store(offset, Ordering::Relaxed);
        slot.body_len
            .store(record.body.len() as u64, Ordering::Relaxed);
        let (encoded_len, tag) = record
            .encoded
            .map_or((0, 0), |(bytes, tag)| (bytes.len(), tag));
        slot.encoded_offset
            .store(offset + record.body.len() as u64, Ordering::Relaxed);
        slot.encoded_len
            .store(encoded_len as u64, Ordering::Relaxed);
        slot.encoding.store(tag, Ordering::Relaxed);
        slot.stamp.store(record.stamp, Ordering::Relaxed);
        slot.extra.store(record.extra, Ordering::Relaxed);
        slot.pins.store(0, Ordering::Relaxed);
        slot.generation.store(generation, Ordering::Relaxed);
        slot.state.store(SLOT_LIVE, Ordering::Release);
        Ok(())
    }

    /// Drop the record under `key`. Its bytes stay in the ring until the
    /// cursor reaches them; a reader holding them keeps them until it is done.
    pub fn forget(&self, key: Key) -> Result<bool> {
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
            if slot.state.load(Ordering::Relaxed) == SLOT_LIVE {
                live += 1;
            }
            if slot.state.load(Ordering::Relaxed) != SLOT_EMPTY {
                evict(slot);
            }
        }
        self.inner
            .header()
            .generation
            .fetch_add(1, Ordering::AcqRel);
        Ok(live)
    }

    /// Live records.
    pub fn len(&self) -> usize {
        self.inner
            .slots()
            .iter()
            .filter(|slot| slot.state.load(Ordering::Relaxed) == SLOT_LIVE)
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

    fn find_locked(&self, key: Key) -> Option<usize> {
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
    fn free_slot_locked(&self, key: Key) -> Result<usize> {
        let slots = self.inner.slots();
        let mask = ARENA_SLOTS - 1;
        let start = key.lo() as usize & mask;
        for probe in 0..ARENA_SLOTS {
            let index = (start + probe) & mask;
            let slot = &slots[index];
            match slot.state.load(Ordering::Acquire) {
                SLOT_EMPTY => return Ok(index),
                SLOT_RETIRED if slot.pins.load(Ordering::SeqCst) == 0 => return Ok(index),
                _ => {}
            }
        }
        Err(Error::Pinned)
    }

    /// Reserve `len` contiguous bytes at the cursor. Every record whose span
    /// meets the reservation is evicted; one a reader is pinning is skipped,
    /// and the reservation restarts past it.
    fn allocate_locked(&self, len: u64) -> Result<u64> {
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
            for slot in slots {
                if slot.state.load(Ordering::Acquire) == SLOT_EMPTY {
                    continue;
                }
                let (span_start, span_end) = slot.span();
                if span_end <= position || span_start >= end {
                    continue;
                }
                if slot.pins.load(Ordering::SeqCst) > 0 {
                    blocked_until = Some(blocked_until.map_or(span_end, |b: u64| b.max(span_end)));
                    continue;
                }
                evict(slot);
                if slot.pins.load(Ordering::SeqCst) > 0 {
                    // A reader got in between: its bytes stay.
                    blocked_until = Some(blocked_until.map_or(span_end, |b: u64| b.max(span_end)));
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

/// Take a slot out of the index. Marks it EMPTY first and RETIRED after if a
/// reader is on it — the order that lets [`Arena::get`] and this agree.
fn evict(slot: &Slot) {
    if slot.state.load(Ordering::Relaxed) == SLOT_EMPTY {
        return;
    }
    slot.state.store(SLOT_EMPTY, Ordering::SeqCst);
    if slot.pins.load(Ordering::SeqCst) > 0 {
        slot.state.store(SLOT_RETIRED, Ordering::SeqCst);
    }
}

fn shift(bytes: Box<[u8]>, start: usize) -> Box<[u8]> {
    let mut vec = bytes.into_vec();
    vec.drain(..start);
    vec.into_boxed_slice()
}

#[cfg(unix)]
fn open_or_create(name: &str) -> Result<ShmRegion> {
    let (region, _initialization_lock) = ShmRegion::open_or_create_locked(name, segment_size())?;
    if region.created() {
        // SAFETY: a fresh segment of segment_size() bytes; header first, then
        // zeroed slots (SLOT_EMPTY is 0), then the ring, which needs nothing.
        unsafe {
            std::ptr::write(region.as_ptr().cast::<Header>(), Header::new());
            std::ptr::write_bytes(
                region.as_ptr().add(size_of::<Header>()),
                0,
                ARENA_SLOTS * size_of::<Slot>(),
            );
        }
    } else {
        // SAFETY: an existing segment large enough for a Header, validated
        // before anything else is read.
        let header = unsafe { &*region.as_ptr().cast::<Header>() };
        if !header.compatible() {
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
    generation: u32,
    body: (u64, u64),
    encoded: Option<(u64, u64, u8)>,
    stamp: u64,
    extra: u64,
}

impl Entry {
    fn pinned(inner: Arc<Inner>, index: usize, generation: u32) -> Self {
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
        Self {
            inner,
            slot: index,
            generation,
            body,
            encoded,
            stamp,
            extra,
        }
    }

    pub fn body(&self) -> &[u8] {
        self.inner.bytes(self.body.0, self.body.1)
    }

    /// The encoded body and its tag, when one was stored.
    pub fn encoded(&self) -> Option<(&[u8], u8)> {
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
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// The identity body as an owner: something a `bytes::Bytes` can be built
    /// over without copying, holding the pin for as long as it lives.
    pub fn into_body(self) -> View {
        View {
            entry: self,
            encoded: false,
        }
    }

    /// The encoded body as an owner, or the entry back if there is none.
    pub fn into_encoded(self) -> std::result::Result<View, Self> {
        if self.encoded.is_some() {
            Ok(View {
                entry: self,
                encoded: true,
            })
        } else {
            Err(self)
        }
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.inner.slots()[self.slot]
            .pins
            .fetch_sub(1, Ordering::SeqCst);
    }
}

impl fmt::Debug for Entry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use orbit_core::Fleet;

    use super::{ARENA_BYTES, ARENA_RECORD_MAX, Arena, Error, Key, Record};

    fn arena() -> Arena {
        Arena::new(Arc::new(Fleet::join("arena-test", 1).unwrap())).unwrap()
    }

    #[test]
    fn put_get_replace_forget() {
        let arena = arena();
        let key = Key::new(7);
        assert!(arena.get(key).is_none());

        arena
            .put(
                key,
                Record::body(b"hello").encoded(b"h", 1).stamp(42).extra(5),
            )
            .unwrap();
        let entry = arena.get(key).unwrap();
        assert_eq!(entry.body(), b"hello");
        assert_eq!(entry.encoded(), Some((&b"h"[..], 1)));
        assert_eq!((entry.stamp(), entry.extra()), (42, 5));
        drop(entry);

        arena.put(key, Record::body(b"world")).unwrap();
        let entry = arena.get(key).unwrap();
        assert_eq!(entry.body(), b"world");
        assert!(entry.encoded().is_none());
        assert_eq!(arena.len(), 1);
        drop(entry);

        assert!(arena.forget(key).unwrap());
        assert!(arena.get(key).is_none());
        assert!(!arena.forget(key).unwrap());
    }

    #[test]
    fn the_ring_evicts_what_the_cursor_reaches_and_skips_what_is_pinned() {
        let arena = arena();
        let chunk = vec![0xAB_u8; ARENA_RECORD_MAX];
        let records = ARENA_BYTES / ARENA_RECORD_MAX; // fills the ring exactly
        for n in 0..records {
            arena
                .put(Key::new(n as u128), Record::body(&chunk))
                .unwrap();
        }
        assert_eq!(arena.len(), records);

        // Pin the first record, then wrap: it must survive, the second must go.
        let pinned = arena.get(Key::new(0)).unwrap();
        arena.put(Key::new(1_000), Record::body(&chunk)).unwrap();
        assert_eq!(pinned.body().len(), ARENA_RECORD_MAX);
        assert!(
            arena.get(Key::new(0)).is_some(),
            "a pinned record is not evicted"
        );
        assert!(
            arena.get(Key::new(1)).is_none(),
            "the cursor moved past the pin"
        );
        drop(pinned);

        let too_big = vec![0_u8; ARENA_RECORD_MAX + 1];
        assert!(matches!(
            arena.put(Key::new(9), Record::body(&too_big)),
            Err(Error::TooLarge { .. })
        ));
    }

    #[test]
    fn reset_empties_the_index_but_not_a_held_entry() {
        let arena = arena();
        arena.put(Key::new(1), Record::body(b"one")).unwrap();
        arena.put(Key::new(2), Record::body(b"two")).unwrap();
        let held = arena.get(Key::new(1)).unwrap();
        let generation = arena.generation();

        assert_eq!(arena.reset().unwrap(), 2);
        assert_eq!(arena.generation(), generation + 1);
        assert!(arena.get(Key::new(1)).is_none());
        assert!(arena.get(Key::new(2)).is_none());
        assert_eq!(held.body(), b"one");
        drop(held);

        // The retired slot is reusable once the reader is gone.
        arena.put(Key::new(1), Record::body(b"again")).unwrap();
        assert_eq!(arena.get(Key::new(1)).unwrap().body(), b"again");
    }

    #[test]
    fn a_view_owns_the_pin() {
        let arena = arena();
        arena
            .put(Key::new(3), Record::body(b"body").encoded(b"enc", 2))
            .unwrap();
        let view = arena.get(Key::new(3)).unwrap().into_encoded().ok().unwrap();
        assert_eq!(view.as_ref(), b"enc");
        let view = arena.get(Key::new(3)).unwrap().into_body();
        assert_eq!(view.as_ref(), b"body");
    }
}

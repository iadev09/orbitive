//! Tag floors for the shared table: a clock and, per tag, the clock value of
//! its last invalidation, in shared memory beside the cache's arena.
//!
//! A tagged record stores the clock it read before it was written, and lives
//! while that clock is at or above each of its tags' floors. Floors live in
//! sets of [`WAYS`] exact entries; a full set widens its lowest floor to the
//! whole set rather than forget it, so a bound only invalidates more.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use orbit_core::Fleet;
use orbit_core::process::ProcessOwner;
#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};

use crate::{Error, Result};

/// Reserved Orbit SHM kind for the shared cache's tag floors.
pub const SHARED_TAG_KIND: u8 = 171;

/// Sets of tag floors in the shared table: one per four arena index slots
/// unless `ORBIT_CACHE_TAG_SETS` overrides it. A power of two. Peers built
/// with a different value are refused when they open the table.
pub const SHARED_TAG_SETS: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_CACHE_TAG_SETS"),
    if orbit_arena::ARENA_SLOTS >= 4 {
        orbit_arena::ARENA_SLOTS / 4
    } else {
        1
    },
);

/// Exact floors per set.
const WAYS: usize = 7;

const MAGIC: u32 = 0x4f_43_54_47; // "OCTG"
const VERSION: u16 = 1;

const _: () = assert!(SHARED_TAG_SETS.is_power_of_two());
const _: () = assert!(size_of::<Header>() == 64);
const _: () = assert!(size_of::<TagSet>() == 128);

const SEGMENT_SIZE: usize =
    size_of::<Header>() + SHARED_TAG_SETS * size_of::<TagSet>();

#[repr(C, align(64))]
struct Header {
    magic: u32,
    version: u16,
    header_size: u16,
    sets: u32,
    ways: u32,
    set_size: u32,
    _reserved_word: u32,
    /// Written once at creation; a tagged record of another instance is a
    /// miss, so records never outlive the floors that judge them.
    instance: u64,
    /// Advanced once by every invalidation.
    clock: AtomicU64,
    _reserved: [u8; 24],
}

/// 128 bytes, following the 64-byte header: the overflow floor and the tag
/// words share the first 64, so a lookup that misses touches one line.
#[repr(C, align(64))]
struct TagSet {
    /// The highest floor evicted from this set, applying to every tag of the
    /// set: one with no way has no lower floor of its own.
    overflow: AtomicU64,
    /// Tag hashes; zero is an empty way (tag hashes are never zero).
    tags: [AtomicU64; WAYS],
    floors: [AtomicU64; WAYS],
    _reserved: u64,
}

impl TagSet {
    /// The floor `tag` is held to.
    ///
    /// A writer raises `overflow` before it overwrites a way's floor and
    /// writes the way's tag last, so a reader that still sees an evicted tag
    /// reads its old floor or a newer one written over it, and one that sees
    /// a new tag sees the floor written before it.
    fn floor(
        &self,
        tag: u64,
    ) -> u64 {
        let own = self
            .tags
            .iter()
            .position(|way| way.load(Ordering::Acquire) == tag)
            .map_or(0, |way| self.floors[way].load(Ordering::Acquire));
        own.max(self.overflow.load(Ordering::Acquire))
    }

    /// Under the writer lock: hold `tag` to at least `floor`.
    fn raise(
        &self,
        tag: u64,
        floor: u64,
    ) {
        let ways = || self.tags.iter().map(|way| way.load(Ordering::Relaxed));
        if let Some(way) = ways().position(|held| held == tag) {
            self.floors[way].fetch_max(floor, Ordering::Release);
            return;
        }
        let way = ways().position(|held| held == 0).unwrap_or_else(|| {
            // A full set: evict the lowest floor, after widening it to the
            // set, so the invalidation it records is never forgotten.
            let (way, lowest) = self
                .floors
                .iter()
                .map(|floor| floor.load(Ordering::Relaxed))
                .enumerate()
                .min_by_key(|&(_, floor)| floor)
                .expect("a set has ways");
            self.overflow.fetch_max(lowest, Ordering::AcqRel);
            way
        });
        self.floors[way].store(floor, Ordering::Release);
        self.tags[way].store(tag, Ordering::Release);
    }
}

#[repr(C, align(128))]
struct Block([u8; 128]);

enum Backing {
    #[cfg(unix)]
    Shm(ShmRegion),
    Memory {
        bytes: Box<[UnsafeCell<Block>]>,
        /// Keeps the fleet's address, which keys the registry, from being
        /// reused by another fleet while this table lives.
        fleet: Arc<Fleet>,
    },
}

pub(crate) struct TagFloors {
    backing: Backing,
    /// The header's instance, fixed for the segment's life.
    instance: u64,
    /// Serializes this process's invalidations; the segment's lock does the
    /// same between processes.
    write: Mutex<()>,
    owner: ProcessOwner,
}

// SAFETY: every shared field is atomic or written once before the table is
// published; nothing in the mapping is thread-affine.
unsafe impl Send for TagFloors {}
unsafe impl Sync for TagFloors {}

/// In-memory tables, found again by their fleet's address.
static MEMORY: Mutex<Vec<Weak<TagFloors>>> = Mutex::new(Vec::new());

impl TagFloors {
    pub(crate) fn open(fleet: &Arc<Fleet>) -> Result<Arc<Self>> {
        let owner = ProcessOwner::current()?;
        if fleet.is_shm() {
            #[cfg(unix)]
            {
                return Self::open_shm(
                    &ring_segment_name(fleet.name(), SHARED_TAG_KIND),
                    owner,
                )
                .map(Arc::new);
            }
            #[cfg(not(unix))]
            unreachable!("non-Unix fleets cannot use POSIX SHM")
        }
        let mut tables =
            MEMORY.lock().unwrap_or_else(|error| error.into_inner());
        tables.retain(|table| table.strong_count() != 0);
        if let Some(table) =
            tables.iter().filter_map(Weak::upgrade).find(|table| {
                matches!(&table.backing, Backing::Memory { fleet: held, .. } if held.ptr_eq(fleet))
            })
        {
            return Ok(table);
        }
        let bytes = std::iter::repeat_with(|| UnsafeCell::new(Block([0; 128])))
            .take(SEGMENT_SIZE.div_ceil(size_of::<Block>()))
            .collect::<Box<[_]>>();
        let base = bytes.as_ptr().cast_mut().cast::<u8>();
        // SAFETY: a zeroed buffer of at least SEGMENT_SIZE bytes, aligned for
        // the header, that nothing else has seen.
        let instance = unsafe { initialize(base) };
        let table = Arc::new(Self {
            backing: Backing::Memory { bytes, fleet: fleet.clone() },
            instance,
            write: Mutex::new(()),
            owner,
        });
        tables.push(Arc::downgrade(&table));
        Ok(table)
    }

    #[cfg(unix)]
    fn open_shm(
        name: &str,
        owner: ProcessOwner,
    ) -> Result<Self> {
        let (region, _initialization_lock) =
            ShmRegion::open_or_create_locked(name, SEGMENT_SIZE)?;
        let instance = if region.created() {
            // SAFETY: a fresh, zeroed, page-aligned segment of SEGMENT_SIZE
            // bytes, which no peer reads before the initialization lock is
            // released.
            unsafe { initialize(region.as_ptr()) }
        } else {
            // SAFETY: an existing segment of at least SEGMENT_SIZE bytes,
            // whose header was written under the initialization lock.
            let header = unsafe { &*region.as_ptr().cast::<Header>() };
            if header.magic != MAGIC
                || header.version != VERSION
                || usize::from(header.header_size) != size_of::<Header>()
                || header.sets as usize != SHARED_TAG_SETS
                || header.ways as usize != WAYS
                || header.set_size as usize != size_of::<TagSet>()
            {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {name} has an incompatible cache tag layout"
                    ),
                )));
            }
            header.instance
        };
        Ok(Self {
            backing: Backing::Shm(region),
            instance,
            write: Mutex::new(()),
            owner,
        })
    }

    fn base(&self) -> *mut u8 {
        match &self.backing {
            #[cfg(unix)]
            Backing::Shm(region) => region.as_ptr(),
            Backing::Memory { bytes, .. } => bytes.as_ptr().cast_mut().cast(),
        }
    }

    fn header(&self) -> &Header {
        // SAFETY: the mapping starts with a header written or validated at
        // open and outlives `self`.
        unsafe { &*self.base().cast::<Header>() }
    }

    fn set(
        &self,
        tag: u64,
    ) -> &TagSet {
        let index = tag as usize & (SHARED_TAG_SETS - 1);
        // SAFETY: SHARED_TAG_SETS sets follow the header; every field read
        // through a shared reference is atomic.
        unsafe {
            &*self
                .base()
                .add(size_of::<Header>() + index * size_of::<TagSet>())
                .cast::<TagSet>()
        }
    }

    pub(crate) fn instance(&self) -> u64 {
        self.instance
    }

    /// What a tagged put stamps its record with. Read before the record is
    /// written: an invalidation that advances the clock after this read
    /// covers the record, wherever it lands.
    pub(crate) fn clock(&self) -> u64 {
        self.header().clock.load(Ordering::Acquire)
    }

    /// Whether a record stamped `clock` and carrying `tags` is newer than
    /// every invalidation of them.
    pub(crate) fn admits(
        &self,
        clock: u64,
        mut tags: impl Iterator<Item = u64>,
    ) -> bool {
        tags.all(|tag| clock >= self.set(tag).floor(tag))
    }

    /// Advance the clock once and hold each tag to it.
    pub(crate) fn invalidate(
        &self,
        tags: &[u64],
    ) -> Result<()> {
        if !self.owner.is_current() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "an inherited cache handle cannot be used after fork",
            )));
        }
        let _local =
            self.write.lock().unwrap_or_else(|error| error.into_inner());
        #[cfg(unix)]
        let _shared = match &self.backing {
            Backing::Shm(region) => Some(region.lock_exclusive()?),
            Backing::Memory { .. } => None,
        };
        let floor = self.header().clock.fetch_add(1, Ordering::AcqRel) + 1;
        for &tag in tags {
            self.set(tag).raise(tag, floor);
        }
        Ok(())
    }

    #[cfg(unix)]
    pub(crate) fn unlink(&self) -> Result<()> {
        match &self.backing {
            Backing::Shm(region) => Ok(region.unlink()?),
            Backing::Memory { .. } => Ok(()),
        }
    }
}

/// Write a header with a fresh instance into a zeroed table; zero sets are
/// empty. Returns the instance.
///
/// # Safety
///
/// `base` points to at least `SEGMENT_SIZE` zeroed bytes, aligned for the
/// header, that no other thread or process reads yet.
unsafe fn initialize(base: *mut u8) -> u64 {
    let instance = fresh_instance(base);
    // SAFETY: by the caller's contract.
    unsafe {
        std::ptr::write(
            base.cast::<Header>(),
            Header {
                magic: MAGIC,
                version: VERSION,
                header_size: size_of::<Header>() as u16,
                sets: SHARED_TAG_SETS as u32,
                ways: WAYS as u32,
                set_size: size_of::<TagSet>() as u32,
                _reserved_word: 0,
                instance,
                clock: AtomicU64::new(0),
                _reserved: [0; 24],
            },
        );
    }
    instance
}

/// Distinct for every table created, so a recreated table never vouches for
/// a record its predecessor judged. Computed once, at creation.
fn fresh_instance(base: *const u8) -> u64 {
    static CREATED: AtomicU64 = AtomicU64::new(0);
    let mut hasher =
        blake3::Hasher::new_derive_key("orbit cache tag table instance v1");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    hasher.update(&now.to_le_bytes());
    hasher.update(&std::process::id().to_le_bytes());
    hasher.update(&(base as usize).to_le_bytes());
    hasher.update(&CREATED.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let hash = hasher.finalize();
    u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap()).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set() -> TagSet {
        // SAFETY: a set is atomics; all zeroes is an empty set.
        unsafe { std::mem::zeroed() }
    }

    #[test]
    fn a_full_set_widens_the_floor_it_evicts_instead_of_forgetting_it() {
        let set = set();
        for tag in 1..=WAYS as u64 {
            set.raise(tag, tag * 10);
        }
        assert_eq!(set.floor(3), 30);
        assert_eq!(set.floor(100), 0);
        // An eighth tag evicts the lowest floor, tag 1's 10, into the set.
        set.raise(100, 80);
        assert_eq!(set.floor(100), 80);
        assert_eq!(set.floor(1), 10);
        assert_eq!(set.floor(200), 10);
        assert_eq!(set.floor(3), 30);
        // A floor never goes down, held or widened.
        set.raise(3, 5);
        assert_eq!(set.floor(3), 30);
    }
}

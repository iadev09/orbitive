//! The name directory: name -> id, for values declared by name.
//!
//! It lives beside the slot table, never inside it, so the table's layout
//! and the value path stay as they are. An entry is written once, under the
//! directory's structural lock, and is immutable for the rest of its epoch,
//! so a reader that finds an occupied entry needs no lock. Only a reset
//! rewrites the directory, and only when nothing else uses the table.

use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

#[cfg(unix)]
use orbit_core::shm::ShmRegion;

use crate::table::{ATOMIC_CAPACITY, lock_unpoisoned};
use crate::{Error, Result};

/// Reserved Orbit SHM kind for the name directory of an SHM fleet's table.
pub const ATOMIC_NAMES_KIND: u8 = 170;

/// The longest name, in bytes, that a declaration accepts.
///
/// Compile-time geometry: `ORBIT_ATOMIC_NAME_MAX` in the application's
/// `.cargo/config.toml` overrides the default. At most 255. Peers built
/// with a different value are refused when they open the directory.
pub const ATOMIC_NAME_MAX: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_ATOMIC_NAME_MAX"),
    112,
);

const _: () =
    assert!(ATOMIC_NAME_MAX > 0 && ATOMIC_NAME_MAX <= u8::MAX as usize);
// 16 bytes of fixed fields, then the name, padded to the 64-byte alignment.
const _: () =
    assert!(size_of::<Entry>() == (16 + ATOMIC_NAME_MAX).next_multiple_of(64));

/// One entry per slot the table can hand out: every entry names a distinct
/// slot of its epoch, so the directory cannot fill before the table does.
const NAMES_CAPACITY: usize = ATOMIC_CAPACITY;

const ENTRY_EMPTY: u8 = 0;
const ENTRY_OCCUPIED: u8 = 1;

const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
const FNV_PRIME: u32 = 0x0100_0193;

/// What [`Orbitals::reset_all`] does to a declared value.
///
/// [`Orbitals::reset_all`]: crate::Orbitals::reset_all
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(u8)]
pub enum Lifetime {
    /// The name is forgotten at a reset; declared again, it starts from
    /// its initial value in a new slot.
    Reset = 1,
    /// The name and its value survive a reset; declared again, it holds
    /// what it held before.
    Kept = 2,
}

impl Lifetime {
    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Reset),
            2 => Some(Self::Kept),
            _ => None,
        }
    }
}

impl fmt::Display for Lifetime {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.write_str(match self {
            Self::Reset => "reset",
            Self::Kept => "kept",
        })
    }
}

/// Refuse a name the directory cannot hold.
pub(crate) fn check_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::NameEmpty);
    }
    if name.len() > ATOMIC_NAME_MAX {
        return Err(Error::NameTooLong {
            len: name.len(),
            max: ATOMIC_NAME_MAX,
        });
    }
    Ok(())
}

/// The rule of [`check_name`], at compile time: [`orbital!`] calls it in a
/// constant, so a declaration whose name the directory cannot hold does
/// not build.
///
/// [`orbital!`]: crate::orbital
#[doc(hidden)]
pub const fn assert_declared_name(name: &str) {
    assert!(!name.is_empty(), "an orbital name must not be empty");
    assert!(
        name.len() <= ATOMIC_NAME_MAX,
        "an orbital name must not be longer than ATOMIC_NAME_MAX bytes"
    );
}

/// FNV-1a over the name's bytes, once per declaration.
pub(crate) fn name_hash(name: &[u8]) -> u32 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in name {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn probe_index(
    hash: u32,
    offset: usize,
) -> usize {
    (hash as usize).wrapping_add(offset) & (NAMES_CAPACITY - 1)
}

/// What a name resolves to: the id as its eight bytes, checked again by
/// the caller, and the lifetime it was declared with.
#[derive(Clone, Copy)]
pub(crate) struct Resolved {
    pub(crate) id: u64,
    pub(crate) lifetime: Option<Lifetime>,
}

/// A kept name, copied out of the directory by a reset.
pub(crate) struct KeptName {
    pub(crate) hash: u32,
    pub(crate) name: Box<[u8]>,
    pub(crate) id: u64,
}

/// One name and the id it resolves to. All zeroes is an empty entry, and
/// every field is atomic, so the mapping is never borrowed `&mut` while
/// another process reads it.
#[repr(C, align(64))]
struct Entry {
    state: AtomicU8,
    len: AtomicU8,
    lifetime: AtomicU8,
    _reserved: u8,
    hash: AtomicU32,
    id: AtomicU64,
    name: [AtomicU8; ATOMIC_NAME_MAX],
}

impl Entry {
    fn occupied(&self) -> bool {
        self.state.load(Ordering::Acquire) == ENTRY_OCCUPIED
    }

    /// After [`Self::occupied`]: its `Acquire` publishes what `install`
    /// wrote, so the fields are read `Relaxed`.
    fn resolves(
        &self,
        hash: u32,
        name: &[u8],
    ) -> Option<Resolved> {
        let matches = self.hash.load(Ordering::Relaxed) == hash
            && usize::from(self.len.load(Ordering::Relaxed)) == name.len()
            && self.name[..name.len()]
                .iter()
                .zip(name)
                .all(|(stored, byte)| stored.load(Ordering::Relaxed) == *byte);
        matches.then(|| self.resolved())
    }

    fn resolved(&self) -> Resolved {
        Resolved {
            id: self.id.load(Ordering::Relaxed),
            lifetime: Lifetime::from_tag(self.lifetime.load(Ordering::Relaxed)),
        }
    }

    /// Under the structural lock. `state` is written last: an installer
    /// that dies before it leaves the entry empty, to be written again.
    fn install(
        &self,
        hash: u32,
        name: &[u8],
        lifetime: Lifetime,
        id: u64,
    ) {
        debug_assert!(name.len() <= ATOMIC_NAME_MAX);
        self.hash.store(hash, Ordering::Relaxed);
        self.len.store(name.len() as u8, Ordering::Relaxed);
        self.lifetime.store(lifetime as u8, Ordering::Relaxed);
        for (index, byte) in self.name.iter().enumerate() {
            byte.store(
                name.get(index).copied().unwrap_or(0),
                Ordering::Relaxed,
            );
        }
        self.id.store(id, Ordering::Relaxed);
        self.state.store(ENTRY_OCCUPIED, Ordering::Release);
    }

    /// Under the structural lock, with the directory's epoch already
    /// invalid. `state` first, so a clear cut short leaves no entry that
    /// looks occupied.
    fn clear(&self) {
        if self.state.load(Ordering::Relaxed) == ENTRY_EMPTY {
            return;
        }
        self.state.store(ENTRY_EMPTY, Ordering::Release);
        self.hash.store(0, Ordering::Relaxed);
        self.len.store(0, Ordering::Relaxed);
        self.lifetime.store(0, Ordering::Relaxed);
        for byte in &self.name {
            byte.store(0, Ordering::Relaxed);
        }
        self.id.store(0, Ordering::Relaxed);
    }

    fn name(&self) -> Box<[u8]> {
        let len = usize::from(self.len.load(Ordering::Relaxed));
        self.name[..len.min(ATOMIC_NAME_MAX)]
            .iter()
            .map(|byte| byte.load(Ordering::Relaxed))
            .collect()
    }
}

/// Where a locked probe stopped.
enum Probe<'a> {
    Found(Resolved),
    Vacant(&'a Entry),
    Full,
}

pub(crate) struct Names {
    backing: Backing,
    /// Serializes installs and resets within this process; the region's
    /// lock does the same between processes.
    structure_lock: Mutex<()>,
}

enum Backing {
    Memory {
        epoch: AtomicU32,
        entries: Box<[Entry]>,
    },
    #[cfg(unix)]
    Shm {
        region: ShmRegion,
    },
}

/// The directory while its structural locks are held.
pub(crate) struct Locked<'a>(&'a Names);

impl Names {
    pub(crate) fn memory() -> Self {
        // SAFETY: an entry is atomics and bytes; all zeroes is empty.
        let entries = (0..NAMES_CAPACITY)
            .map(|_| unsafe { std::mem::zeroed::<Entry>() })
            .collect();
        Self {
            backing: Backing::Memory {
                // Zero is never a table epoch, so the first declaration
                // brings the directory to the table's.
                epoch: AtomicU32::new(0),
                entries,
            },
            structure_lock: Mutex::new(()),
        }
    }

    /// The table epoch the entries belong to. Entries of any other epoch
    /// are not names: the table was reset, or created again, without this
    /// directory being rewritten. Zero while a reset rewrites it.
    fn epoch(&self) -> &AtomicU32 {
        match &self.backing {
            Backing::Memory { epoch, .. } => epoch,
            #[cfg(unix)]
            Backing::Shm { region } => &shm::header(region).epoch,
        }
    }

    fn entries(&self) -> &[Entry] {
        match &self.backing {
            Backing::Memory { entries, .. } => entries,
            #[cfg(unix)]
            Backing::Shm { region } => shm::entries(region),
        }
    }

    /// Without a lock: an occupied entry never changes within its epoch,
    /// and probing chains only grow. A miss means the name was not
    /// installed when the probe passed; a caller about to install asks
    /// again under the lock.
    pub(crate) fn find(
        &self,
        table_epoch: u32,
        hash: u32,
        name: &[u8],
    ) -> Option<Resolved> {
        if self.epoch().load(Ordering::Acquire) != table_epoch {
            return None;
        }
        let entries = self.entries();
        for offset in 0..NAMES_CAPACITY {
            let entry = &entries[probe_index(hash, offset)];
            if !entry.occupied() {
                return None;
            }
            if let Some(resolved) = entry.resolves(hash, name) {
                return Some(resolved);
            }
        }
        None
    }

    /// Hold the process-local lock and, for SHM, the region's lock, which
    /// the kernel releases if this process dies holding it.
    pub(crate) fn with_structure<R>(
        &self,
        operation: impl FnOnce(Locked<'_>) -> Result<R>,
    ) -> Result<R> {
        let _local = lock_unpoisoned(&self.structure_lock);
        #[cfg(unix)]
        let _process = match &self.backing {
            Backing::Shm { region } => Some(region.lock_exclusive()?),
            Backing::Memory { .. } => None,
        };
        operation(Locked(self))
    }
}

impl Locked<'_> {
    /// Find `name`, or install the id `allocate` returns with `lifetime`.
    /// A directory left in another epoch is emptied first.
    pub(crate) fn find_or_install(
        &self,
        table_epoch: u32,
        hash: u32,
        name: &[u8],
        lifetime: Lifetime,
        allocate: impl FnOnce() -> Result<u64>,
    ) -> Result<Resolved> {
        if self.0.epoch().load(Ordering::Acquire) != table_epoch {
            self.clear();
            self.0.epoch().store(table_epoch, Ordering::Release);
        }
        match self.probe(hash, name) {
            Probe::Found(resolved) => Ok(resolved),
            Probe::Vacant(entry) => {
                let id = allocate()?;
                entry.install(hash, name, lifetime, id);
                Ok(Resolved { id, lifetime: Some(lifetime) })
            }
            Probe::Full => Err(Error::Full { capacity: NAMES_CAPACITY }),
        }
    }

    /// The kept names of `table_epoch`; none when the directory is in
    /// another one.
    pub(crate) fn kept(
        &self,
        table_epoch: u32,
    ) -> Vec<KeptName> {
        if self.0.epoch().load(Ordering::Acquire) != table_epoch {
            return Vec::new();
        }
        self.0
            .entries()
            .iter()
            .filter(|entry| entry.occupied())
            .filter_map(|entry| {
                let resolved = entry.resolved();
                (resolved.lifetime == Some(Lifetime::Kept)).then(|| KeptName {
                    hash: entry.hash.load(Ordering::Relaxed),
                    name: entry.name(),
                    id: resolved.id,
                })
            })
            .collect()
    }

    /// Invalidate the directory and empty it. Until [`Self::publish`],
    /// no epoch matches it, so a reset cut short leaves no name that
    /// resolves.
    pub(crate) fn clear(&self) {
        self.0.epoch().store(0, Ordering::Release);
        for entry in self.0.entries() {
            entry.clear();
        }
    }

    /// Install a kept name carried across a reset.
    pub(crate) fn restore(
        &self,
        kept: &KeptName,
    ) {
        if let Probe::Vacant(entry) = self.probe(kept.hash, &kept.name) {
            entry.install(kept.hash, &kept.name, Lifetime::Kept, kept.id);
        }
    }

    pub(crate) fn publish(
        &self,
        table_epoch: u32,
    ) {
        self.0.epoch().store(table_epoch, Ordering::Release);
    }

    fn probe(
        &self,
        hash: u32,
        name: &[u8],
    ) -> Probe<'_> {
        let entries = self.0.entries();
        for offset in 0..NAMES_CAPACITY {
            let entry = &entries[probe_index(hash, offset)];
            if !entry.occupied() {
                return Probe::Vacant(entry);
            }
            if let Some(resolved) = entry.resolves(hash, name) {
                return Probe::Found(resolved);
            }
        }
        Probe::Full
    }
}

#[cfg(unix)]
pub(crate) use shm::unlink as unlink_shm_names;
#[cfg(unix)]
pub(crate) use shm::{open as shm_names, open_existing as existing_shm_names};

#[cfg(unix)]
mod shm {
    use std::{io, ptr};

    use super::*;

    const MAGIC: u32 = 0x4f_52_42_4e; // "ORBN"
    const VERSION: u16 = 1;

    #[repr(C, align(64))]
    pub(super) struct Header {
        magic: u32,
        version: u16,
        header_size: u16,
        capacity: u32,
        entry_size: u32,
        name_max: u32,
        /// The table epoch the entries belong to; zero before the first
        /// declaration and while a reset rewrites the entries.
        pub(super) epoch: AtomicU32,
        _reserved: [u8; 40],
    }

    const _: () = assert!(size_of::<Header>() == 64);

    const SEGMENT_SIZE: usize =
        size_of::<Header>() + NAMES_CAPACITY * size_of::<Entry>();

    pub(super) fn header(region: &ShmRegion) -> &Header {
        // SAFETY: the region is `SEGMENT_SIZE` long, page aligned, and its
        // header was written or validated at attach.
        unsafe { &*region.as_ptr().cast::<Header>() }
    }

    pub(super) fn entries(region: &ShmRegion) -> &[Entry] {
        // SAFETY: as above; the entries follow the header.
        unsafe {
            std::slice::from_raw_parts(
                region.as_ptr().add(size_of::<Header>()).cast::<Entry>(),
                NAMES_CAPACITY,
            )
        }
    }

    /// Attach the directory, creating it when this is the first process
    /// to declare a name.
    pub(crate) fn open(name: &str) -> Result<Names> {
        let (region, _initialization_lock) =
            ShmRegion::open_or_create_locked(name, SEGMENT_SIZE)?;
        if region.created() {
            // SAFETY: the mapping is `SEGMENT_SIZE` long and nobody else
            // reads it before the initialization lock is released.
            unsafe {
                ptr::write_bytes(region.as_ptr(), 0, SEGMENT_SIZE);
                ptr::write(
                    region.as_ptr().cast::<Header>(),
                    Header {
                        magic: MAGIC,
                        version: VERSION,
                        header_size: size_of::<Header>() as u16,
                        capacity: NAMES_CAPACITY as u32,
                        entry_size: size_of::<Entry>() as u32,
                        name_max: ATOMIC_NAME_MAX as u32,
                        epoch: AtomicU32::new(0),
                        _reserved: [0; 40],
                    },
                );
            }
        } else {
            validate(name, &region)?;
        }
        Ok(Names::shm(region))
    }

    /// Attach the directory only if some process has created it: a reset
    /// with no names declared has nothing to keep and creates nothing.
    pub(crate) fn open_existing(name: &str) -> Result<Option<Names>> {
        let Some((region, _initialization_lock)) =
            ShmRegion::open_existing_locked(name, SEGMENT_SIZE)?
        else {
            return Ok(None);
        };
        validate(name, &region)?;
        Ok(Some(Names::shm(region)))
    }

    fn validate(
        name: &str,
        region: &ShmRegion,
    ) -> Result<()> {
        let header = header(region);
        if header.magic != MAGIC {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {name} has wrong magic {:#010x} (expected {MAGIC:#010x})",
                    header.magic
                ),
            )));
        }
        if header.version != VERSION
            || usize::from(header.header_size) != size_of::<Header>()
            || header.capacity as usize != NAMES_CAPACITY
            || header.entry_size as usize != size_of::<Entry>()
            || header.name_max as usize != ATOMIC_NAME_MAX
        {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {name} has an incompatible orbital name directory layout"
                ),
            )));
        }
        Ok(())
    }

    /// Remove the directory's SHM object if there is one, whatever its
    /// geometry: only the header is mapped to reach it.
    pub(crate) fn unlink(name: &str) -> Result<()> {
        if let Some((region, _lock)) =
            ShmRegion::open_existing_locked(name, size_of::<Header>())?
        {
            region.unlink()?;
        }
        Ok(())
    }

    impl Names {
        fn shm(region: ShmRegion) -> Self {
            Self {
                backing: Backing::Shm { region },
                structure_lock: Mutex::new(()),
            }
        }
    }
}

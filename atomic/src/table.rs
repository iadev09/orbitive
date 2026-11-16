//! The slot table: in shared memory for an SHM fleet, on the heap otherwise.

use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};

use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::process::ProcessOwner;
#[cfg(unix)]
use orbit_core::shm::ShmRegion;

use crate::wake::Wake;
use crate::{Error, Kind, OrbitalId, Result};

/// Reserved Orbit SHM kind for the table.
pub const ATOMIC_KIND: u8 = 174;

/// How many values one table can declare between two resets.
///
/// Compile-time geometry: `ORBIT_ATOMIC_CAPACITY` in the application's
/// `.cargo/config.toml` overrides the default. Must be a power of two and
/// fit the 24-bit slot index of an id. Peers built with a different value
/// are refused when they open the table.
pub const ATOMIC_CAPACITY: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_ATOMIC_CAPACITY"),
    4_096,
);

const _: () = assert!(ATOMIC_CAPACITY.is_power_of_two());
const _: () = assert!(ATOMIC_CAPACITY <= 1 << crate::id::SLOT_BITS);
const _: () = assert!(size_of::<Slot>() == 64);

const SLOT_EMPTY: u8 = 0;
const SLOT_OCCUPIED: u8 = 1;

/// The epoch of a table that has never been reset. Zero is never current,
/// so an id of all zeroes after its kind names nothing.
const FIRST_EPOCH: u32 = 1;

/// One value, laid out for shared memory: a cache line of its own, so two
/// hot values never contend, and every field atomic, so the mapping is
/// never borrowed `&mut` while another process reads it. All zeroes is an
/// empty slot.
#[repr(C, align(64))]
pub(crate) struct Slot {
    state: AtomicU8,
    kind: AtomicU8,
    _reserved: [u8; 6],
    pub(crate) value: AtomicU64,
    pub(crate) wake: Wake,
    _padding: [u8; 40],
}

impl Slot {
    /// By the one caller that drew this index from `next`.
    fn install(
        &self,
        kind: Kind,
        word: u64,
    ) {
        self.kind.store(kind as u8, Ordering::Relaxed);
        self.value.store(word, Ordering::Relaxed);
        self.state.store(SLOT_OCCUPIED, Ordering::Release);
    }

    /// `Acquire` on `state` publishes what `install` wrote.
    fn holds(
        &self,
        kind: Kind,
    ) -> bool {
        self.state.load(Ordering::Acquire) == SLOT_OCCUPIED
            && self.kind.load(Ordering::Relaxed) == kind as u8
    }

    /// Only at a quiescent reset.
    fn clear(&self) {
        self.state.store(SLOT_EMPTY, Ordering::Release);
        self.kind.store(0, Ordering::Relaxed);
        self.value.store(0, Ordering::Relaxed);
        self.wake.clear();
    }
}

pub(crate) struct Table {
    backing: Backing,
    /// Serializes resets within this process; the region's lock does the
    /// same between processes.
    reset_lock: Mutex<()>,
}

enum Backing {
    Memory {
        /// Held so the fleet's address, which keys the registry, cannot be
        /// reused by another fleet while this table lives.
        _fleet: Arc<Fleet>,
        epoch: AtomicU32,
        next: AtomicU32,
        slots: Box<[Slot]>,
    },
    #[cfg(unix)]
    Shm { region: ShmRegion, name: Box<str> },
}

impl Table {
    fn memory(fleet: Arc<Fleet>) -> Self {
        // SAFETY: a slot is atomics and bytes; all zeroes is an empty slot.
        let slots = (0..ATOMIC_CAPACITY)
            .map(|_| unsafe { std::mem::zeroed::<Slot>() })
            .collect();
        Self {
            backing: Backing::Memory {
                _fleet: fleet,
                epoch: AtomicU32::new(FIRST_EPOCH),
                next: AtomicU32::new(0),
                slots,
            },
            reset_lock: Mutex::new(()),
        }
    }

    fn epoch(&self) -> &AtomicU32 {
        match &self.backing {
            Backing::Memory { epoch, .. } => epoch,
            #[cfg(unix)]
            Backing::Shm { region, .. } => &shm::header(region).epoch,
        }
    }

    fn next(&self) -> &AtomicU32 {
        match &self.backing {
            Backing::Memory { next, .. } => next,
            #[cfg(unix)]
            Backing::Shm { region, .. } => &shm::header(region).next,
        }
    }

    fn slots(&self) -> &[Slot] {
        match &self.backing {
            Backing::Memory { slots, .. } => slots,
            #[cfg(unix)]
            Backing::Shm { region, .. } => shm::slots(region),
        }
    }

    /// Hand out the next slot. One `fetch_add` decides the owner of an
    /// index; the load before it keeps a full table from being written at
    /// all, so `next` passes the capacity by at most the callers racing in
    /// one window and never wraps.
    pub(crate) fn allocate(
        &self,
        kind: Kind,
        word: u64,
    ) -> Result<(OrbitalId, NonNull<Slot>)> {
        let full = || Error::Full { capacity: ATOMIC_CAPACITY };
        let next = self.next();
        if next.load(Ordering::Relaxed) as usize >= ATOMIC_CAPACITY {
            return Err(full());
        }
        let index = next.fetch_add(1, Ordering::Relaxed);
        let slot = self.slots().get(index as usize).ok_or_else(full)?;
        slot.install(kind, word);
        let epoch = self.epoch().load(Ordering::Acquire);
        Ok((OrbitalId::new(kind, index, epoch), NonNull::from(slot)))
    }

    /// Resolve an id to its slot. Reads only.
    pub(crate) fn open(
        &self,
        id: OrbitalId,
        expected: Kind,
    ) -> Result<NonNull<Slot>> {
        let slot =
            self.slots().get(id.slot() as usize).ok_or(Error::Stale(id))?;
        if self.epoch().load(Ordering::Acquire) != id.epoch() {
            return Err(Error::Stale(id));
        }
        if id.kind() != expected {
            return Err(Error::KindMismatch { id, expected });
        }
        if !slot.holds(expected) {
            return Err(Error::Stale(id));
        }
        Ok(NonNull::from(slot))
    }

    /// Start a new epoch with every slot empty. The caller guarantees that
    /// nothing else uses the table.
    pub(crate) fn reset(&self) -> Result<()> {
        let _local = lock_unpoisoned(&self.reset_lock);
        #[cfg(unix)]
        let _process = match &self.backing {
            Backing::Shm { region, .. } => Some(region.lock_exclusive()?),
            Backing::Memory { .. } => None,
        };
        let epoch = self.epoch();
        let advanced = epoch
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or(Error::EpochExhausted)?;
        epoch.store(advanced, Ordering::Release);
        for slot in self.slots() {
            slot.clear();
        }
        self.next().store(0, Ordering::Release);
        Ok(())
    }

    /// Remove the SHM object. Existing mappings stay valid until their
    /// processes drop them; this process maps the name afresh next time.
    #[cfg(unix)]
    pub(crate) fn unlink(&self) -> Result<()> {
        match &self.backing {
            Backing::Memory { .. } => self.reset(),
            Backing::Shm { region, name } => {
                let mut tables = shm::TABLES.lock()?;
                region.unlink()?;
                tables.remove(&**name);
                Ok(())
            }
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

type MemoryRegistry = HashMap<usize, Weak<Table>>;

static MEMORY_TABLES: LazyLock<Mutex<MemoryRegistry>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// One table per in-memory fleet, found again by the fleet's address.
pub(crate) fn memory_table(fleet: &Arc<Fleet>) -> Arc<Table> {
    let identity = Arc::as_ptr(fleet) as usize;
    let mut tables = lock_unpoisoned(&MEMORY_TABLES);
    tables.retain(|_, table| table.strong_count() > 0);
    if let Some(table) = tables.get(&identity).and_then(Weak::upgrade) {
        return table;
    }
    let table = Arc::new(Table::memory(Arc::clone(fleet)));
    tables.insert(identity, Arc::downgrade(&table));
    table
}

#[cfg(unix)]
pub(crate) use shm::table as shm_table;

#[cfg(unix)]
mod shm {
    use std::sync::atomic::AtomicPtr;
    use std::{io, ptr};

    use super::*;

    const MAGIC: u32 = 0x4f_52_42_4c; // "ORBL"
    const VERSION: u16 = 1;

    #[repr(C, align(64))]
    pub(super) struct Header {
        magic: u32,
        version: u16,
        header_size: u16,
        capacity: u32,
        slot_size: u32,
        /// Advanced by every reset; an id carries the one it was minted in.
        pub(super) epoch: AtomicU32,
        /// The index the next allocation takes.
        pub(super) next: AtomicU32,
        _reserved: [u8; 40],
    }

    const _: () = assert!(size_of::<Header>() == 64);

    const SEGMENT_SIZE: usize =
        size_of::<Header>() + ATOMIC_CAPACITY * size_of::<Slot>();

    pub(super) fn header(region: &ShmRegion) -> &Header {
        // SAFETY: the region is `SEGMENT_SIZE` long, page aligned, and its
        // header was written or validated at attach.
        unsafe { &*region.as_ptr().cast::<Header>() }
    }

    pub(super) fn slots(region: &ShmRegion) -> &[Slot] {
        // SAFETY: as above; the slots follow the header.
        unsafe {
            std::slice::from_raw_parts(
                region.as_ptr().add(size_of::<Header>()).cast::<Slot>(),
                ATOMIC_CAPACITY,
            )
        }
    }

    fn open_or_create(name: &str) -> Result<Table> {
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
                        capacity: ATOMIC_CAPACITY as u32,
                        slot_size: size_of::<Slot>() as u32,
                        epoch: AtomicU32::new(FIRST_EPOCH),
                        next: AtomicU32::new(0),
                        _reserved: [0; 40],
                    },
                );
            }
        } else {
            let header = header(&region);
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
                || header.capacity as usize != ATOMIC_CAPACITY
                || header.slot_size as usize != size_of::<Slot>()
            {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {name} has an incompatible orbital table layout"
                    ),
                )));
            }
        }
        Ok(Table {
            backing: Backing::Shm { region, name: name.into() },
            reset_lock: Mutex::new(()),
        })
    }

    type TableMap = HashMap<Box<str>, Weak<Table>>;

    struct Registry {
        process: ProcessOwner,
        tables: Mutex<TableMap>,
    }

    /// One mapping per table name in a process, and a registry of its own
    /// for a fork child.
    pub(super) struct Tables(AtomicPtr<Registry>);

    pub(super) static TABLES: Tables = Tables(AtomicPtr::new(ptr::null_mut()));

    impl Tables {
        pub(super) fn lock(
            &'static self
        ) -> Result<MutexGuard<'static, TableMap>> {
            let process = ProcessOwner::current()?;
            loop {
                let previous = self.0.load(Ordering::Acquire);
                // SAFETY: published registries stay allocated until
                // process exit.
                if let Some(registry) = unsafe { previous.as_ref() }
                    && registry.process.is_current()
                {
                    return Ok(lock_unpoisoned(&registry.tables));
                }

                // A fork child cannot lock a mutex inherited from its
                // parent, and maps the tables again for itself.
                let registry = Box::into_raw(Box::new(Registry {
                    process,
                    tables: Mutex::default(),
                }));
                if self
                    .0
                    .compare_exchange(
                        previous,
                        registry,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    // SAFETY: this registry is now published and retained.
                    return Ok(lock_unpoisoned(unsafe { &(*registry).tables }));
                }
                // SAFETY: this candidate was never published.
                drop(unsafe { Box::from_raw(registry) });
            }
        }
    }

    pub(crate) fn table(name: &str) -> Result<Arc<Table>> {
        let mut tables = TABLES.lock()?;
        tables.retain(|_, table| table.strong_count() != 0);
        if let Some(table) = tables.get(name).and_then(Weak::upgrade) {
            return Ok(table);
        }
        let table = Arc::new(open_or_create(name)?);
        tables.insert(name.into(), Arc::downgrade(&table));
        Ok(table)
    }
}

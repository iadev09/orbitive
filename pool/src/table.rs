//! The table behind a `Pool` handle: the mapped segment (or its in-memory
//! twin), this process's resource lane, the key table any node may install
//! into, and the process-local readiness pieces. One table per process per
//! fleet and node.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::task::Waker;
use std::thread::JoinHandle;
use std::time::Duration;

#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};
use orbit_core::{Fleet, OrbitEpoch};

use crate::layout::{
    Doorbell, Geometry, Header, KEY_EMPTY, KEY_LIVE, KeySlot, RESOURCE_EMPTY, ResourceSlot,
};
use crate::{
    Error, Incarnation, POOL_KEY_CAPACITY, POOL_KIND, POOL_RESOURCE_LANE_CAPACITY, Result,
    lock_unpoisoned,
};

const FALLBACK_POLL_INTERVAL: Duration = Duration::from_millis(10);

enum Backing {
    Memory(AlignedBytes),
    #[cfg(unix)]
    Shm(ShmRegion),
}

struct AlignedBytes {
    ptr: *mut u8,
    layout: std::alloc::Layout,
}

impl AlignedBytes {
    fn zeroed(size: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(size, 64).expect("segment layout");
        // SAFETY: the layout has a nonzero size (a header at least).
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "pool table allocation failed");
        Self { ptr, layout }
    }
}

impl Drop for AlignedBytes {
    fn drop(&mut self) {
        // SAFETY: allocated with this layout in `zeroed`.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) }
    }
}

// SAFETY: the bytes are only ever read through atomics; the pointer is not
// shared outside `Table`.
unsafe impl Send for AlignedBytes {}
unsafe impl Sync for AlignedBytes {}

/// Wakers parked on a key in this process, and the thread that turns the
/// node's doorbell into their wakes.
struct Driver {
    stop: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<JoinHandle<()>>,
    pid: u32,
}

pub(crate) struct Table {
    /// Held so the fleet's address, which keys the in-memory registry,
    /// cannot be reused by another fleet while this table lives.
    _fleet: Arc<Fleet>,
    backing: Backing,
    geometry: Geometry,
    node: u16,
    incarnation: u64,
    /// This process's allocations in its own lane, and its installs into
    /// the shared key table (those also take the region's process lock).
    structural: Mutex<usize>,
    wakers: Box<[Mutex<Vec<Waker>>]>,
    driver: Mutex<Option<Driver>>,
}

impl Table {
    fn base(&self) -> *mut u8 {
        match &self.backing {
            Backing::Memory(bytes) => bytes.ptr,
            #[cfg(unix)]
            Backing::Shm(region) => region.as_ptr(),
        }
    }

    pub(crate) fn is_shared(&self) -> bool {
        match &self.backing {
            Backing::Memory(_) => false,
            #[cfg(unix)]
            Backing::Shm(_) => true,
        }
    }

    pub(crate) fn geometry(&self) -> &Geometry {
        &self.geometry
    }

    pub(crate) fn node(&self) -> u16 {
        self.node
    }

    pub(crate) fn incarnation(&self) -> u64 {
        self.incarnation
    }

    fn header(&self) -> &Header {
        // SAFETY: written at creation, validated at open; mapping outlives self.
        unsafe { &*self.base().cast::<Header>() }
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.header().epoch.load(Ordering::Acquire)
    }

    fn doorbell(&self, node: usize) -> &Doorbell {
        debug_assert!(node < self.geometry.fleet_capacity);
        // SAFETY: `fleet_capacity` doorbells follow the header; atomics only.
        unsafe {
            &*self
                .base()
                .add(self.geometry.doorbells_offset + node * size_of::<Doorbell>())
                .cast::<Doorbell>()
        }
    }

    fn key_word(&self, offset: usize, node: usize, word: usize) -> &AtomicU64 {
        debug_assert!(node < self.geometry.fleet_capacity && word < self.geometry.key_words);
        // SAFETY: inside the bitmaps by construction of `Geometry`.
        unsafe {
            &*self
                .base()
                .add(offset + (node * self.geometry.key_words + word) * size_of::<AtomicU64>())
                .cast::<AtomicU64>()
        }
    }

    fn pending_word(&self, node: usize, word: usize) -> &AtomicU64 {
        self.key_word(self.geometry.pending_offset, node, word)
    }

    fn interest_word(&self, node: usize, word: usize) -> &AtomicU64 {
        self.key_word(self.geometry.interest_offset, node, word)
    }

    /// Creation claims `node` holds on key `key_index`.
    pub(crate) fn claims(&self, node: usize, key_index: usize) -> &AtomicU32 {
        debug_assert!(node < self.geometry.fleet_capacity && key_index < POOL_KEY_CAPACITY);
        // SAFETY: inside the claims area by construction of `Geometry`.
        unsafe {
            &*self
                .base()
                .add(
                    self.geometry.claims_offset
                        + (node * POOL_KEY_CAPACITY + key_index) * size_of::<AtomicU32>(),
                )
                .cast::<AtomicU32>()
        }
    }

    pub(crate) fn key(&self, index: usize) -> &KeySlot {
        debug_assert!(index < POOL_KEY_CAPACITY);
        // SAFETY: key slots are `key_stride` apart from `keys_offset`;
        // atomics only.
        unsafe {
            &*self
                .base()
                .add(self.geometry.keys_offset + index * self.geometry.key_stride)
                .cast::<KeySlot>()
        }
    }

    /// The resource-slot bitmap that follows key slot `index`.
    pub(crate) fn members(&self, index: usize) -> &[AtomicU64] {
        // SAFETY: `member_words` words follow each key slot inside its stride.
        unsafe {
            std::slice::from_raw_parts(
                self.base()
                    .add(self.geometry.keys_offset + index * self.geometry.key_stride)
                    .add(size_of::<KeySlot>())
                    .cast::<AtomicU64>(),
                self.geometry.member_words,
            )
        }
    }

    pub(crate) fn resources(&self) -> &[ResourceSlot] {
        // SAFETY: `total_resources` slots follow the keys; atomics only.
        unsafe {
            std::slice::from_raw_parts(
                self.base()
                    .add(self.geometry.resources_offset)
                    .cast::<ResourceSlot>(),
                self.geometry.total_resources,
            )
        }
    }

    /// Find the key, installing it if absent. Open addressing on the
    /// caller's 128-bit digest; keys are never removed within an epoch.
    pub(crate) fn key_index(&self, lo: u64, hi: u64) -> Result<usize> {
        if let Some(index) = self.find_key(lo, hi) {
            return Ok(index);
        }
        let _local = lock_unpoisoned(&self.structural);
        #[cfg(unix)]
        let _shared = match &self.backing {
            Backing::Shm(region) => Some(region.lock_exclusive()?),
            Backing::Memory(_) => None,
        };
        let hash = mix(lo, hi);
        for offset in 0..POOL_KEY_CAPACITY {
            let index = (hash as usize).wrapping_add(offset) & (POOL_KEY_CAPACITY - 1);
            let slot = self.key(index);
            match slot.state.load(Ordering::Acquire) {
                KEY_LIVE if slot.holds(lo, hi) => return Ok(index),
                KEY_EMPTY => {
                    slot.key_lo.store(lo, Ordering::Relaxed);
                    slot.key_hi.store(hi, Ordering::Relaxed);
                    slot.counts.store(0, Ordering::Relaxed);
                    slot.changes.store(0, Ordering::Relaxed);
                    slot.waiters.store(0, Ordering::Relaxed);
                    for word in self.members(index) {
                        word.store(0, Ordering::Relaxed);
                    }
                    slot.state.store(KEY_LIVE, Ordering::Release);
                    return Ok(index);
                }
                _ => {}
            }
        }
        Err(Error::KeyFull {
            capacity: POOL_KEY_CAPACITY,
        })
    }

    fn find_key(&self, lo: u64, hi: u64) -> Option<usize> {
        let hash = mix(lo, hi);
        for offset in 0..POOL_KEY_CAPACITY {
            let index = (hash as usize).wrapping_add(offset) & (POOL_KEY_CAPACITY - 1);
            let slot = self.key(index);
            match slot.state.load(Ordering::Acquire) {
                KEY_LIVE if slot.holds(lo, hi) => return Some(index),
                KEY_EMPTY => return None,
                _ => {}
            }
        }
        None
    }

    /// Take a free slot in this process's lane. Returns the index and the
    /// generation it was installed in.
    pub(crate) fn allocate(
        &self,
        key: (u64, u64),
        key_index: usize,
        capacity: u32,
    ) -> Result<(usize, u32)> {
        let mut hint = lock_unpoisoned(&self.structural);
        let lane_start = usize::from(self.node) * POOL_RESOURCE_LANE_CAPACITY;
        let slots = self.resources();
        let now = OrbitEpoch::now().as_unix_ms();
        for offset in 0..POOL_RESOURCE_LANE_CAPACITY {
            let index = lane_start + ((*hint + offset) & (POOL_RESOURCE_LANE_CAPACITY - 1));
            let slot = &slots[index];
            let state = slot.state.load(Ordering::Acquire);
            if (state == RESOURCE_EMPTY || state == crate::layout::RESOURCE_CLOSED)
                && let Some(generation) = slot.install(
                    self.node,
                    self.incarnation,
                    key,
                    key_index as u32,
                    capacity,
                    now,
                )
            {
                *hint = (index - lane_start + 1) & (POOL_RESOURCE_LANE_CAPACITY - 1);
                return Ok((index, generation));
            }
        }
        Err(Error::Full {
            capacity: POOL_RESOURCE_LANE_CAPACITY,
        })
    }

    /// Capacity may have come back on `key_index`: count it for blocking
    /// waiters and ring every node that registered interest.
    pub(crate) fn key_changed(&self, key_index: usize) {
        let key = self.key(key_index);
        key.changes.fetch_add(1, Ordering::SeqCst);
        if key.waiters.load(Ordering::SeqCst) > 0 {
            crate::wake_on(&key.changes);
        }
        let word = key_index / 64;
        let bit = 1_u64 << (key_index % 64);
        if !self.is_shared() {
            self.wake(key_index);
            return;
        }
        for node in 0..self.geometry.fleet_capacity {
            if self.interest_word(node, word).load(Ordering::SeqCst) & bit != 0 {
                self.pending_word(node, word)
                    .fetch_or(bit, Ordering::SeqCst);
                let doorbell = self.doorbell(node);
                doorbell.generation.fetch_add(1, Ordering::SeqCst);
                if doorbell.listening.load(Ordering::SeqCst) > 0 {
                    crate::wake_on(&doorbell.generation);
                }
            }
        }
    }

    fn wake(&self, key_index: usize) {
        let taken = std::mem::take(&mut *lock_unpoisoned(&self.wakers[key_index]));
        for waker in taken {
            waker.wake();
        }
    }

    /// Park a task on a key: remember its waker, mark this node interested,
    /// start the driver on first use.
    pub(crate) fn register(self: &Arc<Self>, key_index: usize, waker: &Waker) -> Result<()> {
        {
            let mut wakers = lock_unpoisoned(&self.wakers[key_index]);
            if !wakers.iter().any(|existing| existing.will_wake(waker)) {
                wakers.push(waker.clone());
            }
        }
        if !self.is_shared() {
            return Ok(());
        }
        self.interest_word(usize::from(self.node), key_index / 64)
            .fetch_or(1 << (key_index % 64), Ordering::SeqCst);
        let mut driver = lock_unpoisoned(&self.driver);
        if driver.is_none() {
            *driver = Some(self.start_driver()?);
        }
        Ok(())
    }

    fn start_driver(self: &Arc<Self>) -> std::io::Result<Driver> {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let target = SendPtr(Arc::as_ptr(self));
        let thread = std::thread::Builder::new()
            .name(format!("orbit-pool-{}-driver", self.node))
            .spawn(move || {
                let target = target;
                // SAFETY: `Table::drop` stops and joins this thread before
                // the mapping goes away.
                let table = unsafe { &*target.0 };
                table.run_driver(&thread_stop);
            })?;
        Ok(Driver {
            stop,
            thread: Some(thread),
            pid: std::process::id(),
        })
    }

    fn run_driver(&self, stop: &std::sync::atomic::AtomicBool) {
        let node = usize::from(self.node);
        let doorbell = self.doorbell(node);
        doorbell
            .incarnation
            .store(self.incarnation, Ordering::SeqCst);
        doorbell.listening.fetch_add(1, Ordering::SeqCst);
        let mut seen = doorbell.generation.load(Ordering::SeqCst);
        while !stop.load(Ordering::Acquire) {
            for word in 0..self.geometry.key_words {
                let mut bits = self.pending_word(node, word).swap(0, Ordering::SeqCst);
                if bits != 0 {
                    // Interest is re-registered by whoever polls again.
                    self.interest_word(node, word)
                        .fetch_and(!bits, Ordering::SeqCst);
                }
                while bits != 0 {
                    let bit = bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    let key_index = word * 64 + bit;
                    if key_index < POOL_KEY_CAPACITY {
                        self.wake(key_index);
                    }
                }
            }
            let now = doorbell.generation.load(Ordering::SeqCst);
            if now != seen {
                seen = now;
                continue;
            }
            match crate::wait_on(&doorbell.generation, seen) {
                Ok(()) => {}
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::Unsupported => {
                    std::thread::sleep(FALLBACK_POLL_INTERVAL);
                }
                Err(_) => break,
            }
        }
        doorbell.listening.fetch_sub(1, Ordering::SeqCst);
    }

    /// A confirmed death: close every resource that incarnation of `node`
    /// owned and drop the creation claims it held.
    pub(crate) fn node_dead(&self, node: u16, incarnation: u64) {
        for (index, slot) in self.resources().iter().enumerate() {
            let state = slot.state.load(Ordering::Acquire);
            if (state != crate::layout::RESOURCE_LIVE && state != crate::layout::RESOURCE_DRAINING)
                || slot.owner_node.load(Ordering::Acquire) != node
                || slot.owner_incarnation.load(Ordering::Acquire) != incarnation
            {
                continue;
            }
            self.close_resource(index, slot);
        }
        for key_index in 0..POOL_KEY_CAPACITY {
            let held = self
                .claims(usize::from(node), key_index)
                .swap(0, Ordering::SeqCst);
            if held > 0 {
                let key = self.key(key_index);
                let _ = key
                    .counts
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |counts| {
                        let (live, creating) = crate::layout::unpack_counts(counts);
                        Some(crate::layout::pack_counts(
                            live,
                            creating.saturating_sub(held),
                        ))
                    });
                self.key_changed(key_index);
            }
        }
        let doorbell = self.doorbell(usize::from(node));
        if doorbell.incarnation.load(Ordering::SeqCst) == incarnation {
            doorbell.listening.store(0, Ordering::SeqCst);
            doorbell.incarnation.store(0, Ordering::SeqCst);
        }
    }

    /// The resource is gone: mark it, take it out of its key's members and
    /// live count, wake the key. The slot is reused by its lane later.
    pub(crate) fn close_resource(&self, index: usize, slot: &ResourceSlot) {
        if slot
            .state
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |state| {
                (state == crate::layout::RESOURCE_LIVE || state == crate::layout::RESOURCE_DRAINING)
                    .then_some(crate::layout::RESOURCE_CLOSED)
            })
            .is_err()
        {
            return;
        }
        let key_index = slot.key_index.load(Ordering::Acquire) as usize;
        self.members(key_index)[index / 64].fetch_and(!(1_u64 << (index % 64)), Ordering::SeqCst);
        let key = self.key(key_index);
        let _ = key
            .counts
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |counts| {
                let (live, creating) = crate::layout::unpack_counts(counts);
                Some(crate::layout::pack_counts(live.saturating_sub(1), creating))
            });
        self.key_changed(key_index);
    }

    pub(crate) fn reset_all(&self) {
        let mut hint = lock_unpoisoned(&self.structural);
        *hint = 0;
        let header = self.header();
        let epoch = header.epoch.load(Ordering::Acquire);
        header.epoch.store(next_epoch(epoch), Ordering::Release);
        for slot in self.resources() {
            slot.state.store(RESOURCE_EMPTY, Ordering::Release);
            slot.generation.store(0, Ordering::Relaxed);
        }
        for index in 0..POOL_KEY_CAPACITY {
            let key = self.key(index);
            key.state.store(KEY_EMPTY, Ordering::Release);
            key.changes.fetch_add(1, Ordering::SeqCst);
            crate::wake_on(&key.changes);
        }
        for node in 0..self.geometry.fleet_capacity {
            for word in 0..self.geometry.key_words {
                self.pending_word(node, word).store(0, Ordering::Relaxed);
                self.interest_word(node, word).store(0, Ordering::Relaxed);
            }
            for key_index in 0..POOL_KEY_CAPACITY {
                self.claims(node, key_index).store(0, Ordering::Relaxed);
            }
        }
        for wakers in &self.wakers {
            lock_unpoisoned(wakers).clear();
        }
    }

    #[cfg(unix)]
    pub(crate) fn unlink(&self) -> Result<()> {
        match &self.backing {
            Backing::Memory(_) => {
                self.reset_all();
                Ok(())
            }
            Backing::Shm(region) => region.unlink().map_err(Error::Io),
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        if let Some(mut driver) = lock_unpoisoned(&self.driver).take() {
            driver.stop.store(true, Ordering::Release);
            let generation = &self.doorbell(usize::from(self.node)).generation;
            generation.fetch_add(1, Ordering::SeqCst);
            crate::wake_on(generation);
            if let Some(thread) = driver.thread.take()
                && driver.pid == std::process::id()
            {
                let _ = thread.join();
            }
        }
    }
}

struct SendPtr<T>(*const T);

// SAFETY: the pointee is `Sync` and outlives the thread (see `start_driver`).
unsafe impl<T: Sync> Send for SendPtr<T> {}

fn mix(lo: u64, hi: u64) -> u64 {
    // The caller brings a digest; one multiply spreads a weak one.
    (lo ^ hi.rotate_left(32)).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn next_epoch(previous: u64) -> u64 {
    OrbitEpoch::now().as_unix_ms().max(previous + 1)
}

#[derive(Hash, PartialEq, Eq)]
enum Key {
    Memory(usize),
    #[cfg(unix)]
    Shm(String, u16),
}

static TABLES: LazyLock<Mutex<HashMap<Key, Weak<Table>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) fn open(fleet: &Arc<Fleet>, incarnation: Incarnation) -> Result<Arc<Table>> {
    let key = if fleet.is_shm() {
        #[cfg(unix)]
        {
            Key::Shm(
                ring_segment_name(fleet.name(), POOL_KIND),
                fleet.node_id().get(),
            )
        }
        #[cfg(not(unix))]
        unreachable!("non-Unix fleets cannot use POSIX SHM")
    } else {
        Key::Memory(Arc::as_ptr(fleet) as usize)
    };
    let mut tables = lock_unpoisoned(&TABLES);
    tables.retain(|_, table| table.strong_count() > 0);
    if let Some(table) = tables.get(&key).and_then(Weak::upgrade) {
        if table.incarnation != incarnation.get() {
            return Err(Error::Malformed(format!(
                "this process already opened the pool table as incarnation {}",
                table.incarnation
            )));
        }
        return Ok(table);
    }
    let geometry = Geometry::new(fleet.fleet_capacity());
    let backing = match &key {
        Key::Memory(_) => {
            let bytes = AlignedBytes::zeroed(geometry.segment_size);
            // SAFETY: freshly allocated, aligned, large enough for the header.
            unsafe {
                std::ptr::write(
                    bytes.ptr.cast::<Header>(),
                    Header::new(fleet.fleet_capacity(), geometry.key_stride, next_epoch(0)),
                )
            };
            Backing::Memory(bytes)
        }
        #[cfg(unix)]
        Key::Shm(name, _) => Backing::Shm(open_shm(name, fleet.fleet_capacity(), &geometry)?),
    };
    let table = Arc::new(Table {
        _fleet: Arc::clone(fleet),
        backing,
        geometry,
        node: fleet.node_id().get(),
        incarnation: incarnation.get(),
        structural: Mutex::new(0),
        wakers: (0..POOL_KEY_CAPACITY)
            .map(|_| Mutex::new(Vec::new()))
            .collect(),
        driver: Mutex::new(None),
    });
    tables.insert(key, Arc::downgrade(&table));
    Ok(table)
}

#[cfg(unix)]
fn open_shm(name: &str, fleet_capacity: u16, geometry: &Geometry) -> Result<ShmRegion> {
    use std::io;

    let (region, _initialization_lock) =
        ShmRegion::open_or_create_locked(name, geometry.segment_size)?;
    if region.created() {
        // SAFETY: a fresh zero-filled mapping at least a header long.
        unsafe {
            std::ptr::write(
                region.as_ptr().cast::<Header>(),
                Header::new(fleet_capacity, geometry.key_stride, next_epoch(0)),
            );
        }
    } else {
        if region.len() < geometry.segment_size {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("SHM segment {name} is smaller than the pool table"),
            )));
        }
        // SAFETY: the region is at least a header long.
        let header = unsafe { &*region.as_ptr().cast::<Header>() };
        if header.magic != crate::layout::MAGIC {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {name} has wrong magic 0x{:08X} (expected 0x{:08X})",
                    header.magic,
                    crate::layout::MAGIC
                ),
            )));
        }
        if !header.compatible(fleet_capacity, geometry.key_stride) {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("SHM segment {name} has an incompatible pool-table layout"),
            )));
        }
    }
    Ok(region)
}

/// Bytes the segment needs for `fleet_capacity` lanes.
pub fn segment_size(fleet_capacity: u16) -> usize {
    Geometry::new(fleet_capacity).segment_size
}

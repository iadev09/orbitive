//! The table behind a `Streams` handle: the mapped segment (or its in-memory
//! twin), the lane this process allocates in, and the process-local
//! readiness pieces. One table per process per fleet and node, so two
//! handles in one process share one driver and one waker registry.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};

#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};
use orbit_core::{Fleet, OrbitEpoch};

use crate::layout::{
    Direction, Doorbell, FLAG_READER_GONE, FLAG_RESET, Geometry, Header, SIDE_CLAIMED, SIDE_DEAD,
    SLOT_EMPTY, SLOT_LIVE, Slot,
};
use crate::wake::{Doorstep, Driver, Interest, Registry};
use crate::readiness::{Readiness, Signal};
use crate::{Error, Incarnation, Result, StreamSpec, lock_unpoisoned};

enum Backing {
    Memory(AlignedBytes),
    #[cfg(unix)]
    Shm(ShmRegion),
}

/// Heap bytes aligned like the mapping, so slots keep their cache lines in
/// the standalone backend too.
struct AlignedBytes {
    ptr: *mut u8,
    layout: std::alloc::Layout,
}

impl AlignedBytes {
    fn zeroed(size: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(size, 64).expect("segment layout");
        // SAFETY: the layout has a nonzero size (a header at least).
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "stream table allocation failed");
        Self { ptr, layout }
    }
}

impl Drop for AlignedBytes {
    fn drop(&mut self) {
        // SAFETY: allocated with this layout in `zeroed`.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) }
    }
}

// SAFETY: the bytes are only ever read through atomics or under the
// direction ownership rules; the pointer is not shared outside `Table`.
unsafe impl Send for AlignedBytes {}
unsafe impl Sync for AlignedBytes {}

pub(crate) struct Table {
    /// Held so the fleet's address, which keys the in-memory registry,
    /// cannot be reused by another fleet while this table lives.
    _fleet: Arc<Fleet>,
    backing: Backing,
    geometry: Geometry,
    /// The segment's kind, from the spec this table was opened with.
    kind: u8,
    node: u16,
    incarnation: u64,
    /// Serialises this process's allocations in its own lane. One live
    /// process per node id is already the fleet's rule for per-node rings,
    /// so no cross-process lock is needed here.
    allocate: Mutex<usize>,
    pub(crate) registry: Registry,
    driver: Mutex<Option<Driver>>,
    /// The driver's end of this process's readiness descriptor, once
    /// somebody has asked for one. Read on every drain, so it is a
    /// `OnceLock` rather than a lock.
    readiness: std::sync::OnceLock<Signal>,
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

    /// The kind this table's segment lives under.
    pub(crate) fn kind(&self) -> u8 {
        self.kind
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
        // SAFETY: `fleet_capacity` doorbells follow the header, zeroed at
        // creation; every field is atomic.
        unsafe {
            &*self
                .base()
                .add(self.geometry.doorbells_offset + node * size_of::<Doorbell>())
                .cast::<Doorbell>()
        }
    }

    fn bitmap_word(&self, offset: usize, node: usize, word: usize) -> &AtomicU64 {
        debug_assert!(node < self.geometry.fleet_capacity && word < self.geometry.bitmap_words);
        // SAFETY: both bitmaps sit between the doorbells and the slots;
        // atomic words only.
        unsafe {
            &*self
                .base()
                .add(offset + (node * self.geometry.bitmap_words + word) * size_of::<AtomicU64>())
                .cast::<AtomicU64>()
        }
    }

    fn pending_word(&self, node: usize, word: usize) -> &AtomicU64 {
        self.bitmap_word(self.geometry.pending_offset, node, word)
    }

    fn offer_word(&self, node: usize, word: usize) -> &AtomicU64 {
        self.bitmap_word(self.geometry.offers_offset, node, word)
    }

    pub(crate) fn slots(&self) -> &[Slot] {
        // SAFETY: `total_slots` slots follow the bitmaps, zeroed at creation;
        // every field is atomic, so shared references are sound.
        unsafe {
            std::slice::from_raw_parts(
                self.base().add(self.geometry.slots_offset).cast::<Slot>(),
                self.geometry.total_slots,
            )
        }
    }

    /// The bytes of one direction's ring. Only the direction's writer
    /// writes and only its reader reads, at disjoint positions.
    pub(crate) fn buffer(&self, slot: usize, direction: usize) -> *mut u8 {
        // SAFETY: inside the segment by construction of `Geometry`.
        unsafe {
            self.base()
                .add(self.geometry.buffer_offset(slot, direction))
        }
    }

    /// Take a free slot in this process's lane and install a fresh stream.
    /// Returns the slot index and its generation.
    pub(crate) fn allocate(&self) -> Result<(usize, u32)> {
        let mut hint = lock_unpoisoned(&self.allocate);
        let lane_capacity = self.geometry.lane_capacity;
        let lane_start = usize::from(self.node) * lane_capacity;
        let slots = self.slots();
        for offset in 0..lane_capacity {
            let index = lane_start + ((*hint + offset) & (lane_capacity - 1));
            let slot = &slots[index];
            if slot.state.load(Ordering::Acquire) == SLOT_EMPTY
                && let Some(generation) = slot.install(self.node, self.incarnation)
            {
                *hint = (index - lane_start + 1) & (lane_capacity - 1);
                return Ok((index, generation));
            }
        }
        Err(Error::Full {
            capacity: lane_capacity,
        })
    }

    /// After anything changed on `direction` of `slot`: count it for
    /// blocking waiters, then, when `ring` says the change is one a parked
    /// task could be waiting for, tell the processes holding the stream's
    /// sides. Blocking waiters are always counted; they park on the word
    /// itself and are woken only when present.
    pub(crate) fn notify(&self, index: usize, slot: &Slot, direction: &Direction, ring: bool) {
        direction.changes.fetch_add(1, Ordering::SeqCst);
        if direction.waiters.load(Ordering::SeqCst) > 0 {
            crate::wake_on(&direction.changes);
        }
        if !ring {
            return;
        }
        if !self.is_shared() {
            self.registry.wake(index);
            self.signal_readiness();
            return;
        }
        let first = usize::from(slot.node[0].load(Ordering::Acquire));
        let second = usize::from(slot.node[1].load(Ordering::Acquire));
        if first < self.geometry.fleet_capacity {
            self.ring(first, Some(index));
        }
        if second < self.geometry.fleet_capacity && second != first {
            self.ring(second, Some(index));
        }
    }

    /// Set the slot's bit for `node` (when there is one) and ring its
    /// doorbell if a driver or a blocking waiter is listening.
    fn ring(&self, node: usize, pending: Option<usize>) {
        if let Some(index) = pending {
            self.pending_word(node, index / 64)
                .fetch_or(1 << (index % 64), Ordering::SeqCst);
        }
        let doorbell = self.doorbell(node);
        doorbell.generation.fetch_add(1, Ordering::SeqCst);
        if doorbell.listening.load(Ordering::SeqCst) > 0 {
            crate::wake_on(&doorbell.generation);
        }
    }

    /// Put the stream in `node`'s offer bitmap and ring it.
    pub(crate) fn offer(&self, index: usize, node: u16) -> Result<()> {
        let node = usize::from(node);
        if node >= self.geometry.fleet_capacity {
            return Err(Error::Malformed(format!(
                "node {node} is outside the fleet"
            )));
        }
        // One process holds every node of a memory table: the offer goes to
        // whoever takes offers here, and the local waker is told directly.
        let node = if self.is_shared() {
            node
        } else {
            usize::from(self.node)
        };
        self.offer_word(node, index / 64)
            .fetch_or(1 << (index % 64), Ordering::SeqCst);
        self.ring(node, None);
        if !self.is_shared() {
            self.registry.wake_offer();
            self.signal_readiness();
        }
        Ok(())
    }

    /// Take one offered slot for this node, if any, clearing its bit.
    pub(crate) fn take_offer(&self) -> Option<usize> {
        let node = usize::from(self.node);
        for word in 0..self.geometry.bitmap_words {
            let place = self.offer_word(node, word);
            let mut bits = place.load(Ordering::SeqCst);
            while bits != 0 {
                let bit = bits.trailing_zeros() as u64;
                let mask = 1_u64 << bit;
                if place.fetch_and(!mask, Ordering::SeqCst) & mask != 0 {
                    let index = word * 64 + bit as usize;
                    if index < self.geometry.total_slots {
                        return Some(index);
                    }
                }
                bits &= !mask;
            }
        }
        None
    }

    fn has_offer(&self) -> bool {
        let node = usize::from(self.node);
        (0..self.geometry.bitmap_words)
            .any(|word| self.offer_word(node, word).load(Ordering::SeqCst) != 0)
    }

    /// Park the calling thread until an offer arrives for this node.
    pub(crate) fn wait_offer(&self) -> Result<()> {
        let doorbell = self.doorbell(usize::from(self.node));
        loop {
            if self.has_offer() {
                return Ok(());
            }
            doorbell.listening.fetch_add(1, Ordering::SeqCst);
            let seen = doorbell.generation.load(Ordering::SeqCst);
            let outcome = if self.has_offer() {
                Ok(())
            } else {
                crate::wait_on(&doorbell.generation, seen)
            };
            doorbell.listening.fetch_sub(1, Ordering::SeqCst);
            outcome?;
        }
    }

    /// Park a task on `slot`; in a fleet this also starts the process's
    /// driver the first time anyone asks.
    pub(crate) fn register(
        self: &Arc<Self>,
        slot: usize,
        interest: Interest,
        waker: &std::task::Waker,
    ) -> Result<()> {
        self.registry.register(slot, interest, waker);
        self.ensure_driver()
    }

    pub(crate) fn register_offer(self: &Arc<Self>, waker: &std::task::Waker) -> Result<()> {
        self.registry.register_offer(waker);
        self.ensure_driver()
    }

    /// Hand out this table's readiness descriptor, once. The driver is
    /// what signals it, so asking for one starts it.
    pub(crate) fn take_readiness(self: &Arc<Self>) -> Result<Readiness> {
        let (readiness, signal) = crate::readiness::pair()?;
        self.readiness.set(signal).map_err(|_| {
            Error::Malformed(
                "this process already took the stream table's readiness descriptor".to_owned(),
            )
        })?;
        self.ensure_driver()?;
        Ok(readiness)
    }

    fn signal_readiness(&self) {
        if let Some(signal) = self.readiness.get() {
            signal.signal();
        }
    }

    fn ensure_driver(self: &Arc<Self>) -> Result<()> {
        if self.is_shared() {
            let mut driver = lock_unpoisoned(&self.driver);
            if driver.is_none() {
                *driver = Some(Driver::start(
                    Arc::as_ptr(self),
                    format!("orbit-stream-{}-driver", self.node),
                )?);
            }
        }
        Ok(())
    }

    /// A confirmed death: every side that incarnation of `node` held is
    /// finished. Its writes reset, its reads are gone, the other holder is
    /// woken; the slot itself empties only when nobody holds it any more.
    pub(crate) fn node_dead(&self, node: u16, incarnation: u64) {
        for (index, slot) in self.slots().iter().enumerate() {
            if slot.state.load(Ordering::Acquire) != SLOT_LIVE {
                continue;
            }
            let mut touched = false;
            for side in 0..2 {
                if slot.claimed[side].load(Ordering::SeqCst) != SIDE_CLAIMED
                    || slot.node[side].load(Ordering::Acquire) != node
                    || slot.incarnation[side].load(Ordering::Acquire) != incarnation
                {
                    continue;
                }
                if slot.claimed[side]
                    .compare_exchange(SIDE_CLAIMED, SIDE_DEAD, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
                {
                    continue;
                }
                touched = true;
                let writes = &slot.directions[side];
                let reads = &slot.directions[1 - side];
                writes.flags.fetch_or(FLAG_RESET, Ordering::SeqCst);
                reads.flags.fetch_or(FLAG_READER_GONE, Ordering::SeqCst);
                self.notify(index, slot, writes, true);
                self.notify(index, slot, reads, true);
            }
            if touched
                && slot
                    .claimed
                    .iter()
                    .all(|side| side.load(Ordering::SeqCst) != SIDE_CLAIMED)
            {
                let _ = slot.state.compare_exchange(
                    SLOT_LIVE,
                    SLOT_EMPTY,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        }
        let doorbell = self.doorbell(usize::from(node));
        if doorbell.incarnation.load(Ordering::SeqCst) == incarnation {
            doorbell.listening.store(0, Ordering::SeqCst);
            doorbell.incarnation.store(0, Ordering::SeqCst);
        }
    }

    /// Clear every slot, doorbell and bitmap during quiescent owner boot and
    /// start a new epoch, so every ticket minted before is refused.
    pub(crate) fn reset_all(&self) {
        let mut hint = lock_unpoisoned(&self.allocate);
        *hint = 0;
        let header = self.header();
        let epoch = header.epoch.load(Ordering::Acquire);
        header.epoch.store(next_epoch(epoch), Ordering::Release);
        for slot in self.slots() {
            slot.state.store(SLOT_EMPTY, Ordering::Release);
            slot.generation.store(0, Ordering::Relaxed);
            for direction in &slot.directions {
                direction.changes.fetch_add(1, Ordering::SeqCst);
                crate::wake_on(&direction.changes);
            }
        }
        for node in 0..self.geometry.fleet_capacity {
            for word in 0..self.geometry.bitmap_words {
                self.pending_word(node, word).store(0, Ordering::Relaxed);
                self.offer_word(node, word).store(0, Ordering::Relaxed);
            }
        }
        self.registry.clear();
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

/// A fresh epoch: the clock, unless the previous epoch is already ahead
/// of it, so a recreated segment never repeats an epoch a ticket may hold.
fn next_epoch(previous: u64) -> u64 {
    OrbitEpoch::now().as_unix_ms().max(previous + 1)
}

impl Doorstep for Table {
    fn generation(&self) -> &AtomicU32 {
        &self.doorbell(usize::from(self.node)).generation
    }

    fn listening(&self, delta: i32) {
        let doorbell = self.doorbell(usize::from(self.node));
        if delta > 0 {
            doorbell
                .incarnation
                .store(self.incarnation, Ordering::SeqCst);
            doorbell.listening.fetch_add(1, Ordering::SeqCst);
        } else {
            doorbell.listening.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn drain(&self) {
        let node = usize::from(self.node);
        for word in 0..self.geometry.bitmap_words {
            let mut bits = self.pending_word(node, word).swap(0, Ordering::SeqCst);
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let index = word * 64 + bit;
                if index < self.geometry.total_slots {
                    self.registry.wake(index);
                }
            }
        }
        if self.has_offer() {
            self.registry.wake_offer();
        }
        // After the bits are taken, never before: a consumer that drains
        // the descriptor and re-tries its streams cannot miss what this
        // pass just made ready.
        self.signal_readiness();
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        if let Some(mut driver) = lock_unpoisoned(&self.driver).take() {
            driver.stop(self.generation());
        }
    }
}

#[derive(Hash, PartialEq, Eq)]
enum Key {
    Memory(usize, u8),
    #[cfg(unix)]
    Shm(String, u16),
}

static TABLES: LazyLock<Mutex<HashMap<Key, Weak<Table>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The table `spec` names for `fleet`, shared by every handle in this
/// process. The kind is part of the identity in both backings: two specs
/// are two tables, in one process as in the fleet.
pub(crate) fn open(
    fleet: &Arc<Fleet>,
    incarnation: Incarnation,
    spec: StreamSpec,
) -> Result<Arc<Table>> {
    let key = if fleet.is_shm() {
        #[cfg(unix)]
        {
            Key::Shm(
                ring_segment_name(fleet.name(), spec.kind),
                fleet.node_id().get(),
            )
        }
        #[cfg(not(unix))]
        unreachable!("non-Unix fleets cannot use POSIX SHM")
    } else {
        Key::Memory(Arc::as_ptr(fleet) as usize, spec.kind)
    };
    let mut tables = lock_unpoisoned(&TABLES);
    tables.retain(|_, table| table.strong_count() > 0);
    if let Some(table) = tables.get(&key).and_then(Weak::upgrade) {
        if table.incarnation != incarnation.get() {
            return Err(Error::Malformed(format!(
                "this process already opened the stream table as incarnation {}",
                table.incarnation
            )));
        }
        // One kind, one geometry: a second spec for the same segment is a
        // mismatch here rather than a silently shared table.
        if table.geometry.lane_capacity != spec.lane_capacity
            || table.geometry.buffer_bytes != spec.buffer_bytes
        {
            return Err(Error::Malformed(format!(
                "this process already opened kind {} with lane_capacity={} buffer_bytes={}",
                spec.kind, table.geometry.lane_capacity, table.geometry.buffer_bytes
            )));
        }
        return Ok(table);
    }
    let geometry = Geometry::new(fleet.fleet_capacity(), spec);
    let backing = match &key {
        Key::Memory(..) => {
            let bytes = AlignedBytes::zeroed(geometry.segment_size);
            // SAFETY: freshly allocated, aligned, large enough for the header.
            unsafe {
                std::ptr::write(
                    bytes.ptr.cast::<Header>(),
                    Header::new(fleet.fleet_capacity(), &geometry, next_epoch(0)),
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
        kind: spec.kind,
        node: fleet.node_id().get(),
        incarnation: incarnation.get(),
        allocate: Mutex::new(0),
        registry: Registry::new(geometry.total_slots),
        driver: Mutex::new(None),
        readiness: std::sync::OnceLock::new(),
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
        // The kernel hands out zero pages; only the header is written, so
        // the buffers stay untouched and unbacked until a stream uses them.
        // SAFETY: a fresh mapping at least a header long.
        unsafe {
            std::ptr::write(
                region.as_ptr().cast::<Header>(),
                Header::new(fleet_capacity, geometry, next_epoch(0)),
            );
        }
    } else {
        if region.len() < geometry.segment_size {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("SHM segment {name} is smaller than the stream table"),
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
        if !header.compatible(fleet_capacity, geometry) {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("SHM segment {name} has an incompatible stream-table layout"),
            )));
        }
    }
    Ok(region)
}

/// Bytes the segment needs for `fleet_capacity` lanes; what a lifecycle
/// tool validates an existing object against without mapping it read-write.
pub fn segment_size(fleet_capacity: u16) -> usize {
    segment_size_for(fleet_capacity, StreamSpec::DEFAULT)
}

/// Bytes `spec`'s segment needs for `fleet_capacity` lanes.
pub fn segment_size_for(fleet_capacity: u16, spec: StreamSpec) -> usize {
    Geometry::new(fleet_capacity, spec).segment_size
}

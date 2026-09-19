use std::collections::HashMap;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};

use super::ChunkDescriptor;
use crate::wake::{Doorstep, Driver};
use crate::{Error, Incarnation, Result, lock_unpoisoned};

const MAGIC: u32 = 0x4F_50_41_59; // "OPAY"
const VERSION: u16 = 4;

const SLOT_FREE: u8 = 0;
const SLOT_RESERVED: u8 = 1;
const SLOT_LIVE: u8 = 2;
const SLOT_READING: u8 = 3;
const SLOT_RECLAIMING: u8 = 4;

/// Geometry and SHM identity of one fleet-wide payload arena.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PayloadArenaSpec {
    pub kind: u8,
    /// Physical allocation units available to each fleet node.
    pub slots_per_node: usize,
    /// Bytes in one physical allocation unit.
    pub slot_size: usize
}

impl PayloadArenaSpec {
    pub const fn new(
        kind: u8,
        slots_per_node: usize,
        slot_size: usize
    ) -> Self {
        Self { kind, slots_per_node, slot_size }
    }

    pub const fn lane_bytes(self) -> usize {
        self.slots_per_node * self.slot_size
    }

    pub const fn slots_for(
        self,
        payload_len: usize
    ) -> usize {
        payload_len.div_ceil(self.slot_size)
    }

    fn validate(self) -> Result<()> {
        if self.slots_per_node == 0
            || self.slot_size == 0
            || !self.slots_per_node.is_power_of_two()
            || !self.slot_size.is_power_of_two()
            || self.slots_per_node > u32::MAX as usize
            || self.slot_size > u32::MAX as usize
        {
            return Err(Error::Malformed(format!(
                "payload arena kind={} slots_per_node={} slot_size={}: both geometry values must be non-zero powers of two fitting u32",
                self.kind, self.slots_per_node, self.slot_size
            )));
        }
        Ok(())
    }
}

#[repr(C, align(64))]
struct Header {
    magic: u32,
    version: u16,
    header_size: u16,
    slot_meta_size: u32,
    slot_size: u32,
    slots_per_node: u32,
    fleet_capacity: u16,
    _reserved: [u8; 2],
    next_generation: AtomicU64,
    epoch: AtomicU64,
    _reserved_credit: [u8; 24]
}

impl Header {
    fn new(
        fleet_capacity: u16,
        spec: PayloadArenaSpec
    ) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            header_size: size_of::<Self>() as u16,
            slot_meta_size: size_of::<SlotMeta>() as u32,
            slot_size: spec.slot_size as u32,
            slots_per_node: spec.slots_per_node as u32,
            fleet_capacity,
            _reserved: [0; 2],
            next_generation: AtomicU64::new(1),
            epoch: AtomicU64::new(1),
            _reserved_credit: [0; 24]
        }
    }

    fn compatible(
        &self,
        fleet_capacity: u16,
        spec: PayloadArenaSpec
    ) -> bool {
        self.magic == MAGIC
            && self.version == VERSION
            && self.header_size as usize == size_of::<Self>()
            && self.slot_meta_size as usize == size_of::<SlotMeta>()
            && self.slot_size as usize == spec.slot_size
            && self.slots_per_node as usize == spec.slots_per_node
            && self.fleet_capacity == fleet_capacity
    }
}

#[repr(C, align(64))]
struct CreditWord {
    generation: AtomicU32,
    waiters: AtomicU32,
    _padding: [u8; 56]
}

#[repr(C, align(8))]
struct SlotMeta {
    state: AtomicU8,
    _reserved: AtomicU8,
    reader_node: std::sync::atomic::AtomicU16,
    allocation_first: AtomicU64,
    generation: AtomicU64,
    producer_incarnation: AtomicU64,
    reader_incarnation: AtomicU64
}

const _: () = assert!(size_of::<Header>() == 64);
const _: () = assert!(size_of::<CreditWord>() == 64);
const _: () = assert!(size_of::<SlotMeta>() == 40);

#[derive(Clone, Copy)]
struct Geometry {
    slots_per_node: usize,
    slot_size: usize,
    total_slots: usize,
    credits_offset: usize,
    metadata_offset: usize,
    payload_offset: usize,
    segment_size: usize
}

impl Geometry {
    fn new(
        fleet_capacity: u16,
        spec: PayloadArenaSpec
    ) -> Self {
        let total_slots = usize::from(fleet_capacity) * spec.slots_per_node;
        let credits_offset = size_of::<Header>();
        let metadata_offset =
            credits_offset + usize::from(fleet_capacity) * size_of::<CreditWord>();
        let payload_offset =
            (metadata_offset + total_slots * size_of::<SlotMeta>()).next_multiple_of(64);
        let segment_size = payload_offset + total_slots * spec.slot_size;
        Self {
            slots_per_node: spec.slots_per_node,
            slot_size: spec.slot_size,
            total_slots,
            credits_offset,
            metadata_offset,
            payload_offset,
            segment_size
        }
    }
}

enum Backing {
    Memory(AlignedBytes),
    #[cfg(unix)]
    Shm(ShmRegion)
}

struct AlignedBytes {
    ptr: *mut u8,
    layout: std::alloc::Layout
}

impl AlignedBytes {
    fn zeroed(size: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(size, 64).expect("payload arena layout");
        // SAFETY: geometry always includes the non-empty header.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "payload arena allocation failed");
        Self { ptr, layout }
    }
}

impl Drop for AlignedBytes {
    fn drop(&mut self) {
        // SAFETY: allocated by `alloc_zeroed` with this exact layout.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) }
    }
}

// SAFETY: mapped bytes are accessed through atomics and allocation ownership.
unsafe impl Send for AlignedBytes {}
unsafe impl Sync for AlignedBytes {}

struct Arena {
    _fleet: Arc<Fleet>,
    backing: Backing,
    geometry: Geometry,
    kind: u8,
    node: u16,
    incarnation: Incarnation,
    allocation: Mutex<usize>,
    credit_wakers: Mutex<Vec<Waker>>,
    driver: Mutex<Option<Driver>>
}

impl Arena {
    fn base(&self) -> *mut u8 {
        match &self.backing {
            Backing::Memory(bytes) => bytes.ptr,
            #[cfg(unix)]
            Backing::Shm(region) => region.as_ptr()
        }
    }

    fn header(&self) -> &Header {
        // SAFETY: every backing begins with the initialized arena header.
        unsafe { &*self.base().cast::<Header>() }
    }

    fn metadata(&self) -> &[SlotMeta] {
        // SAFETY: geometry reserves this aligned range for atomic metadata.
        unsafe {
            std::slice::from_raw_parts(
                self.base().add(self.geometry.metadata_offset).cast::<SlotMeta>(),
                self.geometry.total_slots
            )
        }
    }

    fn credits(&self) -> &[CreditWord] {
        // SAFETY: geometry reserves one cache-line-aligned word per fleet node.
        unsafe {
            std::slice::from_raw_parts(
                self.base().add(self.geometry.credits_offset).cast::<CreditWord>(),
                self.geometry.total_slots / self.geometry.slots_per_node
            )
        }
    }

    fn local_credit(&self) -> &CreditWord {
        &self.credits()[usize::from(self.node)]
    }

    fn payload(
        &self,
        absolute_slot: usize
    ) -> *mut u8 {
        // SAFETY: callers validate the slot against `total_slots`.
        unsafe {
            self.base().add(self.geometry.payload_offset + absolute_slot * self.geometry.slot_size)
        }
    }

    fn reserve(
        self: &Arc<Self>,
        payload_len: usize
    ) -> Result<Publication> {
        let count = self.required_slots(payload_len)?;

        let mut hint = lock_unpoisoned(&self.allocation);
        let lane_start = usize::from(self.node) * self.geometry.slots_per_node;
        let candidate = (0..self.geometry.slots_per_node)
            .map(|offset| (*hint + offset) & (self.geometry.slots_per_node - 1))
            .find(|&local| {
                local + count <= self.geometry.slots_per_node
                    && self.metadata()[lane_start + local..lane_start + local + count]
                        .iter()
                        .all(|slot| slot.state.load(Ordering::Acquire) == SLOT_FREE)
            })
            .ok_or(Error::PayloadFull { requested_slots: count })?;
        let first = lane_start + candidate;
        for slot in &self.metadata()[first..first + count] {
            slot.state.store(SLOT_RESERVED, Ordering::Relaxed);
        }
        let mut generation = self.header().next_generation.fetch_add(1, Ordering::Relaxed);
        if generation == 0 {
            generation = self.header().next_generation.fetch_add(1, Ordering::Relaxed);
        }
        for slot in &self.metadata()[first..first + count] {
            slot.allocation_first.store(first as u64, Ordering::Relaxed);
            slot.generation.store(generation, Ordering::Relaxed);
            slot.producer_incarnation.store(self.incarnation.get(), Ordering::Relaxed);
            slot.reader_node.store(u16::MAX, Ordering::Relaxed);
            slot.reader_incarnation.store(0, Ordering::Relaxed);
        }
        self.metadata()[first].state.store(SLOT_RESERVED, Ordering::Release);
        *hint = (candidate + count) & (self.geometry.slots_per_node - 1);
        Ok(Publication {
            arena: Arc::clone(self),
            first,
            count,
            payload_len,
            generation,
            published: false
        })
    }

    fn release(
        &self,
        first: usize,
        count: usize,
        generation: u64
    ) {
        let slots = &self.metadata()[first..first + count];
        if slots.iter().any(|slot| slot.generation.load(Ordering::Acquire) != generation) {
            return;
        }
        for slot in slots.iter().skip(1) {
            slot.state.store(SLOT_FREE, Ordering::Relaxed);
        }
        slots[0].state.store(SLOT_FREE, Ordering::Release);
        let owner = first / self.geometry.slots_per_node;
        let credit = &self.credits()[owner];
        credit.generation.fetch_add(1, Ordering::SeqCst);
        if credit.waiters.load(Ordering::SeqCst) > 0 {
            crate::wake_on(&credit.generation);
        }
    }

    fn required_slots(
        &self,
        payload_len: usize
    ) -> Result<usize> {
        if payload_len == 0 {
            return Err(Error::Malformed("an empty payload needs no arena allocation".to_owned()));
        }
        let count = payload_len.div_ceil(self.geometry.slot_size);
        if count > self.geometry.slots_per_node {
            return Err(Error::PayloadTooLarge {
                len: payload_len,
                capacity: self.geometry.slots_per_node * self.geometry.slot_size
            });
        }
        Ok(count)
    }

    fn has_run(
        &self,
        count: usize
    ) -> bool {
        let lane_start = usize::from(self.node) * self.geometry.slots_per_node;
        (0..=self.geometry.slots_per_node - count).any(|local| {
            self.metadata()[lane_start + local..lane_start + local + count]
                .iter()
                .all(|slot| slot.state.load(Ordering::Acquire) == SLOT_FREE)
        })
    }

    fn wait_available(
        &self,
        count: usize
    ) -> Result<()> {
        let credit = self.local_credit();
        loop {
            if self.has_run(count) {
                return Ok(());
            }
            credit.waiters.fetch_add(1, Ordering::SeqCst);
            let seen = credit.generation.load(Ordering::SeqCst);
            let outcome =
                if self.has_run(count) { Ok(()) } else { crate::wait_on(&credit.generation, seen) };
            credit.waiters.fetch_sub(1, Ordering::SeqCst);
            outcome?;
        }
    }

    fn poll_available(
        self: &Arc<Self>,
        count: usize,
        cx: &mut Context<'_>
    ) -> Poll<Result<()>> {
        if self.has_run(count) {
            return Poll::Ready(Ok(()));
        }
        {
            let mut wakers = lock_unpoisoned(&self.credit_wakers);
            if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                wakers.push(cx.waker().clone());
            }
        }
        if let Err(error) = self.ensure_driver() {
            return Poll::Ready(Err(error));
        }
        if self.has_run(count) { Poll::Ready(Ok(())) } else { Poll::Pending }
    }

    fn ensure_driver(self: &Arc<Self>) -> Result<()> {
        let mut driver = lock_unpoisoned(&self.driver);
        if driver.is_none() {
            *driver = Some(Driver::start(
                Arc::as_ptr(self),
                format!("orbit-payload-{}-{}-driver", self.kind, self.node)
            )?);
        }
        Ok(())
    }

    fn extent_count(
        &self,
        first: usize,
        generation: u64
    ) -> usize {
        let lane_end = (first / self.geometry.slots_per_node + 1) * self.geometry.slots_per_node;
        self.metadata()[first..lane_end]
            .iter()
            .take_while(|slot| {
                slot.allocation_first.load(Ordering::Acquire) as usize == first
                    && slot.generation.load(Ordering::Acquire) == generation
            })
            .count()
    }

    fn node_dead(
        &self,
        node: u16,
        incarnation: Incarnation
    ) {
        let metadata = self.metadata();
        for first in 0..metadata.len() {
            let slot = &metadata[first];
            let state = slot.state.load(Ordering::Acquire);
            let producer_node = first / self.geometry.slots_per_node;
            let allocation_start = slot.allocation_first.load(Ordering::Acquire) as usize == first;
            let abandoned_publication = allocation_start
                && matches!(state, SLOT_RESERVED | SLOT_LIVE)
                && producer_node == usize::from(node)
                && slot.producer_incarnation.load(Ordering::Acquire) == incarnation.get();
            let abandoned_read = state == SLOT_READING
                && slot.reader_node.load(Ordering::Acquire) == node
                && slot.reader_incarnation.load(Ordering::Acquire) == incarnation.get();
            if !abandoned_publication && !abandoned_read {
                continue;
            }
            if slot
                .state
                .compare_exchange(state, SLOT_RECLAIMING, Ordering::SeqCst, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let generation = slot.generation.load(Ordering::Acquire);
            let count = self.extent_count(first, generation);
            if count != 0 {
                self.release(first, count, generation);
            }
        }
    }
}

impl Doorstep for Arena {
    fn generation(&self) -> &AtomicU32 {
        &self.local_credit().generation
    }

    fn listening(
        &self,
        delta: i32
    ) {
        if delta > 0 {
            self.local_credit().waiters.fetch_add(1, Ordering::SeqCst);
        } else {
            self.local_credit().waiters.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn drain(&self) {
        let wakers = std::mem::take(&mut *lock_unpoisoned(&self.credit_wakers));
        for waker in wakers {
            waker.wake();
        }
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        if let Some(mut driver) = lock_unpoisoned(&self.driver).take() {
            driver.stop(&self.local_credit().generation);
        }
    }
}

/// One fleet-wide payload allocation table. Every producer allocates from its
/// node's exclusive lane, independent of which exchange side it currently holds.
#[derive(Clone)]
pub struct PayloadArena {
    arena: Arc<Arena>
}

impl PayloadArena {
    pub fn open(
        fleet: Arc<Fleet>,
        incarnation: Incarnation,
        spec: PayloadArenaSpec
    ) -> Result<Self> {
        spec.validate()?;
        Ok(Self { arena: open(&fleet, incarnation, spec)? })
    }

    pub fn kind(&self) -> u8 {
        self.arena.kind
    }

    pub fn slot_size(&self) -> usize {
        self.arena.geometry.slot_size
    }

    pub fn slots_per_node(&self) -> usize {
        self.arena.geometry.slots_per_node
    }

    /// Park this thread until a run large enough for `payload_len` may be
    /// available. Allocation still decides the race after the wake.
    pub fn wait_available(
        &self,
        payload_len: usize
    ) -> Result<()> {
        let count = self.arena.required_slots(payload_len)?;
        self.arena.wait_available(count)
    }

    /// Task readiness for this node's payload credit. Credit returns are
    /// coalesced through one shared generation and one local driver.
    pub fn poll_available(
        &self,
        payload_len: usize,
        cx: &mut Context<'_>
    ) -> Poll<Result<()>> {
        let count = match self.arena.required_slots(payload_len) {
            Ok(count) => count,
            Err(error) => return Poll::Ready(Err(error))
        };
        self.arena.poll_available(count, cx)
    }

    /// Clear this arena only while the fleet is known quiescent. Normal
    /// attach and replacement never call this implicitly.
    pub fn reset_all(&self) {
        let mut hint = lock_unpoisoned(&self.arena.allocation);
        *hint = 0;
        for slot in self.arena.metadata() {
            slot.state.store(SLOT_FREE, Ordering::Release);
        }
        for credit in self.arena.credits() {
            credit.generation.fetch_add(1, Ordering::SeqCst);
            crate::wake_on(&credit.generation);
        }
    }

    /// Reclaim only allocations owned by a process incarnation whose death
    /// was confirmed by the embedder. A producer's unpublished or unread
    /// chunks and a dead reader's held chunks return; a chunk already held by
    /// a surviving consumer remains that consumer's.
    pub fn node_dead(
        &self,
        node: orbit_core::NodeId,
        incarnation: Incarnation
    ) {
        self.arena.node_dead(node.get(), incarnation);
    }

    /// Remove the arena's SHM name. Existing mappings remain valid.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        match &self.arena.backing {
            Backing::Memory(_) => {
                self.reset_all();
                Ok(())
            }
            Backing::Shm(region) => region.unlink().map_err(Error::Io)
        }
    }

    #[cfg(test)]
    pub(crate) fn publish(
        &self,
        payload: &[u8]
    ) -> Result<Publication> {
        let mut publication = self.reserve(payload.len())?;
        publication.as_mut_slice().copy_from_slice(payload);
        publication.make_live();
        Ok(publication)
    }

    pub(crate) fn reserve(
        &self,
        payload_len: usize
    ) -> Result<Publication> {
        self.arena.reserve(payload_len)
    }

    pub(crate) fn read(
        &self,
        descriptor: ChunkDescriptor
    ) -> Result<PayloadChunk> {
        if descriptor.arena_kind() != self.kind() {
            return Err(Error::Malformed(format!(
                "chunk names payload arena kind {}, opened kind is {}",
                descriptor.arena_kind(),
                self.kind()
            )));
        }
        let owner = usize::from(descriptor.owner_node());
        let first_local = descriptor.first_slot() as usize;
        let count = descriptor.slot_count() as usize;
        let len = descriptor.payload_len() as usize;
        if owner >= self.arena.geometry.total_slots / self.arena.geometry.slots_per_node
            || count == 0
            || first_local + count > self.arena.geometry.slots_per_node
            || len == 0
            || len > count * self.arena.geometry.slot_size
            || (count > 1 && len <= (count - 1) * self.arena.geometry.slot_size)
        {
            return Err(Error::Malformed(
                "chunk descriptor is outside its payload lane".to_owned()
            ));
        }
        let first = owner * self.arena.geometry.slots_per_node + first_local;
        let slots = &self.arena.metadata()[first..first + count];
        let generation = descriptor.allocation_generation();
        if slots.iter().any(|slot| slot.generation.load(Ordering::Acquire) != generation)
            || slots.iter().skip(1).any(|slot| slot.state.load(Ordering::Acquire) != SLOT_RESERVED)
            || slots[0]
                .state
                .compare_exchange(SLOT_LIVE, SLOT_READING, Ordering::Acquire, Ordering::Acquire)
                .is_err()
        {
            return Err(Error::Malformed(
                "chunk allocation is stale or already consumed".to_owned()
            ));
        }
        slots[0].reader_incarnation.store(self.arena.incarnation.get(), Ordering::Release);
        slots[0].reader_node.store(self.arena.node, Ordering::Release);
        Ok(PayloadChunk {
            arena: Arc::clone(&self.arena),
            first,
            count,
            len,
            generation,
            descriptor
        })
    }
}

pub(crate) struct Publication {
    arena: Arc<Arena>,
    first: usize,
    count: usize,
    payload_len: usize,
    generation: u64,
    published: bool
}

impl Publication {
    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: this publication owns a contiguous reserved extent and no
        // consumer can observe it before the control descriptor is sent.
        unsafe { std::slice::from_raw_parts(self.arena.payload(self.first), self.payload_len) }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: this publication exclusively owns a contiguous reserved
        // extent until it is made live and its control descriptor is sent.
        unsafe { std::slice::from_raw_parts_mut(self.arena.payload(self.first), self.payload_len) }
    }

    pub(crate) fn coordinates(&self) -> (u64, u32, u32, u32, u8, u16) {
        let first_local = self.first % self.arena.geometry.slots_per_node;
        (
            self.generation,
            first_local as u32,
            self.count as u32,
            self.payload_len as u32,
            self.arena.kind,
            self.arena.node
        )
    }

    pub(crate) fn truncate(
        &mut self,
        payload_len: usize
    ) -> Result<()> {
        if payload_len == 0 || payload_len > self.payload_len {
            return Err(Error::Malformed(format!(
                "committed payload length {payload_len} is outside reserved capacity {}",
                self.payload_len
            )));
        }
        let count = self.arena.required_slots(payload_len)?;
        if count < self.count {
            for slot in &self.arena.metadata()[self.first + count..self.first + self.count] {
                slot.state.store(SLOT_FREE, Ordering::Release);
            }
            self.count = count;
            let credit = self.arena.local_credit();
            credit.generation.fetch_add(1, Ordering::SeqCst);
            if credit.waiters.load(Ordering::SeqCst) > 0 {
                crate::wake_on(&credit.generation);
            }
        }
        self.payload_len = payload_len;
        Ok(())
    }

    pub(crate) fn make_live(&self) {
        self.arena.metadata()[self.first].state.store(SLOT_LIVE, Ordering::Release);
    }

    pub(crate) fn mark_published(mut self) {
        self.published = true;
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        if !self.published {
            self.arena.release(self.first, self.count, self.generation);
        }
    }
}

/// An immutable chunk borrowed from a payload arena. Dropping the guard is
/// the consumer's credit return to its producer node's lane.
pub struct PayloadChunk {
    arena: Arc<Arena>,
    first: usize,
    count: usize,
    len: usize,
    generation: u64,
    descriptor: ChunkDescriptor
}

impl PayloadChunk {
    pub const fn descriptor(&self) -> ChunkDescriptor {
        self.descriptor
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the allocation is immutable between publication and this
        // guard's release, and `len` was validated against the run.
        unsafe { std::slice::from_raw_parts(self.arena.payload(self.first), self.len) }
    }
}

impl Deref for PayloadChunk {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl Drop for PayloadChunk {
    fn drop(&mut self) {
        self.arena.release(self.first, self.count, self.generation);
    }
}

#[derive(Hash, Eq, PartialEq)]
enum Key {
    Memory(usize, u8),
    #[cfg(unix)]
    Shm(String, u16)
}

static ARENAS: LazyLock<Mutex<HashMap<Key, Weak<Arena>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn open(
    fleet: &Arc<Fleet>,
    incarnation: Incarnation,
    spec: PayloadArenaSpec
) -> Result<Arc<Arena>> {
    if !crate::waits_supported() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "payload arenas need a platform that can wait on a shared word"
        )));
    }
    let key = if fleet.is_shm() {
        #[cfg(unix)]
        {
            Key::Shm(ring_segment_name(fleet.name(), spec.kind), fleet.node_id().get())
        }
        #[cfg(not(unix))]
        unreachable!("non-Unix fleets cannot use POSIX SHM")
    } else {
        Key::Memory(Arc::as_ptr(fleet) as usize, spec.kind)
    };
    let mut arenas = lock_unpoisoned(&ARENAS);
    arenas.retain(|_, arena| arena.strong_count() > 0);
    if let Some(arena) = arenas.get(&key).and_then(Weak::upgrade) {
        if arena.incarnation != incarnation {
            return Err(Error::Malformed(format!(
                "this process already opened payload kind {} as incarnation {}",
                spec.kind,
                arena.incarnation.get()
            )));
        }
        if arena.geometry.slots_per_node != spec.slots_per_node
            || arena.geometry.slot_size != spec.slot_size
        {
            return Err(Error::Malformed(format!(
                "this process already opened payload kind {} with slots_per_node={} slot_size={}",
                spec.kind, arena.geometry.slots_per_node, arena.geometry.slot_size
            )));
        }
        return Ok(arena);
    }

    let geometry = Geometry::new(fleet.fleet_capacity(), spec);
    let backing = match &key {
        Key::Memory(..) => {
            let bytes = AlignedBytes::zeroed(geometry.segment_size);
            // SAFETY: fresh aligned memory is at least one header long.
            unsafe {
                std::ptr::write(
                    bytes.ptr.cast::<Header>(),
                    Header::new(fleet.fleet_capacity(), spec)
                );
            }
            Backing::Memory(bytes)
        }
        #[cfg(unix)]
        Key::Shm(name, _) => Backing::Shm(open_shm(name, fleet.fleet_capacity(), spec, geometry)?)
    };
    let arena = Arc::new(Arena {
        _fleet: Arc::clone(fleet),
        backing,
        geometry,
        kind: spec.kind,
        node: fleet.node_id().get(),
        incarnation,
        allocation: Mutex::new(0),
        credit_wakers: Mutex::new(Vec::new()),
        driver: Mutex::new(None)
    });
    arenas.insert(key, Arc::downgrade(&arena));
    Ok(arena)
}

#[cfg(unix)]
fn open_shm(
    name: &str,
    fleet_capacity: u16,
    spec: PayloadArenaSpec,
    geometry: Geometry
) -> Result<ShmRegion> {
    use std::io;

    let (region, _initialization_lock) =
        ShmRegion::open_or_create_locked(name, geometry.segment_size)?;
    if region.created() {
        // SAFETY: a fresh mapping is aligned and at least one header long.
        unsafe {
            std::ptr::write(region.as_ptr().cast::<Header>(), Header::new(fleet_capacity, spec));
        }
    } else {
        // SAFETY: the open verified the minimum segment size.
        let header = unsafe { &*region.as_ptr().cast::<Header>() };
        if header.magic != MAGIC {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("SHM segment {name} has the wrong payload-arena magic")
            )));
        }
        if !header.compatible(fleet_capacity, spec) {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("SHM segment {name} has an incompatible payload-arena layout")
            )));
        }
    }
    Ok(region)
}

pub fn segment_size_for(
    fleet_capacity: u16,
    spec: PayloadArenaSpec
) -> usize {
    Geometry::new(fleet_capacity, spec).segment_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{ExchangeId, Flow};

    fn fleet() -> Arc<Fleet> {
        Arc::new(Fleet::join("payload-arena-test", 2).expect("fleet"))
    }

    fn descriptor(
        publication: &Publication,
        chunk_id: u64
    ) -> ChunkDescriptor {
        let (generation, first, count, len, kind, owner) = publication.coordinates();
        ChunkDescriptor::new(
            ExchangeId::from_stream_id(crate::StreamId::from_net_id(orbit_core::NetId64::make(
                240, 0, 1
            ))),
            Flow::AtoB,
            chunk_id,
            generation,
            first,
            count,
            len,
            kind,
            owner
        )
    }

    #[test]
    fn one_chunk_spans_the_number_of_slots_its_payload_needs() {
        let arena =
            PayloadArena::open(fleet(), Incarnation::new(1), PayloadArenaSpec::new(241, 8, 256))
                .expect("arena");
        let bytes = vec![0x5a; 777];
        let publication = arena.publish(&bytes).expect("publication");
        let descriptor = descriptor(&publication, 1);
        assert_eq!(descriptor.slot_count(), 4);
        publication.mark_published();

        let chunk = arena.read(descriptor).expect("chunk");
        assert_eq!(&*chunk, bytes);
    }

    #[test]
    fn dropping_a_chunk_returns_the_whole_extent_as_credit() {
        let arena =
            PayloadArena::open(fleet(), Incarnation::new(1), PayloadArenaSpec::new(242, 4, 64))
                .expect("arena");
        let first = arena.publish(&vec![1; 256]).expect("fills lane");
        let descriptor = descriptor(&first, 1);
        first.mark_published();
        assert!(matches!(arena.publish(&[2]), Err(Error::PayloadFull { .. })));
        drop(arena.read(descriptor).expect("consume"));
        assert!(arena.publish(&[3]).is_ok());
    }

    #[test]
    fn a_cancelled_publication_never_spends_credit() {
        let arena =
            PayloadArena::open(fleet(), Incarnation::new(1), PayloadArenaSpec::new(243, 2, 64))
                .expect("arena");
        drop(arena.publish(&vec![1; 128]).expect("reserved publication"));
        assert!(arena.publish(&vec![2; 128]).is_ok());
    }

    #[test]
    fn returned_credit_wakes_a_parked_producer() {
        let arena =
            PayloadArena::open(fleet(), Incarnation::new(1), PayloadArenaSpec::new(244, 2, 64))
                .expect("arena");
        let publication = arena.publish(&vec![1; 128]).expect("fills lane");
        let descriptor = descriptor(&publication, 1);
        publication.mark_published();
        let chunk = arena.read(descriptor).expect("held chunk");

        let waiting = arena.clone();
        let waiter = std::thread::spawn(move || waiting.wait_available(1));
        std::thread::sleep(std::time::Duration::from_millis(20));
        drop(chunk);
        waiter.join().expect("waiter").expect("credit wake");
    }

    #[cfg(unix)]
    #[test]
    fn returned_credit_wakes_only_the_owning_producer_node() {
        let name: &'static str = Box::leak(format!("pc{:x}", std::process::id()).into_boxed_str());
        let spec = PayloadArenaSpec::new(248, 1, 64);
        let producer = PayloadArena::open(
            Arc::new(
                Fleet::join_shm_as(name, 2, orbit_core::NodeId::ZERO).expect("producer fleet")
            ),
            Incarnation::new(1),
            spec
        )
        .expect("producer arena");
        producer.reset_all();
        let consumer = PayloadArena::open(
            Arc::new(
                Fleet::join_shm_as(name, 2, orbit_core::NodeId::new(1)).expect("consumer fleet")
            ),
            Incarnation::new(2),
            spec
        )
        .expect("consumer arena");

        let publication = producer.publish(&[7; 64]).expect("fill producer lane");
        let descriptor = descriptor(&publication, 1);
        publication.mark_published();
        let chunk = consumer.read(descriptor).expect("consume producer chunk");

        let waiting = producer.clone();
        let waiter = std::thread::spawn(move || waiting.wait_available(64));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while producer.arena.local_credit().waiters.load(Ordering::SeqCst) == 0 {
            assert!(std::time::Instant::now() < deadline, "producer did not park");
            std::thread::yield_now();
        }
        assert_eq!(
            consumer.arena.local_credit().waiters.load(Ordering::SeqCst),
            0,
            "an unrelated node must not share the producer's credit wait word",
        );

        drop(chunk);
        waiter.join().expect("waiter thread").expect("producer wake");
        producer.unlink().expect("unlink payload arena");
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn returned_credit_wakes_a_pending_task() {
        use std::future::poll_fn;

        let arena =
            PayloadArena::open(fleet(), Incarnation::new(1), PayloadArenaSpec::new(247, 2, 64))
                .expect("arena");
        let publication = arena.publish(&vec![1; 128]).expect("fills lane");
        let descriptor = descriptor(&publication, 1);
        publication.mark_published();
        let chunk = arena.read(descriptor).expect("held chunk");

        let waiting = arena.clone();
        let waiter = tokio::spawn(async move { poll_fn(|cx| waiting.poll_available(1, cx)).await });
        tokio::task::yield_now().await;
        drop(chunk);
        tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
            .await
            .expect("credit task stayed pending")
            .expect("credit task")
            .expect("credit wake");
    }

    #[test]
    fn death_reclaims_only_unpublished_or_held_allocations() {
        let arena =
            PayloadArena::open(fleet(), Incarnation::new(7), PayloadArenaSpec::new(245, 2, 64))
                .expect("arena");

        let abandoned = arena.arena.reserve(128).expect("reserved by producer");
        arena.node_dead(orbit_core::NodeId::ZERO, Incarnation::new(7));
        let replacement = arena.publish(&vec![2; 128]).expect("producer credit reclaimed");
        drop(abandoned);
        let replacement_descriptor = descriptor(&replacement, 2);
        replacement.mark_published();
        let held = arena.read(replacement_descriptor).expect("held by reader");

        arena.node_dead(orbit_core::NodeId::ZERO, Incarnation::new(6));
        assert!(matches!(arena.publish(&[3]), Err(Error::PayloadFull { .. })));
        arena.node_dead(orbit_core::NodeId::ZERO, Incarnation::new(7));
        let after_reader_death = arena.publish(&vec![4; 128]).expect("reader credit reclaimed");
        drop(held);
        let after_descriptor = descriptor(&after_reader_death, 3);
        after_reader_death.mark_published();
        assert!(arena.read(after_descriptor).is_ok());
    }

    #[test]
    fn committed_unread_payload_is_reclaimed_with_its_dead_producer() {
        let arena =
            PayloadArena::open(fleet(), Incarnation::new(8), PayloadArenaSpec::new(246, 2, 64))
                .expect("arena");
        let publication = arena.publish(b"survives").expect("publication");
        let descriptor = descriptor(&publication, 1);
        publication.mark_published();

        arena.node_dead(orbit_core::NodeId::ZERO, Incarnation::new(8));
        assert!(arena.read(descriptor).is_err());
        assert!(arena.publish(&vec![9; 128]).is_ok());
    }
}

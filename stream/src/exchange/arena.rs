use std::collections::HashMap;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};

use super::ChunkDescriptor;
use crate::{Error, Incarnation, Result, lock_unpoisoned};

const MAGIC: u32 = 0x4F_50_41_59; // "OPAY"
const VERSION: u16 = 1;

const SLOT_FREE: u8 = 0;
const SLOT_RESERVED: u8 = 1;
const SLOT_LIVE: u8 = 2;
const SLOT_READING: u8 = 3;

/// Geometry and SHM identity of one directional payload arena.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PayloadArenaSpec {
    pub kind: u8,
    /// Physical allocation units available to each fleet node.
    pub slots_per_node: usize,
    /// Bytes in one physical allocation unit.
    pub slot_size: usize,
}

impl PayloadArenaSpec {
    pub const fn new(
        kind: u8,
        slots_per_node: usize,
        slot_size: usize,
    ) -> Self {
        Self { kind, slots_per_node, slot_size }
    }

    pub const fn lane_bytes(self) -> usize {
        self.slots_per_node * self.slot_size
    }

    pub const fn slots_for(
        self,
        payload_len: usize,
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
    _padding: [u8; 24],
}

impl Header {
    fn new(
        fleet_capacity: u16,
        spec: PayloadArenaSpec,
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
            _padding: [0; 24],
        }
    }

    fn compatible(
        &self,
        fleet_capacity: u16,
        spec: PayloadArenaSpec,
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

#[repr(C, align(16))]
struct SlotMeta {
    state: AtomicU8,
    _reserved: [u8; 7],
    generation: AtomicU64,
}

const _: () = assert!(size_of::<Header>() == 64);
const _: () = assert!(size_of::<SlotMeta>() == 16);

#[derive(Clone, Copy)]
struct Geometry {
    slots_per_node: usize,
    slot_size: usize,
    total_slots: usize,
    metadata_offset: usize,
    payload_offset: usize,
    segment_size: usize,
}

impl Geometry {
    fn new(
        fleet_capacity: u16,
        spec: PayloadArenaSpec,
    ) -> Self {
        let total_slots = usize::from(fleet_capacity) * spec.slots_per_node;
        let metadata_offset = size_of::<Header>();
        let payload_offset =
            (metadata_offset + total_slots * size_of::<SlotMeta>()).next_multiple_of(64);
        let segment_size = payload_offset + total_slots * spec.slot_size;
        Self {
            slots_per_node: spec.slots_per_node,
            slot_size: spec.slot_size,
            total_slots,
            metadata_offset,
            payload_offset,
            segment_size,
        }
    }
}

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
    _incarnation: Incarnation,
    allocation: Mutex<usize>,
}

impl Arena {
    fn base(&self) -> *mut u8 {
        match &self.backing {
            Backing::Memory(bytes) => bytes.ptr,
            #[cfg(unix)]
            Backing::Shm(region) => region.as_ptr(),
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
                self.geometry.total_slots,
            )
        }
    }

    fn payload(
        &self,
        absolute_slot: usize,
    ) -> *mut u8 {
        // SAFETY: callers validate the slot against `total_slots`.
        unsafe {
            self.base().add(self.geometry.payload_offset + absolute_slot * self.geometry.slot_size)
        }
    }

    fn reserve(
        self: &Arc<Self>,
        payload_len: usize,
    ) -> Result<Publication> {
        if payload_len == 0 {
            return Err(Error::Malformed("an empty payload needs no arena allocation".to_owned()));
        }
        let count = payload_len.div_ceil(self.geometry.slot_size);
        if count > self.geometry.slots_per_node {
            return Err(Error::PayloadTooLarge {
                len: payload_len,
                capacity: self.geometry.slots_per_node * self.geometry.slot_size,
            });
        }

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
            slot.generation.store(generation, Ordering::Relaxed);
        }
        *hint = (candidate + count) & (self.geometry.slots_per_node - 1);
        Ok(Publication {
            arena: Arc::clone(self),
            first,
            count,
            payload_len,
            generation,
            published: false,
        })
    }

    fn release(
        &self,
        first: usize,
        count: usize,
        generation: u64,
    ) {
        let slots = &self.metadata()[first..first + count];
        if slots.iter().any(|slot| slot.generation.load(Ordering::Acquire) != generation) {
            return;
        }
        for slot in slots.iter().skip(1) {
            slot.state.store(SLOT_FREE, Ordering::Relaxed);
        }
        slots[0].state.store(SLOT_FREE, Ordering::Release);
    }
}

/// One directional, fleet-wide payload allocation table.
#[derive(Clone)]
pub struct PayloadArena {
    arena: Arc<Arena>,
}

impl PayloadArena {
    pub fn open(
        fleet: Arc<Fleet>,
        incarnation: Incarnation,
        spec: PayloadArenaSpec,
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

    pub(crate) fn publish(
        &self,
        payload: &[u8],
    ) -> Result<Publication> {
        let publication = self.arena.reserve(payload.len())?;
        // SAFETY: the run is exclusively reserved by this producer and is
        // contiguous in the payload mapping.
        unsafe {
            std::ptr::copy_nonoverlapping(
                payload.as_ptr(),
                self.arena.payload(publication.first),
                payload.len(),
            );
        }
        self.arena.metadata()[publication.first].state.store(SLOT_LIVE, Ordering::Release);
        Ok(publication)
    }

    pub(crate) fn read(
        &self,
        descriptor: ChunkDescriptor,
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
                "chunk descriptor is outside its payload lane".to_owned(),
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
                "chunk allocation is stale or already consumed".to_owned(),
            ));
        }
        Ok(PayloadChunk {
            arena: Arc::clone(&self.arena),
            first,
            count,
            len,
            generation,
            descriptor,
        })
    }
}

pub(crate) struct Publication {
    arena: Arc<Arena>,
    first: usize,
    count: usize,
    payload_len: usize,
    generation: u64,
    published: bool,
}

impl Publication {
    pub(crate) fn coordinates(&self) -> (u64, u32, u32, u32, u8, u16) {
        let first_local = self.first % self.arena.geometry.slots_per_node;
        (
            self.generation,
            first_local as u32,
            self.count as u32,
            self.payload_len as u32,
            self.arena.kind,
            self.arena.node,
        )
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
/// the consumer's credit return to that directional arena.
pub struct PayloadChunk {
    arena: Arc<Arena>,
    first: usize,
    count: usize,
    len: usize,
    generation: u64,
    descriptor: ChunkDescriptor,
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
    Shm(String, u16),
}

static ARENAS: LazyLock<Mutex<HashMap<Key, Weak<Arena>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn open(
    fleet: &Arc<Fleet>,
    incarnation: Incarnation,
    spec: PayloadArenaSpec,
) -> Result<Arc<Arena>> {
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
        if arena._incarnation != incarnation {
            return Err(Error::Malformed(format!(
                "this process already opened payload kind {} as incarnation {}",
                spec.kind,
                arena._incarnation.get()
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
                    Header::new(fleet.fleet_capacity(), spec),
                );
            }
            Backing::Memory(bytes)
        }
        #[cfg(unix)]
        Key::Shm(name, _) => Backing::Shm(open_shm(name, fleet.fleet_capacity(), spec, geometry)?),
    };
    let arena = Arc::new(Arena {
        _fleet: Arc::clone(fleet),
        backing,
        geometry,
        kind: spec.kind,
        node: fleet.node_id().get(),
        _incarnation: incarnation,
        allocation: Mutex::new(0),
    });
    arenas.insert(key, Arc::downgrade(&arena));
    Ok(arena)
}

#[cfg(unix)]
fn open_shm(
    name: &str,
    fleet_capacity: u16,
    spec: PayloadArenaSpec,
    geometry: Geometry,
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
                format!("SHM segment {name} has the wrong payload-arena magic"),
            )));
        }
        if !header.compatible(fleet_capacity, spec) {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("SHM segment {name} has an incompatible payload-arena layout"),
            )));
        }
    }
    Ok(region)
}

pub fn segment_size_for(
    fleet_capacity: u16,
    spec: PayloadArenaSpec,
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
        chunk_id: u64,
    ) -> ChunkDescriptor {
        let (generation, first, count, len, kind, owner) = publication.coordinates();
        ChunkDescriptor::new(
            ExchangeId::from_stream_id(crate::StreamId::from_net_id(orbit_core::NetId64::make(
                240, 0, 1,
            ))),
            Flow::Request,
            chunk_id,
            generation,
            first,
            count,
            len,
            kind,
            owner,
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
}

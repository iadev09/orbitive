//! The segment: a header, per-node doorbells and bitmaps (pending news,
//! capacity interest), per-node creation-claim counters, the key table,
//! the resource table. Every field a peer can touch is atomic; the same
//! layout is allocated on the heap for a process-local fleet.

use std::mem::size_of;
use std::sync::atomic::{AtomicU8, AtomicU16, AtomicU32, AtomicU64, Ordering};

use crate::PoolSpec;

pub(crate) const MAGIC: u32 = 0x50_4F_4F_4C; // "POOL"
pub(crate) const VERSION: u16 = 1;

pub(crate) const KEY_EMPTY: u8 = 0;
pub(crate) const KEY_LIVE: u8 = 1;

pub(crate) const RESOURCE_EMPTY: u8 = 0;
pub(crate) const RESOURCE_LIVE: u8 = 1;
/// No new leases; the ones out finish. Set by the owner.
pub(crate) const RESOURCE_DRAINING: u8 = 2;
/// Gone: unregistered by the owner or closed by a death report. Stays
/// until the owner's lane reuses the slot under a new generation.
pub(crate) const RESOURCE_CLOSED: u8 = 3;
/// The generation field ran out under this epoch.
pub(crate) const RESOURCE_EXHAUSTED: u8 = 4;

pub(crate) const SLOT_BITS: u32 = 16;
pub(crate) const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
pub(crate) const GENERATION_BITS: u32 = 40 - SLOT_BITS;
pub(crate) const GENERATION_MASK: u32 = (1 << GENERATION_BITS) - 1;
pub(crate) const GENERATION_LIMIT: u32 = if cfg!(test) { 4 } else { GENERATION_MASK };

#[repr(C, align(64))]
pub(crate) struct Header {
    pub(crate) magic: u32,
    pub(crate) version: u16,
    pub(crate) header_size: u16,
    pub(crate) key_capacity: u32,
    pub(crate) lane_capacity: u32,
    pub(crate) key_stride: u32,
    pub(crate) resource_size: u32,
    pub(crate) fleet_capacity: u16,
    pub(crate) flags: u8,
    _reserved: [u8; 5],
    pub(crate) epoch: AtomicU64,
    _reserved2: [u8; 24],
}

impl Header {
    pub(crate) fn new(fleet_capacity: u16, geometry: &Geometry, epoch: u64) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            header_size: size_of::<Self>() as u16,
            key_capacity: geometry.key_capacity as u32,
            lane_capacity: geometry.lane_capacity as u32,
            key_stride: geometry.key_stride as u32,
            resource_size: size_of::<ResourceSlot>() as u32,
            fleet_capacity,
            flags: u8::from(geometry.fleet_availability),
            _reserved: [0; 5],
            epoch: AtomicU64::new(epoch),
            _reserved2: [0; 24],
        }
    }

    /// The spec a peer built its side with is part of the wire contract:
    /// a segment opened under one kind answers only to that kind's
    /// capacities.
    pub(crate) fn compatible(&self, fleet_capacity: u16, geometry: &Geometry) -> bool {
        self.magic == MAGIC
            && self.version == VERSION
            && usize::from(self.header_size) == size_of::<Self>()
            && self.key_capacity as usize == geometry.key_capacity
            && self.lane_capacity as usize == geometry.lane_capacity
            && self.key_stride as usize == geometry.key_stride
            && self.resource_size as usize == size_of::<ResourceSlot>()
            && self.fleet_capacity == fleet_capacity
            && self.flags == u8::from(geometry.fleet_availability)
    }
}

#[repr(C, align(64))]
pub(crate) struct Doorbell {
    pub(crate) generation: AtomicU32,
    pub(crate) listening: AtomicU32,
    pub(crate) incarnation: AtomicU64,
    _padding: [u8; 48],
}

/// One key: what resources are usable for, and the fleet-wide creation
/// budget for it. Followed in memory by `member_words` words naming the
/// resource slots registered under it, so a key's candidates are read
/// without scanning the resource table.
#[repr(C, align(64))]
pub(crate) struct KeySlot {
    pub(crate) state: AtomicU8,
    _reserved: [u8; 7],
    pub(crate) key_lo: AtomicU64,
    pub(crate) key_hi: AtomicU64,
    /// `live << 32 | creating`, changed with one CAS so a creation claim
    /// sees both counts at once.
    pub(crate) counts: AtomicU64,
    /// Bumped whenever capacity may have come back (a release, an
    /// unregister, a closed resource); the word waiters park on.
    pub(crate) changes: AtomicU32,
    pub(crate) waiters: AtomicU32,
    /// Free units across this key. Used only by specs that opt into fleet
    /// availability; it lives in former padding so default table geometry is
    /// unchanged.
    pub(crate) available: AtomicU32,
    _padding: [u8; 20],
}

impl KeySlot {
    pub(crate) fn holds(&self, lo: u64, hi: u64) -> bool {
        self.state.load(Ordering::Acquire) == KEY_LIVE
            && self.key_lo.load(Ordering::Relaxed) == lo
            && self.key_hi.load(Ordering::Relaxed) == hi
    }
}

/// Also the shape of a resource's `units`: `(reserved, active)`.
pub(crate) const fn unpack_counts(counts: u64) -> (u32, u32) {
    ((counts >> 32) as u32, counts as u32)
}

pub(crate) const fn pack_counts(live: u32, creating: u32) -> u64 {
    ((live as u64) << 32) | creating as u64
}

/// One resource somebody owns. The opaque object stays with the owner;
/// this is what the fleet sees of it.
#[repr(C, align(64))]
pub(crate) struct ResourceSlot {
    pub(crate) state: AtomicU8,
    _reserved: u8,
    pub(crate) owner_node: AtomicU16,
    pub(crate) generation: AtomicU32,
    pub(crate) owner_incarnation: AtomicU64,
    pub(crate) key_lo: AtomicU64,
    pub(crate) key_hi: AtomicU64,
    pub(crate) capacity: AtomicU32,
    /// Index of the key slot, so a release wakes the key without a lookup.
    pub(crate) key_index: AtomicU32,
    /// `reserved << 32 | active`: units a caller has reserved but the
    /// owner has not accepted yet, and units the owner is executing.
    /// Capacity applies to their sum, so a reservation nobody accepted
    /// still keeps its unit until the owner reconciles it away.
    pub(crate) units: AtomicU64,
    /// Monotonic per slot; every reservation takes the next value, so a
    /// lease can be told from every earlier lease on the same slot.
    pub(crate) fence: AtomicU64,
    /// When the newest reservation was taken, for selection.
    pub(crate) last_reserve_ms: AtomicU64,
    _padding: [u8; 64],
    /// Reservations the owner has not accepted yet, by fence. A lease is
    /// accepted by taking its fence out of here exactly once, so a second
    /// accept, or one arriving after the owner aged the entry out, is
    /// refused. Bounded: a resource with this many unaccepted
    /// reservations refuses further ones until the owner catches up.
    pub(crate) pending: [Reservation; PENDING_RESERVATIONS],
}

/// One reservation the owner has not seen yet. `since_ms` is written
/// before `fence` is published, so a reader that sees the fence sees when
/// it was taken.
#[repr(C)]
pub(crate) struct Reservation {
    pub(crate) fence: AtomicU64,
    pub(crate) since_ms: AtomicU64,
}

/// Unaccepted reservations one resource can hold at once. Part of the
/// slot ABI, not a tunable: an owner that is this far behind is the
/// problem, not the table.
pub const PENDING_RESERVATIONS: usize = 16;

impl ResourceSlot {
    pub(crate) fn is(&self, generation: u32) -> bool {
        let state = self.state.load(Ordering::Acquire);
        (state == RESOURCE_LIVE || state == RESOURCE_DRAINING)
            && self.generation.load(Ordering::Relaxed) == generation
    }

    /// Under the owner's lane lock. `None` marks the slot exhausted.
    pub(crate) fn install(
        &self,
        owner: u16,
        incarnation: u64,
        key: (u64, u64),
        key_index: u32,
        capacity: u32,
        now_ms: u64,
    ) -> Option<u32> {
        let generation = self.generation.load(Ordering::Relaxed);
        if generation >= GENERATION_LIMIT {
            self.state.store(RESOURCE_EXHAUSTED, Ordering::Release);
            return None;
        }
        let generation = generation + 1;
        self.owner_node.store(owner, Ordering::Relaxed);
        self.owner_incarnation.store(incarnation, Ordering::Relaxed);
        self.key_lo.store(key.0, Ordering::Relaxed);
        self.key_hi.store(key.1, Ordering::Relaxed);
        self.capacity.store(capacity, Ordering::Relaxed);
        self.key_index.store(key_index, Ordering::Relaxed);
        self.units.store(0, Ordering::Relaxed);
        self.fence.store(0, Ordering::Relaxed);
        self.last_reserve_ms.store(now_ms, Ordering::Relaxed);
        for reservation in &self.pending {
            reservation.fence.store(0, Ordering::Relaxed);
            reservation.since_ms.store(0, Ordering::Relaxed);
        }
        self.generation.store(generation, Ordering::Relaxed);
        self.state.store(RESOURCE_LIVE, Ordering::Release);
        Some(generation)
    }
}

const _: () = assert!(size_of::<Header>() == 64);
const _: () = assert!(size_of::<Doorbell>() == 64);
const _: () = assert!(size_of::<KeySlot>() == 64);
const _: () = assert!(size_of::<ResourceSlot>() == 128 + 16 * PENDING_RESERVATIONS);

#[derive(Clone, Copy, Debug)]
pub(crate) struct Geometry {
    pub(crate) key_capacity: usize,
    pub(crate) lane_capacity: usize,
    pub(crate) fleet_capacity: usize,
    pub(crate) total_resources: usize,
    pub(crate) fleet_availability: bool,
    /// Words in a resource bitmap (key members).
    pub(crate) member_words: usize,
    /// Words in a key bitmap (pending, interest).
    pub(crate) key_words: usize,
    /// Bytes from one key slot to the next: the slot plus its members.
    pub(crate) key_stride: usize,
    pub(crate) doorbells_offset: usize,
    pub(crate) pending_offset: usize,
    pub(crate) interest_offset: usize,
    pub(crate) claims_offset: usize,
    pub(crate) keys_offset: usize,
    pub(crate) resources_offset: usize,
    pub(crate) segment_size: usize,
}

impl Geometry {
    pub(crate) fn new(fleet_capacity: u16, spec: PoolSpec) -> Self {
        let PoolSpec { key_capacity, lane_capacity, fleet_availability, .. } = spec;
        let fleet_capacity = usize::from(fleet_capacity);
        let total_resources = fleet_capacity * lane_capacity;
        let member_words = total_resources.div_ceil(64);
        let key_words = key_capacity.div_ceil(64);
        let key_stride =
            (size_of::<KeySlot>() + member_words * size_of::<AtomicU64>()).next_multiple_of(64);
        let key_bitmap_bytes = fleet_capacity * key_words * size_of::<AtomicU64>();
        let doorbells_offset = size_of::<Header>();
        let pending_offset = doorbells_offset + fleet_capacity * size_of::<Doorbell>();
        let interest_offset = pending_offset + key_bitmap_bytes;
        let claims_offset = (interest_offset + key_bitmap_bytes).next_multiple_of(64);
        let claims_bytes = fleet_capacity * key_capacity * size_of::<AtomicU32>();
        let keys_offset = (claims_offset + claims_bytes).next_multiple_of(64);
        let resources_offset = keys_offset + key_capacity * key_stride;
        let segment_size = resources_offset + total_resources * size_of::<ResourceSlot>();
        Self {
            key_capacity,
            lane_capacity,
            fleet_capacity,
            total_resources,
            fleet_availability,
            member_words,
            key_words,
            key_stride,
            doorbells_offset,
            pending_offset,
            interest_offset,
            claims_offset,
            keys_offset,
            resources_offset,
            segment_size,
        }
    }
}

//! The segment: one header, a doorbell and two bitmaps (pending news,
//! offered streams) per fleet node, then the slot table, then the byte
//! buffers. Every field a peer can touch is atomic, so the mapping is never
//! borrowed `&mut` while another process reads it, and the layout is the
//! same in memory and in shared memory.

use std::mem::size_of;
use std::sync::atomic::{AtomicU8, AtomicU16, AtomicU32, AtomicU64, Ordering};

use crate::{STREAM_BUFFER_BYTES, STREAM_LANE_CAPACITY};

pub(crate) const MAGIC: u32 = 0x53_54_52_4D; // "STRM"
pub(crate) const VERSION: u16 = 2;

pub(crate) const SLOT_EMPTY: u8 = 0;
pub(crate) const SLOT_LIVE: u8 = 1;
/// The slot's generation field ran out under this epoch; it is not reused
/// until a reset starts a new epoch.
pub(crate) const SLOT_EXHAUSTED: u8 = 2;

pub(crate) const SIDE_FREE: u8 = 0;
pub(crate) const SIDE_CLAIMED: u8 = 1;
pub(crate) const SIDE_RELEASED: u8 = 2;
/// The process holding this side was reported dead; nothing more happens
/// on it, and nobody can claim it.
pub(crate) const SIDE_DEAD: u8 = 3;

/// The writer committed its last byte; what is buffered still drains.
pub(crate) const FLAG_FIN: u8 = 1;
/// The writer abandoned the direction; buffered bytes are discarded.
pub(crate) const FLAG_RESET: u8 = 2;
/// The reader is gone; nothing written here will ever be read.
pub(crate) const FLAG_READER_GONE: u8 = 4;

/// Bits of the 40-bit `NetId64` counter that name the slot inside its lane;
/// the rest is the slot generation. The counter is a packed allocation
/// identity, not a sequence number: a free slot is found and its own
/// generation advanced.
pub(crate) const SLOT_BITS: u32 = 16;
pub(crate) const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
pub(crate) const GENERATION_BITS: u32 = 40 - SLOT_BITS;
pub(crate) const GENERATION_MASK: u32 = (1 << GENERATION_BITS) - 1;
/// The last generation a slot may be installed in. Never wraps: a slot
/// that reaches it is exhausted for the rest of the epoch. Unit tests
/// shrink it so exhaustion is exercised instead of assumed.
pub(crate) const GENERATION_LIMIT: u32 = if cfg!(test) { 4 } else { GENERATION_MASK };

#[repr(C, align(64))]
pub(crate) struct Header {
    pub(crate) magic: u32,
    pub(crate) version: u16,
    pub(crate) header_size: u16,
    pub(crate) lane_capacity: u32,
    pub(crate) slot_size: u32,
    pub(crate) buffer_bytes: u32,
    pub(crate) fleet_capacity: u16,
    _reserved: [u8; 2],
    /// One lifetime of this table's contents. Set when the segment is
    /// created, advanced by every quiescent reset, and carried in every
    /// ticket, so a ticket from before a reset never matches after it.
    pub(crate) epoch: AtomicU64,
    _reserved2: [u8; 32],
}

impl Header {
    pub(crate) fn new(fleet_capacity: u16, epoch: u64) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            header_size: size_of::<Self>() as u16,
            lane_capacity: STREAM_LANE_CAPACITY as u32,
            slot_size: size_of::<Slot>() as u32,
            buffer_bytes: STREAM_BUFFER_BYTES as u32,
            fleet_capacity,
            _reserved: [0; 2],
            epoch: AtomicU64::new(epoch),
            _reserved2: [0; 32],
        }
    }

    pub(crate) fn compatible(&self, fleet_capacity: u16) -> bool {
        self.magic == MAGIC
            && self.version == VERSION
            && usize::from(self.header_size) == size_of::<Self>()
            && self.lane_capacity as usize == STREAM_LANE_CAPACITY
            && self.slot_size as usize == size_of::<Slot>()
            && self.buffer_bytes as usize == STREAM_BUFFER_BYTES
            && self.fleet_capacity == fleet_capacity
    }
}

/// One per fleet node: what a writer rings when that node's process has
/// something to look at, and whether anyone there is listening.
#[repr(C, align(64))]
pub(crate) struct Doorbell {
    pub(crate) generation: AtomicU32,
    pub(crate) listening: AtomicU32,
    /// Whose driver is listening, so a death report for that incarnation
    /// can stop writers from ringing a bell nobody answers.
    pub(crate) incarnation: AtomicU64,
    _padding: [u8; 48],
}

/// One direction of a stream: a single-producer, single-consumer byte ring
/// described by two monotonic positions. `head` is owned by the writer,
/// `tail` by the reader; the buffer index is the position masked.
#[repr(C)]
pub(crate) struct Direction {
    pub(crate) head: AtomicU64,
    pub(crate) tail: AtomicU64,
    pub(crate) flags: AtomicU8,
    _reserved: [u8; 3],
    /// Bumped on every commit, consume and flag change; the word a blocking
    /// waiter parks on. 32 bits because that is what the platform waits want.
    pub(crate) changes: AtomicU32,
    pub(crate) waiters: AtomicU32,
    _padding: [u8; 4],
}

impl Direction {
    pub(crate) fn clear(&self) {
        self.head.store(0, Ordering::Relaxed);
        self.tail.store(0, Ordering::Relaxed);
        self.flags.store(0, Ordering::Relaxed);
        self.changes.store(0, Ordering::Relaxed);
        self.waiters.store(0, Ordering::Relaxed);
    }

    pub(crate) fn flags(&self) -> u8 {
        self.flags.load(Ordering::Acquire)
    }
}

/// One stream. A cache line of its own for the control words, so two hot
/// streams never contend on metadata.
#[repr(C, align(64))]
pub(crate) struct Slot {
    pub(crate) state: AtomicU8,
    _reserved: [u8; 3],
    pub(crate) generation: AtomicU32,
    /// Per side: free, claimed, released, dead.
    pub(crate) claimed: [AtomicU8; 2],
    _reserved2: [u8; 2],
    /// Per side: the fleet node whose process holds it, for the doorbell.
    pub(crate) node: [AtomicU16; 2],
    /// Per side: which incarnation of that node, for death reports.
    pub(crate) incarnation: [AtomicU64; 2],
    _padding: [u8; 32],
    /// `directions[0]` carries A -> B, `directions[1]` carries B -> A.
    pub(crate) directions: [Direction; 2],
}

impl Slot {
    pub(crate) fn is(&self, generation: u32) -> bool {
        // `Acquire` on `state` publishes the generation written by `install`.
        self.state.load(Ordering::Acquire) == SLOT_LIVE
            && self.generation.load(Ordering::Relaxed) == generation
    }

    /// Under the lane owner's allocation lock. Returns the generation the
    /// new stream lives in, bumped on every install so no released id
    /// matches; `None` marks the slot exhausted instead of wrapping.
    pub(crate) fn install(&self, creator: u16, incarnation: u64) -> Option<u32> {
        let generation = self.generation.load(Ordering::Relaxed);
        if generation >= GENERATION_LIMIT {
            self.state.store(SLOT_EXHAUSTED, Ordering::Release);
            return None;
        }
        let generation = generation + 1;
        self.claimed[0].store(SIDE_CLAIMED, Ordering::Relaxed);
        self.claimed[1].store(SIDE_FREE, Ordering::Relaxed);
        self.node[0].store(creator, Ordering::Relaxed);
        self.node[1].store(u16::MAX, Ordering::Relaxed);
        self.incarnation[0].store(incarnation, Ordering::Relaxed);
        self.incarnation[1].store(0, Ordering::Relaxed);
        for direction in &self.directions {
            direction.clear();
        }
        self.generation.store(generation, Ordering::Relaxed);
        self.state.store(SLOT_LIVE, Ordering::Release);
        Some(generation)
    }
}

const _: () = assert!(size_of::<Header>() == 64);
const _: () = assert!(size_of::<Doorbell>() == 64);
const _: () = assert!(size_of::<Direction>() == 32);
const _: () = assert!(size_of::<Slot>() == 128);
const _: () = assert!(STREAM_LANE_CAPACITY.is_power_of_two());
const _: () = assert!(STREAM_LANE_CAPACITY <= 1 << SLOT_BITS);
const _: () = assert!(STREAM_BUFFER_BYTES.is_power_of_two());
const _: () = assert!(STREAM_BUFFER_BYTES <= u32::MAX as usize);

/// Where everything sits, computed once per opened table from the fleet
/// capacity, which is runtime geometry like a ring's lane count.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Geometry {
    pub(crate) fleet_capacity: usize,
    pub(crate) total_slots: usize,
    pub(crate) bitmap_words: usize,
    pub(crate) doorbells_offset: usize,
    pub(crate) pending_offset: usize,
    pub(crate) offers_offset: usize,
    pub(crate) slots_offset: usize,
    pub(crate) buffers_offset: usize,
    pub(crate) segment_size: usize,
}

impl Geometry {
    pub(crate) fn new(fleet_capacity: u16) -> Self {
        let fleet_capacity = usize::from(fleet_capacity);
        let total_slots = fleet_capacity * STREAM_LANE_CAPACITY;
        let bitmap_words = total_slots.div_ceil(64);
        let bitmap_bytes = fleet_capacity * bitmap_words * size_of::<AtomicU64>();
        let doorbells_offset = size_of::<Header>();
        let pending_offset = doorbells_offset + fleet_capacity * size_of::<Doorbell>();
        let offers_offset = pending_offset + bitmap_bytes;
        let slots_offset = (offers_offset + bitmap_bytes).next_multiple_of(64);
        let buffers_offset = slots_offset + total_slots * size_of::<Slot>();
        let segment_size = buffers_offset + total_slots * 2 * STREAM_BUFFER_BYTES;
        Self {
            fleet_capacity,
            total_slots,
            bitmap_words,
            doorbells_offset,
            pending_offset,
            offers_offset,
            slots_offset,
            buffers_offset,
            segment_size,
        }
    }

    pub(crate) fn buffer_offset(&self, slot: usize, direction: usize) -> usize {
        self.buffers_offset + (slot * 2 + direction) * STREAM_BUFFER_BYTES
    }
}

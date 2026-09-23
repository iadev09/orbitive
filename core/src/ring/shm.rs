//! `ShmRing` — POSIX SHM-backed ring buffer (V1 substrate).
//!
//! The runtime substrate that finally makes Orbit's "fleet sees each
//! other" promise real. Cross-process visibility is a memory-coherent
//! `mmap` with shared atomics; no kernel round-trips per access.
//!
//! ## Layout (single SHM segment per `OrbitTyped` kind)
//!
//! ```text
//! ┌──────────────────────────┬───────────────────────────┬──────────────┐
//! │ ShmRingHeader (64B)      │ LaneHeader[0..L]          │ Lane slots   │
//! │ kind/spec/topology       │ one head per writer lane  │ L × N slots  │
//! └──────────────────────────┴───────────────────────────┴──────────────┘
//! ```
//!
//! Shared topology retains the lock-free multi-writer claim-before-commit
//! protocol. Shared-ordered topology serializes writers with a
//! process-recoverable kernel advisory lock, commits the slot, and only then
//! advances the head.
//! Per-node topology assigns disjoint slots to every node and requires one
//! active process per node id. Concurrent tasks inside that process are
//! serialized locally, and the lane head is published only after the slot
//! reaches its committed sequence. A process dying during a per-node write
//! therefore leaves no visible hole for other lanes.
//!
//! ## Slot publication protocol
//!
//! 1. choose/claim the lane counter
//! 2. `slot = slots[counter % capacity]`
//! 3. `slot.seq = 2*counter + 1` (odd → writing)
//! 4. fill `id`, `kind`, `ver`, `payload_len`, `payload[..len]`
//! 5. `slot.seq = 2*counter + 2` (even → committed)
//! 6. for per-node lanes, release-store `head = counter + 1`
//!
//! ## Read protocol
//!
//! 1. read `seq_pre = slot.seq` (Acquire)
//! 2. if odd → writer in progress, return None / retry
//! 3. read all fields into locals
//! 4. read `seq_post = slot.seq` (Acquire)
//! 5. if `seq_pre != seq_post` → torn write, retry
//! 6. validate counter encoded in seq matches the one we want
//!
//! Tearing is detected, never silently merged. A reader that loses
//! the race against the writer simply retries (or returns None for
//! head-read).
//!
//! ## Per-kind payload size
//!
//! Every `OrbitTyped::KIND` declares its own `RingSpec`. The payload
//! capacity determines that SHM segment's slot stride; unrelated lanes
//! no longer pay for one global maximum. Capacity and payload capacity
//! are persisted in the header and verified by every attaching process.

#![cfg(unix)]

use std::ptr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering, fence};

use bytes::Bytes;

use crate::NodeId;
use crate::id::NetId64;
use crate::ring::{Frame, RingSpec, RingTopology};
use crate::shm::{self, ShmRegion};

// ─────────────────────────────────────────────────────────────────────
// On-disk (well, on-SHM) layout
// ─────────────────────────────────────────────────────────────────────

const MAGIC: u32 = 0x4F524254; // "ORBT" big-endian when displayed
const VERSION: u32 = 3;
const SLOT_ALIGNMENT: usize = 64;

/// Cache-line aligned to keep the header on its own line.
#[repr(C, align(64))]
struct ShmRingHeader {
    version_counter: AtomicU64,
    capacity: u64,
    magic: u32,
    version: u32,
    payload_capacity: u32,
    slot_stride: u32,
    kind: u8,
    topology: u8,
    lane_count: u16,
    /// Shared futex/umtx word used by notification-enabled ring users.
    /// It lives in the existing reserved header space, so the V3
    /// layout remains compatible with already-created segments.
    notification_generation: AtomicU32,
    /// Explicitly fills one cache line; field order avoids implicit
    /// alignment padding that would otherwise make this header 128B.
    _reserved: [u8; 24]
}

#[repr(C, align(64))]
struct ShmLaneHeader {
    write_pos: AtomicU64,
    _reserved: [u8; 56]
}

/// Fixed prefix of one dynamically-strided SHM slot.
#[repr(C)]
struct ShmSlotHeader {
    /// LMAX-style sequence: odd while a writer is mid-flight, even
    /// once committed. Reader checks seq before/after content read
    /// to detect torn writes.
    seq: AtomicU64,
    /// `NetId64::raw()` of the frame that occupies this slot.
    id: AtomicU64,
    /// `Frame::ver` — caller-supplied version / tick.
    ver: AtomicU64,
    /// Length of the meaningful prefix of `payload`.
    payload_len: AtomicU32,
    /// `Frame::kind` — the message-class byte (state/event/cmd/…).
    kind: AtomicU8,
    _reserved: [u8; 3]
}

// The content fields are atomic for the same reason `seq` is: a reader may be
// copying this slot while a writer overwrites it. The seqlock *detects* that
// afterwards, which is not the same as not having a race — under the memory
// model a torn read through a plain `u64` is undefined however the hardware
// behaves, and Miri or TSan would say so. `Relaxed` is what they need: all the
// ordering is already carried by `seq` and the two fences around it, so these
// compile to the same plain loads and stores they were, with the race removed
// rather than papered over.

const SLOT_HEADER_SIZE: usize = std::mem::size_of::<ShmSlotHeader>();
const HEADER_SIZE: usize = std::mem::size_of::<ShmRingHeader>();
const LANE_HEADER_SIZE: usize = std::mem::size_of::<ShmLaneHeader>();

const _: () = assert!(HEADER_SIZE == 64);
const _: () = assert!(LANE_HEADER_SIZE == 64);
const _: () = assert!(SLOT_HEADER_SIZE == 32);

fn invalid_input(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into())
}

fn lane_count_for(
    spec: RingSpec,
    fleet_capacity: u16
) -> std::io::Result<usize> {
    if fleet_capacity == 0 {
        return Err(invalid_input("ShmRing fleet capacity must be > 0"));
    }
    Ok(match spec.topology {
        RingTopology::Shared | RingTopology::SharedOrdered => 1,
        RingTopology::PerNode => usize::from(fleet_capacity)
    })
}

fn checked_layout(
    spec: RingSpec,
    fleet_capacity: u16
) -> std::io::Result<(usize, usize, usize)> {
    if spec.capacity == 0 {
        return Err(invalid_input("ShmRing capacity must be > 0"));
    }
    if !spec.capacity.is_power_of_two() {
        return Err(invalid_input("ShmRing capacity must be a power of two"));
    }
    if spec.payload_capacity > u32::MAX as usize {
        return Err(invalid_input("ShmRing payload capacity must fit in u32"));
    }

    let unaligned = SLOT_HEADER_SIZE
        .checked_add(spec.payload_capacity)
        .ok_or_else(|| invalid_input("ShmRing slot size overflow"))?;
    let slot_stride = unaligned
        .checked_add(SLOT_ALIGNMENT - 1)
        .map(|value| value & !(SLOT_ALIGNMENT - 1))
        .ok_or_else(|| invalid_input("ShmRing slot stride overflow"))?;
    if slot_stride > u32::MAX as usize {
        return Err(invalid_input("ShmRing slot stride must fit in u32"));
    }
    let lane_count = lane_count_for(spec, fleet_capacity)?;
    let lane_headers_size = lane_count
        .checked_mul(LANE_HEADER_SIZE)
        .ok_or_else(|| invalid_input("ShmRing lane header size overflow"))?;
    let slots_offset = HEADER_SIZE
        .checked_add(lane_headers_size)
        .ok_or_else(|| invalid_input("ShmRing slots offset overflow"))?;
    let slots_per_lane = spec
        .capacity
        .checked_mul(slot_stride)
        .ok_or_else(|| invalid_input("ShmRing lane size overflow"))?;
    let slots_size = lane_count
        .checked_mul(slots_per_lane)
        .ok_or_else(|| invalid_input("ShmRing slots size overflow"))?;
    let segment_size = slots_offset
        .checked_add(slots_size)
        .ok_or_else(|| invalid_input("ShmRing segment size overflow"))?;
    Ok((slot_stride, slots_offset, segment_size))
}

/// Compute the SHM segment size required by a ring spec.
pub fn segment_size_for_spec(spec: RingSpec) -> std::io::Result<usize> {
    segment_size_for_spec_and_fleet(spec, 1)
}

/// Compute the SHM segment size required for a fleet-aware ring spec.
pub fn segment_size_for_spec_and_fleet(
    spec: RingSpec,
    fleet_capacity: u16
) -> std::io::Result<usize> {
    checked_layout(spec, fleet_capacity).map(|(_, _, segment_size)| segment_size)
}

// ─────────────────────────────────────────────────────────────────────
// ShmRing
// ─────────────────────────────────────────────────────────────────────

/// Cross-process ring buffer backed by a POSIX SHM segment.
///
/// Use [`ShmRing::open_or_create`] to attach to (or create) the
/// segment by name. Multiple processes calling this with the same name and
/// matching [`RingSpec`] share the underlying memory; the first process to
/// call it does the one-time header initialization.
pub struct ShmRing {
    region: ShmRegion,
    kind: u8,
    capacity: usize,
    payload_capacity: usize,
    topology: RingTopology,
    lane_count: usize,
    slot_stride: usize,
    slots_offset: usize,
    lane_stride: usize,
    write_locks: Vec<Mutex<()>>
}

impl ShmRing {
    /// Open or create a SHM-backed ring under `fleet_name` for type
    /// kind `kind` with `spec`. The first process to call
    /// this initializes the header; later attachers reuse it.
    pub fn open_or_create(
        fleet_name: &str,
        kind: u8,
        spec: RingSpec
    ) -> std::io::Result<Self> {
        Self::open_or_create_for_fleet(fleet_name, kind, spec, 1)
    }

    /// Open or create a SHM-backed ring using `fleet_capacity` physical
    /// writer lanes when `spec` is [`RingTopology::PerNode`].
    pub fn open_or_create_for_fleet(
        fleet_name: &str,
        kind: u8,
        spec: RingSpec,
        fleet_capacity: u16
    ) -> std::io::Result<Self> {
        let lane_count = lane_count_for(spec, fleet_capacity)?;
        let (slot_stride, slots_offset, size) = checked_layout(spec, fleet_capacity)?;
        let lane_stride = spec
            .capacity
            .checked_mul(slot_stride)
            .ok_or_else(|| invalid_input("ShmRing lane stride overflow"))?;
        let name = shm::ring_segment_name(fleet_name, kind);

        // Locked whatever the topology. Creation is two steps that a peer can
        // arrive between — `shm_open(O_CREAT|O_EXCL)` publishes the name, and
        // the header below is written after it — and the attach path reads that
        // header and rejects a segment whose magic is still zero, with no
        // retry. Only `SharedOrdered` took this lock, so every other ring was
        // one scheduling accident away from a peer failing to boot with
        // "wrong magic 0x00000000"; N workers starting together is exactly the
        // case that produces it. `SharedOrdered` still keeps the lock for its
        // writes — this is the same lock, held for a different reason.
        let (region, _initialization_lock) = ShmRegion::open_or_create_locked(&name, size)?;

        // Initialize header on first creation; subsequent attachers
        // skip and rely on whatever the creator wrote.
        if region.created() {
            // SAFETY: region is mapped, header lives at offset 0,
            // size is right because we just created with this size.
            unsafe {
                let header_ptr = region.as_ptr() as *mut ShmRingHeader;
                ptr::write(
                    header_ptr,
                    ShmRingHeader {
                        version_counter: AtomicU64::new(0),
                        capacity: spec.capacity as u64,
                        magic: MAGIC,
                        version: VERSION,
                        payload_capacity: spec.payload_capacity as u32,
                        slot_stride: slot_stride as u32,
                        kind,
                        topology: spec.topology as u8,
                        lane_count: lane_count as u16,
                        notification_generation: AtomicU32::new(0),
                        _reserved: [0; 24]
                    }
                );

                for lane in 0..lane_count {
                    let lane_ptr = region.as_ptr().add(HEADER_SIZE + lane * LANE_HEADER_SIZE)
                        as *mut ShmLaneHeader;
                    ptr::write(
                        lane_ptr,
                        ShmLaneHeader { write_pos: AtomicU64::new(0), _reserved: [0; 56] }
                    );
                }

                // Zero out the slot region so all `seq` values start
                // at 0 (== "never written, even, slot empty"). 0 is
                // valid as both a u64 atomic and as bytes for our
                // POD slot shape.
                let slots_ptr = region.as_ptr().add(slots_offset);
                ptr::write_bytes(slots_ptr, 0, lane_count * lane_stride);
            }
        } else {
            // Attaching to an existing segment — sanity-check the header.
            // SAFETY: region is mapped; header lives at offset 0.
            let header = unsafe { &*(region.as_ptr() as *const ShmRingHeader) };
            if header.magic != MAGIC {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} has wrong magic 0x{:08X} (expected 0x{:08X})",
                        name, header.magic, MAGIC
                    )
                ));
            }
            if header.version != VERSION {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("SHM segment {} version {} != local {}", name, header.version, VERSION)
                ));
            }
            if header.kind != kind {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("SHM segment {} kind {} != requested {}", name, header.kind, kind)
                ));
            }
            if header.topology != spec.topology as u8 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} topology {} != requested {}",
                        name, header.topology, spec.topology as u8
                    )
                ));
            }
            if header.lane_count as usize != lane_count {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} lane count {} != requested {}",
                        name, header.lane_count, lane_count
                    )
                ));
            }
            if header.capacity as usize != spec.capacity {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} capacity {} != requested {}",
                        name, header.capacity, spec.capacity
                    )
                ));
            }
            if header.payload_capacity as usize != spec.payload_capacity {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} payload capacity {} != requested {}",
                        name, header.payload_capacity, spec.payload_capacity
                    )
                ));
            }
            if header.slot_stride as usize != slot_stride {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} slot stride {} != requested {}",
                        name, header.slot_stride, slot_stride
                    )
                ));
            }
        }

        Ok(Self {
            region,
            kind,
            capacity: spec.capacity,
            payload_capacity: spec.payload_capacity,
            topology: spec.topology,
            lane_count,
            slot_stride,
            slots_offset,
            lane_stride,
            write_locks: (0..lane_count).map(|_| Mutex::new(())).collect()
        })
    }

    /// True when this handle was the one that *created* the SHM
    /// segment (vs attaching to an already-existing one).
    pub fn created(&self) -> bool {
        self.region.created()
    }

    /// Remove the SHM segment name. Existing mappings stay valid;
    /// new opens will fail until `open_or_create` recreates it.
    /// Call from the owner process at fleet shutdown.
    pub fn unlink(&self) -> std::io::Result<()> {
        self.region.unlink()
    }

    /// The KIND byte this ring carries.
    pub fn kind(&self) -> u8 {
        self.kind
    }

    /// Slot count per lane.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of physical writer lanes in this segment.
    pub fn lane_count(&self) -> usize {
        self.lane_count
    }

    /// Maximum inline payload bytes for this ring lane.
    pub fn payload_capacity(&self) -> usize {
        self.payload_capacity
    }

    pub fn spec(&self) -> RingSpec {
        RingSpec {
            capacity: self.capacity,
            payload_capacity: self.payload_capacity,
            topology: self.topology
        }
    }

    fn lane_header(
        &self,
        lane: usize
    ) -> &ShmLaneHeader {
        debug_assert!(lane < self.lane_count);
        unsafe {
            &*(self.region.as_ptr().add(HEADER_SIZE + lane * LANE_HEADER_SIZE)
                as *const ShmLaneHeader)
        }
    }

    #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    pub(crate) fn notification_generation(&self) -> &AtomicU32 {
        // SAFETY: the mapped header was initialized before this ring handle
        // was returned and remains mapped for the lifetime of `self`.
        unsafe { &(*(self.region.as_ptr() as *const ShmRingHeader)).notification_generation }
    }

    fn slot_ptr(
        &self,
        lane: usize,
        idx: usize
    ) -> *mut ShmSlotHeader {
        debug_assert!(lane < self.lane_count);
        debug_assert!(idx < self.capacity);
        // SAFETY: slots region begins at `slots_offset`; lane and idx
        // are bounded by the validated mapping layout.
        unsafe {
            let base = self.region.as_ptr().add(self.slots_offset + lane * self.lane_stride);
            base.add(idx * self.slot_stride).cast::<ShmSlotHeader>()
        }
    }

    /// The payload bytes, as the atomics they have to be for the same reason
    /// the header fields are.
    ///
    /// The cost is a byte at a time instead of a `memcpy`, over payloads this
    /// workspace sizes in tens to hundreds of bytes — 18 for a metric sample,
    /// 1024 at the largest — published at snapshot rather than request rate.
    /// Word-at-a-time would need the payload region padded to a word multiple,
    /// which changes the segment layout and so the compatibility check every
    /// attaching process makes; worth doing if a payload ever grows enough to
    /// notice, and not before.
    unsafe fn payload_ptr(slot_ptr: *mut ShmSlotHeader) -> *mut AtomicU8 {
        unsafe { slot_ptr.cast::<u8>().add(SLOT_HEADER_SIZE).cast::<AtomicU8>() }
    }

    /// Head of the sole shared lane, or lane zero for a per-node ring.
    pub fn head(&self) -> u64 {
        self.lane_header(0).write_pos.load(Ordering::Acquire)
    }

    /// Current committed head for `node_id`'s lane.
    pub fn lane_head(
        &self,
        node_id: NodeId
    ) -> u64 {
        let lane = self.lane_index(node_id);
        self.lane_header(lane).write_pos.load(Ordering::Acquire)
    }

    /// Allocate one non-zero semantic version shared by all writer lanes and
    /// processes attached to this ring.
    pub fn next_version(&self) -> u64 {
        let header = unsafe { &*(self.region.as_ptr() as *const ShmRingHeader) };
        header
            .version_counter
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .expect("SHM ring semantic version exhausted")
    }

    /// Last semantic version allocated by any process attached to this ring.
    pub fn current_version(&self) -> u64 {
        let header = unsafe { &*(self.region.as_ptr() as *const ShmRingHeader) };
        header.version_counter.load(Ordering::Acquire)
    }

    /// Append a frame. Atomically reserves the next counter, mints
    /// the [`NetId64`], writes the slot. Returns the minted id.
    pub fn write(
        &self,
        node_id: NodeId,
        frame_kind: u8,
        ver: u64,
        payload: Bytes
    ) -> std::io::Result<NetId64> {
        if payload.len() > self.payload_capacity {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "payload {} > ring payload capacity {}",
                    payload.len(),
                    self.payload_capacity
                )
            ));
        }

        let lane = self.lane_index(node_id);
        match self.topology {
            RingTopology::Shared => {
                let counter = self.lane_header(lane).write_pos.fetch_add(1, Ordering::AcqRel);
                Ok(self.write_slot(lane, node_id, counter, frame_kind, ver, &payload))
            }
            RingTopology::PerNode => {
                let _write =
                    self.write_locks[lane].lock().unwrap_or_else(|error| error.into_inner());
                let lane_header = self.lane_header(lane);
                let counter = lane_header.write_pos.load(Ordering::Relaxed);
                let id = self.write_slot(lane, node_id, counter, frame_kind, ver, &payload);
                lane_header.write_pos.store(counter.wrapping_add(1), Ordering::Release);
                Ok(id)
            }
            RingTopology::SharedOrdered => {
                let _write =
                    self.write_locks[lane].lock().unwrap_or_else(|error| error.into_inner());
                let _cross_process = self.region.lock_exclusive()?;
                let lane_header = self.lane_header(lane);
                let counter = lane_header.write_pos.load(Ordering::Relaxed);
                let id = self.write_slot(lane, node_id, counter, frame_kind, ver, &payload);
                lane_header.write_pos.store(counter.wrapping_add(1), Ordering::Release);
                Ok(id)
            }
        }
    }

    /// Append one contiguous batch to a lane and return consecutive ids.
    ///
    /// Per-node and shared-ordered lane heads advance only after the complete
    /// batch has committed. The batch may not exceed the ring capacity.
    pub fn write_batch(
        &self,
        node_id: NodeId,
        frame_kind: u8,
        ver: u64,
        payloads: Vec<Bytes>
    ) -> std::io::Result<Vec<NetId64>> {
        if payloads.len() > self.capacity {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("batch {} > ring capacity {}", payloads.len(), self.capacity)
            ));
        }
        if let Some(payload) = payloads.iter().find(|payload| payload.len() > self.payload_capacity)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "payload {} > ring payload capacity {}",
                    payload.len(),
                    self.payload_capacity
                )
            ));
        }
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let lane = self.lane_index(node_id);
        let write_slots = |start: u64, payloads: Vec<Bytes>| {
            payloads
                .into_iter()
                .enumerate()
                .map(|(offset, payload)| {
                    self.write_slot(
                        lane,
                        node_id,
                        start.wrapping_add(offset as u64),
                        frame_kind,
                        ver,
                        &payload
                    )
                })
                .collect::<Vec<_>>()
        };

        match self.topology {
            RingTopology::Shared => {
                let start = self
                    .lane_header(lane)
                    .write_pos
                    .fetch_add(payloads.len() as u64, Ordering::AcqRel);
                Ok(write_slots(start, payloads))
            }
            RingTopology::PerNode => {
                let _write =
                    self.write_locks[lane].lock().unwrap_or_else(|error| error.into_inner());
                let lane_header = self.lane_header(lane);
                let start = lane_header.write_pos.load(Ordering::Relaxed);
                let ids = write_slots(start, payloads);
                lane_header
                    .write_pos
                    .store(start.wrapping_add(ids.len() as u64), Ordering::Release);
                Ok(ids)
            }
            RingTopology::SharedOrdered => {
                let _write =
                    self.write_locks[lane].lock().unwrap_or_else(|error| error.into_inner());
                let _cross_process = self.region.lock_exclusive()?;
                let lane_header = self.lane_header(lane);
                let start = lane_header.write_pos.load(Ordering::Relaxed);
                let ids = write_slots(start, payloads);
                lane_header
                    .write_pos
                    .store(start.wrapping_add(ids.len() as u64), Ordering::Release);
                Ok(ids)
            }
        }
    }

    /// Read the slot whose counter matches `id.counter()`. Returns
    /// `None` if the slot has been overwritten, was never written,
    /// or a torn read could not be reconciled across two retries.
    pub fn read(
        &self,
        id: NetId64
    ) -> Option<Frame> {
        if id.kind() != self.kind {
            return None;
        }
        let lane = self.lane_index_for_frame(id)?;
        let counter = id.counter();
        let slot_idx = (counter as usize) & (self.capacity - 1);
        let slot_ptr = self.slot_ptr(lane, slot_idx);

        // Two retries — torn writes happen but resolve quickly.
        for _ in 0..3 {
            let Some(frame) = (unsafe { read_committed_frame(slot_ptr, self.payload_capacity) })
            else {
                continue;
            };
            if frame.id.counter() == counter {
                return Some(frame);
            } else {
                // Slot now holds a different (later) id — wraparound.
                return None;
            }
        }
        None
    }

    /// Read the most recent frame (head - 1). Returns `None` if no
    /// write has happened yet, or a torn read could not resolve.
    pub fn read_head(&self) -> Option<Frame> {
        let head = self.head();
        if head == 0 {
            return None;
        }
        let counter = head - 1;
        let slot_idx = (counter as usize) & (self.capacity - 1);
        let slot_ptr = self.slot_ptr(0, slot_idx);

        for _ in 0..3 {
            if let Some(frame) = unsafe { read_committed_frame(slot_ptr, self.payload_capacity) } {
                return Some(frame);
            }
        }
        None
    }

    /// Read whatever frame currently occupies the slot at
    /// `counter % capacity`, regardless of which counter is stored
    /// there. Used by walking readers that need slot-by-slot access
    /// without knowing the writer's `NetId64` ahead of time.
    pub fn read_at(
        &self,
        counter: u64
    ) -> Option<Frame> {
        self.read_lane_index_at(0, counter)
    }

    /// Read the frame currently occupying `node_id`'s lane slot.
    pub fn read_lane_at(
        &self,
        node_id: NodeId,
        counter: u64
    ) -> Option<Frame> {
        let lane = self.lane_index(node_id);
        self.read_lane_index_at(lane, counter)
    }

    fn read_lane_index_at(
        &self,
        lane: usize,
        counter: u64
    ) -> Option<Frame> {
        let slot_idx = (counter as usize) & (self.capacity - 1);
        let slot_ptr = self.slot_ptr(lane, slot_idx);
        for _ in 0..3 {
            if let Some(frame) = unsafe { read_committed_frame(slot_ptr, self.payload_capacity) } {
                return Some(frame);
            }
        }
        None
    }

    pub(crate) fn read_state_at(
        &self,
        counter: u64
    ) -> crate::ring::cursor::RingRead {
        self.read_lane_index_state_at(0, counter)
    }

    pub(crate) fn read_lane_state_at(
        &self,
        node_id: NodeId,
        counter: u64
    ) -> crate::ring::cursor::RingRead {
        let lane = self.lane_index(node_id);
        self.read_lane_index_state_at(lane, counter)
    }

    fn read_lane_index_state_at(
        &self,
        lane: usize,
        counter: u64
    ) -> crate::ring::cursor::RingRead {
        use crate::ring::cursor::RingRead;

        let slot_idx = (counter as usize) & (self.capacity - 1);
        let slot_ptr = self.slot_ptr(lane, slot_idx);
        let expected_committed =
            counter.checked_mul(2).and_then(|value| value.checked_add(2)).expect("seq overflow");

        for _ in 0..3 {
            let sequence = unsafe { &*slot_ptr }.seq.load(Ordering::Acquire);
            if sequence < expected_committed {
                return if self.topology != RingTopology::Shared {
                    RingRead::Unavailable
                } else {
                    RingRead::Pending
                };
            }
            if sequence > expected_committed {
                return RingRead::Unavailable;
            }
            if let Some(frame) = unsafe { read_committed_frame(slot_ptr, self.payload_capacity) } {
                return if frame.id.counter() == counter {
                    RingRead::Ready(frame)
                } else {
                    RingRead::Unavailable
                };
            }
        }

        let sequence = unsafe { &*slot_ptr }.seq.load(Ordering::Acquire);
        if sequence < expected_committed {
            if self.topology != RingTopology::Shared {
                RingRead::Unavailable
            } else {
                RingRead::Pending
            }
        } else {
            RingRead::Unavailable
        }
    }

    /// Clear all slots and reset every lane head to zero.
    ///
    /// Intended for owner-controlled boot-time cleanup. Do not call
    /// while other processes are publishing to this ring: it rewrites
    /// the shared slot memory in place.
    pub fn reset(&self) {
        // SAFETY: the region is mapped and the slot area begins at
        // HEADER_SIZE. The caller must ensure the ring is quiescent.
        unsafe {
            let slots_ptr = self.region.as_ptr().add(self.slots_offset);
            ptr::write_bytes(slots_ptr, 0, self.lane_count * self.lane_stride);
        }
        for lane in 0..self.lane_count {
            self.lane_header(lane).write_pos.store(0, Ordering::Release);
        }
        let header = unsafe { &*(self.region.as_ptr() as *const ShmRingHeader) };
        header.version_counter.store(0, Ordering::Release);
    }

    fn lane_index(
        &self,
        node_id: NodeId
    ) -> usize {
        let lane = match self.topology {
            RingTopology::Shared | RingTopology::SharedOrdered => 0,
            RingTopology::PerNode => usize::from(node_id.get())
        };
        assert!(
            lane < self.lane_count,
            "node {} is outside SHM ring lane count {}",
            node_id.get(),
            self.lane_count
        );
        lane
    }

    fn lane_index_for_frame(
        &self,
        id: NetId64
    ) -> Option<usize> {
        let lane = match self.topology {
            RingTopology::Shared | RingTopology::SharedOrdered => 0,
            RingTopology::PerNode => usize::from(id.node())
        };
        (lane < self.lane_count).then_some(lane)
    }

    fn write_slot(
        &self,
        lane: usize,
        node_id: NodeId,
        counter: u64,
        frame_kind: u8,
        ver: u64,
        payload: &[u8]
    ) -> NetId64 {
        let id = NetId64::make(self.kind, node_id.get(), counter);
        let slot_idx = (counter as usize) & (self.capacity - 1);
        let slot_ptr = self.slot_ptr(lane, slot_idx);

        // Disruptor-style write: seq goes odd → write content → seq goes even.
        //
        // The odd marker is stored `Relaxed` and followed by a `Release`
        // *fence*, not stored `Release`. A release store orders the accesses
        // that come *before* it and says nothing about the ones after, so the
        // content writes below were free to become visible ahead of the marker:
        // a reader would then see an even seq on both sides of a slot that was
        // being torn underneath it. The fence is what forbids that, because it
        // orders everything before it — the marker — ahead of every store after
        // it.
        //
        // The closing store stays `Release`: there it is the content writes
        // that must be visible first, which is exactly what a release store
        // gives. It pairs with the reader's `Acquire` fence.
        //
        // x86's store-store ordering hides the difference; aarch64 does not.
        unsafe {
            let slot = &*slot_ptr;
            let mid_seq = counter
                .checked_mul(2)
                .and_then(|value| value.checked_add(1))
                .expect("seq overflow");
            let final_seq = mid_seq.wrapping_add(1);

            slot.seq.store(mid_seq, Ordering::Relaxed);
            fence(Ordering::Release);
            slot.id.store(id.raw(), Ordering::Relaxed);
            slot.ver.store(ver, Ordering::Relaxed);
            slot.payload_len.store(payload.len() as u32, Ordering::Relaxed);
            slot.kind.store(frame_kind, Ordering::Relaxed);
            let bytes = Self::payload_ptr(slot_ptr);
            for (index, byte) in payload.iter().enumerate() {
                (*bytes.add(index)).store(*byte, Ordering::Relaxed);
            }
            slot.seq.store(final_seq, Ordering::Release);
        }

        id
    }
}

// ─────────────────────────────────────────────────────────────────────
// Read-only inspection
// ─────────────────────────────────────────────────────────────────────

/// Persisted geometry of one existing Orbit SHM ring.
///
/// The values come from the mapped ring header. An observer does not need the
/// producer's fleet capacity or compile-time [`RingSpec`] to inspect raw
/// frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShmRingMetadata {
    /// Exact POSIX SHM object name that was opened.
    pub name: String,
    /// Number of bytes reported by the existing SHM object.
    pub segment_size: usize,
    /// Persisted Orbit ring format version.
    pub format_version: u32,
    /// Stable Orbit type kind carried by this ring.
    pub kind: u8,
    /// Persisted per-lane capacity, payload limit, and topology.
    pub spec: RingSpec,
    /// Number of physical lanes present in the mapping.
    pub lane_count: usize
}

/// Read-only attachment to one existing Orbit SHM ring.
///
/// Attaching never creates, initializes, resets, locks, or unlinks a segment.
/// Dropping the view only unmaps this process's local mapping. Use
/// [`ShmRingView::lane`] to obtain a
/// [`RingFrameSource`](crate::ring::cursor::RingFrameSource) for one physical
/// lane.
pub struct ShmRingView {
    region: ShmRegion,
    metadata: ShmRingMetadata,
    slot_stride: usize,
    slots_offset: usize,
    lane_stride: usize
}

impl std::fmt::Debug for ShmRingView {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>
    ) -> std::fmt::Result {
        formatter.debug_struct("ShmRingView").field("metadata", &self.metadata).finish()
    }
}

impl ShmRingView {
    /// Attach to an existing ring owned by the effective user.
    pub fn attach_existing(
        fleet_name: &str,
        kind: u8
    ) -> std::io::Result<Self> {
        // SAFETY: `geteuid` has no error path.
        let uid = unsafe { libc::geteuid() };
        Self::attach_existing_for_uid(fleet_name, kind, uid)
    }

    /// Attach to an existing ring under an explicit uid-scoped name.
    ///
    /// POSIX permissions still decide whether the calling process may open
    /// another user's object.
    pub fn attach_existing_for_uid(
        fleet_name: &str,
        kind: u8,
        uid: u32
    ) -> std::io::Result<Self> {
        let name = shm::ring_segment_name_for_uid(fleet_name, kind, uid);
        let region = ShmRegion::open_existing_read_only(&name, HEADER_SIZE)?;

        // SAFETY: the mapping is at least HEADER_SIZE bytes and the header is
        // cache-line aligned at offset zero.
        let header = unsafe { &*(region.as_ptr() as *const ShmRingHeader) };
        if header.magic != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {} has wrong magic 0x{:08X} (expected 0x{:08X})",
                    name, header.magic, MAGIC
                )
            ));
        }
        if header.version != VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("SHM segment {} version {} != local {}", name, header.version, VERSION)
            ));
        }
        if header.kind != kind {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("SHM segment {} kind {} != requested {}", name, header.kind, kind)
            ));
        }

        let topology = match header.topology {
            value if value == RingTopology::Shared as u8 => RingTopology::Shared,
            value if value == RingTopology::PerNode as u8 => RingTopology::PerNode,
            value if value == RingTopology::SharedOrdered as u8 => RingTopology::SharedOrdered,
            value => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("SHM segment {name} has unknown topology {value}")
                ));
            }
        };
        let capacity = usize::try_from(header.capacity).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {name} capacity {} does not fit this platform",
                    header.capacity
                )
            )
        })?;
        let lane_count = usize::from(header.lane_count);
        if lane_count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("SHM segment {name} has zero lanes")
            ));
        }
        if topology != RingTopology::PerNode && lane_count != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {name} topology {topology:?} requires one lane, found {lane_count}"
                )
            ));
        }
        let spec =
            RingSpec { capacity, payload_capacity: header.payload_capacity as usize, topology };
        let fleet_capacity = if topology == RingTopology::PerNode { header.lane_count } else { 1 };
        let (slot_stride, slots_offset, expected_size) = checked_layout(spec, fleet_capacity)
            .map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("SHM segment {name} has invalid geometry: {error}")
                )
            })?;
        if header.slot_stride as usize != slot_stride {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {} slot stride {} != geometry {}",
                    name, header.slot_stride, slot_stride
                )
            ));
        }
        if region.len() < expected_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {} size {} is smaller than persisted geometry {}",
                    name,
                    region.len(),
                    expected_size
                )
            ));
        }
        let lane_stride = capacity
            .checked_mul(slot_stride)
            .ok_or_else(|| invalid_input(format!("SHM segment {name} lane stride overflow")))?;

        Ok(Self {
            metadata: ShmRingMetadata {
                name,
                segment_size: region.len(),
                format_version: header.version,
                kind,
                spec,
                lane_count
            },
            region,
            slot_stride,
            slots_offset,
            lane_stride
        })
    }

    /// Attach and also verify the caller's typed ring contract.
    pub fn attach_typed<T: crate::OrbitTyped>(fleet_name: &str) -> std::io::Result<Self> {
        let view = Self::attach_existing(fleet_name, T::KIND)?;
        if view.metadata.spec != T::RING_SPEC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "OrbitTyped KIND {} declares {:?}; existing spec is {:?}",
                    T::KIND,
                    T::RING_SPEC,
                    view.metadata.spec
                )
            ));
        }
        Ok(view)
    }

    pub fn metadata(&self) -> &ShmRingMetadata {
        &self.metadata
    }

    /// Last semantic version allocated by the ring at observation time.
    pub fn current_version(&self) -> u64 {
        self.header().version_counter.load(Ordering::Acquire)
    }

    /// Current notification generation used by native readiness bridges.
    pub fn notification_generation(&self) -> u32 {
        self.header().notification_generation.load(Ordering::Acquire)
    }

    /// Select one physical lane as a generic read-only frame source.
    pub fn lane(
        &self,
        lane: usize
    ) -> std::io::Result<ShmRingLaneView<'_>> {
        if lane >= self.metadata.lane_count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("lane {lane} is outside SHM ring lane count {}", self.metadata.lane_count)
            ));
        }
        Ok(ShmRingLaneView { ring: self, lane })
    }

    fn header(&self) -> &ShmRingHeader {
        // SAFETY: construction validated the mapped header.
        unsafe { &*(self.region.as_ptr() as *const ShmRingHeader) }
    }

    fn lane_header(
        &self,
        lane: usize
    ) -> &ShmLaneHeader {
        debug_assert!(lane < self.metadata.lane_count);
        // SAFETY: construction validated lane count and the complete geometry.
        unsafe {
            &*(self.region.as_ptr().add(HEADER_SIZE + lane * LANE_HEADER_SIZE)
                as *const ShmLaneHeader)
        }
    }

    fn slot_ptr(
        &self,
        lane: usize,
        counter: u64
    ) -> *mut ShmSlotHeader {
        debug_assert!(lane < self.metadata.lane_count);
        let slot = (counter as usize) & (self.metadata.spec.capacity - 1);
        // SAFETY: construction checked the complete persisted geometry and the
        // lane/slot indices are bounded above.
        unsafe {
            self.region
                .as_ptr()
                .add(self.slots_offset + lane * self.lane_stride + slot * self.slot_stride)
                .cast::<ShmSlotHeader>()
        }
    }

    fn read_lane_state_at(
        &self,
        lane: usize,
        counter: u64
    ) -> crate::ring::cursor::RingRead {
        use crate::ring::cursor::RingRead;

        let slot_ptr = self.slot_ptr(lane, counter);
        let Some(expected_committed) =
            counter.checked_mul(2).and_then(|value| value.checked_add(2))
        else {
            return RingRead::Unavailable;
        };

        for _ in 0..3 {
            // SAFETY: `slot_ptr` addresses a validated slot in the mapping.
            let sequence = unsafe { &*slot_ptr }.seq.load(Ordering::Acquire);
            if sequence < expected_committed {
                return if self.metadata.spec.topology == RingTopology::Shared {
                    RingRead::Pending
                } else {
                    RingRead::Unavailable
                };
            }
            if sequence > expected_committed {
                return RingRead::Unavailable;
            }
            // SAFETY: `slot_ptr` addresses a validated slot in the mapping.
            if let Some(frame) =
                unsafe { read_committed_frame(slot_ptr, self.metadata.spec.payload_capacity) }
            {
                let lane_matches = self.metadata.spec.topology != RingTopology::PerNode
                    || usize::from(frame.id.node()) == lane;
                return if frame.id.kind() == self.metadata.kind
                    && frame.id.counter() == counter
                    && lane_matches
                {
                    RingRead::Ready(frame)
                } else {
                    RingRead::Unavailable
                };
            }
        }
        RingRead::Unavailable
    }
}

/// One physical lane of a [`ShmRingView`].
#[derive(Clone, Copy, Debug)]
pub struct ShmRingLaneView<'a> {
    ring: &'a ShmRingView,
    lane: usize
}

impl ShmRingLaneView<'_> {
    pub fn index(self) -> usize {
        self.lane
    }

    /// Snapshot the currently visible committed/reserved head.
    pub fn head(self) -> u64 {
        self.ring.lane_header(self.lane).write_pos.load(Ordering::Acquire)
    }

    /// Counter range that may still be retained at the observed head.
    pub fn retained_range(self) -> std::ops::Range<u64> {
        let head = self.head();
        head.saturating_sub(self.ring.metadata.spec.capacity as u64)..head
    }

    /// Read the exact counter if its frame is still committed in this lane.
    pub fn read_at(
        self,
        counter: u64
    ) -> Option<Frame> {
        match self.ring.read_lane_state_at(self.lane, counter) {
            crate::ring::cursor::RingRead::Ready(frame) => Some(frame),
            crate::ring::cursor::RingRead::Pending | crate::ring::cursor::RingRead::Unavailable => {
                None
            }
        }
    }

    /// Read the frame immediately below the observed head.
    pub fn read_head(self) -> Option<Frame> {
        let head = self.head();
        head.checked_sub(1).and_then(|counter| self.read_at(counter))
    }
}

impl crate::ring::cursor::RingFrameSource for ShmRingLaneView<'_> {
    fn kind(&self) -> u8 {
        self.ring.metadata.kind
    }

    fn head(&self) -> u64 {
        (*self).head()
    }

    fn capacity(&self) -> usize {
        self.ring.metadata.spec.capacity
    }

    fn read_at(
        &self,
        counter: u64
    ) -> Option<Frame> {
        (*self).read_at(counter)
    }

    fn read_state_at(
        &self,
        counter: u64
    ) -> crate::ring::cursor::RingRead {
        self.ring.read_lane_state_at(self.lane, counter)
    }
}

// ─────────────────────────────────────────────────────────────────────
// ShmRingRegistry — per-fleet, per-KIND map of ShmRings
// ─────────────────────────────────────────────────────────────────────

/// Type-keyed registry of [`ShmRing`]s held by a fleet. One ring per
/// `OrbitTyped::KIND` byte; created on demand the first time a kind
/// is published or queried.
pub struct ShmRingRegistry {
    fleet_name: String,
    fleet_capacity: u16,
    rings: dashmap::DashMap<u8, std::sync::Arc<ShmRing>>
}

impl ShmRingRegistry {
    pub fn new(
        fleet_name: impl Into<String>,
        fleet_capacity: u16
    ) -> Self {
        Self { fleet_name: fleet_name.into(), fleet_capacity, rings: dashmap::DashMap::new() }
    }

    /// Get-or-create the SHM ring for `kind`. Failure here means the
    /// SHM open or attach failed (permissions, name too long, etc.)
    /// and is propagated as `io::Error`.
    pub fn get_or_create_for<T: crate::OrbitTyped>(
        &self
    ) -> std::io::Result<std::sync::Arc<ShmRing>> {
        if let Some(entry) = self.rings.get(&T::KIND) {
            if entry.spec() != T::RING_SPEC {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "OrbitTyped KIND {} was reused with ring spec {:?}; existing spec is {:?}",
                        T::KIND,
                        T::RING_SPEC,
                        entry.spec()
                    )
                ));
            }
            return Ok(entry.clone());
        }
        let ring = std::sync::Arc::new(ShmRing::open_or_create_for_fleet(
            &self.fleet_name,
            T::KIND,
            T::RING_SPEC,
            self.fleet_capacity
        )?);
        let entry = self.rings.entry(T::KIND).or_insert_with(|| ring.clone());
        if entry.spec() != T::RING_SPEC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "OrbitTyped KIND {} raced with ring spec {:?}; installed spec is {:?}",
                    T::KIND,
                    T::RING_SPEC,
                    entry.spec()
                )
            ));
        }
        Ok(entry.clone())
    }

    /// Look up a ring that has already been created for `kind`.
    /// Returns `None` if no such ring exists yet.
    pub fn lookup(
        &self,
        kind: u8
    ) -> Option<std::sync::Arc<ShmRing>> {
        self.rings.get(&kind).map(|e| e.clone())
    }
}

/// Read a slot's content; returns `None` if the seq indicates an
/// in-flight write or if pre/post seqs disagree (torn read).
///
/// # Safety
///
/// `slot_ptr` must point at a valid `ShmSlot` mapped into our
/// address space and aligned per the `repr(C, align(64))` layout.
unsafe fn read_committed_frame(
    slot_ptr: *mut ShmSlotHeader,
    payload_capacity: usize
) -> Option<Frame> {
    let slot = unsafe { &*slot_ptr };
    let seq_pre = slot.seq.load(Ordering::Acquire);
    if seq_pre == 0 {
        // never written
        return None;
    }
    if seq_pre & 1 == 1 {
        // writer in progress
        return None;
    }

    // Read content fields. `Relaxed` throughout: the seq load above and the
    // fence below are what order this, and a value read here is only trusted
    // once the two seqs agree.
    let id = NetId64::from_raw(slot.id.load(Ordering::Relaxed));
    let kind = slot.kind.load(Ordering::Relaxed);
    let ver = slot.ver.load(Ordering::Relaxed);
    let payload_len = slot.payload_len.load(Ordering::Relaxed) as usize;
    if payload_len > payload_capacity {
        // corrupt — bail
        return None;
    }
    // `with_capacity` + `push` rather than `vec![0; len]` + overwrite: the
    // zeroing would be written over by the very next loop, and a walking reader
    // over a 16k-slot ring pays that twice for every byte it reads. The push
    // cannot reallocate — the capacity is reserved above.
    let payload_src = unsafe { ShmRing::payload_ptr(slot_ptr) };
    let mut payload_buf = Vec::with_capacity(payload_len);
    for index in 0..payload_len {
        payload_buf.push(unsafe { (*payload_src.add(index)).load(Ordering::Relaxed) });
    }

    // The mirror of the writer's fence. `Acquire` on a load orders the accesses
    // that come *after* it, so reading the closing seq with `Acquire` would
    // leave the content reads above free to be reordered past it — and the
    // comparison below would then be comparing against a slot it never actually
    // read. An acquire fence orders those reads ahead of the load that follows,
    // which is the guarantee this check is asking for; the load itself needs no
    // ordering of its own once the fence is there.
    fence(Ordering::Acquire);
    let seq_post = slot.seq.load(Ordering::Relaxed);
    if seq_pre != seq_post {
        // torn write — caller can retry
        return None;
    }

    Some(Frame { id, kind, ver, payload: Bytes::from(payload_buf) })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use nix::sys::wait::{WaitStatus, waitpid};
    use nix::unistd::{ForkResult, fork};

    use super::*;
    use crate::ring::cursor::{RingCursor, poll_ring};

    #[test]
    fn cursor_retries_a_claimed_slot_after_it_commits() {
        static TEST_ID: AtomicU64 = AtomicU64::new(0);

        let test_id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let fleet_name = format!("p{:x}{test_id:x}", std::process::id());
        let ring = ShmRing::open_or_create(&fleet_name, 199, RingSpec::new(4, 16))
            .expect("create test ring");
        ring.reset();
        let slot_ptr = ring.slot_ptr(0, 0);

        ring.lane_header(0).write_pos.store(1, Ordering::Release);
        unsafe { &*slot_ptr }.seq.store(1, Ordering::Release);

        let mut cursor = RingCursor::from_start();
        let pending = poll_ring(&ring, &mut cursor);
        assert!(pending.is_empty());
        assert_eq!(cursor.next_counter(), 0);

        let payload = b"ready";
        unsafe {
            let slot = &*slot_ptr;
            slot.id.store(NetId64::make(199, NodeId::ZERO.get(), 0).raw(), Ordering::Relaxed);
            slot.ver.store(7, Ordering::Relaxed);
            slot.payload_len.store(payload.len() as u32, Ordering::Relaxed);
            slot.kind.store(1, Ordering::Relaxed);
            let bytes = ShmRing::payload_ptr(slot_ptr);
            for (index, byte) in payload.iter().enumerate() {
                (*bytes.add(index)).store(*byte, Ordering::Relaxed);
            }
        }
        unsafe { &*slot_ptr }.seq.store(2, Ordering::Release);

        let committed = poll_ring(&ring, &mut cursor);
        assert_eq!(committed.frames.len(), 1);
        assert_eq!(&committed.frames[0].payload[..], payload);
        assert_eq!(cursor.next_counter(), 1);

        ring.unlink().expect("unlink test ring");
    }

    #[test]
    fn per_node_head_ignores_a_writer_that_dies_before_commit() {
        static TEST_ID: AtomicU64 = AtomicU64::new(0);

        let test_id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let fleet_name = format!("d{:x}{test_id:x}", std::process::id());
        let spec = RingSpec::per_node(4, 16);
        let abandoned = ShmRing::open_or_create_for_fleet(&fleet_name, 198, spec, 2)
            .expect("create per-node test ring");
        abandoned.reset();

        let slot_ptr = abandoned.slot_ptr(1, 0);
        unsafe { &*slot_ptr }.seq.store(1, Ordering::Release);
        assert_eq!(abandoned.lane_head(NodeId::new(1)), 0);

        let replacement = ShmRing::open_or_create_for_fleet(&fleet_name, 198, spec, 2)
            .expect("replacement attaches");
        let id = replacement
            .write(NodeId::new(1), 1, 9, Bytes::from_static(b"recovered"))
            .expect("replacement commits");

        assert_eq!(id.counter(), 0);
        assert_eq!(replacement.lane_head(NodeId::new(1)), 1);
        assert_eq!(&replacement.read(id).expect("frame visible").payload[..], b"recovered");

        replacement.unlink().expect("unlink test ring");
    }

    #[test]
    fn per_node_lane_serializes_concurrent_local_publishers() {
        static TEST_ID: AtomicU64 = AtomicU64::new(0);

        let test_id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let fleet_name = format!("c{:x}{test_id:x}", std::process::id());
        let ring = std::sync::Arc::new(
            ShmRing::open_or_create_for_fleet(&fleet_name, 197, RingSpec::per_node(512, 0), 2)
                .expect("create concurrent writer ring")
        );
        ring.reset();

        let mut writers = Vec::new();
        for _ in 0..4 {
            let ring = ring.clone();
            writers.push(std::thread::spawn(move || {
                (0..64)
                    .map(|_| {
                        ring.write(NodeId::new(1), 1, 0, Bytes::new()).expect("publish").counter()
                    })
                    .collect::<Vec<_>>()
            }));
        }

        let mut counters = writers
            .into_iter()
            .flat_map(|writer| writer.join().expect("writer joins"))
            .collect::<Vec<_>>();
        counters.sort_unstable();
        assert_eq!(counters, (0..256).collect::<Vec<_>>());
        assert_eq!(ring.lane_head(NodeId::new(1)), 256);

        ring.unlink().expect("unlink test ring");
    }

    #[test]
    fn shared_ordered_recovers_after_a_locked_writer_process_dies() {
        static TEST_ID: AtomicU64 = AtomicU64::new(0);

        let test_id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let fleet_name = format!("o{:x}{test_id:x}", std::process::id());
        let spec = RingSpec::shared_ordered(4, 16);
        let ring = ShmRing::open_or_create_for_fleet(&fleet_name, 196, spec, 2)
            .expect("create shared-ordered test ring");
        ring.reset();

        match unsafe { fork() }.expect("fork test writer") {
            ForkResult::Child => {
                // Use the handle inherited while it was idle. It must not carry
                // a persistent lock descriptor across fork; the child opens a
                // fresh file description for this critical section.
                let _lock = ring.region.lock_exclusive().expect("child locks ring");
                let slot_ptr = ring.slot_ptr(0, 0);
                unsafe { &*slot_ptr }.seq.store(1, Ordering::Release);

                // Exit without destructors: the kernel, not Rust cleanup,
                // must release the descriptor lock.
                unsafe { libc::_exit(0) };
            }
            ForkResult::Parent { child } => {
                let status = waitpid(child, None).expect("wait for abandoned writer");
                assert!(matches!(status, WaitStatus::Exited(_, 0)));

                // A separately opened handle must acquire the lock after the
                // child dies. If the original region retained an idle flock fd,
                // fork would duplicate its open-file-description into the child
                // and the dead writer's lock would still be attached here.
                let replacement = ShmRing::open_or_create_for_fleet(&fleet_name, 196, spec, 2)
                    .expect("replacement attaches");
                let recovered_lock = replacement
                    .region
                    .try_lock_exclusive()
                    .expect("kernel released dead writer lock");
                drop(recovered_lock);

                let id = replacement
                    .write(NodeId::new(1), 1, 9, Bytes::from_static(b"recovered"))
                    .expect("replacement commits counter zero");
                assert_eq!(id.counter(), 0);
                assert_eq!(replacement.head(), 1);
                assert_eq!(
                    &replacement.read(id).expect("recovered frame").payload[..],
                    b"recovered"
                );

                replacement.unlink().expect("unlink shared-ordered test ring");
            }
        }
    }
}

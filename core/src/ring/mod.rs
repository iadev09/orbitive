//! Ring buffers — orbit-core's runtime substrate.
//!
//! > *"orbit runtime yani ring"* — the place where the fleet's
//! > shared state actually lives at the lowest level. Higher-level
//! > shapes (cache mutations, metrics snapshots, event streams, etc.)
//! > reduce to *one or more rings*.
//!
//! ## Shape
//!
//! One [`Ring`] per [`OrbitTyped`] kind. A ring is one or more fixed-size
//! circular lanes of [`Frame`]s. Shared rings use one lock-free multi-writer
//! claim sequence. Shared-ordered rings serialize writers and expose only
//! committed counters. Per-node rings give every fleet member a disjoint lane
//! whose head advances only after a frame is committed. When a lane head
//! exceeds its capacity, the oldest slot in that lane is overwritten.
//!
//! The frame layout mirrors the `nwd1` seed (see VISION §13):
//!
//! ```text
//! ┌──────────┬──────┬──────┬─────────────┐
//! │ id (8)   │ kind │ ver  │ payload (N) │
//! │ NetId64  │  u8  │ u64  │   bytes     │
//! └──────────┴──────┴──────┴─────────────┘
//! ```
//!
//! Two `kind` bytes coexist on the wire and they mean different
//! things (intentional, two-axis encoding):
//!
//! - `frame.id.kind()` — *which Rust type* (the data shape).
//! - `frame.kind`      — *which message class* (state / event /
//!   command / ack / invalidate / …). V0 leaves this open at `0`;
//!   concrete classes appear when subscriber semantics arrive.
//!
//! ## V0 backing
//!
//! `RwLock<Option<Frame>>` per slot — simple, correct, slow. The SHM
//! backing uses per-slot sequence numbers to prevent torn reads. Per-node
//! lanes serialize concurrent publishers inside one process, commit the
//! slot, then release-store their lane head.
//!
//! ## Who mints
//!
//! Writes go through [`Ring::write`], which mints the [`NetId64`]. The
//! COUNTER part is the position inside either the shared sequence or the
//! writer node's lane. NetId64s are therefore minted
//! **server-side, by the writer process**. Browsers / external
//! clients receive ids; they do not generate them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;

use crate::NodeId;
use crate::OrbitTyped;
use crate::id::NetId64;

pub mod cursor;
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
mod readiness;
#[cfg(unix)]
pub mod shm;

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
pub use readiness::RingEventFd;

/// Writer ownership for one [`OrbitTyped`] ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RingTopology {
    /// Every fleet member publishes into one shared multi-writer sequence.
    Shared = 0,
    /// Every fleet member owns an independent single-process writer lane.
    /// The embedder must not run two active processes with the same node id;
    /// local concurrent tasks are serialized by the ring handle.
    PerNode = 1,
    /// Every fleet member publishes into one globally ordered sequence.
    /// Writers are serialized by a process-recoverable OS lock associated
    /// with the SHM name, and the head advances only after the slot commits.
    SharedOrdered = 2,
}

/// Physical policy for one [`OrbitTyped`] ring.
///
/// The policy is part of the wire contract: every process using the
/// same `OrbitTyped::KIND` must declare the same values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingSpec {
    /// Number of slots retained by the ring, per lane.
    pub capacity: usize,
    /// Maximum payload bytes stored inline in each slot.
    pub payload_capacity: usize,
    /// How writers own and publish physical lanes.
    pub topology: RingTopology,
}

impl RingSpec {
    pub const fn new(capacity: usize, payload_capacity: usize) -> Self {
        Self {
            capacity,
            payload_capacity,
            topology: RingTopology::Shared,
        }
    }

    /// Declare one independent writer lane per fleet node.
    pub const fn per_node(capacity: usize, payload_capacity: usize) -> Self {
        Self {
            capacity,
            payload_capacity,
            topology: RingTopology::PerNode,
        }
    }

    /// Declare one crash-recoverable, globally ordered writer lane.
    pub const fn shared_ordered(capacity: usize, payload_capacity: usize) -> Self {
        Self {
            capacity,
            payload_capacity,
            topology: RingTopology::SharedOrdered,
        }
    }

    pub(crate) fn assert_valid(self) {
        assert!(self.capacity > 0, "ring capacity must be > 0");
        assert!(
            self.capacity.is_power_of_two(),
            "ring capacity must be a power of two"
        );
        assert!(
            self.payload_capacity <= u32::MAX as usize,
            "ring payload capacity must fit in u32"
        );
    }
}

struct RingLane {
    write_pos: AtomicU64,
    write_lock: Mutex<()>,
    slots: Vec<RwLock<Option<Frame>>>,
}

impl RingLane {
    fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(RwLock::new(None));
        }
        Self {
            write_pos: AtomicU64::new(0),
            write_lock: Mutex::new(()),
            slots,
        }
    }
}

/// One record in a ring — the on-wire shape (mirrors `nwd1::Frame`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub id: NetId64,
    pub kind: u8,
    pub ver: u64,
    pub payload: Bytes,
}

/// A fixed-capacity, fleet-wide append-only log keyed on KIND byte.
///
/// V0 is single-process; V1 is SHM-backed. The API is the same.
pub struct Ring {
    /// The KIND this ring carries — equals `T::KIND` for the
    /// `OrbitTyped` value-shape it's storing.
    kind: u8,
    /// Number of slots; constant for the ring's lifetime.
    capacity: usize,
    /// Maximum inline payload bytes for this ring lane.
    payload_capacity: usize,
    topology: RingTopology,
    /// Ring-wide semantic version allocator shared by every writer lane.
    version_counter: AtomicU64,
    lanes: Vec<RingLane>,
}

impl Ring {
    /// Create the process-local ring declared by `T::RING_SPEC` for a
    /// single-member fleet.
    pub fn new<T: OrbitTyped>() -> Self {
        Self::new_for_fleet::<T>(1)
    }

    /// Create the process-local ring declared by `T::RING_SPEC` with the
    /// physical lane count required by `fleet_capacity`.
    pub fn new_for_fleet<T: OrbitTyped>(fleet_capacity: u16) -> Self {
        assert!(fleet_capacity > 0, "ring fleet capacity must be > 0");
        let spec = T::RING_SPEC;
        spec.assert_valid();
        let capacity = spec.capacity;
        let lane_count = match spec.topology {
            RingTopology::Shared | RingTopology::SharedOrdered => 1,
            RingTopology::PerNode => usize::from(fleet_capacity),
        };
        let mut lanes = Vec::with_capacity(lane_count);
        for _ in 0..lane_count {
            lanes.push(RingLane::new(capacity));
        }
        Self {
            kind: T::KIND,
            capacity,
            payload_capacity: spec.payload_capacity,
            topology: spec.topology,
            version_counter: AtomicU64::new(0),
            lanes,
        }
    }

    /// The KIND byte this ring carries (equals `T::KIND`).
    pub fn kind(&self) -> u8 {
        self.kind
    }

    /// Total slot count — fixed at construction.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Maximum inline payload bytes for this ring lane.
    pub fn payload_capacity(&self) -> usize {
        self.payload_capacity
    }

    pub fn spec(&self) -> RingSpec {
        RingSpec {
            capacity: self.capacity,
            payload_capacity: self.payload_capacity,
            topology: self.topology,
        }
    }

    /// Head of the sole shared lane, or lane zero for a per-node ring.
    pub fn head(&self) -> u64 {
        self.lanes[0].write_pos.load(Ordering::Acquire)
    }

    /// Number of physical writer lanes in this ring.
    pub fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    /// Current head for `node_id`'s logical lane.
    pub fn lane_head(&self, node_id: NodeId) -> u64 {
        self.lane(node_id).write_pos.load(Ordering::Acquire)
    }

    /// Allocate one non-zero semantic version shared by every writer lane.
    ///
    /// This counter is independent of physical ring positions. Semantic
    /// layers can use it when per-node lanes need one deterministic
    /// last-write-wins order.
    pub fn next_version(&self) -> u64 {
        self.version_counter
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .expect("ring semantic version exhausted")
    }

    /// Last semantic version allocated for this ring.
    pub fn current_version(&self) -> u64 {
        self.version_counter.load(Ordering::Acquire)
    }

    /// Append a frame. Atomically reserves the next counter, mints
    /// the [`NetId64`], and writes the frame into the corresponding
    /// slot. Returns the minted id.
    ///
    /// `frame_kind` is the message class byte (V0: pass `0`).
    /// `ver` is the version / tick at write time (V0: caller's
    /// choice).
    pub fn write(&self, node_id: NodeId, frame_kind: u8, ver: u64, payload: Bytes) -> NetId64 {
        assert!(
            payload.len() <= self.payload_capacity,
            "payload {} > ring payload capacity {}",
            payload.len(),
            self.payload_capacity
        );
        let lane = self.lane(node_id);
        match self.topology {
            RingTopology::Shared => {
                let counter = lane.write_pos.fetch_add(1, Ordering::AcqRel);
                self.write_frame(lane, node_id, counter, frame_kind, ver, payload)
            }
            RingTopology::PerNode | RingTopology::SharedOrdered => {
                let _write = lane
                    .write_lock
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let counter = lane.write_pos.load(Ordering::Relaxed);
                let id = self.write_frame(lane, node_id, counter, frame_kind, ver, payload);
                lane.write_pos
                    .store(counter.wrapping_add(1), Ordering::Release);
                id
            }
        }
    }

    /// Append a contiguous batch to one lane and return its consecutive ids.
    ///
    /// Per-node and shared-ordered lanes expose the new head only after the
    /// whole batch commits. An empty batch is a no-op. A batch larger than the
    /// ring is rejected because its first frames could not remain addressable
    /// when the method returns.
    pub fn write_batch(
        &self,
        node_id: NodeId,
        frame_kind: u8,
        ver: u64,
        payloads: Vec<Bytes>,
    ) -> Vec<NetId64> {
        assert!(
            payloads.len() <= self.capacity,
            "batch {} > ring capacity {}",
            payloads.len(),
            self.capacity
        );
        for payload in &payloads {
            assert!(
                payload.len() <= self.payload_capacity,
                "payload {} > ring payload capacity {}",
                payload.len(),
                self.payload_capacity
            );
        }
        if payloads.is_empty() {
            return Vec::new();
        }

        let lane = self.lane(node_id);
        match self.topology {
            RingTopology::Shared => {
                let start = lane
                    .write_pos
                    .fetch_add(payloads.len() as u64, Ordering::AcqRel);
                payloads
                    .into_iter()
                    .enumerate()
                    .map(|(offset, payload)| {
                        self.write_frame(
                            lane,
                            node_id,
                            start.wrapping_add(offset as u64),
                            frame_kind,
                            ver,
                            payload,
                        )
                    })
                    .collect()
            }
            RingTopology::PerNode | RingTopology::SharedOrdered => {
                let _write = lane
                    .write_lock
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let start = lane.write_pos.load(Ordering::Relaxed);
                let ids = payloads
                    .into_iter()
                    .enumerate()
                    .map(|(offset, payload)| {
                        self.write_frame(
                            lane,
                            node_id,
                            start.wrapping_add(offset as u64),
                            frame_kind,
                            ver,
                            payload,
                        )
                    })
                    .collect::<Vec<_>>();
                lane.write_pos
                    .store(start.wrapping_add(ids.len() as u64), Ordering::Release);
                ids
            }
        }
    }

    /// Read the slot that the given [`NetId64`]'s counter points at.
    ///
    /// Returns:
    /// - `Some(frame)` if the slot's stored id matches the queried id
    ///   exactly (the slot has not been overwritten by a later writer).
    /// - `None` if the slot is empty, has wrapped past, or holds a
    ///   different id than the one asked for.
    pub fn read(&self, id: NetId64) -> Option<Frame> {
        if id.kind() != self.kind {
            return None;
        }
        let lane = self.lane_for_frame(id)?;
        let slot_idx = (id.counter() as usize) % self.capacity;
        let guard = lane.slots[slot_idx].read().expect("ring slot poisoned");
        match &*guard {
            Some(f) if f.id == id => Some(f.clone()),
            _ => None,
        }
    }

    /// Read the most recent frame, regardless of who wrote it.
    /// Useful for "what's the current state?" — ignores
    /// counter-by-counter walking.
    pub fn read_head(&self) -> Option<Frame> {
        let head = self.head();
        if head == 0 {
            return None;
        }
        let slot_idx = ((head - 1) as usize) % self.capacity;
        self.lanes[0].slots[slot_idx]
            .read()
            .expect("ring slot poisoned")
            .clone()
    }

    /// Read whatever frame currently occupies the slot at
    /// `counter % capacity`, regardless of which counter is
    /// stored in it. Used by walking readers that need slot-by-slot
    /// access without knowing the writer's `NetId64` ahead of time.
    ///
    /// Returns `None` if the slot is empty.
    pub fn read_at(&self, counter: u64) -> Option<Frame> {
        let slot_idx = (counter as usize) % self.capacity;
        self.lanes[0].slots[slot_idx]
            .read()
            .expect("ring slot poisoned")
            .clone()
    }

    pub(crate) fn read_state_at(&self, counter: u64) -> cursor::RingRead {
        match self.read_at(counter) {
            Some(frame) if frame.id.counter() == counter => cursor::RingRead::Ready(frame),
            Some(frame) if frame.id.counter() > counter => cursor::RingRead::Unavailable,
            Some(_) | None => cursor::RingRead::Pending,
        }
    }

    pub(crate) fn read_lane_at(&self, node_id: NodeId, counter: u64) -> Option<Frame> {
        let lane = self.lane(node_id);
        let slot_idx = (counter as usize) % self.capacity;
        lane.slots[slot_idx]
            .read()
            .expect("ring slot poisoned")
            .clone()
    }

    pub(crate) fn read_lane_state_at(&self, node_id: NodeId, counter: u64) -> cursor::RingRead {
        match self.read_lane_at(node_id, counter) {
            Some(frame) if frame.id.counter() == counter => cursor::RingRead::Ready(frame),
            Some(frame) if frame.id.counter() > counter => cursor::RingRead::Unavailable,
            Some(_) | None if self.topology != RingTopology::Shared => {
                cursor::RingRead::Unavailable
            }
            Some(_) | None => cursor::RingRead::Pending,
        }
    }

    /// Clear all slots and reset every lane head to zero.
    ///
    /// Intended for owner-controlled boot-time cleanup. Do not call
    /// while other threads are publishing to this ring.
    pub fn reset(&self) {
        for lane in &self.lanes {
            for slot in &lane.slots {
                *slot.write().expect("ring slot poisoned") = None;
            }
            lane.write_pos.store(0, Ordering::Release);
        }
        self.version_counter.store(0, Ordering::Release);
    }

    fn lane(&self, node_id: NodeId) -> &RingLane {
        let index = match self.topology {
            RingTopology::Shared | RingTopology::SharedOrdered => 0,
            RingTopology::PerNode => usize::from(node_id.get()),
        };
        self.lanes.get(index).unwrap_or_else(|| {
            panic!(
                "node {} is outside ring lane count {}",
                node_id.get(),
                self.lanes.len()
            )
        })
    }

    fn lane_for_frame(&self, id: NetId64) -> Option<&RingLane> {
        let index = match self.topology {
            RingTopology::Shared | RingTopology::SharedOrdered => 0,
            RingTopology::PerNode => usize::from(id.node()),
        };
        self.lanes.get(index)
    }

    fn write_frame(
        &self,
        lane: &RingLane,
        node_id: NodeId,
        counter: u64,
        frame_kind: u8,
        ver: u64,
        payload: Bytes,
    ) -> NetId64 {
        let id = NetId64::make(self.kind, node_id.get(), counter);
        let slot_idx = (counter as usize) % self.capacity;
        let frame = Frame {
            id,
            kind: frame_kind,
            ver,
            payload,
        };
        let mut guard = lane.slots[slot_idx].write().expect("ring slot poisoned");
        *guard = Some(frame);
        id
    }
}

impl cursor::RingFrameSource for Ring {
    fn kind(&self) -> u8 {
        Ring::kind(self)
    }

    fn head(&self) -> u64 {
        Ring::head(self)
    }

    fn capacity(&self) -> usize {
        Ring::capacity(self)
    }

    fn read_at(&self, counter: u64) -> Option<Frame> {
        Ring::read_at(self, counter)
    }

    fn read_state_at(&self, counter: u64) -> cursor::RingRead {
        Ring::read_state_at(self, counter)
    }
}

#[cfg(unix)]
impl cursor::RingFrameSource for shm::ShmRing {
    fn kind(&self) -> u8 {
        shm::ShmRing::kind(self)
    }

    fn head(&self) -> u64 {
        shm::ShmRing::head(self)
    }

    fn capacity(&self) -> usize {
        shm::ShmRing::capacity(self)
    }

    fn read_at(&self, counter: u64) -> Option<Frame> {
        shm::ShmRing::read_at(self, counter)
    }

    fn read_state_at(&self, counter: u64) -> cursor::RingRead {
        shm::ShmRing::read_state_at(self, counter)
    }
}

impl std::fmt::Debug for Ring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ring")
            .field("kind", &self.kind)
            .field("capacity", &self.capacity)
            .field("payload_capacity", &self.payload_capacity)
            .field("topology", &self.topology)
            .field("lane_count", &self.lanes.len())
            .field("head", &self.head())
            .finish()
    }
}

/// Type-keyed registry of rings. A `Fleet` holds one of these and
/// hands out `Arc<Ring>` per `OrbitTyped` kind on demand.
pub(crate) struct RingRegistry {
    fleet_capacity: u16,
    rings: dashmap::DashMap<u8, Arc<Ring>>,
}

impl RingRegistry {
    pub fn new(fleet_capacity: u16) -> Self {
        Self {
            fleet_capacity,
            rings: dashmap::DashMap::new(),
        }
    }

    /// Get-or-create the ring declared by `T`.
    pub fn get_or_create<T: OrbitTyped>(&self) -> Arc<Ring> {
        let ring = self
            .rings
            .entry(T::KIND)
            .or_insert_with(|| Arc::new(Ring::new_for_fleet::<T>(self.fleet_capacity)))
            .clone();
        assert_eq!(
            ring.spec(),
            T::RING_SPEC,
            "OrbitTyped KIND {} was reused with a different ring spec",
            T::KIND
        );
        ring
    }

    /// Look up a ring by KIND byte (e.g. when only the id is known).
    pub fn lookup(&self, kind: u8) -> Option<Arc<Ring>> {
        self.rings.get(&kind).map(|e| e.clone())
    }
}

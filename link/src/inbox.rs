//! One bounded mailbox per fleet node: many writers, the lane's owner reads.
//!
//! Every node has a lane. An edge worker writes an exchange header into the
//! lane of the service worker it picked; that worker reads its own lane and
//! nobody else's. Replies travel the same way in the other direction, which is
//! why one table serves both — a node is either an edge or a service
//! worker, so its lane only ever receives one kind of frame.
//!
//! ## A full lane refuses
//!
//! `orbit_core::Ring` is a fan-out log and overwrites: `slot = counter %
//! capacity`, unconditionally, with no `Result` to refuse through. Correct for
//! telemetry, fatal here — an overwritten request is a request that vanished
//! while its caller waited. A writer that finds this lane full gets
//! [`Error::InboxFull`] and the caller answers 503.
//!
//! ## Why a sequence per slot rather than one commit counter
//!
//! Writers reserve in order and finish out of order: a long header block
//! commits after a short one that started later. A single `commit` counter
//! would make the reader wait for the slowest writer before it could see
//! anything behind it, so one slow producer would stall every producer after
//! it.
//!
//! Each slot carries its own sequence instead, and the reader looks only at
//! the slot it is waiting for:
//!
//! ```text
//! seq == position              the slot is free for this round
//! seq == position + 1          a writer has filled it
//! seq == position + capacity   consumed; free for the next round
//! ```
//!
//! Nothing here takes a lock. A `std::sync::Mutex` in shared memory belongs to
//! the process that created it, and a writer that dies holding one leaves the
//! lane wedged for every other process. Atomics have no owner.

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use super::{Error, Result};

/// Marks a mapped region as this layout, so a segment left by a different
/// shape is recognised rather than read as garbage.
pub(crate) const INBOX_MAGIC: u32 = 0x4F54_5849; // "OTXI"

/// The layout's own version, separate from the geometry. Geometry changes are
/// caught by Orbit, which refuses a segment sized differently from the code's;
/// this catches a change that keeps the size and moves the meaning.
///
/// 3: a claimed lane's owner holds the lane's lock for as long as it lives, so
/// a lane whose lock can be taken belongs to a process that is gone. A build
/// before it holds no lock and would read as dead, so the two never share a
/// table.
pub(crate) const INBOX_VERSION: u32 = 3;

/// A lane taken back from a process that is gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reclaimed {
    pub lane: usize,
    /// The incarnation its body streams are stamped with.
    pub incarnation: u64,
    pub pid: u32
}

/// Longest service name a lane can advertise. Long enough for the names
/// a site route already uses; a longer one is a configuration error, not a
/// truncation.
pub const LANE_NAME_MAX: usize = 47;

/// Longest role tag a lane can carry: `asgi`, `php`, `edge`. A word, not a
/// description.
pub const LANE_ROLE_MAX: usize = 8;

/// Per-lane state, in two cache lines on purpose.
///
/// The first line is written on every frame — reservation, read position, the
/// wake word. The second is written once when a process claims the lane and
/// read only when an edge is choosing a target. Keeping them apart means the
/// choosing side can read identity without touching the line the hot counters
/// live on.
#[repr(C, align(64))]
pub(crate) struct LaneHeader {
    /// Next position a writer may claim. Writers CAS this; nobody else writes
    /// it.
    pub(crate) reserve: AtomicU64,
    /// Next position the owner will read. Only the owner writes it.
    pub(crate) read: AtomicU64,
    /// How many live consumers serve this lane. A writer that finds zero
    /// refuses rather than filling a mailbox nobody empties.
    pub(crate) consumers: AtomicU32,
    /// Bumped on every commit: the word a parked reader waits on.
    pub(crate) changes: AtomicU32,
    /// The incarnation of the process that owns this lane. A consumer count
    /// left behind by a dead process is stale, and shared memory outlives the
    /// process that wrote it.
    pub(crate) incarnation: AtomicU64,
    pub(crate) waiters: AtomicU32,
    _padding: [u8; 20],

    // ── second line: identity, written once per claim ──────────────────
    /// Which service this lane serves.
    ///
    /// A fleet may hold several — an edge belongs to exactly one fleet, so
    /// every service it reaches is in that fleet with it. The name lives
    /// here rather than in a table of its own: identity, presence and
    /// incarnation are one fact about a lane and want one place, and a
    /// separate table would be a second thing to keep true.
    ///
    /// Read as bytes under the lane's own ordering: a claim publishes the
    /// length last, so a reader that sees a length sees the bytes before it.
    pub(crate) service: [u8; LANE_NAME_MAX],
    pub(crate) service_len: AtomicU8,
    /// The process that holds this lane.
    ///
    /// Written for the operator, not for the protocol: nothing here routes by
    /// it. It is what answers "is this lane a live process or something a
    /// crashed one left behind", which the rest of the header cannot say — a
    /// consumer count is a claim, and a claim outlives the process that made
    /// it.
    ///
    /// Zero means the owner did not say. That is what every lane written
    /// before this field existed reports, and it is why the layout version
    /// stays where it is: the bytes were padding and padding was zeroed, so no
    /// reader of either build misreads the other's.
    ///
    /// A hint, not proof. A process id can be reused, so "alive" can name a
    /// different process; turning death into a 502 needs the death itself, not
    /// this.
    pub(crate) pid: AtomicU32,
    /// What serves the name: `asgi`, `php`, `edge`.
    ///
    /// The name is the only address, so two roles under one name would
    /// share an inbox and split its requests between them without a word.
    /// This is what lets a join see that and refuse. Several processes of one
    /// role under a name are how a name scales; two roles are two
    /// services that were given one name.
    ///
    /// Written in bytes that were padding, like `pid`: a lane written before
    /// it existed reads as length zero — a role that did not say — and the
    /// layout version stays where it is.
    pub(crate) role: [u8; LANE_ROLE_MAX],
    pub(crate) role_len: AtomicU8,
    _identity_padding: [u8; 3]
}

// Two cache lines exactly: the identity grew into padding, not past it.
const _: () = assert!(size_of::<LaneHeader>() == 128);

/// One frame's slot. `seq` is the whole protocol; `len` and the payload are
/// only read once `seq` says the slot is filled.
#[repr(C, align(64))]
pub(crate) struct SlotHeader {
    pub(crate) seq: AtomicU64,
    pub(crate) len: AtomicU32,
    _padding: [u8; 52]
}

/// What a writer must know to refuse correctly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    /// There is room and someone is reading.
    Accepted,
    /// The lane holds `capacity` frames the owner has not taken yet.
    Full,
    /// No live consumer. Writing would leave a request nobody answers.
    NoConsumer
}

/// A lane's geometry, shared by both ends because Orbit refuses to open a
/// segment shaped differently from the code's.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InboxGeometry {
    pub capacity: usize,
    pub payload_capacity: usize
}

impl InboxGeometry {
    pub const fn new(
        capacity: usize,
        payload_capacity: usize
    ) -> Self {
        Self { capacity, payload_capacity }
    }

    /// Bytes one lane occupies: its header, then a slot header and payload for
    /// every position.
    pub const fn lane_bytes(&self) -> usize {
        size_of::<LaneHeader>() + self.capacity * (size_of::<SlotHeader>() + self.payload_capacity)
    }

    /// What a fleet's whole table occupies.
    pub const fn segment_bytes(
        &self,
        fleet_capacity: usize
    ) -> usize {
        size_of::<TableHeader>() + fleet_capacity * self.lane_bytes()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.capacity == 0 || !self.capacity.is_power_of_two() {
            return Err(Error::Malformed("inbox capacity must be a power of two"));
        }
        if self.payload_capacity == 0 {
            return Err(Error::Malformed("inbox payload capacity must not be zero"));
        }
        Ok(())
    }
}

/// The segment's own header, checked before a single lane is touched.
#[repr(C, align(64))]
pub(crate) struct TableHeader {
    pub(crate) magic: AtomicU32,
    pub(crate) version: AtomicU32,
    pub(crate) fleet_capacity: AtomicU32,
    pub(crate) capacity: AtomicU32,
    pub(crate) payload_capacity: AtomicU32,
    /// Bumped whenever a lane's identity changes: a claim publishing a name,
    /// or a release taking one away.
    ///
    /// Which lanes serve a name is settled by those two events and by nothing
    /// else, so a caller that has read it once may keep the answer until this
    /// number moves. Depth is not in it — depth changes with every frame and
    /// is read per request, because it is the load signal rather than the
    /// address.
    pub(crate) directory: AtomicU64,
    _padding: [u8; 36]
}

/// A mapped inbox table.
///
/// Holds a raw pointer into shared memory rather than a slice, because the
/// bytes are written by other processes: a `&[u8]` would promise Rust that
/// nothing else mutates them, and something else always does.
pub struct Inbox {
    base: *mut u8,
    /// How many lanes the table holds.
    ///
    /// It is the fleet capacity today because a lane belongs to a node: a
    /// process claims the index it was given. If lanes were ever leased from a
    /// pool instead — so one process could hold several, one per service
    /// it serves — this is the number that would stop being the fleet's. The
    /// rest of the interface already speaks in lanes rather than nodes, so
    /// that change would not reach the edge.
    lanes: usize,
    geometry: InboxGeometry
}

// The pointer addresses shared memory whose only mutable state is atomics.
unsafe impl Send for Inbox {}
unsafe impl Sync for Inbox {}

impl Inbox {
    /// Take ownership of a mapped region as a fresh table.
    ///
    /// Only the table header is written. All-zero is already a table of free
    /// lanes, and a lane is set up by whoever takes it (`claim_lane`, `join`),
    /// so nothing here touches a lane's pages: the kernel commits a page when
    /// it is first written, and a lane nobody claims costs no memory. Zeroing
    /// the whole segment here once made every byte of it resident — 69 MiB
    /// for 32 lanes of 128 frames of 17 KiB, whether or not a lane was used.
    ///
    /// # Safety
    ///
    /// `base` must point at `geometry.segment_bytes(fleet_capacity)` writable
    /// bytes, all zero, that stay mapped for as long as this `Inbox` lives. A
    /// newly created shared-memory object is: `ftruncate` extends it with
    /// zeros.
    pub unsafe fn initialize(
        base: *mut u8,
        fleet_capacity: usize,
        geometry: InboxGeometry
    ) -> Result<Self> {
        geometry.validate()?;
        let inbox = Self { base, lanes: fleet_capacity, geometry };

        let header = inbox.table_header();
        header.fleet_capacity.store(fleet_capacity as u32, Ordering::Relaxed);
        header.capacity.store(geometry.capacity as u32, Ordering::Relaxed);
        header.payload_capacity.store(geometry.payload_capacity as u32, Ordering::Relaxed);
        header.version.store(INBOX_VERSION, Ordering::Relaxed);
        // Magic last: a reader that sees it sees a finished header.
        header.magic.store(INBOX_MAGIC, Ordering::Release);
        Ok(inbox)
    }

    /// Attach to a table another process created, refusing anything whose
    /// header does not match this build.
    ///
    /// # Safety
    ///
    /// As [`Inbox::initialize`].
    pub unsafe fn attach(
        base: *mut u8,
        fleet_capacity: usize,
        geometry: InboxGeometry
    ) -> Result<Self> {
        geometry.validate()?;
        let inbox = Self { base, lanes: fleet_capacity, geometry };
        let header = inbox.table_header();
        if header.magic.load(Ordering::Acquire) != INBOX_MAGIC {
            return Err(Error::Malformed("inbox segment is not an inbox table"));
        }
        if header.version.load(Ordering::Relaxed) != INBOX_VERSION {
            return Err(Error::Malformed("inbox segment was written by another layout version"));
        }
        if header.fleet_capacity.load(Ordering::Relaxed) as usize != fleet_capacity
            || header.capacity.load(Ordering::Relaxed) as usize != geometry.capacity
            || header.payload_capacity.load(Ordering::Relaxed) as usize != geometry.payload_capacity
        {
            return Err(Error::Malformed("inbox segment geometry differs from this build"));
        }
        Ok(inbox)
    }

    pub fn geometry(&self) -> InboxGeometry {
        self.geometry
    }

    pub fn lanes(&self) -> usize {
        self.lanes
    }

    /// Clear one lane and claim it for this incarnation.
    ///
    /// The owner calls this at boot. Its lane has one reader — itself — so
    /// nothing anyone else is reading is destroyed, and what it discards is
    /// work addressed to an incarnation that is gone: every writer waiting on
    /// those frames has a caller that gave up long ago.
    ///
    /// This is not the `ExistingBootPolicy::Reuse` that protects cells and
    /// counters. Those are shared state; a mailbox is single-owner and its
    /// contents are in flight, not accumulated.
    pub fn claim_lane(
        &self,
        lane_index: usize,
        service: &str,
        role: &str,
        incarnation: u64
    ) -> Result<()> {
        self.check_lane(lane_index)?;
        check_identity(service, role)?;
        self.reset_lane(lane_index);
        let lane = self.lane_header(lane_index);
        lane.consumers.store(1, Ordering::Release);
        self.publish_identity(lane, service, role, incarnation);
        self.wake(lane);
        Ok(())
    }

    /// Take a free lane and say which service this process serves.
    ///
    /// There is no pool and no allocator. A service process starts on its
    /// own — its own binary, its own virtual environment, its own moment — and
    /// introduces itself here. Several processes serving the same name know
    /// nothing about each other and do not need to: the lane is won with a
    /// compare-and-swap on the consumer count, so exactly one of them takes
    /// it.
    ///
    /// The name is published after the lane is won and cleared. In between,
    /// the lane is claimed but anonymous — and [`Inbox::targets`] matches on
    /// the name, so nothing is addressed to a lane that is still being made
    /// ready.
    ///
    /// The index that comes back is this process's address in the fleet, for
    /// the inbox and for its body streams alike.
    ///
    /// `hold` takes a free lane's hold before the lane is claimed: whatever it
    /// returns lives as long as the claim, and a lane it cannot hold is some
    /// live process's. Over shared memory that is the lane's lock
    /// ([`LinkSegment::join`](super::LinkSegment::join)); a
    /// table in private memory has nobody else to hold it.
    ///
    /// A name already served by another role is refused, naming the lane
    /// and process that hold it: two roles under one name would share this
    /// inbox and split its requests between them. The check runs before the
    /// lane is taken and again once this lane is published, because two
    /// processes of different roles joining at the same moment each pass
    /// the first; the second sees the other, and both refuse — a name given to
    /// two services is a configuration error either way, and it is caught
    /// at the door rather than half served.
    pub fn join<H>(
        &self,
        service: &str,
        role: &str,
        incarnation: u64,
        mut hold: impl FnMut(usize) -> Result<Option<H>>
    ) -> Result<(usize, H)> {
        check_identity(service, role)?;
        self.refuse_other_role(service, role, None)?;
        for lane_index in 0..self.lanes {
            let lane = self.lane_header(lane_index);
            if lane.consumers.load(Ordering::Acquire) != 0 {
                continue;
            }
            let Some(held) = hold(lane_index)? else {
                continue;
            };
            if lane.consumers.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire).is_err() {
                continue;
            }
            self.reset_lane(lane_index);
            self.publish_identity(lane, service, role, incarnation);
            if let Err(conflict) = self.refuse_other_role(service, role, Some(lane_index)) {
                let _ = self.release_lane(lane_index);
                return Err(conflict);
            }
            self.wake(lane);
            return Ok((lane_index, held));
        }
        Err(Error::NoLane)
    }

    /// Take back a claimed lane whose owner is known to be gone, and say who it
    /// was: the incarnation is what its body streams are stamped with.
    ///
    /// The caller vouches for the death — over shared memory, by holding the
    /// lane's lock. `None` when the lane was not claimed.
    pub fn reclaim(
        &self,
        lane_index: usize
    ) -> Result<Option<Reclaimed>> {
        self.check_lane(lane_index)?;
        let lane = self.lane_header(lane_index);
        if lane.consumers.load(Ordering::Acquire) == 0 {
            return Ok(None);
        }
        let reclaimed = Reclaimed {
            lane: lane_index,
            incarnation: lane.incarnation.load(Ordering::Acquire),
            pid: lane.pid.load(Ordering::Acquire)
        };
        self.release_lane(lane_index)?;
        Ok(Some(reclaimed))
    }

    /// A lane that serves `service` for another role, as an error.
    ///
    /// Only claimed lanes count: a lane its owner released holds nothing, and a
    /// lane a dead process left is taken back before a join gets here.
    fn refuse_other_role(
        &self,
        service: &str,
        role: &str,
        own: Option<usize>
    ) -> Result<()> {
        for lane_index in self.serving(service) {
            if Some(lane_index) == own {
                continue;
            }
            let lane = self.lane_header(lane_index);
            if lane.consumers.load(Ordering::Acquire) == 0 {
                continue;
            }
            let held_by = self.role(lane_index)?.unwrap_or_default();
            if held_by != role {
                return Err(Error::RoleConflict {
                    name: service.to_owned(),
                    role: role.to_owned(),
                    held_by: if held_by.is_empty() { "an unnamed role".into() } else { held_by },
                    lane: lane_index,
                    pid: lane.pid.load(Ordering::Acquire)
                });
            }
        }
        Ok(())
    }

    /// Park until something changes in this lane.
    ///
    /// `expected` is the value [`Inbox::wait_word`] gave before the caller
    /// last drained, and the kernel compares it again under its own lock: a
    /// writer that committed in between means the wait returns instead of
    /// starting. There is no interval and no deadline here — the wait ends
    /// because someone wrote, or because [`Inbox::nudge`] woke it on purpose.
    pub fn park(
        &self,
        lane_index: usize,
        expected: u32
    ) -> Result<()> {
        self.check_lane(lane_index)?;
        let lane = self.lane_header(lane_index);
        // Announce before waiting: a writer bumps the word and then reads this
        // count, so the two orders cross. If it reads zero anyway, it bumped
        // the word first and the kernel's own comparison ends the wait below.
        lane.waiters.fetch_add(1, Ordering::AcqRel);
        let waited = orbit_core::sync::wait_word(&lane.changes, expected);
        lane.waiters.fetch_sub(1, Ordering::AcqRel);
        waited.map_err(Error::Io)
    }

    /// Wake this lane's reader without writing anything.
    ///
    /// For the one case that is not a frame: the owner is shutting down and
    /// the thread parked on the lane has to notice. A flag the reader checks
    /// plus this, rather than a timeout it would otherwise have to wait out.
    pub fn nudge(
        &self,
        lane_index: usize
    ) -> Result<()> {
        self.check_lane(lane_index)?;
        self.wake(self.lane_header(lane_index));
        Ok(())
    }

    /// The service this lane serves, if it is claimed.
    pub fn service(
        &self,
        lane_index: usize
    ) -> Result<Option<String>> {
        self.check_lane(lane_index)?;
        let lane = self.lane_header(lane_index);
        let len = lane.service_len.load(Ordering::Acquire) as usize;
        if len == 0 || len > LANE_NAME_MAX {
            return Ok(None);
        }
        let bytes = unsafe { std::slice::from_raw_parts(lane.service.as_ptr(), len) };
        Ok(String::from_utf8(bytes.to_vec()).ok())
    }

    /// The process that claimed this lane, if it said.
    pub fn owner(
        &self,
        lane_index: usize
    ) -> Result<Option<u32>> {
        self.check_lane(lane_index)?;
        Ok(match self.lane_header(lane_index).pid.load(Ordering::Acquire) {
            0 => None,
            pid => Some(pid)
        })
    }

    /// What serves this lane's name, if its owner said.
    pub fn role(
        &self,
        lane_index: usize
    ) -> Result<Option<String>> {
        self.check_lane(lane_index)?;
        let lane = self.lane_header(lane_index);
        let len = lane.role_len.load(Ordering::Acquire) as usize;
        if len == 0 || len > LANE_ROLE_MAX {
            return Ok(None);
        }
        let bytes = unsafe { std::slice::from_raw_parts(lane.role.as_ptr(), len) };
        Ok(String::from_utf8(bytes.to_vec()).ok())
    }

    /// Whether this lane advertises exactly this service.
    ///
    /// Compared as bytes. [`Inbox::service`] allocates a `String`, and
    /// this is on the dispatch path: every lane in the table is asked, for
    /// every exchange. An allocation per lane per request made the table cost
    /// grow with the fleet, which is the one thing a target scan must not do.
    fn serves(
        &self,
        lane_index: usize,
        service: &[u8]
    ) -> bool {
        let lane = self.lane_header(lane_index);
        let len = lane.service_len.load(Ordering::Acquire) as usize;
        if len != service.len() || len > LANE_NAME_MAX {
            return false;
        }
        let name = unsafe { std::slice::from_raw_parts(lane.service.as_ptr(), len) };
        name == service
    }

    /// Lanes serving `service` that are ready to take a frame, idlest
    /// first.
    ///
    /// This is the whole target-selection story: not a registry, not a health
    /// report, not a load metric anyone publishes — the queue an edge is about
    /// to write into, read directly. A worker that joined a moment ago is
    /// already here; one that left is already gone.
    pub fn targets(
        &self,
        service: &str
    ) -> Vec<(usize, u64)> {
        self.ready(&self.serving(service))
    }

    /// The generation of the lane directory: which lanes serve which name.
    ///
    /// A caller that resolved a name while this number held may use that
    /// answer until it moves. It changes when a process claims a lane or
    /// gives one up, and at no other time — a frame written, read or refused
    /// leaves it alone.
    pub fn directory(&self) -> u64 {
        self.table_header().directory.load(Ordering::Acquire)
    }

    /// Which lanes advertise this name, live or not.
    ///
    /// The walk over every lane lives here, so a caller that keeps the result
    /// pays it once per directory generation rather than once per exchange.
    /// Presence and capacity are deliberately not checked: those change with
    /// traffic, and a list that changed with traffic could not be kept.
    pub fn serving(
        &self,
        service: &str
    ) -> Vec<usize> {
        let wanted = service.as_bytes();
        (0..self.lanes).filter(|lane| self.serves(*lane, wanted)).collect()
    }

    /// Of these lanes, the ones that will take a frame now, idlest first.
    ///
    /// This is the per-exchange half: admission and depth are read every time
    /// because both answer "now", and depth is what the choice is made on.
    pub fn ready(
        &self,
        lanes: &[usize]
    ) -> Vec<(usize, u64)> {
        let mut found: Vec<(usize, u64)> = lanes
            .iter()
            .copied()
            .filter(|lane| matches!(self.admits(*lane), Ok(Admission::Accepted)))
            .filter_map(|lane| self.depth(lane).ok().map(|depth| (lane, depth)))
            .collect();
        found.sort_by_key(|(_, depth)| *depth);
        found
    }

    /// Give up a lane: its owner is leaving and writers should stop choosing
    /// it. In-flight frames are left where they are; the next incarnation
    /// clears them.
    pub fn release_lane(
        &self,
        lane_index: usize
    ) -> Result<()> {
        self.check_lane(lane_index)?;
        let lane = self.lane_header(lane_index);
        lane.consumers.store(0, Ordering::Release);
        lane.service_len.store(0, Ordering::Release);
        lane.role_len.store(0, Ordering::Release);
        lane.pid.store(0, Ordering::Release);
        self.table_header().directory.fetch_add(1, Ordering::AcqRel);
        self.wake(lane);
        Ok(())
    }

    /// Whether a writer may choose this lane, and why not when it may not.
    ///
    /// Capacity and presence, never a duration: a writer refused here has been
    /// refused now, not after waiting.
    pub fn admits(
        &self,
        lane_index: usize
    ) -> Result<Admission> {
        self.check_lane(lane_index)?;
        let lane = self.lane_header(lane_index);
        if lane.consumers.load(Ordering::Acquire) == 0 {
            return Ok(Admission::NoConsumer);
        }
        let reserve = lane.reserve.load(Ordering::Acquire);
        let read = lane.read.load(Ordering::Acquire);
        if reserve.wrapping_sub(read) >= self.geometry.capacity as u64 {
            return Ok(Admission::Full);
        }
        Ok(Admission::Accepted)
    }

    /// Frames written to this lane that its owner has not taken yet.
    ///
    /// This is what lets an edge choose a target by reading rather than
    /// guessing: with a socket you would need the other side to report its
    /// load; here the queue is the shared memory you are about to write into.
    pub fn depth(
        &self,
        lane_index: usize
    ) -> Result<u64> {
        self.check_lane(lane_index)?;
        let lane = self.lane_header(lane_index);
        let reserve = lane.reserve.load(Ordering::Acquire);
        let read = lane.read.load(Ordering::Acquire);
        Ok(reserve.wrapping_sub(read))
    }

    pub fn consumers(
        &self,
        lane_index: usize
    ) -> Result<u32> {
        self.check_lane(lane_index)?;
        Ok(self.lane_header(lane_index).consumers.load(Ordering::Acquire))
    }

    pub fn incarnation(
        &self,
        lane_index: usize
    ) -> Result<u64> {
        self.check_lane(lane_index)?;
        Ok(self.lane_header(lane_index).incarnation.load(Ordering::Acquire))
    }

    /// Put one frame in a node's lane.
    ///
    /// Reserves a position with a compare-and-swap that only succeeds while
    /// there is room, so a full lane is refused without having mutated
    /// anything. A plain `fetch_add` would have to be undone, and undoing a
    /// reservation that other writers have already passed is not possible.
    pub fn write(
        &self,
        lane_index: usize,
        payload: &[u8]
    ) -> Result<u64> {
        self.check_lane(lane_index)?;
        if payload.len() > self.geometry.payload_capacity {
            return Err(Error::FrameTooLarge);
        }
        let lane = self.lane_header(lane_index);
        let capacity = self.geometry.capacity as u64;

        let position = loop {
            if lane.consumers.load(Ordering::Acquire) == 0 {
                return Err(Error::InboxFull);
            }
            let reserve = lane.reserve.load(Ordering::Acquire);
            let read = lane.read.load(Ordering::Acquire);
            if reserve.wrapping_sub(read) >= capacity {
                return Err(Error::InboxFull);
            }
            let slot = self.slot_header(lane_index, reserve);
            // The owner may not have released the previous round of this slot
            // yet even though its read position has moved past it.
            if slot.seq.load(Ordering::Acquire) != reserve {
                std::hint::spin_loop();
                continue;
            }
            if lane
                .reserve
                .compare_exchange_weak(reserve, reserve + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break reserve;
            }
            std::hint::spin_loop();
        };

        let slot = self.slot_header(lane_index, position);
        unsafe {
            std::ptr::copy_nonoverlapping(
                payload.as_ptr(),
                self.payload_ptr(lane_index, position),
                payload.len()
            );
        }
        slot.len.store(payload.len() as u32, Ordering::Relaxed);
        // Publishes the payload: a reader that sees this sequence sees the
        // bytes written before it.
        slot.seq.store(position + 1, Ordering::Release);
        self.wake(lane);
        Ok(position)
    }

    /// Take the next frame from this node's own lane, if one is there.
    ///
    /// Only the lane's owner may call this. It copies out rather than lending
    /// a slice, because the slot is released the moment it returns and another
    /// process may fill it immediately.
    pub fn read(
        &self,
        lane_index: usize,
        out: &mut Vec<u8>
    ) -> Result<bool> {
        self.check_lane(lane_index)?;
        let lane = self.lane_header(lane_index);
        let position = lane.read.load(Ordering::Relaxed);
        let slot = self.slot_header(lane_index, position);
        if slot.seq.load(Ordering::Acquire) != position + 1 {
            return Ok(false);
        }

        let len = slot.len.load(Ordering::Relaxed) as usize;
        out.clear();
        out.reserve(len);
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.payload_ptr(lane_index, position),
                out.spare_capacity_mut().as_mut_ptr().cast::<u8>(),
                len
            );
            out.set_len(len);
        }

        lane.read.store(position + 1, Ordering::Release);
        // Free for the round `capacity` positions from now.
        slot.seq.store(position + self.geometry.capacity as u64, Ordering::Release);
        // No wake. Nobody waits on a consume — a writer that finds the lane
        // full is refused, never parked — and the only waiter there is, the
        // owner, is the one reading. Bumping the word here spoiled its own
        // token: every drain moved `changes` past the value it would park on,
        // so each wake-up paid a park that returned at once before the one
        // that waited.
        Ok(true)
    }

    /// The word a parked reader waits on, and its current value.
    pub fn wait_word(
        &self,
        lane_index: usize
    ) -> Result<(&AtomicU32, u32)> {
        self.check_lane(lane_index)?;
        let lane = self.lane_header(lane_index);
        Ok((&lane.changes, lane.changes.load(Ordering::Acquire)))
    }

    fn reset_lane(
        &self,
        lane_index: usize
    ) {
        let lane = self.lane_header(lane_index);
        lane.reserve.store(0, Ordering::Relaxed);
        lane.read.store(0, Ordering::Relaxed);
        // Not `consumers`: presence is how a lane is won, and clearing it here
        // would hand a just-claimed lane back to the next process looking for
        // a free one. `claim_lane` sets it after this; `join` set it before.
        lane.service_len.store(0, Ordering::Relaxed);
        lane.role_len.store(0, Ordering::Relaxed);
        lane.waiters.store(0, Ordering::Relaxed);
        for position in 0..self.geometry.capacity {
            let slot = self.slot_header(lane_index, position as u64);
            slot.len.store(0, Ordering::Relaxed);
            slot.seq.store(position as u64, Ordering::Release);
        }
    }

    /// Write a lane's identity: the bytes first, the length last, so a reader
    /// that sees a length sees the name it belongs to.
    fn publish_identity(
        &self,
        lane: &LaneHeader,
        service: &str,
        role: &str,
        incarnation: u64
    ) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                service.as_ptr(),
                lane.service.as_ptr().cast_mut(),
                service.len()
            );
            std::ptr::copy_nonoverlapping(role.as_ptr(), lane.role.as_ptr().cast_mut(), role.len());
        }
        // The role before the name: a reader that sees the name sees who
        // serves it.
        lane.role_len.store(role.len() as u8, Ordering::Release);
        lane.service_len.store(service.len() as u8, Ordering::Release);
        lane.incarnation.store(incarnation, Ordering::Release);
        lane.pid.store(std::process::id(), Ordering::Release);
        // Last, and after the name: a caller that sees the new number sees
        // every byte that made it new.
        self.table_header().directory.fetch_add(1, Ordering::AcqRel);
    }

    /// Bump the word a reader watches, and wake it if one is parked.
    ///
    /// The bump comes first and the waiter count is read after, so a reader
    /// that registered itself in between is still seen; one that registers
    /// after finds the word already moved and does not start the wait.
    fn wake(
        &self,
        lane: &LaneHeader
    ) {
        lane.changes.fetch_add(1, Ordering::Release);
        if lane.waiters.load(Ordering::Acquire) > 0 {
            let _ = orbit_core::sync::wake_word(&lane.changes);
        }
    }

    fn check_lane(
        &self,
        lane_index: usize
    ) -> Result<()> {
        if lane_index >= self.lanes {
            return Err(Error::Malformed("lane is outside the table"));
        }
        Ok(())
    }

    fn table_header(&self) -> &TableHeader {
        unsafe { &*self.base.cast::<TableHeader>() }
    }

    fn lane_base(
        &self,
        lane_index: usize
    ) -> *mut u8 {
        unsafe { self.base.add(size_of::<TableHeader>() + lane_index * self.geometry.lane_bytes()) }
    }

    fn lane_header(
        &self,
        lane_index: usize
    ) -> &LaneHeader {
        unsafe { &*self.lane_base(lane_index).cast::<LaneHeader>() }
    }

    fn slot_base(
        &self,
        lane_index: usize,
        position: u64
    ) -> *mut u8 {
        let index = (position as usize) & (self.geometry.capacity - 1);
        let stride = size_of::<SlotHeader>() + self.geometry.payload_capacity;
        unsafe { self.lane_base(lane_index).add(size_of::<LaneHeader>() + index * stride) }
    }

    fn slot_header(
        &self,
        lane_index: usize,
        position: u64
    ) -> &SlotHeader {
        unsafe { &*self.slot_base(lane_index, position).cast::<SlotHeader>() }
    }

    fn payload_ptr(
        &self,
        lane_index: usize,
        position: u64
    ) -> *mut u8 {
        unsafe { self.slot_base(lane_index, position).add(size_of::<SlotHeader>()) }
    }
}

/// A lane's identity must fit it: a name and a role tag, neither empty,
/// neither truncated.
fn check_identity(
    service: &str,
    role: &str
) -> Result<()> {
    if service.is_empty() || service.len() > LANE_NAME_MAX {
        return Err(Error::Malformed("service name does not fit a lane"));
    }
    if role.is_empty() || role.len() > LANE_ROLE_MAX || !role.is_ascii() {
        return Err(Error::Malformed("role tag does not fit a lane"));
    }
    Ok(())
}

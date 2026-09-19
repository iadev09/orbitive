//! Fleet-wide resource leases, reservations and creation claims over Orbit
//! shared memory.
//!
//! A resource is something one process owns and others may use through
//! it: an upstream connection, a worker, a slot in anything with a limit.
//! The owner [`Pool::register`]s it under a [`Key`] (the caller's digest
//! of "what this is usable for") with a capacity: one for an exclusive
//! thing, more for one that admits several users at once. Any process in
//! the fleet then reads the [`Pool::candidates`] for a key, picks one by
//! its own policy, and [`Pool::reserve`]s capacity on it: one compare-and-
//! swap that either hands back a [`Lease`] or says [`Error::Busy`]. The
//! opaque object never moves; a lease is the right to ask its owner to use
//! it. The owner [`Pool::accept`]s the lease, does the work, and its
//! [`Execution`] guard gives the unit back when the work is over. A caller
//! that gives up frees nothing: only the owner knows when the resource is
//! idle again, so a reservation the owner never saw is aged out by the
//! owner's [`Pool::reconcile`], never by a caller's timeout.
//!
//! A per-key creation budget keeps a fleet that finds a key empty from
//! creating everything at once: [`Pool::claim_create`] counts live and
//! in-progress resources together. Waiters park on the key until capacity
//! comes back, in a thread or in a task.
//!
//! The pool decides nothing and carries nothing: which candidate wins is
//! the caller's policy, and the bytes of a remote use travel over
//! `orbit-stream`. Standalone it lives in process memory; in a fleet, in
//! the shared segment a [`PoolSpec`] names — kind [`POOL_KIND`] by
//! default, and one fleet may hold several independent pools.

use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use std::{fmt, io};

use orbit_core::{Fleet, NetId64, NodeId, OrbitEpoch};

mod layout;
mod policy;
#[cfg(feature = "stream")]
mod session;
mod table;

pub use layout::PENDING_RESERVATIONS;
use layout::{
    GENERATION_MASK, RESOURCE_DRAINING, RESOURCE_LIVE, ResourceSlot, SLOT_BITS, SLOT_MASK,
    pack_counts, unpack_counts
};
#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
pub use orbit_core::readiness::Readiness;
pub use policy::{Decision, Limits, LocalFirst, LocalOnly, Policy, Reason};
#[cfg(feature = "stream")]
pub use session::{ExchangeSessionPlan, ExchangeSessionStart};
use table::Table;
pub use table::{segment_size, segment_size_for};

/// Reserved Orbit SHM kind for the default pool segment. Another pool
/// names its own through a [`PoolSpec`].
pub const POOL_KIND: u8 = 247;
/// Distinct keys one fleet epoch can name at once, in the default spec.
///
/// Compile-time geometry: `ORBIT_POOL_KEY_CAPACITY`, a power of two. It is
/// the *default* pool's value; a [`PoolSpec`] gives another pool another
/// one.
pub const POOL_KEY_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_POOL_KEY_CAPACITY"), 256);
/// Resources one fleet node can have registered at once, in the default
/// spec.
///
/// Compile-time geometry: `ORBIT_POOL_RESOURCE_LANE_CAPACITY`, a power of
/// two, at most 65 536; a [`PoolSpec`] gives another pool another one.
pub const POOL_RESOURCE_LANE_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_POOL_RESOURCE_LANE_CAPACITY"), 256);

/// Which segment a [`Pool`] uses, and how big it is.
///
/// One fleet can hold several independent pools: an upstream's origins and
/// an outbound client's targets share neither a budget, a key space nor an
/// epoch, so each names its own kind. A kind is a fleet-wide identity —
/// every process opening it must pass the same capacities, and the
/// segment's header refuses a peer that does not. [`PoolSpec::DEFAULT`] is
/// what [`Pool::new`] opens; its values are the compile-time geometry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PoolSpec {
    /// The Orbit SHM kind, and with it the segment name.
    pub kind: u8,
    /// Distinct keys this pool can name at once. A power of two.
    pub key_capacity: usize,
    /// Resources one fleet node can register at once. A power of two, at
    /// most 65 536.
    pub lane_capacity: usize
}

impl PoolSpec {
    pub const DEFAULT: Self = Self::new(POOL_KIND, POOL_KEY_CAPACITY, POOL_RESOURCE_LANE_CAPACITY);

    pub const fn new(
        kind: u8,
        key_capacity: usize,
        lane_capacity: usize
    ) -> Self {
        Self { kind, key_capacity, lane_capacity }
    }

    /// What the compile-time geometry used to assert. A spec is checked
    /// once, when its table is opened.
    fn validate(self) -> Result<()> {
        if self.key_capacity == 0
            || self.lane_capacity == 0
            || !self.key_capacity.is_power_of_two()
            || !self.lane_capacity.is_power_of_two()
            || self.lane_capacity > 1 << SLOT_BITS
        {
            return Err(Error::Malformed(format!(
                "pool spec kind={} key_capacity={} lane_capacity={}: both are powers of two, and a lane holds at most {}",
                self.kind,
                self.key_capacity,
                self.lane_capacity,
                1_usize << SLOT_BITS
            )));
        }
        Ok(())
    }
}

impl Default for PoolSpec {
    fn default() -> Self {
        Self::DEFAULT
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A pending entry between being claimed and carrying its fence. Never a
/// real fence: fences start at one and count up.
const PLACING: u64 = u64::MAX;

/// Additions are expected: a cause discovered later lands here rather
/// than in a new major version, so a caller matches what it handles and
/// leaves the rest to a catch-all.
#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    /// The id names a slot nothing occupies, or a generation that ended.
    Stale(ResourceId),
    /// Every unit of the resource's capacity is leased right now.
    Busy(ResourceId),
    /// The owner is draining it; no new leases.
    Draining(ResourceId),
    /// Only the owner may do this to a resource.
    NotOwner(ResourceId),
    /// The lease is not among the resource's unaccepted reservations: it
    /// was accepted already, aged out by the owner's reconcile, or taken
    /// on a generation that ended.
    NotReserved(Lease),
    /// The key's creation budget is spent: live plus in-progress reached
    /// the limit the caller gave.
    CreationBudget {
        key: Key,
        max_live: u32
    },
    /// Every key slot is taken.
    KeyFull {
        capacity: usize
    },
    /// Every resource slot in this process's lane is taken.
    Full {
        capacity: usize
    },
    Malformed(String),
    Io(io::Error)
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        match self {
            Self::Stale(id) => write!(f, "resource {id} has ended"),
            Self::Busy(id) => write!(f, "resource {id} has no capacity left"),
            Self::Draining(id) => write!(f, "resource {id} is draining"),
            Self::NotOwner(id) => write!(f, "resource {id} belongs to another node"),
            Self::NotReserved(lease) => {
                write!(f, "lease {} on {} is not an unaccepted reservation", lease.fence, lease.id)
            }
            Self::CreationBudget { key, max_live } => {
                write!(f, "creation budget for {key} is spent: max_live={max_live}")
            }
            Self::KeyFull { capacity } => write!(f, "pool key table is full: capacity={capacity}"),
            Self::Full { capacity } => write!(f, "pool lane is full: capacity={capacity}"),
            Self::Malformed(text) => write!(f, "not a pool resource id: {text:?}"),
            Self::Io(error) => write!(f, "Orbit pool io error: {error}")
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// What a resource is usable for. The pool never hashes: the caller brings
/// a 128-bit digest of its real key (an origin plus everything that forbids
/// reuse across contexts), so two resources with equal keys are
/// interchangeable by the caller's own definition.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Key(u128);

impl Key {
    pub const fn new(digest: u128) -> Self {
        Self(digest)
    }

    pub const fn from_bytes(digest: [u8; 16]) -> Self {
        Self(u128::from_le_bytes(digest))
    }

    pub const fn get(self) -> u128 {
        self.0
    }

    const fn parts(self) -> (u64, u64) {
        (self.0 as u64, (self.0 >> 64) as u64)
    }
}

impl fmt::Display for Key {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        write!(f, "key:{:032x}", self.0)
    }
}

/// Which life of a process owns a resource or holds a claim. Supplied by
/// the embedder, one value per process life; see `orbit-stream` for the
/// same idea.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Incarnation(u64);

impl Incarnation {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The address of one resource: a [`NetId64`] whose node is the owner's
/// lane and whose counter is the slot and its generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceId(NetId64);

impl ResourceId {
    pub const fn from_net_id(id: NetId64) -> Self {
        Self(id)
    }

    pub const fn net_id(self) -> NetId64 {
        self.0
    }

    pub const fn kind(self) -> u8 {
        self.0.kind()
    }

    /// The owner's node.
    pub const fn node(self) -> u16 {
        self.0.node()
    }

    pub const fn slot(self) -> u32 {
        (self.0.counter() & SLOT_MASK) as u32
    }

    pub const fn generation(self) -> u32 {
        (self.0.counter() >> SLOT_BITS) as u32
    }

    fn make(
        kind: u8,
        node: u16,
        slot: u32,
        generation: u32
    ) -> Self {
        Self(NetId64::make(
            kind,
            node,
            ((generation as u64) << SLOT_BITS) | (slot as u64 & SLOT_MASK)
        ))
    }
}

impl fmt::Display for ResourceId {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ResourceId {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        text.parse::<NetId64>().map(Self).map_err(|_| Error::Malformed(text.to_owned()))
    }
}

/// Where a resource stands, as read from the table: a snapshot, never a
/// reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum State {
    Live,
    Draining
}

/// One resource usable for a key, as the fleet sees it right now.
/// Fields are expected to be added as the table learns to report more,
/// so this is read rather than constructed from outside.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Candidate {
    pub id: ResourceId,
    pub owner: NodeId,
    pub owner_incarnation: Incarnation,
    /// Owned by this process.
    pub local: bool,
    pub state: State,
    pub capacity: u32,
    /// Units reserved by callers and not yet accepted by the owner.
    pub reserved: u32,
    /// Units the owner is executing.
    pub active: u32,
    pub last_reserve_ms: u64
}

impl Candidate {
    pub fn free(&self) -> u32 {
        self.capacity.saturating_sub(self.reserved + self.active)
    }
}

/// Reserved capacity on one resource. Plain data: it crosses processes
/// as numbers, and the owner validates it against the slot before use.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Lease {
    pub id: ResourceId,
    /// Strictly increasing per slot; a later lease on the same resource
    /// has a larger fence, so a stale holder can be told from the current.
    pub fence: u64,
    pub holder: NodeId,
    pub holder_incarnation: Incarnation
}

/// The fleet's pool table. Cheap to clone; every clone in a process is
/// the same table, driver and wakers.
#[derive(Clone)]
pub struct Pool {
    table: Arc<Table>
}

impl Pool {
    /// Open the fleet's default pool table.
    pub fn new(
        fleet: Arc<Fleet>,
        incarnation: Incarnation
    ) -> Result<Self> {
        Self::with_spec(fleet, incarnation, PoolSpec::DEFAULT)
    }

    /// Open the table `spec` names. Independent specs are independent
    /// pools: separate segments, separate budgets, separate epochs, and a
    /// `reset_all` on one leaves the others alone. A process may hold as
    /// many as it has specs, but each under one incarnation.
    pub fn with_spec(
        fleet: Arc<Fleet>,
        incarnation: Incarnation,
        spec: PoolSpec
    ) -> Result<Self> {
        spec.validate()?;
        Ok(Self { table: table::open(&fleet, incarnation, spec)? })
    }

    /// The kind this pool's segment lives under.
    pub fn kind(&self) -> u8 {
        self.table.kind()
    }

    /// A descriptor that becomes readable when a key this node
    /// [`Pool::watch`]es may have changed, for a runtime that parks on
    /// descriptors rather than on wakers or on the key's word.
    ///
    /// Edge-triggered and coalescing: drain it, then re-try what you
    /// wanted — `claim_create`, `reserve`, `acquire`. It composes, which
    /// is the point: a worker waiting for its next request and for pool
    /// capacity puts both descriptors in one poll set and gives that call
    /// its deadline, instead of choosing which one to block on.
    ///
    /// One per table; a second caller is refused rather than handed a
    /// descriptor whose signals the first would drain.
    pub fn readiness(&self) -> Result<Readiness> {
        self.table.take_readiness()
    }

    /// Ask to be signalled when `key` changes.
    ///
    /// Interest is taken by the driver when it delivers, exactly as a
    /// waker is, so this is re-armed before each wait — take the
    /// [`Pool::version`], try, watch, then wait, and a change between the
    /// try and the wait is seen rather than missed.
    pub fn watch(
        &self,
        key: Key
    ) -> Result<()> {
        let (lo, hi) = key.parts();
        let key_index = self.table.key_index(lo, hi)?;
        self.table.watch(key_index)
    }

    pub fn node(&self) -> NodeId {
        NodeId::new(self.table.node())
    }

    pub fn incarnation(&self) -> Incarnation {
        Incarnation::new(self.table.incarnation())
    }

    pub fn epoch(&self) -> u64 {
        self.table.epoch()
    }

    /// Make a resource this process owns visible under `key` with
    /// `capacity` concurrent leases (1 for an exclusive resource).
    pub fn register(
        &self,
        key: Key,
        capacity: u32
    ) -> Result<ResourceId> {
        if capacity == 0 {
            return Err(Error::Malformed("capacity must be at least one".to_owned()));
        }
        let (lo, hi) = key.parts();
        let key_index = self.table.key_index(lo, hi)?;
        let (index, generation) = self.table.allocate((lo, hi), key_index, capacity)?;
        self.table.members(key_index)[index / 64].fetch_or(1 << (index % 64), Ordering::SeqCst);
        let _ = self.table.key(key_index).counts.try_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |counts| {
                let (live, creating) = unpack_counts(counts);
                Some(pack_counts(live + 1, creating))
            }
        );
        self.table.key_changed(key_index);
        Ok(ResourceId::make(
            self.table.kind(),
            self.table.node(),
            (index % self.table.geometry().lane_capacity) as u32,
            generation
        ))
    }

    /// Take the resource away. Leases out on it become stale.
    pub fn unregister(
        &self,
        id: ResourceId
    ) -> Result<()> {
        let (index, slot) = self.owned(id)?;
        self.table.close_resource(index, slot);
        Ok(())
    }

    /// No new leases; the ones out finish at their own pace.
    pub fn drain(
        &self,
        id: ResourceId
    ) -> Result<()> {
        let (_, slot) = self.owned(id)?;
        let _ = slot.state.compare_exchange(
            RESOURCE_LIVE,
            RESOURCE_DRAINING,
            Ordering::SeqCst,
            Ordering::SeqCst
        );
        Ok(())
    }

    /// The owner's truth: `active` becomes the table's active count, and
    /// every reservation older than `grace` that nobody brought to the
    /// owner is aged out, so its unit returns and a late accept of it is
    /// refused. `grace` bounds how long an abandoned reservation keeps a
    /// unit; it says nothing about running work.
    pub fn reconcile(
        &self,
        id: ResourceId,
        active: u32,
        grace: std::time::Duration
    ) -> Result<()> {
        let (_, slot) = self.owned(id)?;
        let now = OrbitEpoch::now().as_unix_ms();
        let mut aged = 0_u32;
        for reservation in &slot.pending {
            let fence = reservation.fence.load(Ordering::Acquire);
            if fence == 0 || fence == PLACING {
                continue;
            }
            let since = reservation.since_ms.load(Ordering::Relaxed);
            if now.saturating_sub(since) > grace.as_millis() as u64
                && reservation
                    .fence
                    .compare_exchange(fence, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                aged += 1;
            }
        }
        let mut freed = false;
        let _ = slot.units.try_update(Ordering::SeqCst, Ordering::SeqCst, |units| {
            let (reserved, was_active) = unpack_counts(units);
            let reserved = reserved.saturating_sub(aged);
            freed = reserved + active < unpack_counts(units).0 + was_active;
            Some(pack_counts(reserved, active))
        });
        if freed {
            self.table.key_changed(slot.key_index.load(Ordering::Acquire) as usize);
        }
        Ok(())
    }

    /// Every resource registered under `key`, in table order.
    pub fn candidates(
        &self,
        key: Key
    ) -> Vec<Candidate> {
        let (lo, hi) = key.parts();
        let Ok(key_index) = self.table.key_index(lo, hi) else {
            return Vec::new();
        };
        let mut found = Vec::new();
        for (word_index, word) in self.table.members(key_index).iter().enumerate() {
            let mut bits = word.load(Ordering::SeqCst);
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let index = word_index * 64 + bit;
                if let Some(candidate) = self.candidate_at(index, (lo, hi)) {
                    found.push(candidate);
                }
            }
        }
        found
    }

    fn candidate_at(
        &self,
        index: usize,
        key: (u64, u64)
    ) -> Option<Candidate> {
        let slot = self.table.resources().get(index)?;
        let state = match slot.state.load(Ordering::Acquire) {
            RESOURCE_LIVE => State::Live,
            RESOURCE_DRAINING => State::Draining,
            _ => return None
        };
        if slot.key_lo.load(Ordering::Relaxed) != key.0
            || slot.key_hi.load(Ordering::Relaxed) != key.1
        {
            return None;
        }
        let owner = slot.owner_node.load(Ordering::Acquire);
        Some(Candidate {
            id: ResourceId::make(
                self.table.kind(),
                owner,
                (index % self.table.geometry().lane_capacity) as u32,
                slot.generation.load(Ordering::Relaxed)
            ),
            owner: NodeId::new(owner),
            owner_incarnation: Incarnation::new(slot.owner_incarnation.load(Ordering::Relaxed)),
            local: owner == self.table.node(),
            state,
            capacity: slot.capacity.load(Ordering::Relaxed),
            reserved: unpack_counts(slot.units.load(Ordering::SeqCst)).0,
            active: unpack_counts(slot.units.load(Ordering::SeqCst)).1,
            last_reserve_ms: slot.last_reserve_ms.load(Ordering::Relaxed)
        })
    }

    /// One unit of the resource's capacity, or [`Error::Busy`]. One
    /// compare-and-swap; a snapshot that showed room is not a lease.
    pub fn reserve(
        &self,
        id: ResourceId
    ) -> Result<Lease> {
        let (_, slot) = self.locate(id)?;
        match slot.state.load(Ordering::Acquire) {
            RESOURCE_LIVE => {}
            RESOURCE_DRAINING => return Err(Error::Draining(id)),
            _ => return Err(Error::Stale(id))
        }
        let capacity = slot.capacity.load(Ordering::Relaxed);
        slot.units
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |units| {
                let (reserved, active) = unpack_counts(units);
                (reserved + active < capacity).then(|| pack_counts(reserved + 1, active))
            })
            .map_err(|_| Error::Busy(id))?;
        // The slot may have ended between the state check and the count;
        // give the unit back rather than hold a lease on the next tenant.
        if !slot.is(id.generation()) {
            let _ = slot.units.try_update(Ordering::SeqCst, Ordering::SeqCst, |units| {
                let (reserved, active) = unpack_counts(units);
                Some(pack_counts(reserved.saturating_sub(1), active))
            });
            return Err(Error::Stale(id));
        }
        let fence = slot.fence.fetch_add(1, Ordering::SeqCst) + 1;
        let now = OrbitEpoch::now().as_unix_ms();
        let mut placed = false;
        for reservation in &slot.pending {
            // Claim the entry, stamp it, then publish the fence, so a
            // reader that sees the fence sees this reservation's time and
            // no other reservation's entry is ever touched.
            if reservation
                .fence
                .compare_exchange(0, PLACING, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                reservation.since_ms.store(now, Ordering::Relaxed);
                reservation.fence.store(fence, Ordering::SeqCst);
                placed = true;
                break;
            }
        }
        if !placed {
            // The owner is behind; give the unit back and say busy.
            let _ = slot.units.try_update(Ordering::SeqCst, Ordering::SeqCst, |units| {
                let (reserved, active) = unpack_counts(units);
                Some(pack_counts(reserved.saturating_sub(1), active))
            });
            return Err(Error::Busy(id));
        }
        slot.last_reserve_ms.store(now, Ordering::Release);
        Ok(Lease { id, fence, holder: self.node(), holder_incarnation: self.incarnation() })
    }

    /// The owner takes a lease a caller brought it: the unit moves from
    /// reserved to active and the returned guard gives it back when the
    /// work is over, however it ends. Only the owner can accept, and only
    /// while the resource is live in the lease's generation. A reservation
    /// that never made it here is not the caller's to undo; the owner's
    /// [`Pool::reconcile`] ages it out.
    pub fn accept(
        &self,
        lease: Lease
    ) -> Result<Execution> {
        let (_, slot) = self.owned(lease.id)?;
        if slot.state.load(Ordering::Acquire) != RESOURCE_LIVE {
            return Err(Error::Draining(lease.id));
        }
        // Exactly one accept per reservation: the fence leaves the pending
        // set here or the lease is not ours to run.
        let taken = slot.pending.iter().any(|reservation| {
            reservation
                .fence
                .compare_exchange(lease.fence, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        });
        if !taken {
            return Err(Error::NotReserved(lease));
        }
        let _ = slot.units.try_update(Ordering::SeqCst, Ordering::SeqCst, |units| {
            let (reserved, active) = unpack_counts(units);
            Some(pack_counts(reserved.saturating_sub(1), active + 1))
        });
        Ok(Execution { table: Arc::clone(&self.table), lease })
    }

    /// Whether `lease` is the current state of its resource: the resource
    /// is live in that generation and the fence has not been passed by a
    /// later lease's release.
    pub fn is_current(
        &self,
        lease: Lease
    ) -> bool {
        self.locate(lease.id).map(|(_, slot)| slot.is(lease.id.generation())).unwrap_or(false)
    }

    /// One decision for `key`: snapshot the candidates, ask `policy`, then
    /// reserve or claim what it chose. A candidate that turns out busy is a
    /// lost race, retried with a fresh snapshot up to `limits.attempts`
    /// times; after that the answer is [`Plan::Wait`]. Nothing is executed
    /// and nothing is transported here.
    pub fn acquire(
        &self,
        key: Key,
        limits: &Limits,
        policy: &dyn Policy
    ) -> Result<Plan> {
        for _ in 0..limits.attempts.max(1) {
            let candidates = self.candidates(key);
            let budget = self.budget(key);
            match policy.decide(key, &candidates, budget, limits) {
                Decision::Reuse(id) => match self.reserve(id) {
                    Ok(lease) if id.node() == self.table.node() => {
                        return Ok(Plan::LocalReuse(lease));
                    }
                    Ok(lease) => return Ok(Plan::RemoteReuse(lease)),
                    Err(Error::Busy(_) | Error::Stale(_) | Error::Draining(_)) => continue,
                    Err(error) => return Err(error)
                },
                Decision::Create => match self.claim_create(key, limits.max_live) {
                    Ok(permit) => return Ok(Plan::Create(permit)),
                    Err(Error::CreationBudget { .. }) => continue,
                    Err(error) => return Err(error)
                },
                Decision::Wait => return Ok(Plan::Wait(self.version(key)?)),
                Decision::Reject(reason) => return Ok(Plan::Reject(reason))
            }
        }
        Ok(Plan::Wait(self.version(key)?))
    }

    /// Claim one unit of the key's creation budget: live resources plus
    /// claims in progress stay under `max_live`. Drop the permit when the
    /// resource is registered (or the attempt failed).
    pub fn claim_create(
        &self,
        key: Key,
        max_live: u32
    ) -> Result<CreationPermit> {
        let (lo, hi) = key.parts();
        let key_index = self.table.key_index(lo, hi)?;
        self.table
            .key(key_index)
            .counts
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |counts| {
                let (live, creating) = unpack_counts(counts);
                (live + creating < max_live).then(|| pack_counts(live, creating + 1))
            })
            .map_err(|_| Error::CreationBudget { key, max_live })?;
        self.table.claims(usize::from(self.table.node()), key_index).fetch_add(1, Ordering::SeqCst);
        Ok(CreationPermit { table: Arc::clone(&self.table), key_index })
    }

    /// The key's live and in-progress counts, for the caller's growth
    /// decisions.
    pub fn budget(
        &self,
        key: Key
    ) -> (u32, u32) {
        let (lo, hi) = key.parts();
        match self.table.key_index(lo, hi) {
            Ok(key_index) => unpack_counts(self.table.key(key_index).counts.load(Ordering::SeqCst)),
            Err(_) => (0, 0)
        }
    }

    /// The same wait, bounded: `None` is the timeout and nothing else.
    ///
    /// This is what a caller with a deadline of its own uses — an
    /// admission window, a request that must answer busy rather than
    /// queue forever. Take the version before the attempt, as with
    /// [`Pool::wait_capacity`], so a change between the two is seen
    /// instead of waited for.
    #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    pub fn wait_capacity_timeout(
        &self,
        key: Key,
        since: u32,
        timeout: Duration
    ) -> Result<Option<u32>> {
        let (lo, hi) = key.parts();
        let key_index = self.table.key_index(lo, hi)?;
        let slot = self.table.key(key_index);
        let deadline = Instant::now() + timeout;
        loop {
            let now = slot.changes.load(Ordering::SeqCst);
            if now != since {
                return Ok(Some(now));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            slot.waiters.fetch_add(1, Ordering::SeqCst);
            let outcome = if slot.changes.load(Ordering::SeqCst) == since {
                wait_on_timeout(&slot.changes, since, left)
            } else {
                Ok(true)
            };
            slot.waiters.fetch_sub(1, Ordering::SeqCst);
            if !outcome? {
                // The deadline passed. One last look: a change may have
                // landed between the wait giving up and this line.
                let now = slot.changes.load(Ordering::SeqCst);
                return Ok((now != since).then_some(now));
            }
        }
    }

    /// The key's change count; what [`Pool::wait_capacity`] waits past.
    pub fn version(
        &self,
        key: Key
    ) -> Result<u32> {
        let (lo, hi) = key.parts();
        let key_index = self.table.key_index(lo, hi)?;
        Ok(self.table.key(key_index).changes.load(Ordering::SeqCst))
    }

    /// Park the thread until the key has changed since `since`: a release,
    /// an unregister, a closed resource, a dropped claim. Coalescing: any
    /// number of changes wake once. Returns the count now.
    pub fn wait_capacity(
        &self,
        key: Key,
        since: u32
    ) -> Result<u32> {
        let (lo, hi) = key.parts();
        let key_index = self.table.key_index(lo, hi)?;
        let slot = self.table.key(key_index);
        loop {
            let now = slot.changes.load(Ordering::SeqCst);
            if now != since {
                return Ok(now);
            }
            slot.waiters.fetch_add(1, Ordering::SeqCst);
            let outcome = if slot.changes.load(Ordering::SeqCst) == since {
                wait_on(&slot.changes, since)
            } else {
                Ok(())
            };
            slot.waiters.fetch_sub(1, Ordering::SeqCst);
            outcome?;
        }
    }

    /// Readiness for a task: `Ready` with the count now once the key has
    /// changed since `since`; otherwise the waker is registered and
    /// `Pending` comes back.
    pub fn poll_capacity(
        &self,
        key: Key,
        since: u32,
        cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Result<u32>> {
        let (lo, hi) = key.parts();
        let key_index = match self.table.key_index(lo, hi) {
            Ok(index) => index,
            Err(error) => return std::task::Poll::Ready(Err(error))
        };
        let changes = &self.table.key(key_index).changes;
        let now = changes.load(Ordering::SeqCst);
        if now != since {
            return std::task::Poll::Ready(Ok(now));
        }
        if let Err(error) = self.table.register(key_index, cx.waker()) {
            return std::task::Poll::Ready(Err(error));
        }
        let now = changes.load(Ordering::SeqCst);
        if now != since { std::task::Poll::Ready(Ok(now)) } else { std::task::Poll::Pending }
    }

    /// Every process generation that still owns a resource here.
    ///
    /// For a supervisor that lost its record of who was running — its own
    /// restart, with workers adopted rather than replaced — and has to
    /// decide what to report dead. The table is the authority: a
    /// `(node, incarnation)` in this list holds resources whose units are
    /// still counted against their keys, whether or not that process
    /// exists.
    ///
    /// It says who is *in* the table, never who is alive; the caller
    /// subtracts the generations it knows are running and reports the
    /// rest. Creation claims are not represented here — they are counted
    /// per node without a generation, and [`Pool::node_dead`] returns them
    /// whichever incarnation it names.
    pub fn owners(&self) -> Vec<(NodeId, Incarnation)> {
        let mut seen: Vec<(NodeId, Incarnation)> = Vec::new();
        for slot in self.table.resources() {
            let state = slot.state.load(Ordering::Acquire);
            if state != crate::layout::RESOURCE_LIVE && state != crate::layout::RESOURCE_DRAINING {
                continue;
            }
            let owner = (
                NodeId::new(slot.owner_node.load(Ordering::Acquire)),
                Incarnation::new(slot.owner_incarnation.load(Ordering::Acquire))
            );
            if !seen.contains(&owner) {
                seen.push(owner);
            }
        }
        seen.sort_by_key(|(node, incarnation)| (node.get(), incarnation.get()));
        seen
    }

    /// A confirmed death, reported by whoever supervises processes: every
    /// resource that incarnation of `node` owned is closed and the
    /// creation claims it held are returned. Leases it held on others'
    /// resources are those owners' to reconcile.
    pub fn node_dead(
        &self,
        node: NodeId,
        incarnation: Incarnation
    ) {
        self.table.node_dead(node.get(), incarnation.get());
    }

    /// Clear the table during quiescent owner boot and start a new epoch.
    pub fn reset_all(&self) {
        self.table.reset_all();
    }

    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        self.table.unlink()
    }

    fn locate(
        &self,
        id: ResourceId
    ) -> Result<(usize, &ResourceSlot)> {
        let geometry = self.table.geometry();
        if id.kind() != self.table.kind()
            || usize::from(id.node()) >= geometry.fleet_capacity
            || id.slot() as usize >= geometry.lane_capacity
            || id.generation() == 0
            || id.generation() > GENERATION_MASK
        {
            return Err(Error::Malformed(id.to_string()));
        }
        let index = usize::from(id.node()) * geometry.lane_capacity + id.slot() as usize;
        Ok((index, &self.table.resources()[index]))
    }

    fn owned(
        &self,
        id: ResourceId
    ) -> Result<(usize, &ResourceSlot)> {
        let (index, slot) = self.locate(id)?;
        if !slot.is(id.generation()) {
            return Err(Error::Stale(id));
        }
        if slot.owner_node.load(Ordering::Acquire) != self.table.node()
            || slot.owner_incarnation.load(Ordering::Acquire) != self.table.incarnation()
        {
            return Err(Error::NotOwner(id));
        }
        Ok((index, slot))
    }
}

/// What [`Pool::acquire`] committed to. Reuse variants hold a reservation
/// already taken; `Create` holds creation budget already claimed. Neither
/// is advisory.
#[derive(Debug)]
pub enum Plan {
    /// Capacity reserved on a resource this process owns: execute here,
    /// no stream, no shared-memory hop.
    LocalReuse(Lease),
    /// Capacity reserved on another process's resource: bring the lease
    /// to the owner over a stream; the owner accepts and completes.
    RemoteReuse(Lease),
    /// Budget claimed: make the resource, register it, finish the permit.
    Create(CreationPermit),
    /// Nothing usable now; wait past this key version and ask again.
    Wait(u32),
    Reject(Reason)
}

/// The owner's guard over one accepted lease. Dropping it, or
/// [`Execution::complete`], gives the unit back and wakes the key. It is
/// the only way capacity returns: a caller that vanished mid-way changes
/// nothing until the owner's work has actually ended.
pub struct Execution {
    table: Arc<Table>,
    lease: Lease
}

impl Execution {
    pub fn lease(&self) -> Lease {
        self.lease
    }

    pub fn complete(self) {}
}

impl Drop for Execution {
    fn drop(&mut self) {
        let index = usize::from(self.lease.id.node()) * self.table.geometry().lane_capacity
            + self.lease.id.slot() as usize;
        let slot = &self.table.resources()[index];
        // Only while the resource is still the one we accepted on: a
        // closed or reinstalled slot has nothing of ours to give back.
        if slot.generation.load(Ordering::Acquire) != self.lease.id.generation() {
            return;
        }
        let _ = slot.units.try_update(Ordering::SeqCst, Ordering::SeqCst, |units| {
            let (reserved, active) = unpack_counts(units);
            Some(pack_counts(reserved, active.saturating_sub(1)))
        });
        self.table.key_changed(slot.key_index.load(Ordering::Acquire) as usize);
    }
}

/// One unit of a key's creation budget, held while a resource is being
/// made. Dropping it gives the unit back, whether or not a resource was
/// registered meanwhile.
pub struct CreationPermit {
    table: Arc<Table>,
    key_index: usize
}

impl CreationPermit {
    /// The resource exists now (or never will); the claim is over.
    pub fn finish(self) {}
}

impl fmt::Debug for CreationPermit {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        f.debug_struct("CreationPermit").field("key_index", &self.key_index).finish_non_exhaustive()
    }
}

impl Drop for CreationPermit {
    fn drop(&mut self) {
        let _ = self.table.key(self.key_index).counts.try_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |counts| {
                let (live, creating) = unpack_counts(counts);
                Some(pack_counts(live, creating.saturating_sub(1)))
            }
        );
        let _ = self.table.claims(usize::from(self.table.node()), self.key_index).try_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |held| held.checked_sub(1)
        );
        self.table.key_changed(self.key_index);
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
/// Whether this build can park on a shared word. A table refuses to open
/// where it cannot: a sleep loop wearing the shape of a wait is worse
/// than a clear no, and nothing above here should have to ask again.
#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
pub(crate) fn waits_supported() -> bool {
    orbit_core::sync::supported()
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
pub(crate) fn waits_supported() -> bool {
    false
}

/// Park until the word moves or `timeout` passes. `false` is the
/// timeout and nothing else.
#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
pub(crate) fn wait_on_timeout(
    word: &AtomicU32,
    expected: u32,
    timeout: Duration
) -> Result<bool> {
    orbit_core::sync::wait_word_timeout(word, expected, timeout).map_err(Error::Io)
}

pub(crate) fn wait_on(
    word: &AtomicU32,
    expected: u32
) -> Result<()> {
    orbit_core::sync::wait_word(word, expected).map_err(Error::Io)
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
pub(crate) fn wait_on(
    _word: &AtomicU32,
    _expected: u32
) -> Result<()> {
    Err(Error::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "orbit-pool needs a platform that can wait on a shared word"
    )))
}

pub(crate) fn wake_on(word: &AtomicU32) {
    #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    let _ = orbit_core::sync::wake_word(word);
    #[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
    let _ = word;
}

pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use orbit_core::{Fleet, NodeId};

    use super::{
        Error, Incarnation, Key, Limits, LocalFirst, LocalOnly, POOL_RESOURCE_LANE_CAPACITY, Plan,
        Pool, State
    };

    fn pool(name: &'static str) -> Pool {
        Pool::new(Arc::new(Fleet::join(name, 2).unwrap()), Incarnation::new(1)).unwrap()
    }

    const KEY: Key = Key::new(0xC0FFEE);

    #[test]
    fn an_exclusive_resource_is_leased_once_and_freed_by_the_owner() {
        let pool = pool("pool-exclusive");
        let id = pool.register(KEY, 1).unwrap();
        assert_eq!(id.node(), 0);
        let candidates = pool.candidates(KEY);
        assert_eq!(candidates.len(), 1);
        assert!(candidates[0].local);
        assert_eq!(candidates[0].free(), 1);

        let lease = pool.reserve(id).unwrap();
        assert_eq!(lease.fence, 1);
        assert!(matches!(pool.reserve(id), Err(Error::Busy(_))));
        assert_eq!(pool.candidates(KEY)[0].reserved, 1);

        let execution = pool.accept(lease).unwrap();
        let snapshot = pool.candidates(KEY)[0];
        assert_eq!((snapshot.reserved, snapshot.active), (0, 1));
        // Still busy while the owner works, whatever the caller does.
        assert!(matches!(pool.reserve(id), Err(Error::Busy(_))));
        execution.complete();
        let again = pool.reserve(id).unwrap();
        assert_eq!(again.fence, 2);
        drop(pool.accept(again).unwrap());
        assert_eq!(pool.candidates(KEY)[0].free(), 1);
    }

    #[test]
    fn an_abandoned_reservation_is_aged_out_by_the_owner_not_by_time_alone() {
        let pool = pool("pool-abandon");
        let id = pool.register(KEY, 1).unwrap();
        let abandoned = pool.reserve(id).unwrap();
        assert!(matches!(pool.reserve(id), Err(Error::Busy(_))));
        // Within the grace the reservation is honoured.
        pool.reconcile(id, 0, Duration::from_secs(60)).unwrap();
        assert!(matches!(pool.reserve(id), Err(Error::Busy(_))));
        // Past it, the owner's reconcile frees the unit it never saw, and a
        // late accept of that lease is refused rather than counted again.
        std::thread::sleep(Duration::from_millis(5));
        pool.reconcile(id, 0, Duration::from_millis(1)).unwrap();
        assert!(matches!(pool.accept(abandoned), Err(Error::NotReserved(_))));
        assert!(pool.reserve(id).is_ok());
    }

    #[test]
    fn a_lease_is_accepted_exactly_once_and_leases_are_told_apart() {
        let pool = pool("pool-fence");
        let id = pool.register(KEY, 2).unwrap();
        let first = pool.reserve(id).unwrap();
        let second = pool.reserve(id).unwrap();
        assert_ne!(first.fence, second.fence);
        // Out of order, each once.
        let running_second = pool.accept(second).unwrap();
        assert!(matches!(pool.accept(second), Err(Error::NotReserved(_))));
        let running_first = pool.accept(first).unwrap();
        assert!(matches!(pool.accept(first), Err(Error::NotReserved(_))));
        let snapshot = pool.candidates(KEY)[0];
        assert_eq!((snapshot.reserved, snapshot.active), (0, 2));
        drop(running_first);
        drop(running_second);
        assert_eq!(pool.candidates(KEY)[0].free(), 2);

        // Aging is per reservation: an old one goes, a fresh one stays.
        let old = pool.reserve(id).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        let fresh = pool.reserve(id).unwrap();
        pool.reconcile(id, 0, Duration::from_millis(2)).unwrap();
        assert!(matches!(pool.accept(old), Err(Error::NotReserved(_))));
        assert!(pool.accept(fresh).is_ok());
    }

    #[test]
    fn an_owner_far_behind_makes_the_resource_refuse_reservations() {
        let pool = pool("pool-pending");
        let id = pool.register(KEY, u32::MAX).unwrap();
        let leases =
            (0..super::PENDING_RESERVATIONS).map(|_| pool.reserve(id).unwrap()).collect::<Vec<_>>();
        assert!(matches!(pool.reserve(id), Err(Error::Busy(_))));
        let running = pool.accept(leases[0]).unwrap();
        assert!(pool.reserve(id).is_ok());
        drop(running);
    }

    #[test]
    fn capacity_counts_and_drain_refuses_new_leases() {
        let pool = pool("pool-capacity");
        let id = pool.register(KEY, 3).unwrap();
        let leases = (0..3).map(|_| pool.reserve(id).unwrap()).collect::<Vec<_>>();
        assert!(matches!(pool.reserve(id), Err(Error::Busy(_))));
        let running = pool.accept(leases[0]).unwrap();
        pool.drain(id).unwrap();
        assert!(matches!(pool.reserve(id), Err(Error::Draining(_))));
        assert!(matches!(pool.accept(leases[1]), Err(Error::Draining(_))));
        assert_eq!(pool.candidates(KEY)[0].state, State::Draining);
        running.complete();
        pool.unregister(id).unwrap();
        assert!(matches!(pool.accept(leases[2]), Err(Error::Stale(_))));
        assert!(pool.candidates(KEY).is_empty());
        assert_eq!(pool.budget(KEY), (0, 0));
    }

    #[test]
    fn a_reused_slot_makes_the_old_id_stale() {
        let pool = pool("pool-stale");
        let first = pool.register(KEY, 1).unwrap();
        let lease = pool.reserve(first).unwrap();
        pool.unregister(first).unwrap();
        for _ in 0..POOL_RESOURCE_LANE_CAPACITY - 1 {
            pool.unregister(pool.register(KEY, 1).unwrap()).unwrap();
        }
        let second = pool.register(KEY, 1).unwrap();
        assert_eq!(second.slot(), first.slot());
        assert_ne!(second.generation(), first.generation());
        assert!(matches!(pool.reserve(first), Err(Error::Stale(_))));
        assert!(matches!(pool.accept(lease), Err(Error::Stale(_))));
        assert!(!pool.is_current(lease));
    }

    #[test]
    fn creation_claims_hold_the_fleet_under_max_live() {
        let pool = pool("pool-claims");
        let first = pool.claim_create(KEY, 2).unwrap();
        let second = pool.claim_create(KEY, 2).unwrap();
        assert!(matches!(
            pool.claim_create(KEY, 2),
            Err(Error::CreationBudget { max_live: 2, .. })
        ));
        assert_eq!(pool.budget(KEY), (0, 2));
        let id = pool.register(KEY, 1).unwrap();
        first.finish();
        assert_eq!(pool.budget(KEY), (1, 1));
        drop(second);
        assert_eq!(pool.budget(KEY), (1, 0));
        assert!(pool.claim_create(KEY, 2).is_ok());
        pool.unregister(id).unwrap();
        assert_eq!(pool.budget(KEY), (0, 0));

        // Under contention, exactly max_live claims succeed.
        let threads = (0..8)
            .map(|_| {
                let pool = pool.clone();
                std::thread::spawn(move || pool.claim_create(KEY, 3).ok())
            })
            .collect::<Vec<_>>();
        let held = threads.into_iter().map(|thread| thread.join().unwrap()).collect::<Vec<_>>();
        assert_eq!(held.iter().filter(|claim| claim.is_some()).count(), 3);
    }

    #[test]
    fn a_waiter_is_woken_when_the_owner_completes() {
        let pool = pool("pool-wait");
        let id = pool.register(KEY, 1).unwrap();
        let execution = pool.accept(pool.reserve(id).unwrap()).unwrap();
        let since = pool.version(KEY).unwrap();
        let waiter = {
            let pool = pool.clone();
            std::thread::spawn(move || pool.wait_capacity(KEY, since))
        };
        std::thread::sleep(Duration::from_millis(30));
        execution.complete();
        assert!(waiter.join().unwrap().unwrap() > since);
        assert!(pool.reserve(id).is_ok());
    }

    #[test]
    fn a_death_report_closes_that_incarnations_resources_and_claims() {
        let pool = pool("pool-dead");
        let id = pool.register(KEY, 2).unwrap();
        let lease = pool.reserve(id).unwrap();
        let _claim = pool.claim_create(KEY, 4).unwrap();
        assert_eq!(pool.budget(KEY), (1, 1));

        pool.node_dead(NodeId::ZERO, Incarnation::new(2));
        assert!(pool.is_current(lease));

        pool.node_dead(NodeId::ZERO, Incarnation::new(1));
        assert!(!pool.is_current(lease));
        assert!(matches!(pool.reserve(id), Err(Error::Stale(_))));
        assert!(pool.candidates(KEY).is_empty());
        assert_eq!(pool.budget(KEY), (0, 0));
        // The permit still held here drops later and must not underflow.
    }

    #[test]
    fn a_reset_starts_a_new_epoch_and_a_slot_can_be_exhausted() {
        let pool = pool("pool-epoch");
        let id = pool.register(KEY, 1).unwrap();
        let before = pool.epoch();
        pool.reset_all();
        assert!(pool.epoch() > before);
        assert!(matches!(pool.reserve(id), Err(Error::Stale(_))));
        assert!(pool.candidates(KEY).is_empty());

        // GENERATION_LIMIT is 4 under test.
        for _ in 0..POOL_RESOURCE_LANE_CAPACITY * 4 {
            pool.unregister(pool.register(KEY, 1).unwrap()).unwrap();
        }
        assert!(matches!(pool.register(KEY, 1), Err(Error::Full { .. })));
        pool.reset_all();
        assert!(pool.register(KEY, 1).is_ok());
    }

    #[test]
    fn acquire_walks_reuse_create_and_wait() {
        let pool = pool("pool-acquire");
        let limits = Limits { max_live: 2, attempts: 2 };
        // Nothing yet: create, within the budget.
        let Plan::Create(permit) = pool.acquire(KEY, &limits, &LocalFirst).unwrap() else {
            panic!("expected Create");
        };
        let id = pool.register(KEY, 1).unwrap();
        permit.finish();
        // A local resource with room: local reuse.
        let Plan::LocalReuse(lease) = pool.acquire(KEY, &limits, &LocalFirst).unwrap() else {
            panic!("expected LocalReuse");
        };
        let execution = pool.accept(lease).unwrap();
        // Busy, budget for one more: create.
        assert!(matches!(pool.acquire(KEY, &limits, &LocalFirst).unwrap(), Plan::Create(_)));
        // Budget spent while the permit above dropped? It did drop: claim
        // it for real, then the only answer left is Wait.
        let _held = pool.claim_create(KEY, 2).unwrap();
        assert!(matches!(pool.acquire(KEY, &limits, &LocalFirst).unwrap(), Plan::Wait(_)));
        assert!(matches!(pool.acquire(KEY, &limits, &LocalOnly).unwrap(), Plan::Wait(_)));
        execution.complete();
        assert!(matches!(pool.acquire(KEY, &limits, &LocalOnly).unwrap(), Plan::LocalReuse(_)));
        let _ = id;
    }
}

use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};

use super::{
    ChunkDescriptor, ChunkGeometry, ChunkPlan, DEFAULT_CHUNK_BYTES, ExchangeId,
    Flow,
};
use crate::wake::{Doorstep, Driver};
use crate::{Error, Incarnation, Result, lock_unpoisoned};

const MAGIC: u32 = 0x4f_50_41_59; // "OPAY"
const VERSION: u16 = 5;

const SLOT_FREE: u64 = 0;
const SLOT_RESERVED: u64 = 1;
const SLOT_LIVE: u64 = 2;
const SLOT_READING: u64 = 3;
const SLOT_RECLAIMING: u64 = 4;
const STATE_BITS: u32 = 3;
const STATE_MASK: u64 = (1 << STATE_BITS) - 1;

fn tagged(
    generation: u64,
    state: u64,
) -> u64 {
    (generation << STATE_BITS) | state
}

/// Geometry and SHM identity of one fleet-wide payload arena.
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

    /// Physical chunk geometry for this arena. Runtime policy chooses the
    /// slot count independently for each request or response.
    pub const fn chunk_geometry(self) -> Option<ChunkGeometry> {
        ChunkGeometry::new(self.slot_size, self.slots_per_node)
    }

    /// Plan a known payload with the benchmark-backed default chunk target,
    /// clamped so one chunk always fits in this node lane.
    pub const fn plan_chunks(
        self,
        data_bytes: usize,
    ) -> Option<ChunkPlan> {
        let lane_bytes = self.lane_bytes();
        let limit = if DEFAULT_CHUNK_BYTES < lane_bytes {
            DEFAULT_CHUNK_BYTES
        } else {
            lane_bytes
        };
        ChunkPlan::for_limit(data_bytes, self.slot_size, limit)
    }

    /// Plan using a runtime-selected number of slots per publication.
    pub const fn plan_chunks_with(
        self,
        data_bytes: usize,
        slots_per_chunk: usize,
    ) -> Option<ChunkPlan> {
        let geometry = match self.chunk_geometry() {
            Some(geometry) => geometry,
            None => return None,
        };
        geometry.plan(data_bytes, slots_per_chunk)
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
    _reserved_credit: [u8; 24],
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
            _reserved_credit: [0; 24],
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

#[repr(C, align(64))]
struct CreditWord {
    generation: AtomicU32,
    waiters: AtomicU32,
    _padding: [u8; 56],
}

#[repr(C, align(8))]
struct SlotMeta {
    flow: AtomicU16,
    reader_node: AtomicU16,
    extent_slots: AtomicU32,
    exchange: AtomicU64,
    allocation: AtomicU64,
    producer_incarnation: AtomicU64,
    reader_incarnation: AtomicU64,
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
    segment_size: usize,
}

impl Geometry {
    fn new(
        fleet_capacity: u16,
        spec: PayloadArenaSpec,
    ) -> Self {
        let total_slots = usize::from(fleet_capacity) * spec.slots_per_node;
        let credits_offset = size_of::<Header>();
        let metadata_offset = credits_offset
            + usize::from(fleet_capacity) * size_of::<CreditWord>();
        let payload_offset = (metadata_offset
            + total_slots * size_of::<SlotMeta>())
        .next_multiple_of(64);
        let segment_size = payload_offset + total_slots * spec.slot_size;
        Self {
            slots_per_node: spec.slots_per_node,
            slot_size: spec.slot_size,
            total_slots,
            credits_offset,
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
        let layout = std::alloc::Layout::from_size_align(size, 64)
            .expect("payload arena layout");
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

/// A sender's credit registration: its exchange's raw id and its flow.
pub(crate) type FlowKey = (u64, u16);

/// What a parked sender is waiting for, as it registers.
pub(crate) struct FlowWaitSpec<'a> {
    pub(crate) key: FlowKey,
    pub(crate) status: &'a crate::WriteStatus,
    /// Slots the next publication needs.
    pub(crate) demand: usize,
    /// The flow's slot allowance.
    pub(crate) limit: usize,
    /// The descriptors the flow has published and may still hold.
    pub(crate) held: &'a [ChunkDescriptor],
}

struct FlowWait {
    waker: Waker,
    status: crate::WriteStatus,
    demand: usize,
    limit: usize,
    held: Vec<ChunkDescriptor>,
}

struct CreatorWait {
    waker: Waker,
    demand: usize,
}

#[derive(Default)]
struct CreditWaits {
    flows: HashMap<FlowKey, FlowWait>,
    creators: Vec<CreatorWait>,
}

/// Whether a flow that has published `held` may publish `count` more slots
/// under `limit`; a descriptor its reader has released no longer counts. The
/// one test for the sender and for the credit driver, so they cannot differ.
pub(crate) fn allowance_admits(
    held: &[ChunkDescriptor],
    limit: usize,
    count: usize,
    retains: impl Fn(ChunkDescriptor) -> bool,
) -> bool {
    let published: usize =
        held.iter().map(|descriptor| descriptor.slot_count() as usize).sum();
    if published + count <= limit {
        return true;
    }
    let Some(mut remaining) = limit.checked_sub(count) else {
        return false;
    };
    for descriptor in held {
        if retains(*descriptor) {
            let Some(left) =
                remaining.checked_sub(descriptor.slot_count() as usize)
            else {
                return false;
            };
            remaining = left;
        }
    }
    true
}

struct Arena {
    _fleet: Arc<Fleet>,
    backing: Backing,
    geometry: Geometry,
    kind: u8,
    node: u16,
    incarnation: Incarnation,
    allocation: Mutex<usize>,
    credit_waits: Mutex<CreditWaits>,
    driver: Mutex<Option<Driver>>,
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
                self.base()
                    .add(self.geometry.metadata_offset)
                    .cast::<SlotMeta>(),
                self.geometry.total_slots,
            )
        }
    }

    fn credits(&self) -> &[CreditWord] {
        // SAFETY: geometry reserves one cache-line-aligned word per fleet node.
        unsafe {
            std::slice::from_raw_parts(
                self.base()
                    .add(self.geometry.credits_offset)
                    .cast::<CreditWord>(),
                self.geometry.total_slots / self.geometry.slots_per_node,
            )
        }
    }

    fn local_credit(&self) -> &CreditWord {
        &self.credits()[usize::from(self.node)]
    }

    fn payload(
        &self,
        absolute_slot: usize,
    ) -> *mut u8 {
        // SAFETY: callers validate the slot against `total_slots`.
        unsafe {
            self.base().add(
                self.geometry.payload_offset
                    + absolute_slot * self.geometry.slot_size,
            )
        }
    }

    fn reserve(
        self: &Arc<Self>,
        payload_len: usize,
    ) -> Result<Publication> {
        let count = self.required_slots(payload_len)?;

        let mut hint = lock_unpoisoned(&self.allocation);
        let lane_start = usize::from(self.node) * self.geometry.slots_per_node;
        let candidate = (0..self.geometry.slots_per_node)
            .map(|offset| (*hint + offset) & (self.geometry.slots_per_node - 1))
            .find(|&local| {
                local + count <= self.geometry.slots_per_node
                    && self.metadata()
                        [lane_start + local..lane_start + local + count]
                        .iter()
                        .all(|slot| {
                            slot.allocation.load(Ordering::Acquire) & STATE_MASK
                                == SLOT_FREE
                        })
            })
            .ok_or(Error::PayloadFull { requested_slots: count })?;
        let first = lane_start + candidate;
        let generation =
            self.header().next_generation.fetch_add(1, Ordering::Relaxed);
        if generation > (u64::MAX >> STATE_BITS) {
            return Err(Error::Malformed(
                "payload allocation generations exhausted".to_owned(),
            ));
        }
        let slots = &self.metadata()[first..first + count];
        // Publish the extent head first so a dead producer's partial
        // reservation can be recovered. Only an exact generation can free it.
        for (index, slot) in slots.iter().enumerate() {
            slot.extent_slots.store(
                if index == 0 { count as u32 } else { 0 },
                Ordering::Relaxed,
            );
            slot.exchange.store(0, Ordering::Relaxed);
            slot.flow.store(0, Ordering::Relaxed);
            slot.producer_incarnation
                .store(self.incarnation.get(), Ordering::Relaxed);
            slot.reader_node.store(u16::MAX, Ordering::Relaxed);
            slot.reader_incarnation.store(0, Ordering::Relaxed);
            slot.allocation
                .store(tagged(generation, SLOT_RESERVED), Ordering::Release);
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
        state: u64,
    ) {
        if self.claim_reclaim(first, generation, state) {
            self.free(first, count, generation);
        }
    }

    fn claim_reclaim(
        &self,
        first: usize,
        generation: u64,
        state: u64,
    ) -> bool {
        self.metadata()[first]
            .allocation
            .compare_exchange(
                tagged(generation, state),
                tagged(generation, SLOT_RECLAIMING),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn free(
        &self,
        first: usize,
        count: usize,
        generation: u64,
    ) {
        // A recovery pass may finish a release interrupted by process death.
        // Exact generation/state CASes also make overlapping releases safe.
        let slots = &self.metadata()[first..first + count];
        for slot in &slots[1..] {
            let _ = slot.allocation.compare_exchange(
                tagged(generation, SLOT_RESERVED),
                tagged(generation, SLOT_FREE),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        if slots[0]
            .allocation
            .compare_exchange(
                tagged(generation, SLOT_RECLAIMING),
                tagged(generation, SLOT_FREE),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.notify_credit(first / self.geometry.slots_per_node);
        }
    }

    fn notify_credit(
        &self,
        node: usize,
    ) {
        let credit = &self.credits()[node];
        credit.generation.fetch_add(1, Ordering::SeqCst);
        if credit.waiters.load(Ordering::SeqCst) > 0 {
            crate::wake_on(&credit.generation);
        }
    }

    fn required_slots(
        &self,
        payload_len: usize,
    ) -> Result<usize> {
        if payload_len == 0 {
            return Err(Error::Malformed(
                "an empty payload needs no arena allocation".to_owned(),
            ));
        }
        let count = payload_len.div_ceil(self.geometry.slot_size);
        if count > self.geometry.slots_per_node {
            return Err(Error::PayloadTooLarge {
                len: payload_len,
                capacity: self.geometry.slots_per_node
                    * self.geometry.slot_size,
            });
        }
        Ok(count)
    }

    fn has_run(
        &self,
        count: usize,
    ) -> bool {
        let lane_start = usize::from(self.node) * self.geometry.slots_per_node;
        (0..=self.geometry.slots_per_node - count).any(|local| {
            self.metadata()[lane_start + local..lane_start + local + count]
                .iter()
                .all(|slot| {
                    slot.allocation.load(Ordering::Acquire) & STATE_MASK
                        == SLOT_FREE
                })
        })
    }

    fn wait_available(
        &self,
        count: usize,
    ) -> Result<()> {
        self.wait_credit(|| Ok(self.has_run(count)))
    }

    fn wait_credit(
        &self,
        ready: impl Fn() -> Result<bool>,
    ) -> Result<()> {
        let credit = self.local_credit();
        loop {
            if ready()? {
                return Ok(());
            }
            credit.waiters.fetch_add(1, Ordering::SeqCst);
            let seen = credit.generation.load(Ordering::SeqCst);
            let outcome = ready().and_then(|ready| {
                if ready {
                    Ok(())
                } else {
                    crate::wait_on(&credit.generation, seen)
                }
            });
            credit.waiters.fetch_sub(1, Ordering::SeqCst);
            outcome?;
        }
    }

    fn poll_available(
        self: &Arc<Self>,
        count: usize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        if self.has_run(count) {
            return Poll::Ready(Ok(()));
        }
        {
            let mut waits = lock_unpoisoned(&self.credit_waits);
            match waits
                .creators
                .iter_mut()
                .find(|wait| wait.waker.will_wake(cx.waker()))
            {
                Some(wait) => wait.demand = count,
                None => waits.creators.push(CreatorWait {
                    waker: cx.waker().clone(),
                    demand: count,
                }),
            }
        }
        self.ensure_driver()?;
        if self.has_run(count) { Poll::Ready(Ok(())) } else { Poll::Pending }
    }

    /// Register a flow's sender for credit, keyed by its exchange and flow, so
    /// a driver pass wakes it only when it may proceed. Check, register,
    /// check again: a return between the two checks is seen by one of them.
    fn poll_flow(
        self: &Arc<Self>,
        wait: FlowWaitSpec<'_>,
        ready: impl Fn() -> Result<bool>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        if ready()? {
            return Poll::Ready(Ok(()));
        }
        {
            let mut waits = lock_unpoisoned(&self.credit_waits);
            let record =
                waits.flows.entry(wait.key).or_insert_with(|| FlowWait {
                    waker: cx.waker().clone(),
                    status: wait.status.clone(),
                    demand: 0,
                    limit: 0,
                    held: Vec::new(),
                });
            if !record.waker.will_wake(cx.waker()) {
                record.waker = cx.waker().clone();
            }
            record.demand = wait.demand;
            record.limit = wait.limit;
            record.held.clear();
            record.held.extend_from_slice(wait.held);
        }
        self.ensure_driver()?;
        if ready()? {
            self.forget_flow(wait.key);
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn forget_flow(
        &self,
        key: FlowKey,
    ) {
        lock_unpoisoned(&self.credit_waits).flows.remove(&key);
    }

    /// Whether a parked flow may proceed now, by the sender's own test.
    /// `longest` is this pass's longest free run, read once when first needed.
    fn flow_may_proceed(
        &self,
        wait: &FlowWait,
        longest: &mut Option<usize>,
    ) -> bool {
        if wait.status.is_ended() {
            return true;
        }
        allowance_admits(&wait.held, wait.limit, wait.demand, |descriptor| {
            self.holds(descriptor)
        }) && *longest.get_or_insert_with(|| self.longest_free_run())
            >= wait.demand
    }

    /// The longest run of free slots in this node's lane: one pass over it,
    /// where testing each waiter's demand would scan it once per waiter.
    fn longest_free_run(&self) -> usize {
        let lane_start = usize::from(self.node) * self.geometry.slots_per_node;
        let mut longest = 0;
        let mut run = 0;
        for slot in &self.metadata()
            [lane_start..lane_start + self.geometry.slots_per_node]
        {
            if slot.allocation.load(Ordering::Acquire) & STATE_MASK == SLOT_FREE
            {
                run += 1;
                longest = longest.max(run);
            } else {
                run = 0;
            }
        }
        longest
    }

    /// Whether `descriptor`'s extent is still allocated to that publication.
    fn holds(
        &self,
        descriptor: ChunkDescriptor,
    ) -> bool {
        let first = usize::from(descriptor.owner_node())
            * self.geometry.slots_per_node
            + descriptor.first_slot() as usize;
        let allocation =
            self.metadata()[first].allocation.load(Ordering::Acquire);
        allocation >> STATE_BITS == descriptor.allocation_generation()
            && allocation & STATE_MASK != SLOT_FREE
    }

    fn ensure_driver(self: &Arc<Self>) -> Result<()> {
        let mut driver = lock_unpoisoned(&self.driver);
        if driver.is_none() {
            *driver = Some(Driver::start(
                Arc::as_ptr(self),
                format!("orbit-payload-{}-{}-driver", self.kind, self.node),
            )?);
        }
        Ok(())
    }

    fn node_dead(
        &self,
        node: u16,
        incarnation: Incarnation,
        dead_flows: &HashSet<(u64, u16)>,
    ) {
        let metadata = self.metadata();
        for (first, slot) in metadata.iter().enumerate() {
            let allocation = slot.allocation.load(Ordering::Acquire);
            let state = allocation & STATE_MASK;
            let generation = allocation >> STATE_BITS;
            let producer_node = first / self.geometry.slots_per_node;
            let count = slot.extent_slots.load(Ordering::Acquire) as usize;
            if count == 0
                || count
                    > self.geometry.slots_per_node
                        - first % self.geometry.slots_per_node
            {
                continue;
            }
            let abandoned_publication =
                matches!(state, SLOT_RESERVED | SLOT_LIVE)
                    && producer_node == usize::from(node)
                    && slot.producer_incarnation.load(Ordering::Acquire)
                        == incarnation.get();
            let abandoned_read = state == SLOT_READING
                && slot.reader_node.load(Ordering::Acquire) == node
                && slot.reader_incarnation.load(Ordering::Acquire)
                    == incarnation.get();
            // A descriptor may still be queued, or the reader may have
            // died between claiming it and recording its exact incarnation.
            let lost_reader = (state == SLOT_LIVE
                || (state == SLOT_READING
                    && slot.reader_node.load(Ordering::Acquire) == u16::MAX))
                && dead_flows.contains(&(
                    slot.exchange.load(Ordering::Acquire),
                    slot.flow.load(Ordering::Acquire),
                ));
            if state == SLOT_RECLAIMING
                || ((abandoned_publication || abandoned_read || lost_reader)
                    && self.claim_reclaim(first, generation, state))
            {
                self.free(first, count, generation);
            }
        }
        // A writer can be parked on credit held by another exchange in its
        // lane when this peer dies. It must still observe the control close.
        for node in 0..self.credits().len() {
            self.notify_credit(node);
        }
    }
}

impl Doorstep for Arena {
    fn generation(&self) -> &AtomicU32 {
        &self.local_credit().generation
    }

    fn listening(
        &self,
        delta: i32,
    ) {
        if delta > 0 {
            self.local_credit().waiters.fetch_add(1, Ordering::SeqCst);
        } else {
            self.local_credit().waiters.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Wake the parked senders and creators that may proceed; the others
    /// stay registered for the next credit change. Woken outside the lock.
    fn drain(&self) {
        let mut ready = Vec::new();
        {
            let mut waits = lock_unpoisoned(&self.credit_waits);
            let mut longest = None;
            ready.extend(
                waits
                    .flows
                    .extract_if(|_, wait| {
                        self.flow_may_proceed(wait, &mut longest)
                    })
                    .map(|(_, wait)| wait.waker),
            );
            if !waits.creators.is_empty() {
                let longest =
                    *longest.get_or_insert_with(|| self.longest_free_run());
                ready.extend(
                    waits
                        .creators
                        .extract_if(.., |wait| longest >= wait.demand)
                        .map(|wait| wait.waker),
                );
            }
        }
        for waker in ready {
            waker.wake();
        }
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        if let Some(mut driver) = self
            .driver
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            driver.stop(&self.local_credit().generation);
        }
    }
}

/// One fleet-wide payload allocation table. Every producer allocates from its
/// node's exclusive lane, independent of which exchange side it currently holds.
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

    pub fn chunk_geometry(&self) -> ChunkGeometry {
        // PayloadArena::open validates both geometry values as non-zero.
        ChunkGeometry::new(self.slot_size(), self.slots_per_node())
            .expect("opened payload arena has valid chunk geometry")
    }

    pub fn plan_chunks(
        &self,
        data_bytes: usize,
    ) -> Result<ChunkPlan> {
        PayloadArenaSpec::new(
            self.kind(),
            self.slots_per_node(),
            self.slot_size(),
        )
        .plan_chunks(data_bytes)
        .ok_or_else(|| {
            Error::Malformed(
                "payload arena cannot plan zero-sized geometry".to_owned(),
            )
        })
    }

    pub fn plan_chunks_with(
        &self,
        data_bytes: usize,
        slots_per_chunk: usize,
    ) -> Result<ChunkPlan> {
        self.chunk_geometry().plan(data_bytes, slots_per_chunk).ok_or_else(|| {
            Error::Malformed(format!(
                "chunk slot count {slots_per_chunk} exceeds payload lane capacity {}",
                self.slots_per_node()
            ))
        })
    }
    /// Park this thread until a run large enough for `payload_len` may be
    /// available. Allocation still decides the race after the wake.
    pub fn wait_available(
        &self,
        payload_len: usize,
    ) -> Result<()> {
        let count = self.arena.required_slots(payload_len)?;
        self.arena.wait_available(count)
    }

    /// Whether this producer lane currently has a contiguous run for the
    /// payload. This is only a readiness snapshot; allocation still decides
    /// a race between concurrent producers.
    pub fn is_available(
        &self,
        payload_len: usize,
    ) -> Result<bool> {
        let count = self.arena.required_slots(payload_len)?;
        Ok(self.arena.has_run(count))
    }

    /// Task readiness for this node's payload credit. Credit returns are
    /// coalesced through one shared generation and one local driver.
    pub fn poll_available(
        &self,
        payload_len: usize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        let count = match self.arena.required_slots(payload_len) {
            Ok(count) => count,
            Err(error) => return Poll::Ready(Err(error)),
        };
        self.arena.poll_available(count, cx)
    }

    /// Clear this arena only while the fleet is known quiescent. Normal
    /// attach and replacement never call this implicitly.
    pub fn reset_all(&self) {
        let mut hint = lock_unpoisoned(&self.arena.allocation);
        *hint = 0;
        for slot in self.arena.metadata() {
            slot.allocation.store(SLOT_FREE, Ordering::Release);
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
        incarnation: Incarnation,
    ) {
        self.node_dead_with_flows(node, incarnation, &HashSet::new());
    }

    pub(crate) fn node_dead_with_flows(
        &self,
        node: orbit_core::NodeId,
        incarnation: Incarnation,
        dead_flows: &HashSet<(u64, u16)>,
    ) {
        self.arena.node_dead(node.get(), incarnation, dead_flows);
    }

    /// Remove the arena's SHM name. Existing mappings remain valid.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        match &self.arena.backing {
            Backing::Memory(_) => {
                self.reset_all();
                Ok(())
            }
            Backing::Shm(region) => region.unlink().map_err(Error::Io),
        }
    }

    #[cfg(test)]
    pub(crate) fn publish(
        &self,
        payload: &[u8],
    ) -> Result<Publication> {
        let mut publication = self.reserve(payload.len())?;
        publication.as_mut_slice().copy_from_slice(payload);
        publication.make_live();
        Ok(publication)
    }

    pub(crate) fn reserve(
        &self,
        payload_len: usize,
    ) -> Result<Publication> {
        self.arena.reserve(payload_len)
    }

    pub(crate) fn read(
        &self,
        descriptor: ChunkDescriptor,
    ) -> Result<PayloadChunk> {
        let (first, count, generation) = self.extent(descriptor)?;
        let len = descriptor.payload_len() as usize;
        let slots = &self.arena.metadata()[first..first + count];
        if slots[0].extent_slots.load(Ordering::Acquire) != count as u32
            || slots[1..].iter().any(|slot| {
                slot.allocation.load(Ordering::Acquire)
                    != tagged(generation, SLOT_RESERVED)
            })
            || slots[0]
                .allocation
                .compare_exchange(
                    tagged(generation, SLOT_LIVE),
                    tagged(generation, SLOT_READING),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return Err(Error::Malformed(
                "chunk allocation is stale or already consumed".to_owned(),
            ));
        }
        slots[0]
            .reader_incarnation
            .store(self.arena.incarnation.get(), Ordering::Release);
        slots[0].reader_node.store(self.arena.node, Ordering::Release);
        Ok(PayloadChunk {
            arena: Arc::clone(&self.arena),
            first,
            count,
            len,
            generation,
            descriptor,
        })
    }

    fn extent(
        &self,
        descriptor: ChunkDescriptor,
    ) -> Result<(usize, usize, u64)> {
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
        let generation = descriptor.allocation_generation();
        if owner
            >= self.arena.geometry.total_slots
                / self.arena.geometry.slots_per_node
            || generation == 0
            || generation > (u64::MAX >> STATE_BITS)
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
        Ok((owner * self.slots_per_node() + first_local, count, generation))
    }

    pub(crate) fn discard(
        &self,
        descriptor: ChunkDescriptor,
    ) {
        if let Ok((first, count, generation)) = self.extent(descriptor)
            && self.arena.metadata()[first].extent_slots.load(Ordering::Acquire)
                == count as u32
        {
            self.arena.release(first, count, generation, SLOT_LIVE);
        }
    }

    pub(crate) fn retains(
        &self,
        descriptor: ChunkDescriptor,
    ) -> bool {
        self.arena.holds(descriptor)
    }

    pub(crate) fn notify_peer(
        &self,
        node: u16,
    ) {
        if usize::from(node) < self.arena.credits().len() {
            self.arena.notify_credit(usize::from(node));
        }
    }

    pub(crate) fn wait_credit(
        &self,
        ready: impl Fn() -> Result<bool>,
    ) -> Result<()> {
        self.arena.wait_credit(ready)
    }

    pub(crate) fn poll_flow(
        &self,
        wait: FlowWaitSpec<'_>,
        ready: impl Fn() -> Result<bool>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        self.arena.poll_flow(wait, ready, cx)
    }

    /// Drop a sender's credit registration, as its producer goes away.
    pub(crate) fn forget_flow(
        &self,
        key: FlowKey,
    ) {
        self.arena.forget_flow(key);
    }

    /// Whether a sender's credit registration is still parked.
    #[cfg(test)]
    pub(crate) fn is_waiting(
        &self,
        key: FlowKey,
    ) -> bool {
        lock_unpoisoned(&self.arena.credit_waits).flows.contains_key(&key)
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
    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: this publication owns a contiguous reserved extent and no
        // consumer can observe it before the control descriptor is sent.
        unsafe {
            std::slice::from_raw_parts(
                self.arena.payload(self.first),
                self.payload_len,
            )
        }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: this publication exclusively owns a contiguous reserved
        // extent until it is made live and its control descriptor is sent.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.arena.payload(self.first),
                self.payload_len,
            )
        }
    }

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

    pub(crate) fn truncate(
        &mut self,
        payload_len: usize,
    ) -> Result<()> {
        if payload_len == 0 || payload_len > self.payload_len {
            return Err(Error::Malformed(format!(
                "committed payload length {payload_len} is outside reserved capacity {}",
                self.payload_len
            )));
        }
        let count = self.arena.required_slots(payload_len)?;
        if count < self.count {
            for slot in &self.arena.metadata()
                [self.first + count..self.first + self.count]
            {
                slot.allocation.store(
                    tagged(self.generation, SLOT_FREE),
                    Ordering::Release,
                );
            }
            self.arena.metadata()[self.first]
                .extent_slots
                .store(count as u32, Ordering::Release);
            self.count = count;
            self.arena.notify_credit(usize::from(self.arena.node));
        }
        self.payload_len = payload_len;
        Ok(())
    }

    pub(crate) fn bind(
        &self,
        exchange: ExchangeId,
        flow: Flow,
    ) {
        let slot = &self.arena.metadata()[self.first];
        slot.exchange
            .store(exchange.stream_id().net_id().raw(), Ordering::Relaxed);
        slot.flow.store(u16::from(flow.wire()), Ordering::Relaxed);
    }

    pub(crate) fn make_live(&self) {
        self.arena.metadata()[self.first]
            .allocation
            .store(tagged(self.generation, SLOT_LIVE), Ordering::Release);
    }

    pub(crate) fn mark_published(mut self) {
        self.published = true;
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        if !self.published {
            // A failed commit may already have marked the extent live.
            if self.arena.claim_reclaim(
                self.first,
                self.generation,
                SLOT_RESERVED,
            ) || self.arena.claim_reclaim(
                self.first,
                self.generation,
                SLOT_LIVE,
            ) {
                self.arena.free(self.first, self.count, self.generation);
            }
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
    descriptor: ChunkDescriptor,
}

impl PayloadChunk {
    pub const fn descriptor(&self) -> ChunkDescriptor {
        self.descriptor
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the allocation is immutable between publication and this
        // guard's release, and `len` was validated against the run.
        unsafe {
            std::slice::from_raw_parts(self.arena.payload(self.first), self.len)
        }
    }
}

impl AsRef<[u8]> for PayloadChunk {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
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
        self.arena.release(
            self.first,
            self.count,
            self.generation,
            SLOT_READING,
        );
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
    if !crate::waits_supported() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "payload arenas need a platform that can wait on a shared word",
        )));
    }
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
                spec.kind,
                arena.geometry.slots_per_node,
                arena.geometry.slot_size
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
        Key::Shm(name, _) => Backing::Shm(open_shm(
            name,
            fleet.fleet_capacity(),
            spec,
            geometry,
        )?),
    };
    let arena = Arc::new(Arena {
        _fleet: Arc::clone(fleet),
        backing,
        geometry,
        kind: spec.kind,
        node: fleet.node_id().get(),
        incarnation,
        allocation: Mutex::new(0),
        credit_waits: Mutex::new(CreditWaits::default()),
        driver: Mutex::new(None),
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
            std::ptr::write(
                region.as_ptr().cast::<Header>(),
                Header::new(fleet_capacity, spec),
            );
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
                format!(
                    "SHM segment {name} has an incompatible payload-arena layout"
                ),
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
        let (generation, first, count, len, kind, owner) =
            publication.coordinates();
        ChunkDescriptor::new(
            ExchangeId::from_stream_id(crate::StreamId::from_net_id(
                orbit_core::NetId64::make(240, 0, 1),
            )),
            Flow::AtoB,
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
        let arena = PayloadArena::open(
            fleet(),
            Incarnation::new(1),
            PayloadArenaSpec::new(241, 8, 256),
        )
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
        let arena = PayloadArena::open(
            fleet(),
            Incarnation::new(1),
            PayloadArenaSpec::new(242, 4, 64),
        )
        .expect("arena");
        let first = arena.publish(&[1; 256]).expect("fills lane");
        let descriptor = descriptor(&first, 1);
        first.mark_published();
        assert!(matches!(arena.publish(&[2]), Err(Error::PayloadFull { .. })));
        drop(arena.read(descriptor).expect("consume"));
        assert!(arena.publish(&[3]).is_ok());
    }

    #[test]
    fn a_cancelled_publication_never_spends_credit() {
        let arena = PayloadArena::open(
            fleet(),
            Incarnation::new(1),
            PayloadArenaSpec::new(243, 2, 64),
        )
        .expect("arena");
        drop(arena.publish(&[1; 128]).expect("reserved publication"));
        assert!(arena.publish(&[2; 128]).is_ok());
    }

    #[test]
    fn returned_credit_wakes_a_parked_producer() {
        let arena = PayloadArena::open(
            fleet(),
            Incarnation::new(1),
            PayloadArenaSpec::new(244, 2, 64),
        )
        .expect("arena");
        let publication = arena.publish(&[1; 128]).expect("fills lane");
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
        let name: &'static str =
            Box::leak(format!("pc{:x}", std::process::id()).into_boxed_str());
        let spec = PayloadArenaSpec::new(248, 1, 64);
        let producer = PayloadArena::open(
            Arc::new(
                Fleet::join_shm_as(name, 2, orbit_core::NodeId::ZERO)
                    .expect("producer fleet"),
            ),
            Incarnation::new(1),
            spec,
        )
        .expect("producer arena");
        producer.reset_all();
        let consumer = PayloadArena::open(
            Arc::new(
                Fleet::join_shm_as(name, 2, orbit_core::NodeId::new(1))
                    .expect("consumer fleet"),
            ),
            Incarnation::new(2),
            spec,
        )
        .expect("consumer arena");

        let publication =
            producer.publish(&[7; 64]).expect("fill producer lane");
        let descriptor = descriptor(&publication, 1);
        publication.mark_published();
        let chunk = consumer.read(descriptor).expect("consume producer chunk");

        let waiting = producer.clone();
        let waiter = std::thread::spawn(move || waiting.wait_available(64));
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(1);
        while producer.arena.local_credit().waiters.load(Ordering::SeqCst) == 0
        {
            assert!(
                std::time::Instant::now() < deadline,
                "producer did not park"
            );
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

        let arena = PayloadArena::open(
            fleet(),
            Incarnation::new(1),
            PayloadArenaSpec::new(247, 2, 64),
        )
        .expect("arena");
        let publication = arena.publish(&[1; 128]).expect("fills lane");
        let descriptor = descriptor(&publication, 1);
        publication.mark_published();
        let chunk = arena.read(descriptor).expect("held chunk");

        let waiting = arena.clone();
        let waiter = tokio::spawn(async move {
            poll_fn(|cx| waiting.poll_available(1, cx)).await
        });
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
        let arena = PayloadArena::open(
            fleet(),
            Incarnation::new(7),
            PayloadArenaSpec::new(245, 2, 64),
        )
        .expect("arena");

        let abandoned = arena.arena.reserve(128).expect("reserved by producer");
        arena.node_dead(orbit_core::NodeId::ZERO, Incarnation::new(7));
        let replacement =
            arena.publish(&[2; 128]).expect("producer credit reclaimed");
        drop(abandoned);
        let replacement_descriptor = descriptor(&replacement, 2);
        replacement.mark_published();
        let held = arena.read(replacement_descriptor).expect("held by reader");

        arena.node_dead(orbit_core::NodeId::ZERO, Incarnation::new(6));
        assert!(matches!(arena.publish(&[3]), Err(Error::PayloadFull { .. })));
        arena.node_dead(orbit_core::NodeId::ZERO, Incarnation::new(7));
        let after_reader_death =
            arena.publish(&[4; 128]).expect("reader credit reclaimed");
        drop(held);
        let after_descriptor = descriptor(&after_reader_death, 3);
        after_reader_death.mark_published();
        assert!(arena.read(after_descriptor).is_ok());
    }

    #[test]
    fn committed_unread_payload_is_reclaimed_with_its_dead_producer() {
        let arena = PayloadArena::open(
            fleet(),
            Incarnation::new(8),
            PayloadArenaSpec::new(246, 2, 64),
        )
        .expect("arena");
        let publication = arena.publish(b"survives").expect("publication");
        let descriptor = descriptor(&publication, 1);
        publication.mark_published();

        arena.node_dead(orbit_core::NodeId::ZERO, Incarnation::new(8));
        assert!(arena.read(descriptor).is_err());
        assert!(arena.publish(&[9; 128]).is_ok());
    }
}

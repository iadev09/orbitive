//! Owner-bound pins protect immutable identity; atomic TAT commits consumption.
//! A frozen descriptor and two banks make identity publication crash-safe.

mod domain;
mod layout;

use std::mem::{ManuallyDrop, size_of};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use orbit_core::FleetName;
use orbit_core::shm::{
    FleetMembership, LaneHold, PreparedLaneGuard, PreparedLaneLock, ShmRegion,
    join_fleet_membership, ring_segment_name, try_hold_lane,
};
use portable_atomic::AtomicU128;

use crate::budget::WorkBudget;
use crate::clock::{Clock, Monotonic};
use crate::gcra::{Gcra, Outcome, Prepared};
use crate::{CasBudget, Decision, Error, Geometry, Key, Policy, QuotaStore};
use layout::{
    Bank, FROZEN, GENERATION, Header, INITIALIZING, Layout, MAGIC, Owner,
    READY, RETIRED, Shard, Slot,
};

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    Initializing,
    BeforeReady,
    Pinned,
    Frozen,
    KeyCopy,
    Metadata,
    BeforePublish,
    Published,
    TatCommitted,
    Recovering,
}

#[cfg(test)]
thread_local! {
    static FAULT: std::cell::Cell<Option<Fault>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn fault(point: Fault) {
    FAULT.with(|selected| {
        if selected.get() == Some(point) {
            tests::crash_checkpoint(point);
        }
    });
}

/// An explicit uid-scoped resource. No quota kind is reserved or chosen here.
/// A different resource is an independent quota authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShmResource {
    fleet: FleetName,
    kind: u8,
}

impl ShmResource {
    pub const fn new(
        fleet: FleetName,
        kind: u8,
    ) -> Self {
        Self { fleet, kind }
    }
    pub fn fleet(self) -> FleetName {
        self.fleet
    }
    pub const fn kind(self) -> u8 {
        self.kind
    }
}

/// Owner capacity is persisted geometry. Recovery attempts are local to each
/// admission call, like CasBudget; zero disables opportunistic recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShmOptions {
    owners: NonZeroUsize,
    recovery_attempts: usize,
}

impl ShmOptions {
    pub const fn new(
        owners: NonZeroUsize,
        recovery_attempts: usize,
    ) -> Self {
        Self { owners, recovery_attempts }
    }
    pub const fn owners(self) -> NonZeroUsize {
        self.owners
    }
    pub const fn recovery_attempts(self) -> usize {
        self.recovery_attempts
    }
}

/// Same admission contract as MemoryQuota. Opens attach to committed state;
/// dropping the last handle releases ownership, never unlinks or resets debt.
#[derive(Clone)]
pub struct ShmQuota {
    inner: Arc<Inner>,
}

struct Inner {
    region: ShmRegion,
    layout: Layout,
    resource: ShmResource,
    geometry: Geometry,
    policy: Policy,
    cas_budget: CasBudget,
    options: ShmOptions,
    algorithm: Gcra,
    seed: [u8; 32],
    clock: Box<dyn Clock>,
    pid: u32,
    owner: usize,
    owner_generation: u64,
    owner_hold: ManuallyDrop<LaneHold>,
    membership: ManuallyDrop<FleetMembership>,
    owner_locks: Vec<PreparedLaneLock>,
    shard_locks: Vec<PreparedLaneLock>,
}

struct Budget {
    cas: WorkBudget,
    recovery: usize,
}

impl Budget {
    fn new(
        cas: CasBudget,
        recovery: usize,
    ) -> Self {
        Self { cas: WorkBudget::new(cas), recovery }
    }
    fn recover(&mut self) -> Result<(), Error> {
        self.recovery = self.recovery.checked_sub(1).ok_or(Error::Busy)?;
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.pid == std::process::id() {
            // All pins borrow Inner. A fork child must not unlock the parent's
            // file description. Reopen after fork; inherited holds last until
            // that child's exec/exit rather than permitting false death proof.
            unsafe {
                ManuallyDrop::drop(&mut self.owner_hold);
                ManuallyDrop::drop(&mut self.membership);
            }
        }
    }
}

impl Inner {
    fn slot(
        &self,
        index: usize,
    ) -> &Slot {
        debug_assert!(index < self.geometry.slots().get());
        // SAFETY: checked layout and index, mmap alignment, mapping owns life.
        unsafe {
            &*self
                .region
                .as_ptr()
                .add(self.layout.slots + index * self.layout.slot_stride)
                .cast::<Slot>()
        }
    }
    fn shard(
        &self,
        index: usize,
    ) -> &Shard {
        debug_assert!(index < self.geometry.shards().get());
        // SAFETY: shards are aligned fixed rows inside the checked mapping.
        unsafe {
            &*self
                .region
                .as_ptr()
                .add(self.layout.shards + index * size_of::<Shard>())
                .cast::<Shard>()
        }
    }
    fn owner(
        &self,
        index: usize,
    ) -> &Owner {
        debug_assert!(index < self.options.owners.get());
        // SAFETY: owner geometry and mapping length were checked at opening.
        unsafe {
            &*self
                .region
                .as_ptr()
                .add(self.layout.owners + index * size_of::<Owner>())
                .cast::<Owner>()
        }
    }
    fn counter(
        &self,
        owner: usize,
        slot: usize,
    ) -> &AtomicU64 {
        debug_assert!(
            owner < self.options.owners.get()
                && slot < self.geometry.slots().get()
        );
        let offset = self.layout.pins
            + (owner * self.geometry.slots().get() + slot)
                * size_of::<AtomicU64>();
        // SAFETY: the complete fixed matrix was checked and initialized once.
        unsafe { &*self.region.as_ptr().add(offset).cast::<AtomicU64>() }
    }
    fn key_ptr(
        &self,
        index: usize,
        bank: usize,
    ) -> *mut u8 {
        // SAFETY: each slot has two fixed key buffers following its header.
        unsafe {
            self.region.as_ptr().add(
                self.layout.slots
                    + index * self.layout.slot_stride
                    + size_of::<Slot>()
                    + bank * self.geometry.key_bytes().get(),
            )
        }
    }
    fn location(
        &self,
        key: Key<'_>,
    ) -> (usize, usize) {
        let mut hash = blake3::Hasher::new_keyed(&self.seed);
        hash.update(b"orbit-quota/key/v1");
        hash.update(&(key.namespace().len() as u64).to_le_bytes());
        hash.update(key.namespace());
        hash.update(&(key.subject().len() as u64).to_le_bytes());
        hash.update(key.subject());
        let digest = hash.finalize();
        let bytes = digest.as_bytes();
        let shard = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize
            % self.geometry.shards().get();
        let start = u64::from_le_bytes(bytes[8..16].try_into().unwrap())
            as usize
            % (self.geometry.slots().get() / self.geometry.shards().get());
        (shard, start)
    }
    fn check_process(&self) -> Result<(), Error> {
        if self.pid != std::process::id() {
            return Err(Error::IncompatibleState);
        }
        if self.owner(self.owner).generation.load(Ordering::Acquire)
            != self.owner_generation
        {
            return Err(Error::CorruptState);
        }
        Ok(())
    }
    fn sample(
        &self,
        shard: usize,
        budget: &mut Budget,
    ) -> Result<u128, Error> {
        let watermark = &self.shard(shard).watermark;
        let mut floor = watermark.load(Ordering::Acquire);
        let now = self.clock.now()?;
        if now < floor {
            return Err(Error::ClockWentBackwards);
        }
        let scaled = self.algorithm.scale(now)?;
        while floor < now {
            budget.cas.take()?;
            match watermark.compare_exchange_weak(
                floor,
                now,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => floor = current,
            }
        }
        Ok(scaled)
    }
    fn lock_shard(
        &self,
        shard: usize,
        budget: &mut Budget,
    ) -> Result<PreparedLaneGuard<'_>, Error> {
        budget.cas.take()?; // PreparedLaneLock makes exactly one local CAS.
        self.shard_locks[shard]
            .try_lock()
            .map_err(Error::Io)?
            .ok_or(Error::Busy)
    }
    fn repair(
        &self,
        index: usize,
        shard: usize,
        budget: &mut Budget,
    ) -> Result<(), Error> {
        budget.recover()?;
        let _hold = self.lock_shard(shard, budget)?;
        self.thaw(index);
        Ok(())
    }
    // Caller holds this shard's kernel lock. A frozen descriptor still names
    // the preceding complete bank. Only an unpublished bank may be partial.
    fn thaw(
        &self,
        index: usize,
    ) {
        let control = &self.slot(index).control;
        let state = control.load(Ordering::SeqCst);
        if state & FROZEN != 0 {
            control.store(state & !FROZEN, Ordering::SeqCst);
        }
    }
    fn pin(
        &self,
        index: usize,
        expected: Option<u64>,
        budget: &mut Budget,
    ) -> Result<Option<Pin<'_>>, Error> {
        let control = self.slot(index).control.load(Ordering::SeqCst);
        if control & FROZEN != 0 {
            return Err(Error::Busy);
        }
        if control & RETIRED != 0 {
            return Ok(None);
        }
        let counter = self.counter(self.owner, index);
        let mut count = counter.load(Ordering::Relaxed);
        loop {
            let next = count.checked_add(1).ok_or(Error::Busy)?;
            budget.cas.take()?;
            match counter.compare_exchange_weak(
                count,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => count = current,
            }
        }
        self.validate_pin(index, control, expected)
    }

    // The caller owns one increment. Failure releases it without reading keys.
    fn validate_pin(
        &self,
        index: usize,
        control: u64,
        expected: Option<u64>,
    ) -> Result<Option<Pin<'_>>, Error> {
        let pin = Pin { inner: self, index, generation: control & GENERATION };
        // SC with freeze + writer's pin scan: either it sees our pin or we
        // see its descriptor. No key byte is accessed before this validation.
        if self.slot(index).control.load(Ordering::SeqCst) != control
            || expected.is_some_and(|generation| generation != pin.generation)
        {
            return Err(Error::Busy); // pin Drop releases the increment.
        }
        #[cfg(test)]
        fault(Fault::Pinned);
        Ok(Some(pin))
    }
    fn clear_dead_pin(
        &self,
        index: usize,
        owner: usize,
        budget: &mut Budget,
    ) -> Result<(), Error> {
        if owner == self.owner {
            return Err(Error::Busy);
        }
        budget.recover()?;
        budget.cas.take()?;
        let _hold = self.owner_locks[owner]
            .try_lock()
            .map_err(Error::Io)?
            .ok_or(Error::Busy)?;
        #[cfg(test)]
        fault(Fault::Recovering);
        // The lifetime hold cannot be acquired while the owner, its pins or
        // a new claimant can operate. Clear ONLY this slot, not the matrix.
        self.counter(owner, index).store(0, Ordering::SeqCst);
        Ok(())
    }
    fn freeze(
        &self,
        index: usize,
        generation: u64,
        budget: &mut Budget,
    ) -> Result<Frozen<'_>, Error> {
        budget.cas.take()?;
        self.slot(index)
            .control
            .compare_exchange(
                generation,
                generation | FROZEN,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map_err(|_| Error::Busy)?;
        let frozen =
            Frozen { inner: self, index, generation, published: false };
        #[cfg(test)]
        fault(Fault::Frozen);
        for owner in 0..self.options.owners.get() {
            if self.counter(owner, index).load(Ordering::SeqCst) != 0 {
                self.clear_dead_pin(index, owner, budget)?;
            }
        }
        Ok(frozen)
    }
    fn consume(
        &self,
        shard: usize,
        pin: Pin<'_>,
        prepared: &Prepared<'_>,
        budget: &mut Budget,
    ) -> Result<Decision, Error> {
        let tat =
            &self.slot(pin.index).banks[(pin.generation & 1) as usize].tat;
        loop {
            let previous = tat.load(Ordering::Acquire);
            let now = self.sample(shard, budget)?;
            match prepared.evaluate(now, Some(previous))? {
                Outcome::Limited { debt } => {
                    drop(pin);
                    return Ok(Decision::Limited {
                        retry_after: self.algorithm.retry_after(debt)?,
                    });
                }
                Outcome::Allowed { tat: next } => {
                    budget.cas.take()?;
                    if tat
                        .compare_exchange_weak(
                            previous,
                            next,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        #[cfg(test)]
                        fault(Fault::TatCommitted);
                        return Ok(Decision::Allowed);
                    }
                }
            }
        }
    }
}

struct Pin<'a> {
    inner: &'a Inner,
    index: usize,
    generation: u64,
}

impl Pin<'_> {
    fn bank(&self) -> &Bank {
        &self.inner.slot(self.index).banks[(self.generation & 1) as usize]
    }
    fn matches(
        &self,
        key: Key<'_>,
    ) -> Result<bool, Error> {
        if self.generation == 0 {
            return Ok(false);
        }
        let bank = self.bank();
        let namespace =
            usize::try_from(bank.namespace_len.load(Ordering::Relaxed))
                .map_err(|_| Error::CorruptState)?;
        let subject = usize::try_from(bank.subject_len.load(Ordering::Relaxed))
            .map_err(|_| Error::CorruptState)?;
        let length =
            namespace.checked_add(subject).ok_or(Error::CorruptState)?;
        if namespace == 0
            || subject == 0
            || length > self.inner.geometry.key_bytes().get()
        {
            return Err(Error::CorruptState);
        }
        if namespace != key.namespace().len() || subject != key.subject().len()
        {
            return Ok(false);
        }
        // SAFETY: this live pin prevents any bank/identity reuse. The checked
        // slice borrows the mapping only for the lifetime of this pin.
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self.inner.key_ptr(self.index, (self.generation & 1) as usize),
                length,
            )
        };
        Ok(&bytes[..namespace] == key.namespace()
            && &bytes[namespace..] == key.subject())
    }
}

impl Drop for Pin<'_> {
    fn drop(&mut self) {
        if self.inner.pid == std::process::id() {
            // Mandatory, unconditional cleanup; not an admission retry.
            self.inner
                .counter(self.inner.owner, self.index)
                .fetch_sub(1, Ordering::SeqCst);
        }
    }
}

struct Frozen<'a> {
    inner: &'a Inner,
    index: usize,
    generation: u64,
    published: bool,
}

impl Frozen<'_> {
    fn publish(
        mut self,
        key: Key<'_>,
        tat: u128,
    ) -> Result<(), Error> {
        let Some(next) =
            self.generation.checked_add(1).filter(|next| *next <= GENERATION)
        else {
            self.inner
                .slot(self.index)
                .control
                .store(self.generation | RETIRED, Ordering::SeqCst);
            self.published = true;
            return Err(Error::CapacityFull);
        };
        let bank_index = (next & 1) as usize;
        let bank = &self.inner.slot(self.index).banks[bank_index];
        // SAFETY: the shard hold plus frozen descriptor exclude writers, and
        // the SC scan excludes all admitted pins. New pins cannot validate
        // this descriptor. The active bank is never touched before commit.
        unsafe {
            let pointer = self.inner.key_ptr(self.index, bank_index);
            std::ptr::copy_nonoverlapping(
                key.namespace().as_ptr(),
                pointer,
                key.namespace().len(),
            );
            #[cfg(test)]
            fault(Fault::KeyCopy);
            std::ptr::copy_nonoverlapping(
                key.subject().as_ptr(),
                pointer.add(key.namespace().len()),
                key.subject().len(),
            );
        }
        bank.namespace_len
            .store(key.namespace().len() as u64, Ordering::Relaxed);
        bank.subject_len.store(key.subject().len() as u64, Ordering::Relaxed);
        #[cfg(test)]
        fault(Fault::Metadata);
        bank.tat.store(tat, Ordering::Relaxed);
        #[cfg(test)]
        fault(Fault::BeforePublish);
        // No fallible work remains. Arm cleanup before the infallible atomic
        // commit so a post-commit unwind can never restore the old revision.
        self.published = true;
        // Single commit: complete identity, generation and initial charge.
        self.inner.slot(self.index).control.store(next, Ordering::SeqCst);
        #[cfg(test)]
        fault(Fault::Published);
        Ok(())
    }
}

impl Drop for Frozen<'_> {
    fn drop(&mut self) {
        if !self.published && self.inner.pid == std::process::id() {
            // Even a panic during the inactive copy preserves the active bank.
            self.inner
                .slot(self.index)
                .control
                .store(self.generation, Ordering::SeqCst);
        }
    }
}

impl ShmQuota {
    pub fn open(
        resource: ShmResource,
        geometry: Geometry,
        policy: Policy,
        cas_budget: CasBudget,
        options: ShmOptions,
    ) -> Result<Self, Error> {
        Self::with_clock(
            resource,
            geometry,
            policy,
            cas_budget,
            options,
            Box::new(Monotonic),
            domain::clock_domain()?,
        )
    }

    fn with_clock(
        resource: ShmResource,
        geometry: Geometry,
        policy: Policy,
        cas_budget: CasBudget,
        options: ShmOptions,
        clock: Box<dyn Clock>,
        domain: [u8; 32],
    ) -> Result<Self, Error> {
        if !AtomicU128::is_lock_free() {
            return Err(Error::UnsupportedAtomics);
        }
        let layout = Layout::new(geometry, options.owners.get())?;
        options
            .owners
            .get()
            .checked_add(geometry.shards().get())
            .ok_or(Error::InvalidGeometry)?;
        let algorithm = Gcra::new(policy)?;
        let now = clock.now()?;
        algorithm
            .prepare(policy.burst())?
            .evaluate(algorithm.scale(now)?, None)?;
        let name = ring_segment_name(resource.fleet.as_str(), resource.kind);
        let membership = join_fleet_membership(resource.fleet.as_str())
            .map_err(Error::Io)?;
        // Opening may serialize initialization; admission never waits on it.
        let (region, initialization) =
            ShmRegion::open_or_create_locked(&name, layout.size)
                .map_err(Error::Io)?;
        let expected =
            layout.spec(geometry, policy, options.owners.get(), domain);
        // SAFETY: mmap covers the checked header and all aligned fixed rows.
        let header = region.as_ptr().cast::<Header>();
        // Borrow atomic fields separately; no shared Header reference spans
        // writes to its non-atomic immutable opening metadata.
        let magic = unsafe { &(*header).magic };
        let state = unsafe { &(*header).state };
        if region.created() {
            magic.store(MAGIC, Ordering::Release);
        } else if magic.load(Ordering::Acquire) != MAGIC {
            return Err(Error::IncompatibleState);
        }
        match state.load(Ordering::Acquire) {
            READY => {
                if unsafe { (*header).spec } != expected {
                    return Err(Error::IncompatibleState);
                }
            }
            0 | INITIALIZING => {
                // No READY publication means no admission has ever been
                // exposed. The initialization hold excludes other openers.
                let mut seed = [0; 32];
                getrandom::fill(&mut seed)
                    .map_err(|_| Error::RandomUnavailable)?;
                state.store(INITIALIZING, Ordering::Release);
                #[cfg(test)]
                fault(Fault::Initializing);
                unsafe {
                    // SAFETY: no opener or operation can use a non-READY table.
                    // Leave the atomic state word intact across interruption.
                    std::ptr::write_bytes(
                        region.as_ptr().add(16),
                        0,
                        layout.size - 16,
                    );
                    std::ptr::copy_nonoverlapping(
                        expected.as_ptr(),
                        std::ptr::addr_of_mut!((*header).spec).cast::<u8>(),
                        expected.len(),
                    );
                    std::ptr::copy_nonoverlapping(
                        seed.as_ptr(),
                        std::ptr::addr_of_mut!((*header).seed).cast::<u8>(),
                        seed.len(),
                    );
                    for shard in 0..geometry.shards().get() {
                        (&*region
                            .as_ptr()
                            .add(layout.shards + shard * size_of::<Shard>())
                            .cast::<Shard>())
                            .watermark
                            .store(now, Ordering::Relaxed);
                    }
                }
                #[cfg(test)]
                fault(Fault::BeforeReady);
                state.store(READY, Ordering::Release);
            }
            _ => return Err(Error::IncompatibleState),
        }
        let seed = unsafe { (*header).seed };
        let mut owner_locks = Vec::new();
        owner_locks
            .try_reserve_exact(options.owners.get())
            .map_err(|_| Error::AllocationFailed)?;
        for owner in 0..options.owners.get() {
            owner_locks
                .push(PreparedLaneLock::open(&name, owner).map_err(Error::Io)?);
        }
        let mut shard_locks = Vec::new();
        shard_locks
            .try_reserve_exact(geometry.shards().get())
            .map_err(|_| Error::AllocationFailed)?;
        for shard in 0..geometry.shards().get() {
            shard_locks.push(
                PreparedLaneLock::open(&name, options.owners.get() + shard)
                    .map_err(Error::Io)?,
            );
        }
        let mut claim = None;
        for owner in 0..options.owners.get() {
            let Some(hold) = try_hold_lane(&name, owner).map_err(Error::Io)?
            else {
                continue;
            };
            // SAFETY: the owner hold excludes the preceding owner and every
            // reclaimer. Full row clearing belongs to opening, not admission.
            let row = unsafe {
                &*region
                    .as_ptr()
                    .add(layout.owners + owner * size_of::<Owner>())
                    .cast::<Owner>()
            };
            let Some(generation) =
                row.generation.load(Ordering::Acquire).checked_add(1)
            else {
                continue;
            };
            for slot in 0..geometry.slots().get() {
                let offset = layout.pins
                    + (owner * geometry.slots().get() + slot)
                        * size_of::<AtomicU64>();
                unsafe {
                    (&*region.as_ptr().add(offset).cast::<AtomicU64>())
                        .store(0, Ordering::SeqCst);
                }
            }
            row.generation.store(generation, Ordering::Release);
            claim = Some((owner, generation, hold));
            break;
        }
        let (owner, owner_generation, owner_hold) =
            claim.ok_or(Error::CapacityFull)?;
        drop(initialization);
        Ok(Self {
            inner: Arc::new(Inner {
                region,
                layout,
                resource,
                geometry,
                policy,
                cas_budget,
                options,
                algorithm,
                seed,
                clock,
                pid: std::process::id(),
                owner,
                owner_generation,
                owner_hold: ManuallyDrop::new(owner_hold),
                membership: ManuallyDrop::new(membership),
                owner_locks,
                shard_locks,
            }),
        })
    }

    pub fn resource(&self) -> ShmResource {
        self.inner.resource
    }
    pub fn cas_budget(&self) -> CasBudget {
        self.inner.cas_budget
    }
    pub fn options(&self) -> ShmOptions {
        self.inner.options
    }
}

impl QuotaStore for ShmQuota {
    fn policy(&self) -> Policy {
        self.inner.policy
    }
    fn geometry(&self) -> Geometry {
        self.inner.geometry
    }

    fn try_consume(
        &self,
        key: Key<'_>,
        cost: NonZeroU64,
    ) -> Result<Decision, Error> {
        let inner = &self.inner;
        inner.check_process()?;
        let length = key
            .namespace()
            .len()
            .checked_add(key.subject().len())
            .ok_or(Error::KeyTooLarge)?;
        if length > inner.geometry.key_bytes().get() {
            return Err(Error::KeyTooLarge);
        }
        let prepared = inner.algorithm.prepare(cost)?;
        let mut budget =
            Budget::new(inner.cas_budget, inner.options.recovery_attempts);
        let (shard, start) = inner.location(key);
        let per_shard =
            inner.geometry.slots().get() / inner.geometry.shards().get();
        for probe in 0..inner.geometry.probes().get() {
            let index = shard * per_shard + (start + probe) % per_shard;
            if inner.slot(index).control.load(Ordering::SeqCst) & FROZEN != 0 {
                inner.repair(index, shard, &mut budget)?;
            }
            if let Some(pin) = inner.pin(index, None, &mut budget)?
                && pin.matches(key)?
            {
                return inner.consume(shard, pin, &prepared, &mut budget);
            }
        }
        let hold = inner.lock_shard(shard, &mut budget)?;
        let now = inner.sample(shard, &mut budget)?;
        let mut candidate = None;
        // Search the complete bounded range before using an earlier free slot.
        for probe in 0..inner.geometry.probes().get() {
            let index = shard * per_shard + (start + probe) % per_shard;
            inner.thaw(index);
            if let Some(pin) = inner.pin(index, None, &mut budget)? {
                if pin.matches(key)? {
                    drop(hold);
                    return inner.consume(shard, pin, &prepared, &mut budget);
                }
                if candidate.is_none()
                    && (pin.generation == 0
                        || pin.bank().tat.load(Ordering::Acquire) <= now)
                {
                    candidate = Some((index, pin.generation));
                }
            }
        }
        let (index, generation) = candidate.ok_or(Error::CapacityFull)?;
        let frozen = inner.freeze(index, generation, &mut budget)?;
        let previous = inner.slot(index).banks[(generation & 1) as usize]
            .tat
            .load(Ordering::Acquire);
        let now = inner.sample(shard, &mut budget)?;
        if generation != 0 && previous > now {
            return Err(Error::Busy);
        }
        let Outcome::Allowed { tat } = prepared.evaluate(now, None)? else {
            return Err(Error::CorruptState);
        };
        frozen.publish(key, tat)?;
        Ok(Decision::Allowed)
    }
}

#[cfg(test)]
#[path = "../../tests/shm.rs"]
mod tests;

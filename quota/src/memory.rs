use std::cell::UnsafeCell;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use portable_atomic::AtomicU128;

use crate::clock::{Clock, Monotonic};
use crate::gcra::{Gcra, Outcome, Prepared};
use crate::{CasBudget, Decision, Error, Geometry, Key, Policy, QuotaStore};

const WRITER: u64 = 1 << 63;
const POISONED: u64 = 1 << 62;
const RETIRED: u64 = 1 << 61;
const READERS: u64 = RETIRED - 1;

/// A bounded process-local store. New stores are independent; clones share
/// identity, debt and the explicit CAS budget. The last handle owns lifetime.
#[derive(Clone)]
pub struct MemoryQuota {
    inner: Arc<Table>,
}

struct Table {
    geometry: Geometry,
    policy: Policy,
    cas_budget: CasBudget,
    algorithm: Gcra,
    hash: RandomState,
    clock: Box<dyn Clock>,
    shards: Vec<Shard>,
}

struct Shard {
    // Only installation/replacement takes this hold. Existing keys never do.
    structure: Mutex<()>,
    watermark: AtomicU128,
    slots: Vec<Slot>,
}

struct Slot {
    gate: AtomicU64,
    identity: UnsafeCell<Identity>,
    tat: AtomicU128,
}

struct Identity {
    generation: u64,
    occupied: bool,
    namespace_len: usize,
    subject_len: usize,
    bytes: Box<[u8]>,
}

// SAFETY: non-atomic identity bytes are read only through a live Pin, and
// written only through an exclusive Writer. The gate cannot enter WRITER
// while any pin exists. Release unpins/publication and Acquire acquisition
// order all identity access. TAT is accessed atomically even while pinned.
unsafe impl Sync for Slot {}

struct WorkBudget {
    remaining: usize,
}

impl WorkBudget {
    fn new(limit: CasBudget) -> Self {
        Self { remaining: limit.attempts().get() }
    }

    fn take(&mut self) -> Result<(), Error> {
        self.remaining = self.remaining.checked_sub(1).ok_or(Error::Busy)?;
        Ok(())
    }
}

struct Pin<'a> {
    slot: &'a Slot,
}

impl Pin<'_> {
    fn identity(&self) -> &Identity {
        // SAFETY: this pin prevents all writers until Drop. The reference
        // cannot outlive the pin, and concurrent pinned readers are read-only.
        unsafe { &*self.slot.identity.get() }
    }

    fn matches(
        &self,
        key: Key<'_>,
    ) -> bool {
        let id = self.identity();
        id.occupied
            && id.namespace_len == key.namespace().len()
            && id.subject_len == key.subject().len()
            && id.bytes[..id.namespace_len] == *key.namespace()
            && id.bytes[id.namespace_len..id.namespace_len + id.subject_len]
                == *key.subject()
    }

    fn commit(
        &self,
        previous: u128,
        next: u128,
        budget: &mut WorkBudget,
    ) -> Result<bool, Error> {
        budget.take()?;
        Ok(self
            .slot
            .tat
            .compare_exchange_weak(
                previous,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok())
    }
}

impl Drop for Pin<'_> {
    fn drop(&mut self) {
        // An unconditional release is mandatory cleanup, not an admission
        // retry. Every successful pin owns exactly one decrement.
        self.slot.gate.fetch_sub(1, Ordering::Release);
    }
}

struct Writer<'a> {
    slot: &'a Slot,
    dirty: bool,
    finished: bool,
}

impl Writer<'_> {
    fn identity(&self) -> &Identity {
        // SAFETY: WRITER excludes both pins and other writers.
        unsafe { &*self.slot.identity.get() }
    }

    fn install(
        mut self,
        generation: u64,
        key: Key<'_>,
        tat: u128,
    ) {
        self.dirty = true;
        // SAFETY: WRITER is exclusive. All lengths were validated before
        // acquisition and backing never changes size after construction.
        let id = unsafe { &mut *self.slot.identity.get() };
        id.bytes[..key.namespace().len()].copy_from_slice(key.namespace());
        let length = key.namespace().len() + key.subject().len();
        id.bytes[key.namespace().len()..length].copy_from_slice(key.subject());
        id.namespace_len = key.namespace().len();
        id.subject_len = key.subject().len();
        id.generation = generation;
        id.occupied = true;
        self.slot.tat.store(tat, Ordering::Relaxed);
        self.publish();
    }

    fn publish(mut self) {
        self.finished = true;
        // Publication commits complete identity and first charge together.
        self.slot.gate.store(0, Ordering::Release);
    }

    fn retire(mut self) {
        self.finished = true;
        self.slot.gate.store(RETIRED, Ordering::Release);
    }
}

impl Drop for Writer<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Errors before mutation cancel cleanly; panic after partial
            // mutation can never republish an incomplete identity.
            self.slot.gate.store(
                if self.dirty { POISONED } else { 0 },
                Ordering::Release,
            );
        }
    }
}

impl Slot {
    fn pin(
        &self,
        expected: Option<u64>,
        budget: &mut WorkBudget,
    ) -> Result<Option<Pin<'_>>, Error> {
        let mut gate = self.gate.load(Ordering::Relaxed);
        loop {
            if gate & POISONED != 0 {
                return Err(Error::CorruptState);
            }
            if gate & RETIRED != 0 {
                return Ok(None);
            }
            if gate & WRITER != 0 || gate == READERS {
                return Err(Error::Busy);
            }
            budget.take()?;
            match self.gate.compare_exchange_weak(
                gate,
                gate + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let pin = Pin { slot: self };
                    // Validate only after acquisition: gate ABA is harmless
                    // because identity cannot change while this pin is held.
                    if expected.is_some_and(|generation| {
                        generation != pin.identity().generation
                    }) {
                        return Err(Error::Busy);
                    }
                    return Ok(Some(pin));
                }
                Err(current) => gate = current,
            }
        }
    }

    fn reserve(
        &self,
        budget: &mut WorkBudget,
    ) -> Result<Writer<'_>, Error> {
        budget.take()?;
        match self.gate.compare_exchange(
            0,
            WRITER,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(Writer { slot: self, dirty: false, finished: false }),
            Err(gate) if gate & POISONED != 0 => Err(Error::CorruptState),
            Err(gate) if gate & RETIRED != 0 => Err(Error::CapacityFull),
            Err(_) => Err(Error::Busy),
        }
    }
}

#[derive(Clone, Copy)]
struct Candidate {
    index: usize,
    generation: u64,
}

impl MemoryQuota {
    /// Allocate fixed backing and require native lock-free u128 operations.
    /// There is no implicit CAS limit or lock-emulated fast path.
    pub fn new(
        geometry: Geometry,
        policy: Policy,
        cas_budget: CasBudget,
    ) -> Result<Self, Error> {
        Self::with_clock(geometry, policy, cas_budget, Box::new(Monotonic))
    }

    fn with_clock(
        geometry: Geometry,
        policy: Policy,
        cas_budget: CasBudget,
        clock: Box<dyn Clock>,
    ) -> Result<Self, Error> {
        if !AtomicU128::is_lock_free() {
            return Err(Error::UnsupportedAtomics);
        }
        let algorithm = Gcra::new(policy)?;
        let now = clock.now()?;
        algorithm
            .prepare(policy.burst())?
            .evaluate(algorithm.scale(now)?, None)?;
        geometry
            .slots()
            .get()
            .checked_mul(geometry.key_bytes().get())
            .ok_or(Error::InvalidGeometry)?;
        let per_shard = geometry.slots().get() / geometry.shards().get();
        let mut shards = Vec::new();
        shards
            .try_reserve_exact(geometry.shards().get())
            .map_err(|_| Error::AllocationFailed)?;
        for _ in 0..geometry.shards().get() {
            let mut slots = Vec::new();
            slots
                .try_reserve_exact(per_shard)
                .map_err(|_| Error::AllocationFailed)?;
            for _ in 0..per_shard {
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(geometry.key_bytes().get())
                    .map_err(|_| Error::AllocationFailed)?;
                bytes.resize(geometry.key_bytes().get(), 0);
                slots.push(Slot {
                    gate: AtomicU64::new(0),
                    identity: UnsafeCell::new(Identity {
                        generation: 0,
                        occupied: false,
                        namespace_len: 0,
                        subject_len: 0,
                        bytes: bytes.into_boxed_slice(),
                    }),
                    tat: AtomicU128::new(0),
                });
            }
            let structure = Mutex::new(());
            drop(structure.try_lock().map_err(|_| Error::CorruptState)?);
            shards.push(Shard {
                structure,
                watermark: AtomicU128::new(now),
                slots,
            });
        }
        Ok(Self {
            inner: Arc::new(Table {
                geometry,
                policy,
                cas_budget,
                algorithm,
                hash: RandomState::new(),
                clock,
                shards,
            }),
        })
    }

    pub fn cas_budget(&self) -> CasBudget {
        self.inner.cas_budget
    }

    fn location(
        &self,
        key: Key<'_>,
    ) -> (usize, usize) {
        let shard =
            self.inner.hash.hash_one((0_u8, key.namespace(), key.subject()));
        let start =
            self.inner.hash.hash_one((1_u8, key.namespace(), key.subject()));
        let per_shard = self.inner.geometry.slots().get()
            / self.inner.geometry.shards().get();
        (
            (shard % self.inner.geometry.shards().get() as u64) as usize,
            (start % per_shard as u64) as usize,
        )
    }

    fn sample(
        &self,
        shard: &Shard,
        budget: &mut WorkBudget,
    ) -> Result<u128, Error> {
        // Observe the floor BEFORE sampling. A concurrent newer publication
        // after this load must not turn a legitimate old sample into rollback.
        let mut floor = shard.watermark.load(Ordering::Acquire);
        let now = self.inner.clock.now()?;
        if now < floor {
            return Err(Error::ClockWentBackwards);
        }
        let scaled = self.inner.algorithm.scale(now)?;
        while floor < now {
            budget.take()?;
            match shard.watermark.compare_exchange_weak(
                floor,
                now,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => floor = current,
            }
        }
        Ok(scaled) // Never clamp the sample forward to a peer's newer time.
    }

    fn consume(
        &self,
        shard: &Shard,
        pin: Pin<'_>,
        prepared: &Prepared<'_>,
        budget: &mut WorkBudget,
    ) -> Result<Decision, Error> {
        loop {
            let previous = pin.slot.tat.load(Ordering::Acquire);
            // Each CAS retry reads state before taking a fresh clock sample.
            let now = self.sample(shard, budget)?;
            match prepared.evaluate(now, Some(previous))? {
                Outcome::Limited { debt } => {
                    drop(pin);
                    return Ok(Decision::Limited {
                        retry_after: self.inner.algorithm.retry_after(debt)?,
                    });
                }
                Outcome::Allowed { tat } => {
                    if pin.commit(previous, tat, budget)? {
                        return Ok(Decision::Allowed);
                    }
                }
            }
        }
    }

    // Caller holds shard.structure. This is the sole identity mutation path.
    fn install(
        &self,
        shard: &Shard,
        candidate: Candidate,
        key: Key<'_>,
        prepared: &Prepared<'_>,
        budget: &mut WorkBudget,
    ) -> Result<Decision, Error> {
        let writer = shard.slots[candidate.index].reserve(budget)?;
        if writer.identity().generation != candidate.generation {
            return Err(Error::Busy);
        }
        let previous = writer.slot.tat.load(Ordering::Acquire);
        let now = self.sample(shard, budget)?;
        if writer.identity().occupied && previous > now {
            return Err(Error::Busy);
        }
        let Some(generation) = writer.identity().generation.checked_add(1)
        else {
            writer.retire();
            return Err(Error::CapacityFull);
        };
        let Outcome::Allowed { tat } = prepared.evaluate(now, None)? else {
            return Err(Error::CorruptState);
        };
        writer.install(generation, key, tat);
        Ok(Decision::Allowed)
    }
}

impl QuotaStore for MemoryQuota {
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
        let table = &self.inner;
        let length = key
            .namespace()
            .len()
            .checked_add(key.subject().len())
            .ok_or(Error::KeyTooLarge)?;
        if length > table.geometry.key_bytes().get() {
            return Err(Error::KeyTooLarge);
        }
        let prepared = table.algorithm.prepare(cost)?;
        let mut budget = WorkBudget::new(table.cas_budget);
        let (index, start) = self.location(key);
        let shard = &table.shards[index];
        if shard.structure.is_poisoned() {
            return Err(Error::CorruptState);
        }
        let mut index = start;
        for _ in 0..table.geometry.probes().get() {
            if let Some(pin) = shard.slots[index].pin(None, &mut budget)?
                && pin.matches(key)
            {
                return self.consume(shard, pin, &prepared, &mut budget);
            }
            index = if index + 1 == shard.slots.len() { 0 } else { index + 1 };
        }
        let structure = match shard.structure.try_lock() {
            Ok(hold) => hold,
            Err(TryLockError::WouldBlock) => return Err(Error::Busy),
            Err(TryLockError::Poisoned(_)) => return Err(Error::CorruptState),
        };
        // Identity installation is serialized. Re-scan every configured probe
        // before replacement; an earlier empty/expired slot cannot hide a key.
        let now = self.sample(shard, &mut budget)?;
        let mut candidate = None;
        let mut index = start;
        for _ in 0..table.geometry.probes().get() {
            if let Some(pin) = shard.slots[index].pin(None, &mut budget)? {
                if pin.matches(key) {
                    drop(structure);
                    return self.consume(shard, pin, &prepared, &mut budget);
                }
                let id = pin.identity();
                if candidate.is_none()
                    && (!id.occupied
                        || pin.slot.tat.load(Ordering::Acquire) <= now)
                {
                    candidate =
                        Some(Candidate { index, generation: id.generation });
                }
            }
            index = if index + 1 == shard.slots.len() { 0 } else { index + 1 };
        }
        self.install(
            shard,
            candidate.ok_or(Error::CapacityFull)?,
            key,
            &prepared,
            &mut budget,
        )
    }
}

#[cfg(test)]
#[path = "../tests/memory.rs"]
mod tests;

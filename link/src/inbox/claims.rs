//! Producer-owned claims. Kernel holds are acquired only by segment lifecycle
//! code. Admission and helping use bounded atomic operations on this table.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};

use super::{Error, Inbox, Result};

pub(super) const FREE: u64 = 0;
pub(super) const RESERVED: u64 = 1;
pub(super) const COMMITTED: u64 = 2;
pub(super) const CANCELLED: u64 = 3;
pub(super) const WRITING: u32 = 1;
pub(super) const REFERENCED: u32 = 2;

#[cfg(test)]
pub(super) fn checkpoint(phase: &str) {
    if std::env::var("ORBIT_LINK_CRASH_PHASE").as_deref() == Ok(phase) {
        // The isolated child owns this process; no atexit/Drop may run.
        #[cfg(unix)]
        unsafe {
            libc::raise(libc::SIGKILL);
        }
        std::process::abort();
    }
}

#[repr(C, align(64))]
struct Owner {
    generation: AtomicU64,
    pid: AtomicU32,
    active: AtomicU32,
    _padding: [u8; 48],
}

#[repr(C, align(64))]
struct Record {
    seq: AtomicU64,
    position: AtomicU64,
    incarnation: AtomicU64,
    generation: AtomicU64,
    lane: AtomicU32,
    flags: AtomicU32,
    _padding: [u8; 24],
}

const _: () = assert!(size_of::<Owner>() == 64);
const _: () = assert!(size_of::<Record>() == 64);

pub(super) const fn registry_bytes(
    lanes: usize,
    capacity: usize,
) -> usize {
    // A lane holder and a lane-less publisher each have a registration budget.
    2 * lanes * (size_of::<Owner>() + capacity * size_of::<Record>())
}

pub(super) struct Producer {
    index: usize,
    generation: u64,
    // Dropped with the Inbox, after all borrowed write operations have ended.
    #[cfg(unix)]
    _hold: orbit_core::shm::LaneHold,
}

#[derive(Clone, Copy)]
pub(super) struct Intent {
    pub(super) position: u64,
    pub(super) incarnation: u64,
    generation: u64,
    pub(super) lane: usize,
}

impl Record {
    fn snapshot(&self) -> Option<Intent> {
        let before = self.seq.load(Ordering::Acquire);
        if before & 1 != 0 {
            return None;
        }
        let intent = Intent {
            position: self.position.load(Ordering::Relaxed),
            incarnation: self.incarnation.load(Ordering::Relaxed),
            generation: self.generation.load(Ordering::Relaxed),
            lane: self.lane.load(Ordering::Relaxed) as usize,
        };
        fence(Ordering::Acquire);
        (before == self.seq.load(Ordering::Relaxed)).then_some(intent)
    }
}

pub(super) struct Reservation<'a> {
    inbox: &'a Inbox,
    record: usize,
    pub(super) position: u64,
    lane: usize,
    pub(super) committed: bool,
}

impl Reservation<'_> {
    pub(super) fn claim(
        &self,
        state: u64,
    ) -> u64 {
        ((self.record as u64 + 1) << 2) | state
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // A valid reservation is never rolled back into FREE. Unwinding
            // cancels it; the reader will skip its admitted position.
            let slot = self.inbox.slot_header(self.lane, self.position);
            let _ = slot.claim.compare_exchange(
                self.claim(RESERVED),
                self.claim(CANCELLED),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            self.inbox.wake(self.lane, self.inbox.lane_header(self.lane));
        }
        self.inbox
            .record(self.record)
            .flags
            .fetch_and(!WRITING, Ordering::Release);
    }
}

impl Inbox {
    fn registry_base(&self) -> *mut u8 {
        unsafe {
            self.base.add(
                self.geometry.segment_bytes(self.lanes)
                    - registry_bytes(self.lanes, self.geometry.capacity),
            )
        }
    }

    fn producer_owner(
        &self,
        index: usize,
    ) -> &Owner {
        unsafe { &*self.registry_base().cast::<Owner>().add(index) }
    }

    fn record(
        &self,
        index: usize,
    ) -> &Record {
        unsafe {
            &*self
                .registry_base()
                .add(2 * self.lanes * size_of::<Owner>())
                .cast::<Record>()
                .add(index)
        }
    }

    fn record_index(
        &self,
        claim: u64,
    ) -> Result<usize> {
        let index = (claim >> 2)
            .checked_sub(1)
            .ok_or(Error::Malformed("inbox claim has no producer"))?
            as usize;
        if index >= 2 * self.lanes * self.geometry.capacity {
            return Err(Error::Malformed(
                "inbox claim producer is outside registry",
            ));
        }
        Ok(index)
    }

    pub(super) fn intent_for_claim(
        &self,
        claim: u64,
    ) -> Result<Intent> {
        self.record(self.record_index(claim)?)
            .snapshot()
            .ok_or(Error::Malformed("referenced inbox intent is unstable"))
    }

    /// Bind a mapping to one producer registration under its cold-path hold.
    #[cfg(unix)]
    pub(crate) fn register_producer(
        &mut self,
        mut hold: impl FnMut(usize) -> Result<Option<orbit_core::shm::LaneHold>>,
    ) -> Result<()> {
        for index in 0..2 * self.lanes {
            let Some(held) = hold(index)? else {
                continue;
            };
            self.recover_owner(index)?;
            if !self.owner_idle(index) {
                continue;
            }
            let owner = self.producer_owner(index);
            let generation =
                owner.generation.load(Ordering::Acquire).checked_add(1).ok_or(
                    Error::Malformed("inbox producer generation exhausted"),
                )?;
            owner.generation.store(generation, Ordering::Relaxed);
            owner.pid.store(std::process::id(), Ordering::Relaxed);
            owner.active.store(1, Ordering::Release);
            self.producer = Some(Producer { index, generation, _hold: held });
            return Ok(());
        }
        Err(Error::InboxFull)
    }

    fn owner_idle(
        &self,
        owner: usize,
    ) -> bool {
        let start = owner * self.geometry.capacity;
        (start..start + self.geometry.capacity)
            .all(|index| self.record(index).flags.load(Ordering::Acquire) == 0)
    }

    /// Cold path only. The caller holds this exact registration's kernel
    /// ownership guard continuously through recovery and any following reuse.
    pub(crate) fn recover_owner(
        &self,
        owner_index: usize,
    ) -> Result<()> {
        let owner = self.producer_owner(owner_index);
        if owner.active.fetch_or(0, Ordering::AcqRel) == 0 {
            return Ok(());
        }
        let generation = owner.generation.load(Ordering::Acquire);
        let start = owner_index * self.geometry.capacity;
        for index in start..start + self.geometry.capacity {
            let record = self.record(index);
            // A no-op RMW observes the current modification, not an old zero
            // from before the dead writer published its intent. Cold path.
            let flags = record.flags.fetch_or(0, Ordering::AcqRel);
            if flags == 0 {
                continue;
            }
            if flags & REFERENCED == 0 {
                record.flags.store(0, Ordering::Release);
                continue;
            }
            let Some(intent) = record.snapshot() else {
                return Err(Error::Malformed(
                    "dead referenced producer intent is unstable",
                ));
            };
            if intent.generation != generation || intent.lane >= self.lanes {
                return Err(Error::Malformed(
                    "dead producer intent identity mismatch",
                ));
            }
            let slot = self.slot_header(intent.lane, intent.position);
            let reserved = ((index as u64 + 1) << 2) | RESERVED;
            let turn = slot.seq.load(Ordering::Acquire);
            let claim = slot.claim.fetch_or(0, Ordering::AcqRel);
            if claim >> 2 != reserved >> 2 {
                // FREE is the reference-release boundary. Consumers never
                // mutate this producer record after publishing FREE.
                record.flags.store(0, Ordering::Release);
                continue;
            }
            if turn == intent.position {
                let _ = slot.claim.compare_exchange(
                    reserved,
                    reserved ^ RESERVED ^ CANCELLED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                #[cfg(test)]
                checkpoint("recovery-cancelled");
                self.help_claim(intent.lane, intent.position)?;
                #[cfg(test)]
                checkpoint("recovery-admitted");
                // Also repeat the wake for COMMITTED-before-wake crashes. Do
                // not retire this registration while retained claims remain.
                self.wake(intent.lane, self.lane_header(intent.lane));
                #[cfg(test)]
                checkpoint("recovery-woken");
            } else if claim == reserved {
                // Death after a tentative wrong-turn claim but before its
                // validation/rollback. No helper could have admitted it.
                if slot
                    .claim
                    .compare_exchange(
                        reserved,
                        FREE,
                        Ordering::Release,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    record.flags.store(0, Ordering::Release);
                    continue;
                }
            }
            record.flags.fetch_and(!WRITING, Ordering::Release);
        }
        if self.owner_idle(owner_index) {
            owner.active.store(0, Ordering::Release);
        }
        Ok(())
    }

    #[cfg(unix)]
    pub(crate) fn recover_producers(
        &self,
        pid: Option<u32>,
        mut hold: impl FnMut(usize) -> Result<Option<orbit_core::shm::LaneHold>>,
    ) -> Result<()> {
        for index in 0..2 * self.lanes {
            let owner = self.producer_owner(index);
            if owner.active.fetch_or(0, Ordering::AcqRel) == 0
                || pid
                    .is_some_and(|pid| owner.pid.load(Ordering::Acquire) != pid)
            {
                continue;
            }
            let generation = owner.generation.load(Ordering::Acquire);
            let Some(_guard) = hold(index)? else {
                continue;
            };
            // Revalidate after taking the guard, not before and unlocked.
            if owner.generation.load(Ordering::Acquire) == generation {
                self.recover_owner(index)?;
            }
        }
        Ok(())
    }

    pub(super) fn reserve_frame(
        &self,
        lane_index: usize,
    ) -> Result<Reservation<'_>> {
        // A claim race is not capacity exhaustion. Help the winner and try
        // the next turn, at most one lane-capacity worth of attempts. This
        // bounds our own work even if other producers continuously advance;
        // it never waits for a live writer to publish its payload.
        for _ in 0..self.geometry.capacity {
            match self.try_reserve_frame(lane_index) {
                Ok(Some(reservation)) => return Ok(reservation),
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::InboxFull)
    }

    fn try_reserve_frame(
        &self,
        lane_index: usize,
    ) -> Result<Option<Reservation<'_>>> {
        let lane = self.lane_header(lane_index);
        if lane.consumers.load(Ordering::Acquire) == 0 {
            return Err(Error::InboxFull);
        }
        let read = lane.read.load(Ordering::Acquire);
        let position = lane.reserve.load(Ordering::Acquire);
        let capacity = self.geometry.capacity as u64;
        if position.wrapping_sub(read) >= capacity {
            return Err(Error::InboxFull);
        }
        position
            .checked_add(capacity)
            .ok_or(Error::Malformed("inbox position exhausted"))?;
        let slot = self.slot_header(lane_index, position);
        if slot.claim.load(Ordering::Acquire) != FREE {
            self.help_claim(lane_index, position)?;
            return self.retry_after_progress(lane, position);
        }
        let (owner_index, generation) = self
            .producer
            .as_ref()
            .map(|p| (p.index, p.generation))
            .unwrap_or((0, 0));
        let start = owner_index * self.geometry.capacity;
        let Some(index) = (start..start + self.geometry.capacity)
            .find(|index| self.take_record(*index))
        else {
            return Err(Error::InboxFull);
        };
        let record = self.record(index);
        let before = record.seq.load(Ordering::Relaxed);
        let odd = before | 1;
        let Some(even) = odd.checked_add(1) else {
            record.flags.store(0, Ordering::Release);
            return Err(Error::Malformed("inbox intent sequence exhausted"));
        };
        record.seq.store(odd, Ordering::Relaxed);
        #[cfg(test)]
        checkpoint("intent-odd");
        fence(Ordering::Release);
        record.position.store(position, Ordering::Relaxed);
        record
            .incarnation
            .store(lane.epoch.load(Ordering::Acquire), Ordering::Relaxed);
        record.generation.store(generation, Ordering::Relaxed);
        record.lane.store(lane_index as u32, Ordering::Relaxed);
        record.seq.store(even, Ordering::Release);
        #[cfg(test)]
        checkpoint("intent-published");
        record.flags.store(WRITING | REFERENCED, Ordering::Release);
        #[cfg(test)]
        checkpoint("before-claim");
        let reserved = ((index as u64 + 1) << 2) | RESERVED;
        if slot
            .claim
            .compare_exchange(
                FREE,
                reserved,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            record.flags.store(0, Ordering::Release);
            self.help_claim(lane_index, position)?;
            return self.retry_after_progress(lane, position);
        }
        if slot.seq.load(Ordering::Acquire) != position {
            // Tentative claim for the wrong full turn: it was never valid.
            let _ = slot.claim.compare_exchange(
                reserved,
                FREE,
                Ordering::Release,
                Ordering::Relaxed,
            );
            record.flags.store(0, Ordering::Release);
            return self.retry_after_progress(lane, position);
        }
        #[cfg(test)]
        checkpoint("claimed");
        let _ = lane.reserve.compare_exchange(
            position,
            position + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        #[cfg(test)]
        checkpoint("admitted");
        Ok(Some(Reservation {
            inbox: self,
            record: index,
            position,
            lane: lane_index,
            committed: false,
        }))
    }

    fn retry_after_progress(
        &self,
        lane: &super::LaneHeader,
        position: u64,
    ) -> Result<Option<Reservation<'_>>> {
        // A failed attempt may follow an advanced cursor, never poll the
        // same live claim waiting for its owner to make progress.
        if lane.reserve.load(Ordering::Acquire) > position {
            Ok(None)
        } else {
            Err(Error::InboxFull)
        }
    }

    fn take_record(
        &self,
        index: usize,
    ) -> bool {
        let record = self.record(index);
        let flags = record.flags.load(Ordering::Acquire);
        if flags & WRITING != 0
            || record
                .flags
                .compare_exchange(
                    flags,
                    flags | WRITING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return false;
        }
        if flags & REFERENCED != 0 {
            // Acquiring the previous writer's Release prevents observing a
            // FREE from before that writer's claim. The physical slot releases
            // the reference; no cross-owner reader RMW touches this record.
            let released = record.snapshot().is_some_and(|intent| {
                intent.lane < self.lanes
                    && self
                        .slot_header(intent.lane, intent.position)
                        .claim
                        .load(Ordering::Acquire)
                        >> 2
                        != index as u64 + 1
            });
            if !released {
                record.flags.fetch_and(!WRITING, Ordering::Release);
                return false;
            }
            record.flags.store(WRITING, Ordering::Relaxed);
        }
        true
    }

    fn help_claim(
        &self,
        lane_index: usize,
        position: u64,
    ) -> Result<()> {
        let slot = self.slot_header(lane_index, position);
        let claim = slot.claim.load(Ordering::Acquire);
        if !matches!(claim & 3, RESERVED | CANCELLED | COMMITTED) {
            return Ok(());
        }
        let index = self.record_index(claim)?;
        let Some(intent) = self.record(index).snapshot() else {
            return Ok(());
        };
        self.help_snapshot(lane_index, position, claim, intent)
    }

    fn help_snapshot(
        &self,
        lane_index: usize,
        position: u64,
        claim: u64,
        intent: Intent,
    ) -> Result<()> {
        let index = self.record_index(claim)?;
        let slot = self.slot_header(lane_index, position);
        let turn = slot.seq.load(Ordering::Acquire);
        if intent.position != position
            || turn != position
            || intent.lane != lane_index
            || (self.producer.is_some()
                && self
                    .producer_owner(index / self.geometry.capacity)
                    .generation
                    .load(Ordering::Acquire)
                    != intent.generation)
            || slot.claim.load(Ordering::Acquire) != claim
        {
            return Ok(());
        }
        // In particular CANCELLED never admits the next turn merely because
        // its reader has published a new turn before clearing the old claim.
        let _ = self.lane_header(lane_index).reserve.compare_exchange(
            position,
            position + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        Ok(())
    }

    pub(super) fn consume_frame(
        &self,
        lane_index: usize,
        position: u64,
        claim: u64,
    ) -> Result<()> {
        let next = position
            .checked_add(self.geometry.capacity as u64)
            .ok_or(Error::Malformed("inbox position exhausted"))?;
        self.record_index(claim)?;
        let slot = self.slot_header(lane_index, position);
        slot.seq.store(next, Ordering::Release);
        #[cfg(test)]
        checkpoint("reader-turn");
        slot.claim.store(FREE, Ordering::Release);
        #[cfg(test)]
        checkpoint("reader-free");
        self.lane_header(lane_index)
            .read
            .store(position + 1, Ordering::Release);
        #[cfg(test)]
        checkpoint("reader-head");
        Ok(())
    }
}

#[cfg(all(test, unix))]
#[path = "../../tests/unit/claims.rs"]
mod tests;

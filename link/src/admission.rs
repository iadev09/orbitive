//! Receiver-owned exchange capacity. Tickets fence one reservation, not a timer.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::{Error, Inbox, Result};

// A claim fits in one atomic: two state bits, a full u16 member index, and
// the remaining bits for a non-wrapping reuse generation. These are wire
// widths, not configurable limits on the number of active exchanges.
const STATE_BITS: u32 = 2;
const OWNER_BITS: u32 = 16;
const GENERATION_SHIFT: u32 = STATE_BITS + OWNER_BITS;
const GENERATION_MAX: u64 = u64::MAX >> GENERATION_SHIFT;
const FREE: u64 = 0;
const RESERVED: u64 = 1;
const ACTIVE: u64 = 2;

/// One exact reservation in a receiver's current lifetime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkTicket {
    pub lane: u16,
    pub epoch: u64,
    pub slot: u32,
    pub claim: u64,
}

impl WorkTicket {
    pub fn origin(self) -> usize {
        ((self.claim >> STATE_BITS) & u64::from(u16::MAX)) as usize
    }
}

/// Per-lane work admission and capacity notifications.
#[repr(C, align(64))]
pub(crate) struct Capacity {
    // Low word: current bound. High word: slots that may still hold work
    // admitted under an earlier, larger bound. Updated together.
    policy: AtomicU64,
    changes: AtomicU64,
    cursor: AtomicU32,
    /// Slots held, never more than are: it rises after a claim and falls
    /// before a release, so a crash between the two leaves it low, never
    /// high. It may say "full" only when every slot is taken; a low count
    /// costs a scan, not admission.
    occupied: AtomicU32,
    /// How many of the window's slots the receiver runs at once; the rest
    /// wait in its own queue, accepted when one of these ends. Zero: all of
    /// them run, the window is the execution bound.
    execution: AtomicU32,
    /// At least one publisher may be waiting for this lane to consume a frame.
    inbox_space_waiting: AtomicU32,
    inbox_space_epoch: AtomicU64,
    _padding: [u8; 24],
}

const _: () = assert!(size_of::<Capacity>() == 64);

pub(crate) const fn bytes(
    lanes: usize,
    slots: usize,
) -> usize {
    lanes * (size_of::<Capacity>() + slots * size_of::<AtomicU64>())
        + lanes * lanes.div_ceil(64) * size_of::<AtomicU64>()
}

impl Inbox {
    fn capacity_base(&self) -> *mut u8 {
        unsafe {
            self.base.add(
                self.geometry.segment_bytes(self.lanes)
                    - bytes(self.lanes, self.geometry.capacity),
            )
        }
    }

    fn capacity_row(
        &self,
        lane: usize,
    ) -> &Capacity {
        unsafe { &*self.capacity_base().cast::<Capacity>().add(lane) }
    }

    fn work_slot(
        &self,
        lane: usize,
        slot: usize,
    ) -> &AtomicU64 {
        unsafe {
            &*self
                .capacity_base()
                .add(self.lanes * size_of::<Capacity>())
                .cast::<AtomicU64>()
                .add(lane * self.geometry.capacity + slot)
        }
    }

    fn capacity_interest(
        &self,
        lane: usize,
        word: usize,
    ) -> &AtomicU64 {
        unsafe {
            &*self
                .capacity_base()
                .add(
                    self.lanes
                        * (size_of::<Capacity>()
                            + self.geometry.capacity * size_of::<AtomicU64>()),
                )
                .cast::<AtomicU64>()
                .add(lane * self.lanes.div_ceil(64) + word)
        }
    }

    /// The receiver's declared bound. None adds no execution ceiling.
    pub fn work_capacity(
        &self,
        lane: usize,
    ) -> Result<Option<usize>> {
        self.check_lane(lane)?;
        let limit =
            self.capacity_row(lane).policy.load(Ordering::SeqCst) as u32;
        Ok((limit != 0).then_some(limit as usize))
    }

    pub(crate) fn configure_capacity(
        &self,
        lane: usize,
        limit: Option<usize>,
    ) -> Result<()> {
        self.validate_work_capacity(limit)?;
        let limit = limit.unwrap_or(0) as u64;
        let row = self.capacity_row(lane);
        // A lane taken back from a dead holder may carry its counts. Zero
        // keeps the held count at or below the truth.
        row.occupied.store(0, Ordering::SeqCst);
        row.execution.store(0, Ordering::SeqCst);
        row.policy.store(limit | (limit << 32), Ordering::SeqCst);
        Ok(())
    }

    /// Declare how many of this receiver's window slots run at once; the
    /// others are a queue the receiver drains itself. Senders read it to
    /// prefer a receiver that will start their exchange now. The caller owns
    /// the receiver lane.
    pub fn set_work_execution(
        &self,
        lane: usize,
        execution: usize,
    ) -> Result<()> {
        self.check_lane(lane)?;
        let window = self.work_capacity(lane)?.ok_or(Error::Malformed(
            "an unbounded receiver has no window to run part of",
        ))?;
        if execution == 0 || execution > window {
            return Err(Error::Malformed(
                "work execution must be positive and within the window",
            ));
        }
        let execution = if execution == window { 0 } else { execution };
        self.capacity_row(lane)
            .execution
            .store(execution as u32, Ordering::SeqCst);
        Ok(())
    }

    /// How many exchanges this receiver runs at once: its declared
    /// execution, else its window. None for an unbounded receiver.
    pub fn work_execution(
        &self,
        lane: usize,
    ) -> Result<Option<usize>> {
        let Some(window) = self.work_capacity(lane)? else {
            return Ok(None);
        };
        let execution =
            self.capacity_row(lane).execution.load(Ordering::SeqCst) as usize;
        Ok(Some(if execution == 0 { window } else { execution.min(window) }))
    }

    /// Window slots held now: reserved, queued and running exchanges. Never
    /// above the truth; a crash can leave it low.
    pub fn work_load(
        &self,
        lane: usize,
    ) -> Result<usize> {
        self.check_lane(lane)?;
        Ok(self.capacity_row(lane).occupied.load(Ordering::SeqCst) as usize)
    }

    /// Change a joined receiver's positive bound without ending any work.
    /// The caller must own the receiver lane. Switching between tracked and
    /// untracked admission requires a new join: old untracked work has no ticket.
    pub(crate) fn update_work_capacity(
        &self,
        lane: usize,
        limit: usize,
    ) -> Result<()> {
        self.check_lane(lane)?;
        self.validate_work_capacity(Some(limit))?;
        if self.consumers(lane)? == 0 {
            return Err(Error::NoConsumer);
        }
        let row = self.capacity_row(lane);
        let mut before = row.policy.load(Ordering::SeqCst);
        loop {
            let previous = before as u32 as usize;
            if previous == 0 {
                return Err(Error::Malformed(
                    "unbounded Link admission requires rejoin before setting a bound",
                ));
            }
            if previous == limit {
                return Ok(());
            }
            let window = ((before >> 32) as usize).max(limit);
            let after = limit as u64 | ((window as u64) << 32);
            match row.policy.compare_exchange(
                before,
                after,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(current) => before = current,
            }
        }
        self.capacity_changed(lane);
        Ok(())
    }

    fn admission_policy(
        &self,
        lane: usize,
    ) -> Result<u64> {
        let row = self.capacity_row(lane);
        let mut policy = row.policy.load(Ordering::SeqCst);
        loop {
            let limit = policy as u32 as usize;
            let window = (policy >> 32) as usize;
            if limit == 0 || window == limit {
                return Ok(policy);
            }
            // Claims above the new boundary retain their full lifetime. Do
            // not refill holes below it until those retiring claims drain.
            if (limit..window).any(|index| {
                self.work_slot(lane, index).load(Ordering::SeqCst) & 3 != FREE
            }) {
                return Err(Error::CapacityFull);
            }
            let trimmed = limit as u64 | ((limit as u64) << 32);
            match row.policy.compare_exchange(
                policy,
                trimmed,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(trimmed),
                Err(current) => policy = current,
            }
        }
    }

    pub(crate) fn validate_work_capacity(
        &self,
        limit: Option<usize>,
    ) -> Result<()> {
        if limit.is_some_and(|n| n == 0 || n > self.geometry.capacity) {
            return Err(Error::Malformed(
                "work capacity must fit the inbox's admission slots",
            ));
        }
        Ok(())
    }

    /// Reserve before acquiring a body slot or publishing work. The caller
    /// must retain its joined lane until its outstanding operations drain.
    pub(crate) fn reserve_work(
        &self,
        lane: usize,
        origin: usize,
    ) -> Result<Option<WorkTicket>> {
        self.check_lane(lane)?;
        self.check_lane(origin)?;
        if self.consumers(lane)? == 0 || self.consumers(origin)? == 0 {
            return Err(Error::NoConsumer);
        }
        let epoch = self.lane_header(lane).epoch.load(Ordering::Acquire);
        let origin_epoch =
            self.lane_header(origin).epoch.load(Ordering::Acquire);
        let policy = self.admission_policy(lane)?;
        let limit = policy as u32 as usize;
        if limit == 0 {
            return Ok(None);
        }
        self.reserve_with_policy(lane, origin, epoch, origin_epoch, policy)
    }

    fn reserve_with_policy(
        &self,
        lane: usize,
        origin: usize,
        epoch: u64,
        origin_epoch: u64,
        policy: u64,
    ) -> Result<Option<WorkTicket>> {
        let limit = policy as u32 as usize;
        let row = self.capacity_row(lane);
        // A full receiver refuses in one load instead of reading every slot:
        // a waiting sender asks again on each release, on every receiver.
        if row.occupied.load(Ordering::SeqCst) as usize >= limit {
            return Err(Error::CapacityFull);
        }
        let start = row.cursor.fetch_add(1, Ordering::Relaxed) as usize % limit;
        let mut exhausted = 0;
        for offset in 0..limit {
            let index = (start + offset) % limit;
            let slot = self.work_slot(lane, index);
            let before = slot.load(Ordering::SeqCst);
            let generation = before >> GENERATION_SHIFT;
            if generation == GENERATION_MAX {
                exhausted += 1;
                continue;
            }
            if before & 3 != FREE {
                continue;
            }
            let claim = ((generation + 1) << GENERATION_SHIFT)
                | ((origin as u64) << STATE_BITS)
                | RESERVED;
            if slot
                .compare_exchange(
                    before,
                    claim,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_err()
            {
                continue;
            }
            row.occupied.fetch_add(1, Ordering::SeqCst);
            let ticket = WorkTicket {
                lane: lane as u16,
                epoch,
                slot: index as u32,
                claim,
            };
            // A producer that raced a resize cannot publish a claim made
            // against an obsolete boundary. Existing published tickets stay valid.
            if row.policy.load(Ordering::SeqCst) != policy {
                self.cancel_work(ticket);
                return Err(Error::CapacityFull);
            }
            if self.lane_header(lane).epoch.load(Ordering::Acquire) != epoch
                || self.lane_header(origin).epoch.load(Ordering::Acquire)
                    != origin_epoch
                || self.consumers(lane)? == 0
                || self.consumers(origin)? == 0
            {
                self.cancel_work(ticket);
                return Err(Error::NoConsumer);
            }
            return Ok(Some(ticket));
        }
        if exhausted == limit {
            return Err(Error::Malformed(
                "work admission generations exhausted",
            ));
        }
        Err(Error::CapacityFull)
    }

    fn ticket_slot(
        &self,
        ticket: WorkTicket,
    ) -> Result<&AtomicU64> {
        self.check_lane(usize::from(ticket.lane))?;
        if ticket.slot as usize >= self.geometry.capacity
            || ticket.claim & 3 != RESERVED
            || ticket.claim >> GENERATION_SHIFT == 0
            || ticket.origin() >= self.lanes
            || self
                .lane_header(usize::from(ticket.lane))
                .epoch
                .load(Ordering::Acquire)
                != ticket.epoch
        {
            return Err(Error::StaleWork);
        }
        Ok(self.work_slot(usize::from(ticket.lane), ticket.slot as usize))
    }

    pub(crate) fn accept_work(
        &self,
        lane: usize,
        ticket: WorkTicket,
    ) -> Result<()> {
        if usize::from(ticket.lane) != lane || self.consumers(lane)? == 0 {
            return Err(Error::StaleWork);
        }
        self.ticket_slot(ticket)?
            .compare_exchange(
                ticket.claim,
                ticket.claim ^ RESERVED ^ ACTIVE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| Error::StaleWork)?;
        Ok(())
    }

    /// Whether this reservation is still held and not yet accepted.
    pub fn check_work(
        &self,
        ticket: WorkTicket,
    ) -> Result<()> {
        if self.ticket_slot(ticket)?.load(Ordering::Acquire) != ticket.claim {
            return Err(Error::StaleWork);
        }
        Ok(())
    }

    pub(crate) fn cancel_work(
        &self,
        ticket: WorkTicket,
    ) {
        self.release_work(ticket, RESERVED);
    }

    /// Take back a published reservation the receiver has not accepted yet.
    /// True: it never started and its slot is free. False: the receiver
    /// accepted it first (or it is gone); its reply is still owed.
    pub fn withdraw_work(
        &self,
        ticket: WorkTicket,
    ) -> bool {
        self.release_work(ticket, RESERVED)
    }

    pub(crate) fn complete_work(
        &self,
        ticket: WorkTicket,
    ) {
        self.release_work(ticket, ACTIVE);
    }

    fn release_work(
        &self,
        ticket: WorkTicket,
        state: u64,
    ) -> bool {
        let Ok(slot) = self.ticket_slot(ticket) else { return false };
        let expected = ticket.claim ^ RESERVED ^ state;
        // Keep the reuse generation even when a receiver lane is replaced.
        // A delayed old guard must never free a successor's reservation.
        let free = ticket.claim & !((1 << GENERATION_SHIFT) - 1);
        let lane = usize::from(ticket.lane);
        if slot.load(Ordering::SeqCst) != expected {
            return false;
        }
        let lowered = self.lower_occupied(lane);
        if slot
            .compare_exchange(
                expected,
                free,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            self.capacity_changed(lane);
            true
        } else {
            if lowered {
                self.capacity_row(lane).occupied.fetch_add(1, Ordering::SeqCst);
            }
            false
        }
    }

    /// Lower the held count before freeing a slot; false when it was already
    /// zero (a crash left it low), so a failed free must not raise it back.
    fn lower_occupied(
        &self,
        lane: usize,
    ) -> bool {
        self.capacity_row(lane)
            .occupied
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_sub(1)
            })
            .is_ok()
    }

    /// Register interest before rechecking reservation availability. Multiple
    /// waits in one member share a bit; cancellation must not clear another
    /// wait's interest. A subsequent change consumes the bit once.
    pub fn watch_capacity(
        &self,
        lane: usize,
        origin: usize,
    ) -> Result<()> {
        self.check_lane(lane)?;
        self.check_lane(origin)?;
        self.capacity_interest(lane, origin / 64)
            .fetch_or(1 << (origin % 64), Ordering::SeqCst);
        Ok(())
    }

    pub(crate) fn advertise_inbox_space(
        &self,
        lane: usize,
        epoch: u64,
    ) {
        self.capacity_row(lane)
            .inbox_space_epoch
            .store(epoch, Ordering::Release);
    }

    /// Whether this exact lane lifetime signals freed inbox space.
    pub fn signals_inbox_space(
        &self,
        lane: usize,
        epoch: u64,
    ) -> bool {
        epoch != 0
            && self.check_lane(lane).is_ok()
            && self.lane_epoch(lane).ok() == Some(epoch)
            && self.capacity_row(lane).inbox_space_epoch.load(Ordering::Acquire)
                == epoch
    }

    /// Register for space in an exact destination inbox. Drive the origin's
    /// receiver for `Received::Capacity` and retry the write after a wake.
    /// False means space is already visible. `NoConsumer` ends the wait.
    ///
    /// Capture the destination epoch before the first write. Take a capacity
    /// generation snapshot before arming, then wait for a change from that
    /// snapshot. Like work admission notifications, wakes may coalesce or
    /// require another check. Both peers must support this notification.
    pub fn watch_inbox_space(
        &self,
        lane: usize,
        epoch: u64,
        origin: usize,
    ) -> Result<bool> {
        self.check_lane(lane)?;
        self.check_lane(origin)?;
        if !self.signals_inbox_space(lane, epoch) {
            return Err(Error::NoConsumer);
        }
        self.watch_capacity(lane, origin)?;
        self.capacity_row(lane).inbox_space_waiting.store(1, Ordering::SeqCst);
        let admission = self.admits(lane)?;
        if self.lane_epoch(lane)? != epoch
            || admission == crate::Admission::NoConsumer
        {
            return Err(Error::NoConsumer);
        }
        Ok(admission == crate::Admission::Full)
    }

    /// The reader has published its next position before checking the arm.
    pub(crate) fn inbox_space_changed(
        &self,
        lane: usize,
    ) {
        let waiting = &self.capacity_row(lane).inbox_space_waiting;
        if waiting.load(Ordering::SeqCst) != 0
            && waiting.swap(0, Ordering::SeqCst) != 0
        {
            self.capacity_changed(lane);
        }
    }

    pub fn capacity_generation(
        &self,
        origin: usize,
    ) -> Result<u64> {
        self.check_lane(origin)?;
        Ok(self.capacity_row(origin).changes.load(Ordering::SeqCst))
    }

    pub(crate) fn capacity_changed(
        &self,
        lane: usize,
    ) {
        for word in 0..self.lanes.div_ceil(64) {
            let mut bits =
                self.capacity_interest(lane, word).swap(0, Ordering::SeqCst);
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let origin = word * 64 + bit;
                if origin < self.lanes {
                    self.capacity_row(origin)
                        .changes
                        .fetch_add(1, Ordering::SeqCst);
                    self.wake(origin, self.lane_header(origin));
                }
            }
        }
    }

    /// Called only under the replaced member's kernel hold. Accepted work
    /// belongs to its receiver, so origin death cancels reservations only.
    pub(crate) fn recover_work(
        &self,
        gone: usize,
    ) {
        for lane in 0..self.lanes {
            for index in 0..self.geometry.capacity {
                let slot = self.work_slot(lane, index);
                let before = slot.load(Ordering::Acquire);
                let state = before & 3;
                let origin =
                    ((before >> STATE_BITS) & u64::from(u16::MAX)) as usize;
                if (lane == gone && state != FREE)
                    || (state == RESERVED && origin == gone)
                {
                    let free = before & !((1 << GENERATION_SHIFT) - 1);
                    let lowered = self.lower_occupied(lane);
                    if slot
                        .compare_exchange(
                            before,
                            free,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_err()
                        && lowered
                    {
                        self.capacity_row(lane)
                            .occupied
                            .fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
            // Recovery also repairs a crash after freeing a claim but before
            // its wake. Only registered interests ring; no timer or hot-path
            // broadcast is introduced.
            self.capacity_changed(lane);
        }
    }
}

#[cfg(unix)]
mod owned {
    use super::*;
    use crate::LinkSegment;
    use std::sync::Arc;

    /// Sender ownership before inbox publication. Drop rolls back only this
    /// unsubmitted reservation. It cannot free accepted receiver work.
    pub struct WorkReservation {
        segment: Arc<LinkSegment>,
        ticket: WorkTicket,
        published: bool,
    }

    impl WorkReservation {
        pub fn ticket(&self) -> WorkTicket {
            self.ticket
        }

        pub fn publish(
            mut self,
            parts: &[&[u8]],
        ) -> Result<u64> {
            let position =
                self.segment.inbox().write_work(self.ticket, parts)?;
            self.published = true;
            Ok(position)
        }
    }

    impl Drop for WorkReservation {
        fn drop(&mut self) {
            if !self.published {
                self.segment.inbox().cancel_work(self.ticket);
            }
        }
    }

    /// Accepted receiver work. Hold through the complete body/session lifetime.
    pub struct WorkExecution {
        segment: Arc<LinkSegment>,
        ticket: WorkTicket,
    }

    impl Drop for WorkExecution {
        fn drop(&mut self) {
            self.segment.inbox().complete_work(self.ticket);
        }
    }

    impl LinkSegment {
        /// Update admission for the receiver lane held by `joined`.
        /// Existing reservations and accepted work keep their tickets.
        pub fn update_work_capacity(
            &self,
            joined: &crate::Joined,
            limit: usize,
        ) -> Result<()> {
            if !joined.belongs_to(self) {
                return Err(Error::StaleWork);
            }
            self.inbox().update_work_capacity(joined.lane, limit)
        }

        pub fn reserve_work(
            self: &Arc<Self>,
            lane: usize,
            origin: usize,
        ) -> Result<Option<WorkReservation>> {
            Ok(self.inbox().reserve_work(lane, origin)?.map(|ticket| {
                WorkReservation {
                    segment: Arc::clone(self),
                    ticket,
                    published: false,
                }
            }))
        }

        pub fn accept_work(
            self: &Arc<Self>,
            lane: usize,
            ticket: WorkTicket,
        ) -> Result<WorkExecution> {
            self.inbox().accept_work(lane, ticket)?;
            Ok(WorkExecution { segment: Arc::clone(self), ticket })
        }
    }
}

#[cfg(unix)]
pub use owned::{WorkExecution, WorkReservation};

#[cfg(test)]
#[path = "../tests/unit/admission.rs"]
mod tests;

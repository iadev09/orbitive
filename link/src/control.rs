//! Bounded exchange control. Replies remain authoritative in the inbox.

use std::mem::{align_of, size_of};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use orbit_core::process::ProcessOwner;
use orbit_core::shm::ShmRegion;

use crate::{Error, Joined, LinkSegment, Result};

const MAGIC: u64 = u64::from_le_bytes(*b"OLCTRL01");
const MASK: u64 = 3;
const FREE: u64 = 0;
const PENDING: u64 = 1;
const CANCELLED: u64 = 2;
const FAILED: u64 = 3;

#[repr(C)]
struct Header {
    magic: AtomicU64,
    lanes: AtomicU64,
    slots_per_lane: AtomicU64,
}

#[repr(C)]
struct Slot {
    state: AtomicU64,
    origin_epoch: AtomicU64,
    runner: AtomicU64,
    runner_epoch: AtomicU64,
}

const _: () = assert!(size_of::<Slot>() == 32);
const _: () =
    assert!(size_of::<Header>().is_multiple_of(align_of::<AtomicU64>()));

/// Identifies a control record in the selected Link's control table.
/// The protocol carries it with the request and correlates the normal reply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeControlTicket {
    pub origin: u16,
    pub origin_epoch: u64,
    pub runner: u16,
    pub runner_epoch: u64,
    pub slot: u32,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeControlState {
    Pending,
    Cancelled,
    Failed,
}

/// A payload-free SHM table with a fixed number of records per caller lane.
/// Its kind and capacity belong to the protocol that opts into the table.
pub struct ExchangeControls {
    segment: Arc<LinkSegment>,
    region: ShmRegion,
    kind: u8,
    slots_per_lane: usize,
    next: AtomicUsize,
    process: ProcessOwner,
}

impl ExchangeControls {
    /// Open the control table in the Link's exact namespace and access scope.
    /// The kind must be distinct from all other resources in that namespace.
    pub fn open(
        segment: Arc<LinkSegment>,
        kind: u8,
        slots_per_lane: u32,
    ) -> Result<Self> {
        let spec = segment.spec();
        if kind == spec.inbox_kind
            || kind == spec.streams.kind
            || slots_per_lane == 0
        {
            return Err(Error::Malformed(
                "invalid Link exchange control geometry",
            ));
        }
        let lanes = segment.inbox().lanes();
        let slots_per_lane = slots_per_lane as usize;
        let size = lanes
            .checked_mul(slots_per_lane)
            .and_then(|slots| slots.checked_mul(size_of::<Slot>()))
            .and_then(|bytes| {
                bytes.checked_add(
                    size_of::<Header>() + lanes * size_of::<AtomicU64>(),
                )
            })
            .ok_or(Error::Malformed(
                "Link exchange control geometry overflow",
            ))?;
        let process = ProcessOwner::current()?;
        let (region, lock) = segment.open_control_region(kind, size)?;
        let table = Self {
            segment,
            region,
            kind,
            slots_per_lane,
            next: AtomicUsize::new(0),
            process,
        };
        // SAFETY: a live, aligned mapping of at least `size` bytes. The
        // initialization lock excludes attachers and new SHM is zero-filled.
        let header = unsafe { &*table.region.as_ptr().cast::<Header>() };
        if table.region.created() {
            header.lanes.store(lanes as u64, Ordering::Relaxed);
            header
                .slots_per_lane
                .store(slots_per_lane as u64, Ordering::Relaxed);
            header.magic.store(MAGIC, Ordering::Release);
        }
        if header.magic.load(Ordering::Acquire) != MAGIC
            || header.lanes.load(Ordering::Relaxed) != lanes as u64
            || header.slots_per_lane.load(Ordering::Relaxed)
                != slots_per_lane as u64
        {
            return Err(Error::Malformed(
                "Link exchange control layout mismatch",
            ));
        }
        drop(lock);
        Ok(table)
    }

    /// Reserve in the caller's own lane. A full table leaves dispatch to the
    /// protocol. Hold the returned lease until the reply is delivered or
    /// the caller stops waiting. `None` means all records are in use.
    pub fn reserve(
        self: &Arc<Self>,
        caller: &Joined,
        runner: usize,
        runner_epoch: u64,
    ) -> Result<Option<ExchangeControl>> {
        self.check_owner(caller)?;
        let inbox = self.segment.inbox();
        let origin_epoch = caller.epoch();
        if inbox.lane_epoch(runner)? != runner_epoch
            || inbox.consumers(runner)? == 0
            || inbox.consumers(caller.lane)? == 0
        {
            return Err(Error::NoConsumer);
        }

        self.prepare_lane(caller.lane, origin_epoch)?;
        let first =
            self.next.fetch_add(1, Ordering::Relaxed) % self.slots_per_lane;
        for offset in 0..self.slots_per_lane {
            let index = (first + offset) % self.slots_per_lane;
            let slot = self.slot(caller.lane, index);
            let old = slot.state.load(Ordering::Acquire);
            if old & MASK != FREE {
                continue;
            }
            // Publish the ticket only after its metadata has been written.
            let Some(generation) = (old & !MASK).checked_add(MASK + 1) else {
                continue;
            };
            if slot
                .state
                .compare_exchange(
                    old,
                    generation | PENDING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                continue;
            }
            slot.origin_epoch.store(origin_epoch, Ordering::Relaxed);
            slot.runner.store(runner as u64, Ordering::Relaxed);
            slot.runner_epoch.store(runner_epoch, Ordering::Release);
            return Ok(Some(ExchangeControl {
                table: Arc::clone(self),
                ticket: ExchangeControlTicket {
                    origin: caller.lane as u16,
                    origin_epoch,
                    runner: runner as u16,
                    runner_epoch,
                    slot: index as u32,
                    generation,
                },
            }));
        }
        Ok(None)
    }

    /// Read the latest control state. A reclaimed or released ticket is stale.
    /// For the Runner, caller-side staleness also ends the wait. For the caller,
    /// Runner retirement alone cannot prove failure: drain committed replies
    /// after producer recovery before abandoning a stale exchange.
    ///
    /// To await a change, drive the local inbox receiver and recheck after
    /// `Received::Capacity` or a directory change. The bell carries no result.
    pub fn state(
        &self,
        ticket: ExchangeControlTicket,
    ) -> Result<ExchangeControlState> {
        self.decode_state(ticket, true)
    }

    fn decode_state(
        &self,
        ticket: ExchangeControlTicket,
        require_runner: bool,
    ) -> Result<ExchangeControlState> {
        let state = self.checked_state(ticket, require_runner)?;
        match state & MASK {
            PENDING => Ok(ExchangeControlState::Pending),
            CANCELLED => Ok(ExchangeControlState::Cancelled),
            FAILED => Ok(ExchangeControlState::Failed),
            _ => Err(Error::StaleWork),
        }
    }

    /// Read the caller's terminal record even after the Runner's lane has
    /// been retired. An unchanged pending record still requires inbox recovery.
    pub fn caller_state(
        &self,
        ticket: ExchangeControlTicket,
    ) -> Result<ExchangeControlState> {
        self.decode_state(ticket, false)
    }

    /// Record a permanent failure to publish a reply that has never been
    /// committed. A transient full inbox does not fail an exchange.
    /// Runner death is not a failure publication: a reply may already be
    /// committed. Recover the dead producer and drain its committed replies.
    pub fn fail(
        &self,
        runner: &Joined,
        ticket: ExchangeControlTicket,
    ) -> Result<bool> {
        self.check_owner(runner)?;
        if runner.lane != usize::from(ticket.runner)
            || runner.epoch() != ticket.runner_epoch
        {
            return Err(Error::StaleWork);
        }
        self.transition(ticket, FAILED, usize::from(ticket.origin))
    }

    /// Unlink this resource once all participants have stopped.
    pub fn unlink(&self) -> Result<()> {
        self.segment.unlink_control_region(&self.region)
    }

    fn check_owner(
        &self,
        joined: &Joined,
    ) -> Result<()> {
        if !self.process.is_current()
            || !joined.belongs_to(&self.segment)
            || self.segment.inbox().last_owner(joined.lane)?.map(|(pid, _)| pid)
                != Some(self.process.pid())
        {
            return Err(Error::StaleWork);
        }
        Ok(())
    }

    fn checked_state(
        &self,
        ticket: ExchangeControlTicket,
        require_runner: bool,
    ) -> Result<u64> {
        let inbox = self.segment.inbox();
        let origin = usize::from(ticket.origin);
        let runner = usize::from(ticket.runner);
        let index = ticket.slot as usize;
        if ticket.generation == 0
            || ticket.generation & MASK != 0
            || index >= self.slots_per_lane
            || inbox.lane_epoch(origin)? != ticket.origin_epoch
            || (require_runner
                && inbox.lane_epoch(runner)? != ticket.runner_epoch)
        {
            return Err(Error::StaleWork);
        }
        let slot = self.slot(origin, index);
        let state = slot.state.load(Ordering::Acquire);
        if state & !MASK != ticket.generation
            || state & MASK == FREE
            || slot.runner_epoch.load(Ordering::Acquire) != ticket.runner_epoch
            || slot.origin_epoch.load(Ordering::Relaxed) != ticket.origin_epoch
            || slot.runner.load(Ordering::Relaxed) != runner as u64
        {
            return Err(Error::StaleWork);
        }
        let state = slot.state.load(Ordering::Acquire);
        if state & !MASK != ticket.generation || state & MASK == FREE {
            return Err(Error::StaleWork);
        }
        Ok(state)
    }

    fn transition(
        &self,
        ticket: ExchangeControlTicket,
        terminal: u64,
        notify: usize,
    ) -> Result<bool> {
        if self.checked_state(ticket, true)? & MASK != PENDING {
            return Ok(false);
        }
        let slot = self.slot(usize::from(ticket.origin), ticket.slot as usize);
        let changed = slot
            .state
            .compare_exchange(
                ticket.generation | PENDING,
                ticket.generation | terminal,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if changed {
            self.segment.inbox().notify_capacity_observer(notify);
        }
        Ok(changed)
    }

    fn prepare_lane(
        &self,
        lane: usize,
        epoch: u64,
    ) -> Result<()> {
        // SAFETY: the table maps one aligned epoch word for each inbox lane.
        let prepared = unsafe {
            &*self
                .region
                .as_ptr()
                .add(size_of::<Header>())
                .cast::<AtomicU64>()
                .add(lane)
        };
        if prepared.load(Ordering::Acquire) != epoch {
            // Only the new lane owner can reset its abandoned records.
            // Serialize the first use across mappings, outside steady state.
            let _lock =
                self.segment.lock_control_region(self.kind, &self.region)?;
            if prepared.load(Ordering::Relaxed) != epoch {
                for index in 0..self.slots_per_lane {
                    self.slot(lane, index)
                        .state
                        .fetch_and(!MASK, Ordering::AcqRel);
                }
                prepared.store(epoch, Ordering::Release);
            }
        }
        Ok(())
    }

    fn slot(
        &self,
        lane: usize,
        index: usize,
    ) -> &Slot {
        // SAFETY: the validated geometry bounds lane/index; only atomics are
        // accessed in the live mapping.
        unsafe {
            &*self
                .region
                .as_ptr()
                .add(
                    size_of::<Header>()
                        + self.segment.inbox().lanes() * size_of::<AtomicU64>(),
                )
                .cast::<Slot>()
                .add(lane * self.slots_per_lane + index)
        }
    }
}

/// Caller-owned control record. Dropping a pending lease invalidates the
/// ticket and wakes its Runner. A Runner treats that stale ticket as cancelled.
/// Keep the caller's lane hold alive until the lease is released.
pub struct ExchangeControl {
    table: Arc<ExchangeControls>,
    ticket: ExchangeControlTicket,
}

impl ExchangeControl {
    pub fn ticket(&self) -> ExchangeControlTicket {
        self.ticket
    }

    /// The terminal state visible to the caller, even after Runner retirement.
    pub fn caller_state(&self) -> Result<ExchangeControlState> {
        self.table.caller_state(self.ticket)
    }

    /// Cancel this exchange without publishing a frame into the Runner inbox.
    pub fn cancel(&self) -> Result<bool> {
        if !self.table.process.is_current() {
            return Err(Error::StaleWork);
        }
        self.table.transition(
            self.ticket,
            CANCELLED,
            usize::from(self.ticket.runner),
        )
    }

    /// Release after receiving a committed reply.
    pub fn complete(self) {
        self.release(false);
    }

    fn release(
        &self,
        cancel: bool,
    ) {
        if !self.table.process.is_current() {
            return;
        }
        let ticket = self.ticket;
        let slot =
            self.table.slot(usize::from(ticket.origin), ticket.slot as usize);
        let previous = slot.state.try_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |state| {
                (state & !MASK == ticket.generation && state & MASK != FREE)
                    .then_some(ticket.generation)
            },
        );
        if cancel && previous == Ok(ticket.generation | PENDING) {
            self.table
                .segment
                .inbox()
                .notify_capacity_observer(usize::from(ticket.runner));
        }
    }
}

impl Drop for ExchangeControl {
    fn drop(&mut self) {
        self.release(true);
    }
}

#[cfg(all(test, feature = "tokio"))]
#[path = "../tests/unit/control.rs"]
mod tests;

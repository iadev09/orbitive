//! Scoped creator-capacity observation, independent of stream byte readers.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::layout::Doorbell;
use crate::table::Table;
use crate::{Result, lock_unpoisoned};

#[derive(Default)]
pub(crate) struct Waiters(Mutex<Registered>);

#[derive(Default)]
struct Registered {
    next: u64,
    wakers: BTreeMap<u64, Option<Waker>>,
}

impl Waiters {
    fn insert(
        &self,
        bell: &Doorbell,
        incarnation: u64,
    ) -> (u64, u32) {
        let mut entries = lock_unpoisoned(&self.0);
        let id = entries.next;
        entries.next =
            id.checked_add(1).expect("capacity registration exhausted");
        let seen = bell.capacity_changes.load(Ordering::SeqCst);
        entries.wakers.insert(id, None);
        bell.incarnation.store(incarnation, Ordering::SeqCst);
        // The caller retries allocation after arming. A concurrent release
        // either observes this flag or is visible to that retry.
        bell.capacity_waiting.store(1, Ordering::SeqCst);
        // Pair the arm with the caller's subsequent Acquire slot scan. If a
        // releaser saw the old flag, its SeqCst EMPTY publication precedes
        // this fence and cannot be missed by that retry.
        std::sync::atomic::fence(Ordering::SeqCst);
        (id, seen)
    }

    fn register(
        &self,
        id: u64,
        waker: &Waker,
    ) {
        let mut entries = lock_unpoisoned(&self.0);
        if let Some(place) = entries.wakers.get_mut(&id)
            && place.as_ref().is_none_or(|old| !old.will_wake(waker))
        {
            *place = Some(waker.clone());
        }
    }

    fn remove(
        &self,
        id: u64,
        bell: &Doorbell,
    ) {
        let mut entries = lock_unpoisoned(&self.0);
        entries.wakers.remove(&id);
        if entries.wakers.is_empty() {
            bell.capacity_waiting.store(0, Ordering::SeqCst);
        }
    }

    pub(crate) fn wake(&self) {
        let wakers: Vec<_> = lock_unpoisoned(&self.0)
            .wakers
            .values_mut()
            .filter_map(Option::take)
            .collect();
        for waker in wakers {
            waker.wake();
        }
    }
}

/// A release in this creator's lane may have freed a slot or made a retained
/// endpoint rearmable. This is a hint, not a reservation: retry the operation.
///
/// Construct **before** retrying a failed create/rearm, then await only if the
/// retry still cannot proceed. Registration is armed at construction, and
/// dropping the observer removes it without touching streams or their owners.
/// Multiple callers may observe the same creator lane independently.
#[must_use]
pub struct CapacityChanged {
    table: Arc<Table>,
    id: Option<u64>,
    seen: u32,
    epoch: u64,
}

impl CapacityChanged {
    pub(crate) fn new(table: Arc<Table>) -> Self {
        let epoch = table.epoch();
        let (id, seen) =
            table.capacity.insert(table.own_doorbell(), table.incarnation());
        Self { table, id: Some(id), seen, epoch }
    }

    fn changed(&self) -> bool {
        self.epoch != self.table.epoch()
            || self.seen
                != self
                    .table
                    .own_doorbell()
                    .capacity_changes
                    .load(Ordering::SeqCst)
    }

    fn finish(&mut self) {
        if let Some(id) = self.id.take() {
            self.table.capacity.remove(id, self.table.own_doorbell());
        }
    }
}

impl Future for CapacityChanged {
    type Output = Result<()>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(id) = this.id else { return Poll::Ready(Ok(())) };
        if this.changed() {
            this.finish();
            return Poll::Ready(Ok(()));
        }
        this.table.capacity.register(id, cx.waker());
        if let Err(error) = this.table.ensure_driver() {
            this.finish();
            return Poll::Ready(Err(error));
        }
        if this.changed() {
            this.finish();
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for CapacityChanged {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
#[path = "../tests/unit/capacity.rs"]
mod tests;

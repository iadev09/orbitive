//! Ownership of counters while a manager future can suspend or be dropped.

use std::time::Instant;

use super::{
    ActiveRequestPolicy, ClientLease, ClientManager, ClientPool, Entry,
};

pub(super) struct Pending<'a, M: ClientManager> {
    // Drop the physical client before returning its capacity and waking a
    // competing creator. Field order preserves this even during unwinding.
    entry: Option<Entry<M::Client>>,
    reservation: Reservation<'a, M>,
}

enum Kind {
    Create { admitted: bool },
    Recycle,
}

struct Reservation<'a, M: ClientManager> {
    pool: &'a ClientPool<M>,
    kind: Option<Kind>,
}

impl<'a, M: ClientManager> Pending<'a, M> {
    /// Capacity has already been reserved under the pool state mutex.
    pub(super) fn create(
        pool: &'a ClientPool<M>,
        admitted: bool,
    ) -> Self {
        Self {
            entry: None,
            reservation: Reservation {
                pool,
                kind: Some(Kind::Create { admitted }),
            },
        }
    }

    pub(super) fn recycle(
        pool: &'a ClientPool<M>,
        entry: Entry<M::Client>,
    ) -> Self {
        Self {
            entry: Some(entry),
            reservation: Reservation { pool, kind: Some(Kind::Recycle) },
        }
    }

    pub(super) fn client(&mut self) -> &mut M::Client {
        &mut self.entry.as_mut().expect("recycling a retained client").client
    }

    pub(super) fn created(
        mut self,
        client: M::Client,
        policy: ActiveRequestPolicy,
    ) -> ClientLease<M> {
        let now = Instant::now();
        self.entry = Some(Entry { client, created_at: now, idle_since: now });
        self.lease(policy)
    }

    pub(super) fn lease(
        mut self,
        policy: ActiveRequestPolicy,
    ) -> ClientLease<M> {
        let lease = ClientLease::new(
            self.reservation.pool.clone(),
            self.entry.take().expect("manager operation produced a client"),
            policy,
        );
        if let Some(Kind::Create { admitted }) = self.reservation.kind.take() {
            self.reservation.pool.finish_create(true, admitted);
        }
        lease
    }

    pub(super) fn warm(
        mut self,
        client: M::Client,
    ) {
        debug_assert!(matches!(
            self.reservation.kind,
            Some(Kind::Create { admitted: false })
        ));
        self.reservation.kind.take();
        self.reservation.pool.finish_warm_create(client);
    }
}

impl<M: ClientManager> Drop for Reservation<'_, M> {
    fn drop(&mut self) {
        match self.kind.take() {
            Some(Kind::Create { admitted }) => {
                self.pool.finish_create(false, admitted)
            }
            Some(Kind::Recycle) => self.pool.retire_one(),
            None => {}
        }
    }
}

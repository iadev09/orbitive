use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};

use super::*;
use crate::support::{Control, Manager, empty, poll_once, ready};

fn controlled(options: PoolOptions) -> (ClientPool<Manager>, Arc<Control>) {
    let control = Arc::new(Control::default());
    (ClientPool::new(Manager(control.clone()), options).unwrap(), control)
}

#[test]
fn dropped_create_and_recycle_preserve_other_leases_and_return_capacity_once() {
    for recycle in [false, true] {
        let (pool, control) = controlled(PoolOptions::new(0, 2, 2, 2, 1));
        let held = ready(
            Box::pin(pool.acquire(ActiveRequestPolicy::Preserve)).as_mut(),
        )
        .unwrap();
        if recycle {
            drop(
                ready(
                    Box::pin(pool.acquire(ActiveRequestPolicy::Preserve))
                        .as_mut(),
                )
                .unwrap(),
            );
            control.recycle_pending.store(true, Ordering::SeqCst);
        } else {
            control.create_pending.store(true, Ordering::SeqCst);
        }
        let mut pending = Box::pin(pool.acquire(ActiveRequestPolicy::Preserve));
        assert!(poll_once(pending.as_mut()).is_pending());
        assert_eq!(pool.stats().in_flight, 2);
        drop(pending);
        let stats = pool.stats();
        assert_eq!((stats.live, stats.creating, stats.in_flight), (1, 0, 1));
        assert_eq!(control.live.load(Ordering::SeqCst), 1);
        control.create_pending.store(false, Ordering::SeqCst);
        control.recycle_pending.store(false, Ordering::SeqCst);
        let replacement = ready(
            Box::pin(pool.acquire(ActiveRequestPolicy::Preserve)).as_mut(),
        )
        .unwrap();
        replacement.discard();
        assert_eq!((pool.stats().live, pool.stats().in_flight), (1, 1));

        let mut drain = Box::pin(pool.drain());
        assert!(
            poll_once(drain.as_mut()).is_pending(),
            "the preserved lease still belongs to its caller"
        );
        drop(held);
        ready(drain.as_mut());
        empty(pool.stats());
        assert_eq!(control.live.load(Ordering::SeqCst), 0);
        assert_eq!(
            control.created.load(Ordering::SeqCst),
            control.dropped.load(Ordering::SeqCst)
        );
    }
}

#[test]
fn dropping_pending_warm_creation_allows_a_new_maintainer_and_drain() {
    let (pool, control) = controlled(PoolOptions::new(1, 1, 1, 1, 1));
    control.create_pending.store(true, Ordering::SeqCst);
    let mut run = Box::pin(pool.run(CancellationToken::new()));
    assert!(poll_once(run.as_mut()).is_pending());
    assert_eq!(
        (pool.stats().live, pool.stats().creating, pool.stats().in_flight),
        (1, 1, 0)
    );
    drop(run);
    empty(pool.stats());

    control.create_pending.store(false, Ordering::SeqCst);
    let mut next = Box::pin(pool.run(CancellationToken::new()));
    assert!(poll_once(next.as_mut()).is_pending());
    assert_eq!(
        (pool.stats().live, pool.stats().idle, pool.stats().creating),
        (1, 1, 0)
    );
    drop(next);
    assert_eq!(
        pool.stats().idle,
        1,
        "committed warm clients must remain retained"
    );
    ready(Box::pin(pool.drain()).as_mut());
    empty(pool.stats());
    assert_eq!(control.live.load(Ordering::SeqCst), 0);
}

#[test]
fn cancelling_pending_warm_creation_releases_it_before_drain() {
    let (pool, control) = controlled(PoolOptions::new(1, 1, 1, 1, 1));
    control.create_pending.store(true, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let mut run = Box::pin(pool.run(cancellation.clone()));
    assert!(poll_once(run.as_mut()).is_pending());
    cancellation.cancel();
    ready(run.as_mut()).unwrap();
    empty(pool.stats());
}

#[test]
fn cancelled_or_failed_manager_work_releases_each_reservation_once() {
    for recycle in [false, true] {
        for cancelled in [false, true] {
            let (pool, control) = controlled(PoolOptions::new(0, 1, 1, 1, 1));
            if recycle {
                drop(
                    ready(
                        Box::pin(pool.acquire(ActiveRequestPolicy::Preserve))
                            .as_mut(),
                    )
                    .unwrap(),
                );
            }
            let token = CancellationToken::new();
            let pending = if recycle {
                &control.recycle_pending
            } else {
                &control.create_pending
            };
            let error = if recycle {
                &control.recycle_error
            } else {
                &control.create_error
            };
            pending.store(cancelled, Ordering::SeqCst);
            error.store(!cancelled, Ordering::SeqCst);
            let mut acquiring = Box::pin(
                pool.acquire_cancellable(ActiveRequestPolicy::Preserve, &token),
            );
            if cancelled {
                assert!(poll_once(acquiring.as_mut()).is_pending());
                token.cancel();
                assert!(matches!(
                    ready(acquiring.as_mut()),
                    Err(AcquireError::Cancelled)
                ));
            } else {
                assert!(matches!(
                    ready(acquiring.as_mut()),
                    Err(AcquireError::Manager(_))
                ));
            }
            drop(acquiring);
            empty(pool.stats());
            assert_eq!(control.live.load(Ordering::SeqCst), 0);
            ready(Box::pin(pool.drain()).as_mut());
        }
    }
}

#[test]
fn dropping_manager_work_wakes_an_already_waiting_drain() {
    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    for recycle in [false, true] {
        let (pool, control) = controlled(PoolOptions::new(0, 1, 1, 1, 1));
        if recycle {
            drop(
                ready(
                    Box::pin(pool.acquire(ActiveRequestPolicy::Preserve))
                        .as_mut(),
                )
                .unwrap(),
            );
            control.recycle_pending.store(true, Ordering::SeqCst);
        } else {
            control.create_pending.store(true, Ordering::SeqCst);
        }
        let mut acquiring =
            Box::pin(pool.acquire(ActiveRequestPolicy::Preserve));
        assert!(poll_once(acquiring.as_mut()).is_pending());
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = Waker::from(wakes.clone());
        let mut drain = Box::pin(pool.drain());
        assert!(
            drain.as_mut().poll(&mut Context::from_waker(&waker)).is_pending()
        );
        drop(acquiring);
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        ready(drain.as_mut());
        empty(pool.stats());
        assert_eq!(control.live.load(Ordering::SeqCst), 0);
    }
}

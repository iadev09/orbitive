use std::sync::atomic::Ordering;

use super::*;
use crate::support::{
    Control, Manager as ControlledManager, empty, poll_once, ready,
};

fn controlled(
    min_idle: u32
) -> (FleetClientPool<ControlledManager>, Arc<Control>) {
    let control = Arc::new(Control::default());
    let pool = pool_with_options(
        ControlledManager(control.clone()),
        FleetPoolOptions::new(min_idle, 1, 1, 1, 1, 1),
    );
    (pool, control)
}

fn no_reservations(
    pool: &FleetClientPool<ControlledManager>,
    control: &Control,
) {
    empty(pool.local.stats());
    assert_eq!(pool.pool.budget(pool.key), (0, 0));
    assert_eq!(pool.pool.admitted(pool.key).unwrap(), 0);
    assert_eq!(pool.pool.available(pool.key).unwrap(), 0);
    assert_eq!(control.live.load(Ordering::SeqCst), 0);
}

#[test]
fn dropped_fleet_create_and_recycle_release_shared_and_local_ownership() {
    for recycle in [false, true] {
        let (pool, control) = controlled(0);
        if recycle {
            drop(
                ready(
                    Box::pin(pool.acquire(b"", ActiveRequestPolicy::Preserve))
                        .as_mut(),
                )
                .unwrap(),
            );
            control.recycle_pending.store(true, Ordering::SeqCst);
        } else {
            control.create_pending.store(true, Ordering::SeqCst);
        }
        let mut acquire =
            Box::pin(pool.acquire(b"", ActiveRequestPolicy::Preserve));
        assert!(poll_once(acquire.as_mut()).is_pending());
        assert_eq!(pool.pool.admitted(pool.key).unwrap(), 1);
        assert_eq!(
            pool.pool.budget(pool.key),
            if recycle { (1, 0) } else { (0, 1) }
        );
        drop(acquire);
        no_reservations(&pool, &control);

        control.create_pending.store(false, Ordering::SeqCst);
        control.recycle_pending.store(false, Ordering::SeqCst);
        let lease = ready(
            Box::pin(pool.acquire(b"", ActiveRequestPolicy::Preserve)).as_mut(),
        )
        .unwrap();
        assert_eq!(pool.pool.budget(pool.key), (1, 0));
        assert_eq!(pool.pool.admitted(pool.key).unwrap(), 1);
        drop(lease);
        assert_eq!(pool.pool.admitted(pool.key).unwrap(), 0);
        assert_eq!(pool.pool.available(pool.key).unwrap(), 1);
        ready(Box::pin(pool.local.drain()).as_mut());
        no_reservations(&pool, &control);
    }
}

#[test]
fn dropped_fleet_warm_creation_releases_creation_permits() {
    for maintainer in [false, true] {
        let (pool, control) = controlled(1);
        control.create_pending.store(true, Ordering::SeqCst);
        let cancellation = CancellationToken::new();
        if maintainer {
            let mut run = Box::pin(pool.run(cancellation.clone()));
            assert!(poll_once(run.as_mut()).is_pending());
            assert_eq!(pool.pool.budget(pool.key), (0, 1));
            drop(run);
        } else {
            let mut warm = Box::pin(pool.warm_one(&cancellation));
            assert!(poll_once(warm.as_mut()).is_pending());
            assert_eq!(pool.pool.budget(pool.key), (0, 1));
            drop(warm);
        }
        no_reservations(&pool, &control);
        control.create_pending.store(false, Ordering::SeqCst);
        assert!(
            ready(Box::pin(pool.warm_one(&cancellation)).as_mut()).unwrap()
        );
        assert_eq!(pool.pool.budget(pool.key), (1, 0));
        assert_eq!(pool.pool.available(pool.key).unwrap(), 1);
        assert_eq!(pool.local.stats().in_flight, 0);
        ready(Box::pin(pool.local.drain()).as_mut());
        no_reservations(&pool, &control);
    }
}

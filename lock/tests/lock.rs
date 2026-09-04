use std::sync::{Arc, Barrier};
use std::time::Duration;

use bytes::Bytes;
use orbit_core::{Fleet, RingSpec};
use orbit_lock::{Lock, LockAcquire, LockKey, LockLayout, LockOwner, LockTransition, LockType};

struct PoolSlot;

impl LockType for PoolSlot {
    const NAMESPACE: &'static str = "test.pool-slot";
}

struct ScheduledOccurrence;

impl LockType for ScheduledOccurrence {
    const NAMESPACE: &'static str = "test.scheduled-occurrence";
}

fn key<T: LockType>(label: &'static [u8]) -> LockKey {
    LockKey::new::<T>(Bytes::from_static(label))
}

#[test]
fn only_key_determines_contention_and_owner_identifies_the_holder() {
    let fleet = Arc::new(Fleet::join("lock_owner", 2).expect("fleet"));
    let first = Lock::new(Arc::clone(&fleet)).expect("first lock handle");
    let second = Lock::new(fleet).expect("second lock handle");
    let slot = key::<PoolSlot>(b"queue:1");
    let owner_a = LockOwner::from("worker-a");
    let owner_b = LockOwner::from("worker-b");

    let LockAcquire::Acquired(lease) = first
        .try_acquire(&slot, &owner_a, Duration::from_secs(30))
        .expect("acquire")
    else {
        panic!("first owner must acquire");
    };
    let LockAcquire::Occupied(holder) = second
        .try_acquire(&slot, &owner_b, Duration::from_secs(30))
        .expect("contended acquire")
    else {
        panic!("second owner must observe contention");
    };

    assert_eq!(holder, lease);
    assert_eq!(holder.owner, owner_a);
}

#[test]
fn namespaces_keep_unrelated_domains_independent() {
    let fleet = Arc::new(Fleet::join("lock_namespace", 1).expect("fleet"));
    let locks = Lock::new(fleet).expect("locks");
    let owner = LockOwner::from("worker-a");

    assert!(matches!(
        locks
            .try_acquire(
                &key::<PoolSlot>(b"same-label"),
                &owner,
                Duration::from_secs(30)
            )
            .expect("pool lock"),
        LockAcquire::Acquired(_)
    ));
    assert!(matches!(
        locks
            .try_acquire(
                &key::<ScheduledOccurrence>(b"same-label"),
                &owner,
                Duration::from_secs(30)
            )
            .expect("occurrence lock"),
        LockAcquire::Acquired(_)
    ));
}

#[test]
fn release_requires_the_current_owner_or_exact_lease() {
    let fleet = Arc::new(Fleet::join("lock_release", 1).expect("fleet"));
    let locks = Lock::new(fleet).expect("locks");
    let slot = key::<PoolSlot>(b"queue:1");
    let owner = LockOwner::from("worker-a");
    let other = LockOwner::from("worker-b");
    let lease = locks
        .try_acquire(&slot, &owner, Duration::from_secs(30))
        .expect("acquire")
        .acquired()
        .expect("winner");

    assert!(
        !locks
            .release_owned(&slot, &other)
            .expect("wrong owner release")
    );
    assert_eq!(locks.current(&slot).expect("current"), Some(lease.clone()));
    assert!(locks.release(&lease).expect("exact release"));
    assert!(
        locks
            .current(&slot)
            .expect("current after release")
            .is_none()
    );
}

#[test]
fn force_release_is_an_explicit_ownerless_escape_hatch() {
    let fleet = Arc::new(Fleet::join("lock_force_release", 1).expect("fleet"));
    let locks = Lock::new(fleet).expect("locks");
    let slot = key::<PoolSlot>(b"queue:1");
    locks
        .try_acquire(&slot, &LockOwner::from("worker-a"), Duration::from_secs(30))
        .expect("acquire")
        .acquired()
        .expect("winner");

    assert!(locks.force_release(&slot).expect("force release"));
    assert!(locks.current(&slot).expect("current").is_none());
}

#[test]
fn stale_lease_cannot_release_a_successor() {
    let fleet = Arc::new(Fleet::join("lock_stale", 1).expect("fleet"));
    let locks = Lock::new(fleet).expect("locks");
    let slot = key::<PoolSlot>(b"queue:1");
    let first = locks
        .try_acquire(
            &slot,
            &LockOwner::from("worker-a"),
            Duration::from_millis(1),
        )
        .expect("first acquire")
        .acquired()
        .expect("first winner");
    std::thread::sleep(Duration::from_millis(3));
    let second = locks
        .try_acquire(&slot, &LockOwner::from("worker-b"), Duration::from_secs(30))
        .expect("second acquire")
        .acquired()
        .expect("second winner");

    assert!(second.fencing_token() > first.fencing_token());
    assert!(!locks.release(&first).expect("stale release"));
    assert_eq!(locks.current(&slot).expect("current"), Some(second));
}

#[test]
fn restore_and_renew_use_the_caller_owner() {
    let fleet = Arc::new(Fleet::join("lock_restore", 1).expect("fleet"));
    let locks = Lock::new(fleet).expect("locks");
    let slot = key::<PoolSlot>(b"queue:1");
    let owner = LockOwner::from("portable-owner-token");
    let lease = locks
        .try_acquire(&slot, &owner, Duration::from_secs(1))
        .expect("acquire")
        .acquired()
        .expect("winner");

    assert_eq!(
        locks.restore(&slot, &owner).expect("restore"),
        Some(lease.clone())
    );
    assert!(
        locks
            .restore(&slot, &LockOwner::from("other"))
            .expect("wrong restore")
            .is_none()
    );
    let renewed = locks
        .renew_owned(&slot, &owner, Duration::from_secs(2))
        .expect("renew")
        .expect("current owner renews");
    assert_eq!(renewed.lock_id, lease.lock_id);
    assert!(renewed.expires_at_ms >= lease.expires_at_ms);
    assert!(renewed.state_revision > lease.state_revision);
}

#[test]
fn transitions_are_published_in_state_revision_order() {
    let fleet = Arc::new(Fleet::join("lock_events", 1).expect("fleet"));
    let actor = Lock::new(Arc::clone(&fleet)).expect("actor");
    let observer = Lock::new(fleet).expect("observer");
    let slot = key::<PoolSlot>(b"queue:1");
    let owner = LockOwner::from("worker-a");
    let lease = actor
        .try_acquire(&slot, &owner, Duration::from_secs(30))
        .expect("acquire")
        .acquired()
        .expect("winner");
    let renewed = actor
        .renew(&lease, Duration::from_secs(60))
        .expect("renew")
        .expect("renewed");
    assert!(actor.release(&renewed).expect("release"));

    let poll = observer.poll();
    assert_eq!(poll.events.len(), 3);
    assert!(matches!(
        poll.events[0].transition,
        LockTransition::Acquired(_)
    ));
    assert!(matches!(
        poll.events[1].transition,
        LockTransition::Renewed(_)
    ));
    assert!(matches!(
        poll.events[2].transition,
        LockTransition::Released { .. }
    ));
    assert!(poll.events.windows(2).all(
        |events| events[0].transition.state_revision() < events[1].transition.state_revision()
    ));
}

struct TinyLayout;

impl LockLayout for TinyLayout {
    const STATE_KIND: u8 = 60;
    const EVENT_RING_KIND: u8 = 61;
    const EVENT_RING_SPEC: RingSpec = RingSpec::per_node(2, 128);
}

#[test]
fn event_ring_wrap_does_not_erase_current_lock_state() {
    let fleet = Arc::new(Fleet::join("lock_wrap", 1).expect("fleet"));
    let locks = Lock::<TinyLayout>::with_layout(fleet).expect("locks");
    let retained = LockKey::from_parts("test.wrap", Bytes::from_static(b"retained"));
    let retained_owner = LockOwner::from("owner");
    let retained_lease = locks
        .try_acquire(&retained, &retained_owner, Duration::from_secs(30))
        .expect("retained acquire")
        .acquired()
        .expect("retained winner");

    for index in 0..4 {
        let key = LockKey::from_parts("test.wrap", Bytes::from(format!("other-{index}")));
        let lease = locks
            .try_acquire(&key, &retained_owner, Duration::from_secs(30))
            .expect("other acquire")
            .acquired()
            .expect("other winner");
        assert!(locks.release(&lease).expect("other release"));
    }

    assert_eq!(
        locks.current(&retained).expect("current"),
        Some(retained_lease)
    );
}

#[test]
fn concurrent_threads_produce_one_winner() {
    let fleet = Arc::new(Fleet::join("lock_threads", 1).expect("fleet"));
    let locks = Lock::new(fleet).expect("locks");
    let barrier = Arc::new(Barrier::new(8));
    let mut threads = Vec::new();
    for index in 0..8 {
        let locks = locks.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            let slot = key::<PoolSlot>(b"queue:1");
            let owner = LockOwner::from(format!("worker-{index}"));
            barrier.wait();
            matches!(
                locks
                    .try_acquire(&slot, &owner, Duration::from_secs(30))
                    .expect("acquire"),
                LockAcquire::Acquired(_)
            )
        }));
    }

    let winners = threads
        .into_iter()
        .map(|thread| thread.join().expect("thread"))
        .filter(|acquired| *acquired)
        .count();
    assert_eq!(winners, 1);
}

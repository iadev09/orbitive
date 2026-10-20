use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use super::*;

#[derive(Default)]
struct FakeClock {
    now: AtomicU64,
    failed: AtomicBool,
}

impl Clock for Arc<FakeClock> {
    fn now(&self) -> Result<u128, Error> {
        if self.failed.load(Ordering::Relaxed) {
            return Err(Error::ClockUnavailable);
        }
        Ok(u128::from(self.now.load(Ordering::Relaxed)))
    }
}

fn count(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn cas_budget() -> CasBudget {
    CasBudget::new(std::num::NonZeroUsize::new(64).unwrap())
}

fn geometry(
    slots: usize,
    shards: usize,
    probes: usize,
) -> Geometry {
    use std::num::NonZeroUsize;
    Geometry::new(
        NonZeroUsize::new(slots).unwrap(),
        NonZeroUsize::new(shards).unwrap(),
        NonZeroUsize::new(16).unwrap(),
        NonZeroUsize::new(probes).unwrap(),
    )
    .unwrap()
}

fn store(
    geometry: Geometry,
    burst: u64,
) -> (MemoryQuota, Arc<FakeClock>) {
    let clock = Arc::new(FakeClock::default());
    let policy =
        Policy::new(count(1), Duration::from_nanos(100), count(burst)).unwrap();
    (
        MemoryQuota::with_clock(
            geometry,
            policy,
            cas_budget(),
            Box::new(Arc::clone(&clock)),
        )
        .unwrap(),
        clock,
    )
}

fn key() -> Key<'static> {
    Key::new(b"test", b"subject").unwrap()
}

#[test]
fn clone_shares_debt_and_new_store_is_independent() {
    let (first, _) = store(geometry(4, 1, 4), 2);
    let clone = first.clone();
    let (second, _) = store(geometry(4, 1, 4), 2);
    assert_eq!(first.try_consume(key(), count(2)).unwrap(), Decision::Allowed);
    drop(first);
    assert!(matches!(
        clone.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
    assert_eq!(second.try_consume(key(), count(2)).unwrap(), Decision::Allowed);
    assert_eq!(clone.policy().burst(), count(2));
    assert_eq!(clone.geometry(), geometry(4, 1, 4));
}

#[test]
fn full_table_cannot_evict_debt_and_reclaims_only_after_full_replenishment() {
    let (quota, clock) = store(geometry(1, 1, 1), 2);
    assert_eq!(quota.try_consume(key(), count(2)).unwrap(), Decision::Allowed);
    for subject in 0..1_000_u64 {
        assert!(matches!(
            quota.try_consume(
                Key::new(b"churn", &subject.to_le_bytes()).unwrap(),
                count(1)
            ),
            Err(Error::CapacityFull)
        ));
    }
    clock.now.store(100, Ordering::Relaxed);
    let replacement = Key::new(b"other", b"key").unwrap();
    assert!(matches!(
        quota.try_consume(replacement, count(1)),
        Err(Error::CapacityFull)
    ));
    assert_eq!(
        quota.try_consume(key(), count(2)).unwrap(),
        Decision::Limited { retry_after: Duration::from_nanos(100) }
    );
    clock.now.store(200, Ordering::Relaxed);
    assert_eq!(
        quota.try_consume(replacement, count(2)).unwrap(),
        Decision::Allowed
    );
    assert!(matches!(
        quota.try_consume(key(), count(1)),
        Err(Error::CapacityFull)
    ));
}

#[test]
fn component_boundaries_and_full_bytes_distinguish_colliding_keys() {
    let (quota, _) = store(geometry(2, 1, 2), 1);
    let left = Key::new(b"ab", b"c").unwrap();
    let right = Key::new(b"a", b"bc").unwrap();
    assert_eq!(quota.try_consume(left, count(1)).unwrap(), Decision::Allowed);
    assert_eq!(quota.try_consume(right, count(1)).unwrap(), Decision::Allowed);
    assert!(matches!(
        quota.try_consume(left, count(1)).unwrap(),
        Decision::Limited { .. }
    ));
    assert!(matches!(
        quota.try_consume(right, count(1)).unwrap(),
        Decision::Limited { .. }
    ));
}

#[test]
fn lookup_checks_for_existing_debt_before_reusing_an_earlier_expired_slot() {
    let (quota, clock) = store(geometry(2, 1, 2), 2);
    // Three subjects in two start buckets guarantee a collision, independent
    // of the per-store random hash seed.
    let subjects = [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()];
    let mut pair = None;
    for (i, &left) in subjects.iter().enumerate() {
        for &right in &subjects[i + 1..] {
            if quota.location(Key::new(b"test", left).unwrap())
                == quota.location(Key::new(b"test", right).unwrap())
            {
                pair = Some((left, right));
            }
        }
    }
    let (left, right) = pair.unwrap();
    let left = Key::new(b"test", left).unwrap();
    let right = Key::new(b"test", right).unwrap();
    quota.try_consume(left, count(1)).unwrap();
    quota.try_consume(right, count(2)).unwrap();
    clock.now.store(100, Ordering::Relaxed);
    assert_eq!(
        quota.try_consume(right, count(2)).unwrap(),
        Decision::Limited { retry_after: Duration::from_nanos(100) }
    );
}

#[test]
fn probe_budget_can_refuse_a_new_key_even_with_free_slots() {
    let (quota, _) = store(geometry(2, 1, 1), 1);
    let subjects = [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()];
    let mut pair = None;
    for (i, &left) in subjects.iter().enumerate() {
        for &right in &subjects[i + 1..] {
            if quota.location(Key::new(b"test", left).unwrap())
                == quota.location(Key::new(b"test", right).unwrap())
            {
                pair = Some((left, right));
            }
        }
    }
    let (left, right) = pair.unwrap();
    quota.try_consume(Key::new(b"test", left).unwrap(), count(1)).unwrap();
    assert!(matches!(
        quota.try_consume(Key::new(b"test", right).unwrap(), count(1)),
        Err(Error::CapacityFull)
    ));
    let mut budget = WorkBudget::new(cas_budget());
    let occupied = quota.inner.shards[0]
        .slots
        .iter()
        .filter(|slot| {
            slot.pin(None, &mut budget)
                .unwrap()
                .is_some_and(|pin| pin.identity().occupied)
        })
        .count();
    assert_eq!(occupied, 1);
}

#[test]
fn invalid_requests_and_clock_failures_do_not_charge_or_replenish() {
    let (quota, clock) = store(geometry(1, 1, 1), 2);
    assert!(matches!(
        quota.try_consume(key(), count(3)),
        Err(Error::CostExceedsBurst)
    ));
    assert!(matches!(
        quota.try_consume(Key::new(b"test", &[0; 17]).unwrap(), count(1)),
        Err(Error::KeyTooLarge)
    ));
    quota.try_consume(key(), count(2)).unwrap();
    clock.failed.store(true, Ordering::Relaxed);
    assert!(matches!(
        quota.try_consume(key(), count(1)),
        Err(Error::ClockUnavailable)
    ));
    clock.failed.store(false, Ordering::Relaxed);
    clock.now.store(50, Ordering::Relaxed);
    assert!(matches!(
        quota.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { .. }
    ));
    clock.now.store(49, Ordering::Relaxed);
    assert!(matches!(
        quota.try_consume(key(), count(1)),
        Err(Error::ClockWentBackwards)
    ));
    clock.now.store(50, Ordering::Relaxed);
    assert_eq!(
        quota.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { retry_after: Duration::from_nanos(50) }
    );
}

#[test]
fn busy_and_poisoned_shards_never_grant_permission() {
    let (quota, _) = store(geometry(1, 1, 1), 1);
    let guard = quota.inner.shards[0].structure.lock().unwrap();
    assert!(matches!(quota.try_consume(key(), count(1)), Err(Error::Busy)));
    drop(guard);
    assert_eq!(quota.try_consume(key(), count(1)).unwrap(), Decision::Allowed);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = quota.inner.shards[0].structure.lock().unwrap();
        panic!("injected writer panic");
    }));
    assert!(result.is_err());
    assert!(matches!(
        quota.try_consume(key(), count(1)),
        Err(Error::CorruptState)
    ));
}

#[test]
fn concurrent_clones_cannot_overshoot_a_frozen_budget() {
    let (quota, _) = store(geometry(4, 1, 4), 64);
    let barrier = Arc::new(Barrier::new(8));
    let accepted = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..8 {
            let quota = quota.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(scope.spawn(move || {
                barrier.wait();
                let mut accepted = 0;
                for _ in 0..256 {
                    match quota.try_consume(key(), count(1)) {
                        Ok(Decision::Allowed) => accepted += 1,
                        Ok(Decision::Limited { .. }) | Err(Error::Busy) => {}
                        outcome => {
                            panic!("unexpected admission result: {outcome:?}")
                        }
                    }
                }
                accepted
            }));
        }
        workers.into_iter().map(|worker| worker.join().unwrap()).sum::<u64>()
    });
    assert!(accepted <= 64);
    // Busy calls consume nothing. Serial admission can fill only what remains.
    for _ in accepted..64 {
        assert_eq!(
            quota.try_consume(key(), count(1)).unwrap(),
            Decision::Allowed
        );
    }
    assert_eq!(
        quota.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { retry_after: Duration::from_nanos(100) }
    );
}

#[test]
fn repeated_reclamation_keeps_the_declared_backing_size() {
    let (quota, clock) = store(geometry(1, 1, 1), 1);
    for attempt in 0..10_000_u64 {
        clock.now.store(attempt * 100, Ordering::Relaxed);
        let bytes = attempt.to_le_bytes();
        let key = Key::new(b"churn", &bytes).unwrap();
        assert_eq!(
            quota.try_consume(key, count(1)).unwrap(),
            Decision::Allowed
        );
        assert!(matches!(
            quota.try_consume(key, count(1)).unwrap(),
            Decision::Limited { .. }
        ));
    }
    let shard = &quota.inner.shards[0];
    assert_eq!(shard.slots.len(), 1);
    let mut budget = WorkBudget::new(cas_budget());
    let pin = shard.slots[0].pin(None, &mut budget).unwrap().unwrap();
    assert_eq!(pin.identity().bytes.len(), 16);
}

#[test]
fn a_busy_shard_does_not_block_admission_in_another_shard() {
    let (quota, _) = store(geometry(8, 2, 4), 1);
    let (index, _) = quota.location(key());
    let _guard = quota.inner.shards[1 - index].structure.lock().unwrap();
    assert_eq!(quota.try_consume(key(), count(1)).unwrap(), Decision::Allowed);
}

#[test]
fn retry_conversion_overflow_does_not_consume_or_poison_the_shard() {
    let clock = Arc::new(FakeClock::default());
    let policy = Policy::new(count(1), Duration::MAX, count(2)).unwrap();
    let quota = MemoryQuota::with_clock(
        geometry(1, 1, 1),
        policy,
        cas_budget(),
        Box::new(clock),
    )
    .unwrap();
    assert_eq!(quota.try_consume(key(), count(2)).unwrap(), Decision::Allowed);
    let tat = Duration::MAX.as_nanos() * 2;
    assert!(matches!(
        quota.try_consume(key(), count(2)),
        Err(Error::ArithmeticOverflow)
    ));
    assert_eq!(quota.inner.shards[0].slots[0].tat.load(Ordering::Acquire), tat);
    assert_eq!(
        quota.try_consume(key(), count(1)).unwrap(),
        Decision::Limited { retry_after: Duration::MAX }
    );
    assert_eq!(quota.inner.shards[0].slots[0].tat.load(Ordering::Acquire), tat);
}

#[path = "allocations.rs"]
mod allocations;

#[path = "memory_cas.rs"]
mod cas;

#[path = "guards_memory.rs"]
mod guards;

#[cfg(unix)]
#[test]
fn public_constructor_uses_a_host_monotonic_clock() {
    let policy =
        Policy::new(count(1), Duration::from_secs(1), count(1)).unwrap();
    let quota =
        MemoryQuota::new(geometry(1, 1, 1), policy, cas_budget()).unwrap();
    assert_eq!(quota.try_consume(key(), count(1)).unwrap(), Decision::Allowed);
}

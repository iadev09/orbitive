use super::*;
use crate::test_allocator::measured;

fn guard(
    slots: usize,
    burst: u64,
) -> (LocalGuard, Arc<FakeClock>) {
    let clock = Arc::new(FakeClock::default());
    let quota = MemoryQuota::with_retention(
        geometry(slots, 1, slots),
        Policy::new(count(1), Duration::from_nanos(100), count(burst)).unwrap(),
        cas_budget(),
        Box::new(Arc::clone(&clock)),
        Retention::Guard,
    )
    .unwrap();
    (LocalGuard { quota }, clock)
}

fn subject(bytes: &[u8]) -> Key<'_> {
    Key::new(b"test", bytes).unwrap()
}

#[test]
fn guard_reclaims_live_debt_but_strict_memory_does_not() {
    let (guard, _) = guard(1, 1);
    let (strict, _) = store(geometry(1, 1, 1), 1);
    for quota in [&guard as &dyn QuotaStore, &strict as &dyn QuotaStore] {
        assert_eq!(
            quota.try_consume(key(), count(1)).unwrap(),
            Decision::Allowed
        );
        assert!(matches!(
            quota.try_consume(key(), count(1)),
            Ok(Decision::Limited { .. })
        ));
    }
    assert!(matches!(
        strict.try_consume(subject(b"new"), count(1)),
        Err(Error::CapacityFull)
    ));
    assert_eq!(
        guard.try_consume(subject(b"new"), count(1)).unwrap(),
        Decision::Allowed
    );
    // The evicted key receives a new allowance: this is protection, not quota.
    assert_eq!(guard.try_consume(key(), count(1)).unwrap(), Decision::Allowed);
}

#[test]
fn denied_access_keeps_a_key_hot_and_only_the_cold_candidate_is_replaced() {
    let (guard, clock) = guard(2, 1);
    guard.try_consume(subject(b"a"), count(1)).unwrap();
    clock.now.store(1, Ordering::Relaxed);
    guard.try_consume(subject(b"b"), count(1)).unwrap();
    clock.now.store(2, Ordering::Relaxed);
    assert!(matches!(
        guard.try_consume(subject(b"a"), count(1)),
        Ok(Decision::Limited { .. })
    ));
    guard.try_consume(subject(b"c"), count(1)).unwrap();
    let quota = &guard.quota;
    let mut work = WorkBudget::new(cas_budget());
    let retained = |key, work: &mut WorkBudget| {
        quota.inner.shards[0].slots.iter().any(|slot| {
            slot.pin(None, work).unwrap().is_some_and(|pin| pin.matches(key))
        })
    };
    assert!(retained(subject(b"a"), &mut work));
    assert!(!retained(subject(b"b"), &mut work));
    assert!(retained(subject(b"c"), &mut work));
}

#[test]
fn replenished_slots_take_precedence_over_colder_live_debt() {
    let (guard, clock) = guard(2, 10);
    guard.try_consume(subject(b"debt"), count(10)).unwrap();
    clock.now.store(1, Ordering::Relaxed);
    guard.try_consume(subject(b"short"), count(1)).unwrap();
    clock.now.store(102, Ordering::Relaxed);
    guard.try_consume(subject(b"new"), count(1)).unwrap();
    assert!(matches!(
        guard.try_consume(subject(b"debt"), count(10)),
        Ok(Decision::Limited { .. })
    ));
}

#[test]
fn guard_revalidates_recency_and_charge_after_candidate_selection() {
    for change_charge in [false, true] {
        let (guard, clock) = guard(1, 2);
        let quota = &guard.quota;
        quota.try_consume(key(), count(1)).unwrap();
        let shard = &quota.inner.shards[0];
        let pin = shard.slots[0]
            .pin(None, &mut WorkBudget::new(cas_budget()))
            .unwrap()
            .unwrap();
        let candidate = Candidate {
            index: 0,
            generation: pin.identity().generation,
            observed: Some(Observation {
                tat: pin.slot.tat.load(Ordering::Acquire),
                seen: pin.slot.last_seen.load(Ordering::Acquire),
            }),
        };
        drop(pin);
        if change_charge {
            // Same clock tick: charge validation must catch the change even
            // when the sampled access timestamp is unchanged.
            quota.try_consume(key(), count(1)).unwrap();
        } else {
            clock.now.store(1, Ordering::Relaxed);
            assert!(matches!(
                quota.try_consume(key(), count(2)),
                Ok(Decision::Limited { .. })
            ));
        }
        let _hold = shard.structure.try_lock().unwrap();
        let prepared = quota.inner.algorithm.prepare(count(1)).unwrap();
        assert!(matches!(
            quota.install(
                shard,
                candidate,
                subject(b"new"),
                &prepared,
                &mut WorkBudget::new(cas_budget())
            ),
            Err(Error::Busy)
        ));
        let pin = shard.slots[0]
            .pin(None, &mut WorkBudget::new(cas_budget()))
            .unwrap()
            .unwrap();
        assert!(pin.matches(key()));
    }
}

#[test]
fn guard_never_waits_for_structural_lock_or_live_pin() {
    let (guard, _) = guard(1, 1);
    guard.try_consume(key(), count(1)).unwrap();
    let shard = &guard.quota.inner.shards[0];
    let hold = shard.structure.try_lock().unwrap();
    assert!(matches!(
        guard.try_consume(subject(b"new"), count(1)),
        Err(Error::Busy)
    ));
    // Existing-key decisions do not need the structural hold.
    assert!(matches!(
        guard.try_consume(key(), count(1)),
        Ok(Decision::Limited { .. })
    ));
    drop(hold);
    let pin = shard.slots[0]
        .pin(None, &mut WorkBudget::new(cas_budget()))
        .unwrap()
        .unwrap();
    assert!(matches!(
        guard.try_consume(subject(b"new"), count(1)),
        Err(Error::Busy)
    ));
    assert!(pin.matches(key()));
    drop(pin);
    assert_eq!(
        guard.try_consume(subject(b"new"), count(1)).unwrap(),
        Decision::Allowed
    );
}

#[test]
fn guard_replacement_and_refusal_paths_allocate_nothing() {
    let (guard, clock) = guard(1, 1);
    let (outcomes, allocations) = measured(|| {
        let first = guard.try_consume(key(), count(1));
        clock.now.store(1, Ordering::Relaxed);
        let denied = guard.try_consume(key(), count(1));
        let replaced = guard.try_consume(subject(b"new"), count(1));
        let large =
            guard.try_consume(Key::new(b"test", &[0; 17]).unwrap(), count(1));
        (first, denied, replaced, large)
    });
    assert_eq!(allocations, 0);
    assert_eq!(outcomes.0.unwrap(), Decision::Allowed);
    assert!(matches!(outcomes.1, Ok(Decision::Limited { .. })));
    assert_eq!(outcomes.2.unwrap(), Decision::Allowed);
    assert!(matches!(outcomes.3, Err(Error::KeyTooLarge)));
}

#[test]
fn independent_local_guards_and_clones_keep_their_scope() {
    let (guard, _) = guard(1, 1);
    let cloned = guard.clone();
    let (independent, _) = self::guard(1, 1);
    guard.try_consume(key(), count(1)).unwrap();
    assert!(matches!(
        cloned.try_consume(key(), count(1)),
        Ok(Decision::Limited { .. })
    ));
    assert_eq!(
        independent.try_consume(key(), count(1)).unwrap(),
        Decision::Allowed
    );
}

#[test]
fn tiny_guard_cas_budget_refuses_without_mutating_identity() {
    let clock = Arc::new(FakeClock::default());
    let quota = MemoryQuota::with_retention(
        geometry(1, 1, 1),
        Policy::new(count(1), Duration::from_nanos(100), count(1)).unwrap(),
        CasBudget::new(std::num::NonZeroUsize::new(1).unwrap()),
        Box::new(clock),
        Retention::Guard,
    )
    .unwrap();
    assert!(matches!(quota.try_consume(key(), count(1)), Err(Error::Busy)));
    assert!(
        !quota.inner.shards[0].slots[0]
            .pin(None, &mut WorkBudget::new(cas_budget()))
            .unwrap()
            .unwrap()
            .identity()
            .occupied
    );
}

#[test]
fn retained_guard_key_keeps_atomic_weighted_burst_under_contention() {
    let (guard, _) = guard(1, 64);
    let start = Barrier::new(4);
    let accepted = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let guard = &guard;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    let mut accepted = 0;
                    for _ in 0..64 {
                        match guard.try_consume(key(), count(2)) {
                            Ok(Decision::Allowed) => accepted += 2,
                            Ok(Decision::Limited { .. }) | Err(Error::Busy) => {
                            }
                            result => panic!("unexpected outcome: {result:?}"),
                        }
                    }
                    accepted
                })
            })
            .collect();
        workers.into_iter().map(|worker| worker.join().unwrap()).sum::<u64>()
    });
    assert!(accepted <= 64);
    for _ in accepted..64 {
        assert_eq!(
            guard.try_consume(key(), count(1)).unwrap(),
            Decision::Allowed
        );
    }
    assert!(matches!(
        guard.try_consume(key(), count(1)),
        Ok(Decision::Limited { .. })
    ));
}

#[test]
fn recency_budget_exhaustion_never_leaves_an_unreported_charge() {
    let clock = Arc::new(FakeClock::default());
    let quota = MemoryQuota::with_retention(
        geometry(1, 1, 1),
        Policy::new(count(1), Duration::from_nanos(100), count(2)).unwrap(),
        CasBudget::new(std::num::NonZeroUsize::new(3).unwrap()),
        Box::new(Arc::clone(&clock)),
        Retention::Guard,
    )
    .unwrap();
    quota.try_consume(key(), count(1)).unwrap();
    clock.now.store(1, Ordering::Relaxed);
    // Pin, watermark and recency use the budget; no attempt remains for TAT.
    assert!(matches!(quota.try_consume(key(), count(1)), Err(Error::Busy)));
    assert_eq!(quota.inner.shards[0].slots[0].tat.load(Ordering::Acquire), 100);
    assert_eq!(
        quota.inner.shards[0].slots[0].last_seen.load(Ordering::Acquire),
        1
    );
}

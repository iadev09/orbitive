use std::num::NonZeroUsize;

use super::*;

fn work(attempts: usize) -> WorkBudget {
    WorkBudget::new(CasBudget::new(NonZeroUsize::new(attempts).unwrap()))
}

fn held(quota: &MemoryQuota) -> Pin<'_> {
    quota.inner.shards[0].slots[0].pin(None, &mut work(64)).unwrap().unwrap()
}

#[test]
fn explicit_budget_exhaustion_never_installs_or_charges() {
    let clock = Arc::new(FakeClock::default());
    let policy =
        Policy::new(count(1), Duration::from_nanos(100), count(1)).unwrap();
    let limit = CasBudget::new(NonZeroUsize::new(1).unwrap());
    let quota = MemoryQuota::with_clock(
        geometry(1, 1, 1),
        policy,
        limit,
        Box::new(clock),
    )
    .unwrap();
    assert_eq!(quota.cas_budget(), limit);
    assert_eq!(quota.clone().cas_budget(), limit);
    for _ in 0..4 {
        assert!(matches!(quota.try_consume(key(), count(1)), Err(Error::Busy)));
        assert!(!held(&quota).identity().occupied);
        assert_eq!(
            quota.inner.shards[0].slots[0].tat.load(Ordering::Acquire),
            0
        );
    }
    let mut budget = work(1);
    budget.take().unwrap();
    assert!(matches!(budget.take(), Err(Error::Busy)));
    assert_eq!(budget.remaining, 0);
}

#[test]
fn multiple_pins_allow_consumption_but_prevent_reuse() {
    let (quota, clock) = store(geometry(1, 1, 1), 1);
    quota.try_consume(key(), count(1)).unwrap();
    clock.now.store(100, Ordering::Relaxed);
    let pin = held(&quota);
    let generation = pin.identity().generation;
    let second = held(&quota);
    let other = Key::new(b"other", b"subject").unwrap();
    assert!(matches!(quota.try_consume(other, count(1)), Err(Error::Busy)));
    // Readers coexist; a pin is not a mutex around another same-key request.
    std::thread::scope(|scope| {
        assert_eq!(
            scope
                .spawn(|| quota.try_consume(key(), count(1)))
                .join()
                .unwrap()
                .unwrap(),
            Decision::Allowed
        );
    });
    assert_eq!(pin.identity().generation, generation);
    drop(second);
    drop(pin);
    assert_eq!(quota.inner.shards[0].slots[0].gate.load(Ordering::Acquire), 0);
    assert!(matches!(
        quota.try_consume(other, count(1)),
        Err(Error::CapacityFull)
    ));
    clock.now.store(200, Ordering::Relaxed);
    assert_eq!(quota.try_consume(other, count(1)).unwrap(), Decision::Allowed);
    assert_eq!(held(&quota).identity().generation, generation + 1);
}

#[test]
fn stale_generation_cannot_pin_or_charge_a_replacement_key() {
    let (quota, clock) = store(geometry(1, 1, 1), 1);
    quota.try_consume(key(), count(1)).unwrap();
    let generation = held(&quota).identity().generation;
    clock.now.store(100, Ordering::Relaxed);
    let other = Key::new(b"other", b"subject").unwrap();
    quota.try_consume(other, count(1)).unwrap();
    let slot = &quota.inner.shards[0].slots[0];
    assert!(matches!(
        slot.pin(Some(generation), &mut work(64)),
        Err(Error::Busy)
    ));
    let pin = held(&quota);
    assert!(!pin.matches(key()));
    assert!(pin.matches(other));
    assert_eq!(pin.identity().generation, generation + 1);
    drop(pin);
    assert_eq!(slot.gate.load(Ordering::Acquire), 0);
    assert_eq!(slot.tat.load(Ordering::Acquire), 200);
}

#[test]
fn a_charge_after_candidate_selection_prevents_reclamation() {
    let (quota, clock) = store(geometry(1, 1, 1), 1);
    quota.try_consume(key(), count(1)).unwrap();
    clock.now.store(100, Ordering::Relaxed);
    let pin = held(&quota);
    let candidate = Candidate {
        index: 0,
        generation: pin.identity().generation,
        observed: None,
    };
    assert_eq!(pin.slot.tat.load(Ordering::Acquire), 100);
    drop(pin);
    let shard = &quota.inner.shards[0];
    let _structure = shard.structure.lock().unwrap();
    // Existing-key admission must work even while installation is serialized.
    std::thread::scope(|scope| {
        assert_eq!(
            scope
                .spawn(|| quota.try_consume(key(), count(1)))
                .join()
                .unwrap()
                .unwrap(),
            Decision::Allowed
        );
    });
    let prepared = quota.inner.algorithm.prepare(count(1)).unwrap();
    let other = Key::new(b"other", b"subject").unwrap();
    assert!(matches!(
        quota.install(shard, candidate, other, &prepared, &mut work(64)),
        Err(Error::Busy)
    ));
    assert!(held(&quota).matches(key()));
    assert_eq!(shard.slots[0].tat.load(Ordering::Acquire), 200);
    assert_eq!(shard.slots[0].gate.load(Ordering::Acquire), 0);
}

#[test]
fn losing_a_tat_cas_never_overwrites_a_winners_debt() {
    let (quota, _) = store(geometry(1, 1, 1), 2);
    quota.try_consume(key(), count(1)).unwrap();
    let left = held(&quota);
    let right = held(&quota);
    let previous = left.slot.tat.load(Ordering::Acquire);
    let prepared = quota.inner.algorithm.prepare(count(1)).unwrap();
    let Outcome::Allowed { tat } =
        prepared.evaluate(0, Some(previous)).unwrap()
    else {
        panic!("expected available burst")
    };
    // Strong CAS makes this deliberately scheduled winner deterministic.
    let mut winner = work(1);
    winner.take().unwrap();
    assert_eq!(
        left.slot.tat.compare_exchange(
            previous,
            tat,
            Ordering::AcqRel,
            Ordering::Acquire
        ),
        Ok(previous)
    );
    let mut loser = work(1);
    assert!(!right.commit(previous, tat, &mut loser).unwrap());
    assert_eq!(loser.remaining, 0);
    assert!(matches!(
        right.commit(previous, tat, &mut loser),
        Err(Error::Busy)
    ));
    assert_eq!(left.slot.tat.load(Ordering::Acquire), 200);
    assert_eq!(
        prepared.evaluate(0, Some(200)).unwrap(),
        Outcome::Limited { debt: 100 }
    );
}

#[test]
fn pins_release_on_unwind_and_writer_panics_never_publish_partial_identity() {
    let (quota, _) = store(geometry(1, 1, 1), 1);
    quota.try_consume(key(), count(1)).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _pin = held(&quota);
        panic!("injected reader unwind");
    }));
    assert!(result.is_err());
    let slot = &quota.inner.shards[0].slots[0];
    assert_eq!(slot.gate.load(Ordering::Acquire), 0);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut writer = slot.reserve(&mut work(64)).unwrap();
        writer.dirty = true;
        // SAFETY: the writer gate is exclusively held; no pin can read this
        // deliberately partial record before the guard poisons it on unwind.
        unsafe {
            (*writer.slot.identity.get()).namespace_len = usize::MAX;
        }
        panic!("injected partial identity write");
    }));
    assert!(result.is_err());
    assert_eq!(slot.gate.load(Ordering::Acquire), POISONED);
    assert!(matches!(
        quota.try_consume(key(), count(1)),
        Err(Error::CorruptState)
    ));
}

#[test]
fn generation_exhaustion_retires_instead_of_wrapping() {
    let (quota, clock) = store(geometry(1, 1, 1), 1);
    quota.try_consume(key(), count(1)).unwrap();
    let slot = &quota.inner.shards[0].slots[0];
    let mut writer = slot.reserve(&mut work(64)).unwrap();
    writer.dirty = true;
    // SAFETY: exclusive writer ownership; retain all other valid identity data.
    unsafe {
        (*writer.slot.identity.get()).generation = u64::MAX;
    }
    writer.publish();
    clock.now.store(100, Ordering::Relaxed);
    assert!(matches!(
        quota.try_consume(Key::new(b"other", b"key").unwrap(), count(1)),
        Err(Error::CapacityFull)
    ));
    assert_eq!(slot.gate.load(Ordering::Acquire), RETIRED);
    assert!(slot.pin(None, &mut work(64)).unwrap().is_none());
    assert!(matches!(
        quota.try_consume(key(), count(1)),
        Err(Error::CapacityFull)
    ));
}

#[test]
fn reader_counter_exhaustion_does_not_wrap_into_writer_bits() {
    let (quota, _) = store(geometry(1, 1, 1), 1);
    let slot = &quota.inner.shards[0].slots[0];
    // No actual readers exist in this isolated fixture.
    slot.gate.store(READERS, Ordering::Release);
    assert!(matches!(slot.pin(None, &mut work(64)), Err(Error::Busy)));
    assert_eq!(slot.gate.load(Ordering::Acquire), READERS);
    slot.gate.store(0, Ordering::Release);
}

#[test]
fn racing_first_installations_create_one_identity_and_one_budget() {
    let (quota, _) = store(geometry(4, 1, 4), 1);
    let barrier = Barrier::new(8);
    let allowed = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    match quota.try_consume(key(), count(1)) {
                        Ok(Decision::Allowed) => 1,
                        Ok(Decision::Limited { .. }) | Err(Error::Busy) => 0,
                        result => panic!("unexpected result: {result:?}"),
                    }
                })
            })
            .collect();
        workers.into_iter().map(|worker| worker.join().unwrap()).sum::<usize>()
    });
    // All first callers may lose bounded races; the serial follow-up can
    // commit at most the untouched unit, never a second identity's budget.
    let follow_up = usize::from(matches!(
        quota.try_consume(key(), count(1)).unwrap(),
        Decision::Allowed
    ));
    assert_eq!(allowed + follow_up, 1);
    let mut budget = work(64);
    let matching = quota.inner.shards[0]
        .slots
        .iter()
        .filter(|slot| {
            slot.pin(None, &mut budget)
                .unwrap()
                .is_some_and(|pin| pin.matches(key()))
        })
        .count();
    assert_eq!(matching, 1);
}

struct OverlapClock {
    now: AtomicU64,
    block_next: AtomicBool,
    entered: Barrier,
    resume: Barrier,
}

impl Clock for Arc<OverlapClock> {
    fn now(&self) -> Result<u128, Error> {
        let sampled = self.now.load(Ordering::Acquire);
        if self.block_next.swap(false, Ordering::AcqRel) {
            self.entered.wait();
            self.resume.wait();
        }
        Ok(u128::from(sampled))
    }
}

fn overlap_store(burst: u64) -> (MemoryQuota, Arc<OverlapClock>) {
    let clock = Arc::new(OverlapClock {
        now: AtomicU64::new(0),
        block_next: AtomicBool::new(false),
        entered: Barrier::new(2),
        resume: Barrier::new(2),
    });
    let policy =
        Policy::new(count(1), Duration::from_nanos(100), count(burst)).unwrap();
    let quota = MemoryQuota::with_clock(
        geometry(1, 1, 1),
        policy,
        cas_budget(),
        Box::new(Arc::clone(&clock)),
    )
    .unwrap();
    (quota, clock)
}

#[test]
fn overlapping_clock_samples_do_not_clamp_or_report_false_regression() {
    let (quota, clock) = overlap_store(1);
    clock.now.store(100, Ordering::Release);
    clock.block_next.store(true, Ordering::Release);
    std::thread::scope(|scope| {
        let old =
            scope.spawn(|| quota.sample(&quota.inner.shards[0], &mut work(64)));
        clock.entered.wait();
        clock.now.store(200, Ordering::Release);
        let newer = quota.sample(&quota.inner.shards[0], &mut work(64));
        clock.resume.wait();
        assert_eq!(newer.unwrap(), 200);
        assert_eq!(old.join().unwrap().unwrap(), 100);
    });
    assert_eq!(quota.inner.shards[0].watermark.load(Ordering::Acquire), 200);
    clock.now.store(199, Ordering::Release);
    assert!(matches!(
        quota.sample(&quota.inner.shards[0], &mut work(64)),
        Err(Error::ClockWentBackwards)
    ));
}

#[test]
fn a_lost_tat_cas_reloads_state_and_takes_a_fresh_clock_sample() {
    let (quota, clock) = overlap_store(3);
    quota.try_consume(key(), count(1)).unwrap();
    clock.block_next.store(true, Ordering::Release);
    std::thread::scope(|scope| {
        let old = scope.spawn(|| quota.try_consume(key(), count(1)));
        clock.entered.wait();
        clock.now.store(200, Ordering::Release);
        let newer = quota.try_consume(key(), count(1));
        clock.resume.wait();
        assert_eq!(newer.unwrap(), Decision::Allowed);
        assert_eq!(old.join().unwrap().unwrap(), Decision::Allowed);
    });
    assert_eq!(quota.inner.shards[0].slots[0].tat.load(Ordering::Acquire), 400);
}

#[test]
fn existing_keys_do_not_take_the_structural_mutex() {
    let (quota, _) = store(geometry(1, 1, 1), 2);
    quota.try_consume(key(), count(1)).unwrap();
    let _hold = quota.inner.shards[0].structure.lock().unwrap();
    assert_eq!(quota.try_consume(key(), count(1)).unwrap(), Decision::Allowed);
}

struct AdvancingClock(AtomicU64);

impl Clock for AdvancingClock {
    fn now(&self) -> Result<u128, Error> {
        // Explicit synthetic time, not a timer or production refill policy.
        Ok(u128::from(self.0.fetch_add(100, Ordering::AcqRel)))
    }
}

#[test]
fn concurrent_unique_key_churn_preserves_identity_and_fixed_capacity() {
    let policy =
        Policy::new(count(1), Duration::from_nanos(100), count(1)).unwrap();
    let quota = MemoryQuota::with_clock(
        geometry(4, 1, 4),
        policy,
        cas_budget(),
        Box::new(AdvancingClock(AtomicU64::new(0))),
    )
    .unwrap();
    quota.try_consume(key(), count(1)).unwrap();
    let barrier = Barrier::new(8);
    std::thread::scope(|scope| {
        for worker in 0..8_u64 {
            let quota = &quota;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for request in 0..2_000_u64 {
                    let subject = (worker * 2_000 + request).to_le_bytes();
                    match quota.try_consume(
                        Key::new(b"churn", &subject).unwrap(),
                        count(1),
                    ) {
                        Ok(Decision::Allowed)
                        | Err(Error::Busy | Error::CapacityFull) => {}
                        other => panic!("invalid churn outcome: {other:?}"),
                    }
                }
            });
        }
    });
    let shard = &quota.inner.shards[0];
    assert_eq!(shard.slots.len(), 4);
    for slot in &shard.slots {
        let pin = slot.pin(None, &mut work(64)).unwrap().unwrap();
        let id = pin.identity();
        assert_eq!(id.bytes.len(), 16);
        if id.occupied {
            assert!(id.generation > 0);
            if &id.bytes[..id.namespace_len] == b"churn" {
                assert_eq!(id.subject_len, 8);
                let value = u64::from_le_bytes(
                    id.bytes[id.namespace_len..id.namespace_len + 8]
                        .try_into()
                        .unwrap(),
                );
                assert!(value < 16_000);
            } else {
                assert!(pin.matches(key()));
            }
        }
        drop(pin);
        assert_eq!(slot.gate.load(Ordering::Acquire), 0);
    }
}

#[test]
fn weighted_concurrent_commits_never_partially_charge_or_exceed_burst() {
    for cost in [2, 3, 5] {
        let (quota, _) = store(geometry(1, 1, 1), 31);
        quota.try_consume(key(), count(1)).unwrap();
        let barrier = Barrier::new(8);
        let accepted =
            std::thread::scope(|scope| {
                let workers: Vec<_> =
                    (0..8)
                        .map(|_| {
                            scope.spawn(|| {
                barrier.wait();
                let mut accepted = 0;
                for _ in 0..128 {
                    match quota.try_consume(key(), count(cost)) {
                        Ok(Decision::Allowed) => accepted += cost,
                        Ok(Decision::Limited { .. }) | Err(Error::Busy) => {},
                        other => panic!("invalid weighted outcome: {other:?}"),
                    }
                }
                accepted
            })
                        })
                        .collect();
                workers
                    .into_iter()
                    .map(|worker| worker.join().unwrap())
                    .sum::<u64>()
            });
        assert_eq!(accepted % cost, 0);
        assert!(accepted <= 30);
        for _ in accepted + 1..31 {
            assert_eq!(
                quota.try_consume(key(), count(1)).unwrap(),
                Decision::Allowed
            );
        }
        assert!(matches!(
            quota.try_consume(key(), count(1)).unwrap(),
            Decision::Limited { .. }
        ));
    }
}

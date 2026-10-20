use super::*;
use crate::test_allocator::measured;

fn guard(
    fixture: &Fixture,
    clock: &Arc<TestClock>,
    slots: usize,
    burst: u64,
) -> FleetGuard {
    FleetGuard {
        quota: ShmQuota::with_retention(
            fixture.0,
            geometry(slots),
            (policy(burst), Retention::Guard),
            attempts(64),
            options(3, 3),
            Box::new(Arc::clone(clock)),
            [7; 32],
        )
        .unwrap(),
    }
}

fn subject(bytes: &[u8]) -> Key<'_> {
    Key::new(b"test", bytes).unwrap()
}

#[test]
fn guard_opens_share_live_debt_and_cold_replacement() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let first = guard(&fixture, &clock, 2, 1);
    let second = guard(&fixture, &clock, 2, 1);
    first.try_consume(subject(b"a"), count(1)).unwrap();
    clock.0.store(1, Ordering::Relaxed);
    second.try_consume(subject(b"b"), count(1)).unwrap();
    clock.0.store(2, Ordering::Relaxed);
    assert!(matches!(
        second.try_consume(subject(b"a"), count(1)),
        Ok(Decision::Limited { .. })
    ));
    second.try_consume(subject(b"c"), count(1)).unwrap();
    assert!(matches!(
        first.try_consume(subject(b"a"), count(1)),
        Ok(Decision::Limited { .. })
    ));
    let inner = &first.quota.inner;
    let mut work = budget();
    let mut retained_b = false;
    let mut retained_c = false;
    for index in 0..2 {
        let pin = inner.pin(index, None, &mut work).unwrap().unwrap();
        retained_b |= pin.matches(subject(b"b")).unwrap();
        retained_c |= pin.matches(subject(b"c")).unwrap();
    }
    assert!(!retained_b);
    assert!(retained_c);
}

#[test]
fn mode_mismatch_is_refused_in_both_directions_without_reset() {
    // These shapes have equal byte size: mode validation must reject the
    // open even when mapping length alone cannot distinguish the formats.
    let shape = Geometry::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(16).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    )
    .unwrap();
    assert_eq!(
        Layout::new(shape, 3).unwrap().size,
        Layout::with_retention(shape, 3, Retention::Guard).unwrap().size,
    );
    for guard_first in [true, false] {
        let fixture = Fixture::new();
        let clock = Arc::new(TestClock::default());
        let open = |retention| {
            ShmQuota::with_retention(
                fixture.0,
                shape,
                (policy(1), retention),
                attempts(64),
                options(3, 3),
                Box::new(Arc::clone(&clock)),
                [7; 32],
            )
        };
        let retention =
            if guard_first { Retention::Guard } else { Retention::Strict };
        let first = open(retention).unwrap();
        first.try_consume(key(), count(1)).unwrap();
        let other_mode =
            if guard_first { Retention::Strict } else { Retention::Guard };
        assert!(matches!(open(other_mode), Err(Error::IncompatibleState)));
        assert!(matches!(
            first.try_consume(key(), count(1)),
            Ok(Decision::Limited { .. })
        ));
    }
}

#[test]
fn guard_reopen_preserves_recency_and_debt() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let first = guard(&fixture, &clock, 2, 1);
    first.try_consume(subject(b"a"), count(1)).unwrap();
    clock.0.store(1, Ordering::Relaxed);
    first.try_consume(subject(b"b"), count(1)).unwrap();
    clock.0.store(2, Ordering::Relaxed);
    first.try_consume(subject(b"a"), count(1)).unwrap();
    drop(first);
    let reopened = guard(&fixture, &clock, 2, 1);
    reopened.try_consume(subject(b"c"), count(1)).unwrap();
    assert!(matches!(
        reopened.try_consume(subject(b"a"), count(1)),
        Ok(Decision::Limited { .. })
    ));
}

#[test]
fn guard_revalidates_charge_and_recency_after_freezing() {
    for change_charge in [false, true] {
        let fixture = Fixture::new();
        let clock = Arc::new(TestClock::default());
        let first = guard(&fixture, &clock, 1, 2);
        let peer = guard(&fixture, &clock, 1, 2);
        first.try_consume(key(), count(1)).unwrap();
        let quota = &first.quota;
        let inner = &quota.inner;
        let pin = inner.pin(0, None, &mut budget()).unwrap().unwrap();
        let candidate = Candidate {
            index: 0,
            generation: pin.generation,
            observed: Some(Observation {
                tat: pin.bank().tat.load(Ordering::Acquire),
                seen: inner
                    .last_seen(0, (pin.generation & 1) as usize)
                    .load(Ordering::Acquire),
            }),
        };
        drop(pin);
        if change_charge {
            peer.try_consume(key(), count(1)).unwrap();
        } else {
            clock.0.store(1, Ordering::Relaxed);
            assert!(matches!(
                peer.try_consume(key(), count(2)),
                Ok(Decision::Limited { .. })
            ));
        }
        let mut work = budget();
        let _hold = inner.lock_shard(0, &mut work).unwrap();
        let prepared = inner.algorithm.prepare(count(1)).unwrap();
        assert!(matches!(
            quota.install(0, candidate, other(), &prepared, &mut work),
            Err(Error::Busy)
        ));
        let pin = inner.pin(0, None, &mut budget()).unwrap().unwrap();
        assert!(pin.matches(key()).unwrap());
    }
}

#[test]
fn guard_replacement_never_waits_for_a_live_pin_or_shard_owner() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let first = guard(&fixture, &clock, 1, 1);
    let peer = guard(&fixture, &clock, 1, 1);
    first.try_consume(key(), count(1)).unwrap();
    let inner = &first.quota.inner;
    let pin = inner.pin(0, None, &mut budget()).unwrap().unwrap();
    assert!(matches!(peer.try_consume(other(), count(1)), Err(Error::Busy)));
    assert!(pin.matches(key()).unwrap());
    drop(pin);
    let _hold = inner.lock_shard(0, &mut budget()).unwrap();
    assert!(matches!(peer.try_consume(other(), count(1)), Err(Error::Busy)));
    assert!(matches!(
        peer.try_consume(key(), count(1)),
        Ok(Decision::Limited { .. })
    ));
}

#[test]
fn guard_publication_keeps_recency_in_the_same_bank_as_identity() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let guard = guard(&fixture, &clock, 1, 1);
    guard.try_consume(key(), count(1)).unwrap();
    let inner = &guard.quota.inner;
    let generation = inner.slot(0).control.load(Ordering::SeqCst);
    let _hold = inner.lock_shard(0, &mut budget()).unwrap();
    {
        let _frozen = inner.freeze(0, generation, &mut budget()).unwrap();
        let inactive = ((generation + 1) & 1) as usize;
        inner.last_seen(0, inactive).store(999, Ordering::Relaxed);
        inner.slot(0).banks[inactive].tat.store(999, Ordering::Relaxed);
        // Cancelling an unpublished replacement must preserve the active bank.
    }
    let pin = inner.pin(0, None, &mut budget()).unwrap().unwrap();
    assert!(pin.matches(key()).unwrap());
    assert_eq!(pin.bank().tat.load(Ordering::Acquire), 100);
    assert_eq!(
        inner.last_seen(0, (generation & 1) as usize).load(Ordering::Acquire),
        0
    );
}

#[test]
fn guard_admission_including_debt_eviction_allocates_nothing() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let guard = guard(&fixture, &clock, 1, 1);
    let (outcomes, allocations) = measured(|| {
        let first = guard.try_consume(key(), count(1));
        clock.0.store(1, Ordering::Relaxed);
        let denied = guard.try_consume(key(), count(1));
        let replaced = guard.try_consume(other(), count(1));
        (first, denied, replaced)
    });
    assert_eq!(allocations, 0);
    assert_eq!(outcomes.0.unwrap(), Decision::Allowed);
    assert!(matches!(outcomes.1, Ok(Decision::Limited { .. })));
    assert_eq!(outcomes.2.unwrap(), Decision::Allowed);
}

#[test]
fn strict_fleet_policy_keeps_live_debt_instead_of_replacing_it() {
    let fixture = Fixture::new();
    let quota: crate::FleetQuota =
        fixture.open(Arc::new(TestClock::default()), 1, 1, 0);
    quota.try_consume(key(), count(1)).unwrap();
    assert!(matches!(
        quota.try_consume(other(), count(1)),
        Err(Error::CapacityFull)
    ));
    assert!(matches!(
        quota.try_consume(key(), count(1)),
        Ok(Decision::Limited { .. })
    ));
}

#[test]
fn guard_sigkill_before_and_after_commit_preserves_the_selected_bank() {
    for mode in [
        "guard-frozen",
        "guard-key",
        "guard-metadata",
        "guard-inactive",
        "guard-publish",
    ] {
        let fixture = Fixture::new();
        let clock = Arc::new(TestClock::default());
        let guard = guard(&fixture, &clock, 1, 2);
        guard.try_consume(key(), count(2)).unwrap();
        clock.0.store(1, Ordering::Relaxed);
        let mut peer = child(&fixture, mode);
        peer.kill();
        // Before descriptor publication the old debt and recency survive;
        // after it the complete new identity, charge and recency survive.
        let inner = &guard.quota.inner;
        let committed = mode == "guard-publish";
        let generation = if committed { 2 } else { 1 };
        let active = (generation & 1) as usize;
        assert_eq!(
            inner.slot(0).banks[active].tat.load(Ordering::Acquire),
            if committed { 101 } else { 200 }
        );
        assert_eq!(
            inner.last_seen(0, active).load(Ordering::Acquire),
            if committed { 1 } else { 0 }
        );
        let expected = if committed { other() } else { key() };
        // Recovering FROZEN may thaw, but it must not reset the committed bank.
        assert!(matches!(
            guard.try_consume(expected, count(2)),
            Ok(Decision::Limited { .. })
        ));
        let pin =
            inner.pin(0, Some(generation), &mut budget()).unwrap().unwrap();
        assert!(pin.matches(expected).unwrap());
    }
}

#[test]
fn guard_dead_pin_requires_exact_owner_release_before_replacement() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let guard = guard(&fixture, &clock, 1, 2);
    guard.try_consume(key(), count(2)).unwrap();
    let mut peer = child(&fixture, "guard-pin");
    assert!(matches!(guard.try_consume(other(), count(1)), Err(Error::Busy)));
    peer.kill();
    assert_eq!(
        guard.try_consume(other(), count(1)).unwrap(),
        Decision::Allowed
    );
}

#[test]
fn guard_cross_process_same_key_does_not_overshoot_without_eviction() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let guard = guard(&fixture, &clock, 1, 2);
    guard.try_consume(key(), count(1)).unwrap();
    let mut peer = child(&fixture, "guard-race");
    peer.resume(b"start\n");
    let ours = match guard.try_consume(key(), count(1)) {
        Ok(Decision::Allowed) => 1,
        Ok(Decision::Limited { .. }) | Err(Error::Busy) => 0,
        result => panic!("unexpected outcome: {result:?}"),
    };
    let mut output = String::new();
    peer.output.read_to_string(&mut output).unwrap();
    assert!(peer.process.wait().unwrap().success());
    let theirs: u64 = output
        .lines()
        .find_map(|line| line.strip_prefix("QUOTA_RESULT:"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(ours + theirs <= 1);
}

#[test]
fn recency_exhaustion_is_busy_without_committing_a_charge() {
    let fixture = Fixture::new();
    let clock = Arc::new(TestClock::default());
    let guard = guard(&fixture, &clock, 1, 2);
    guard.try_consume(key(), count(1)).unwrap();
    let small = ShmQuota::with_retention(
        fixture.0,
        geometry(1),
        (policy(2), Retention::Guard),
        attempts(3),
        options(3, 3),
        Box::new(Arc::clone(&clock)),
        [7; 32],
    )
    .unwrap();
    clock.0.store(1, Ordering::Relaxed);
    assert!(matches!(small.try_consume(key(), count(1)), Err(Error::Busy)));
    let inner = &guard.quota.inner;
    let pin = inner.pin(0, None, &mut budget()).unwrap().unwrap();
    assert_eq!(pin.bank().tat.load(Ordering::Acquire), 100);
    assert_eq!(
        inner
            .last_seen(0, (pin.generation & 1) as usize)
            .load(Ordering::Acquire),
        1
    );
}

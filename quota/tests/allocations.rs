use super::*;
use crate::test_allocator::measured;

#[test]
fn admission_paths_do_not_allocate() {
    let (quota, clock) = store(geometry(1, 1, 1), 2);
    let other = Key::new(b"other", b"subject").unwrap();
    let oversized = Key::new(b"test", &[0; 17]).unwrap();
    let (results, allocations) = measured(|| {
        [
            quota.try_consume(key(), count(1)),
            quota.try_consume(key(), count(1)),
            quota.try_consume(key(), count(1)),
            quota.try_consume(other, count(1)),
            quota.try_consume(oversized, count(1)),
            quota.try_consume(key(), count(3)),
        ]
    });
    assert_eq!(allocations, 0);
    let [allowed, existing, limited, full, large, cost] = results;
    assert_eq!(allowed.unwrap(), Decision::Allowed);
    assert_eq!(existing.unwrap(), Decision::Allowed);
    assert!(matches!(limited.unwrap(), Decision::Limited { .. }));
    assert!(matches!(full, Err(Error::CapacityFull)));
    assert!(matches!(large, Err(Error::KeyTooLarge)));
    assert!(matches!(cost, Err(Error::CostExceedsBurst)));

    clock.now.store(200, Ordering::Relaxed);
    let (result, allocations) = measured(|| quota.try_consume(other, count(1)));
    assert_eq!(result.unwrap(), Decision::Allowed);
    assert_eq!(allocations, 0);

    let mut budget = WorkBudget::new(cas_budget());
    let _guard = quota.inner.shards[0].slots[0].reserve(&mut budget).unwrap();
    let (result, allocations) = measured(|| quota.try_consume(other, count(1)));
    assert!(matches!(result, Err(Error::Busy)));
    assert_eq!(allocations, 0);
}

#[cfg(unix)]
#[test]
fn public_admission_with_host_clock_does_not_allocate() {
    let policy =
        Policy::new(count(1), Duration::from_secs(1), count(2)).unwrap();
    let quota =
        MemoryQuota::new(geometry(1, 1, 1), policy, cas_budget()).unwrap();
    let (results, allocations) = measured(|| {
        [quota.try_consume(key(), count(1)), quota.try_consume(key(), count(1))]
    });
    for result in results {
        assert_eq!(result.unwrap(), Decision::Allowed);
    }
    assert_eq!(allocations, 0);
}

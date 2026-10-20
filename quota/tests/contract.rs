use super::*;

fn count(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

#[test]
fn geometry_bounds_probing_within_an_even_shard() {
    let geometry =
        Geometry::new(count(16), count(4), count(32), count(4)).unwrap();
    assert_eq!(geometry.slots().get(), 16);
    assert_eq!(geometry.shards().get(), 4);
    assert_eq!(geometry.key_bytes().get(), 32);
    assert_eq!(geometry.probes().get(), 4);
    for (slots, shards, probes) in [(15, 4, 1), (4, 8, 1), (16, 4, 5)] {
        assert!(matches!(
            Geometry::new(
                count(slots),
                count(shards),
                count(32),
                count(probes)
            ),
            Err(Error::InvalidGeometry),
        ));
    }
}

#[test]
fn policy_requires_an_explicit_positive_period() {
    let units = NonZeroU64::new(3).unwrap();
    let burst = NonZeroU64::new(2).unwrap();
    assert!(matches!(
        Policy::new(units, Duration::ZERO, burst),
        Err(Error::InvalidPeriod)
    ));
    let period = Duration::from_secs(1);
    let policy = Policy::new(units, period, burst).unwrap();
    assert_eq!(policy.units(), units);
    assert_eq!(policy.period(), period);
    assert_eq!(policy.burst(), burst);
}

#[test]
fn key_component_boundaries_are_identity_and_debug_hides_values() {
    assert!(matches!(Key::new(b"", b"value"), Err(Error::EmptyKey)));
    assert!(matches!(Key::new(b"scope", b""), Err(Error::EmptyKey)));
    assert_ne!(Key::new(b"ab", b"c").unwrap(), Key::new(b"a", b"bc").unwrap());
    let key = Key::new(b"test-scope", b"test-subject").unwrap();
    assert_eq!(key.namespace(), b"test-scope");
    assert_eq!(key.subject(), b"test-subject");
    let debug = format!("{key:?}");
    assert!(!debug.contains("test-scope"));
    assert!(!debug.contains("test-subject"));
}

use std::sync::Arc;

use orbit_core::Fleet;
use orbit_counter::{Counter, Error};

fn counter() -> Counter {
    Counter::new(Arc::new(Fleet::join("counter-test", 1).expect("fleet"))).expect("counter")
}

#[test]
fn missing_key_starts_at_zero_for_updates() {
    let counter = counter();

    assert_eq!(counter.get("hits").expect("get"), None);
    assert_eq!(counter.increment("hits", 2).expect("increment"), 2);
    assert_eq!(counter.decrement("hits", 5).expect("decrement"), -3);
    assert_eq!(counter.get("hits").expect("get"), Some(-3));
}

#[test]
fn clones_share_atomic_values() {
    let counter = counter();
    let peer = counter.clone();

    assert_eq!(counter.increment("shared", 1).expect("increment"), 1);
    assert_eq!(peer.increment("shared", 4).expect("increment"), 5);
    assert_eq!(counter.get("shared").expect("get"), Some(5));
}

#[test]
fn reset_retains_key_at_zero() {
    let counter = counter();

    counter.increment("reset", 9).expect("increment");
    counter.reset("reset").expect("reset");
    assert_eq!(counter.get("reset").expect("get"), Some(0));
    counter.reset("missing").expect("missing reset");
    assert_eq!(counter.get("missing").expect("get"), Some(0));
}

#[test]
fn overflow_keeps_previous_value() {
    let counter = counter();

    counter.increment("max", i64::MAX).expect("initialize max");
    assert!(matches!(counter.increment("max", 1), Err(Error::Overflow)));
    assert_eq!(counter.get("max").expect("get"), Some(i64::MAX));
}

#[test]
fn negative_amount_is_rejected() {
    let counter = counter();

    assert!(matches!(
        counter.increment("bad", -1),
        Err(Error::NegativeAmount(-1))
    ));
    assert_eq!(counter.get("bad").expect("get"), None);
}

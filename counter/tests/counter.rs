use std::sync::Arc;

use orbit_atomic::{Error as AtomicError, Orbitals, orbital};
use orbit_core::Fleet;
use orbit_counter::{Counter, Error};

fn orbitals() -> Orbitals {
    Orbitals::new(Arc::new(Fleet::join("counter-test", 1).expect("fleet")))
        .expect("orbitals")
}

fn counter() -> Counter {
    Counter::new(orbitals())
}

#[test]
fn missing_key_starts_at_zero_for_updates() {
    let counter = counter();

    assert_eq!(counter.get("hits").expect("get"), None);
    assert_eq!(
        counter.get("hits").expect("get"),
        None,
        "a read installs nothing"
    );
    assert_eq!(counter.increment("hits", 2).expect("increment"), 2);
    assert_eq!(counter.decrement("hits", 5).expect("decrement"), -3);
    assert_eq!(counter.get("hits").expect("get"), Some(-3));
}

#[test]
fn a_handle_and_the_key_are_one_value() {
    let counter = counter();
    let handle = counter.open("shared").expect("open");

    assert_eq!(handle.increment(1).expect("increment"), 1);
    assert_eq!(counter.increment("shared", 4).expect("increment"), 5);
    assert_eq!(handle.clone().decrement(2).expect("decrement"), 3);
    assert_eq!(counter.get("shared").expect("get"), Some(3));
    // Already written, so this returns without parking.
    assert_eq!(handle.wait(0).expect("wait"), 3);
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
    let max = counter.open("max").expect("open");

    max.increment(i64::MAX).expect("initialize max");
    assert!(matches!(max.increment(1), Err(Error::Overflow)));
    assert_eq!(max.get(), i64::MAX);

    max.reset();
    max.decrement(i64::MAX).expect("to -max");
    max.decrement(1).expect("to min");
    assert!(matches!(counter.decrement("max", 1), Err(Error::Overflow)));
    assert_eq!(counter.get("max").expect("get"), Some(i64::MIN));
}

#[test]
fn negative_amount_is_rejected() {
    let counter = counter();

    assert!(matches!(
        counter.increment("bad", -1),
        Err(Error::NegativeAmount(-1))
    ));
    assert!(matches!(
        counter.decrement("bad", i64::MIN),
        Err(Error::NegativeAmount(i64::MIN))
    ));
    assert_eq!(counter.get("bad").expect("get"), None);
}

orbital! {
    pub Requests: i64 = 0, "requests", kept
    pub Plain: i64 = 0, "plain"
    pub Ready: bool = false, "ready", kept
}

/// One namespace: a key and a kept `i64` declaration of the same name are
/// the same value, and a key declared as anything else is not a counter.
#[test]
fn a_counter_is_its_kept_declaration() {
    let orbitals = orbitals();
    let counter = Counter::new(orbitals.clone());
    let requests = Requests::open(&orbitals).expect("declare");

    requests.fetch_add(3);
    assert_eq!(counter.increment("requests", 2).expect("increment"), 5);
    assert_eq!(requests.load(), 5);

    Plain::open(&orbitals).expect("declare");
    Ready::open(&orbitals).expect("declare");
    assert!(matches!(
        counter.get("plain"),
        Err(Error::NotCounter(AtomicError::LifetimeMismatch { .. }))
    ));
    assert!(matches!(
        counter.increment("ready", 1),
        Err(Error::NotCounter(AtomicError::KindMismatch { .. }))
    ));
    assert!(matches!(counter.get(""), Err(Error::KeyEmpty)));
}

#[test]
fn a_counter_survives_the_owner_reset() {
    let orbitals = orbitals();
    let counter = Counter::new(orbitals.clone());
    counter.increment("kept", 7).expect("increment");

    orbitals.reset_all().expect("reset");

    assert_eq!(counter.get("kept").expect("get"), Some(7));
    assert_eq!(counter.increment("kept", 1).expect("increment"), 8);
}

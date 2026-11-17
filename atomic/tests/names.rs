//! Declarations by name, over an in-memory table.

use std::sync::Arc;

use orbit_atomic::{
    ATOMIC_CAPACITY, ATOMIC_NAME_MAX, Declared, Error, Kind, Lifetime,
    Orbitals, orbital,
};
use orbit_core::Fleet;

fn orbitals() -> Orbitals {
    Orbitals::new(Arc::new(Fleet::join("example", 1).unwrap())).unwrap()
}

/// A worker restarted under adoption declares again; that must not
/// consume a slot. One slot for any number of declarations, so the rest
/// of the table is still there to allocate.
#[test]
fn declaring_a_name_again_allocates_nothing() {
    let orbitals = orbitals();
    let first = orbitals.declare("hits", 3_i64).unwrap();
    first.fetch_add(1);
    for _ in 0..ATOMIC_CAPACITY {
        let again = orbitals.clone().declare("hits", 100_i64).unwrap();
        assert_eq!(again.id(), first.id());
    }
    assert_eq!(first.load(), 4, "a later initial value is ignored");

    let rest: Vec<_> = (1..ATOMIC_CAPACITY)
        .map(|_| orbitals.allocate(0_u64).unwrap())
        .collect();
    assert_eq!(rest.len(), ATOMIC_CAPACITY - 1);
    assert!(matches!(orbitals.allocate(0_u64), Err(Error::Full { .. })));
}

#[test]
fn a_name_keeps_its_type_and_lifetime() {
    let orbitals = orbitals();
    orbitals.declare("ready", false).unwrap();
    assert!(matches!(
        orbitals.declare("ready", 0_u64),
        Err(Error::KindMismatch { expected: Kind::U64, .. })
    ));
    assert!(matches!(
        orbitals.declare_kept("ready", false),
        Err(Error::LifetimeMismatch { expected: Lifetime::Kept, .. })
    ));
    assert!(matches!(orbitals.declare("", 0_i64), Err(Error::NameEmpty)));
    let long = "n".repeat(ATOMIC_NAME_MAX + 1);
    assert!(matches!(
        orbitals.declare(&long, 0_i64),
        Err(Error::NameTooLong { max: ATOMIC_NAME_MAX, .. })
    ));
}

#[test]
fn a_reset_forgets_plain_names_and_keeps_kept_ones() {
    let orbitals = orbitals();
    let anonymous = orbitals.allocate(9_u64).unwrap().id();
    let plain = orbitals.declare("plain", 0_i64).unwrap();
    let kept = orbitals.declare_kept("kept", 0.5_f64).unwrap();
    plain.fetch_add(7);
    kept.fetch_add(1.0);
    let (plain_id, kept_id) = (plain.id(), kept.id());
    drop((plain, kept));

    orbitals.reset_all().unwrap();

    for id in [anonymous, plain_id, kept_id] {
        assert!(matches!(orbitals.open::<u64>(id), Err(Error::Stale(_))));
    }
    assert_eq!(orbitals.declare("plain", 0_i64).unwrap().load(), 0);
    let kept = orbitals.declare_kept("kept", 0.0_f64).unwrap();
    assert_eq!(kept.load(), 1.5);
    assert_ne!(kept.id(), kept_id);
    // Twice, so a kept value also survives the move a reset made.
    orbitals.reset_all().unwrap();
    assert_eq!(orbitals.declare_kept("kept", 0.0_f64).unwrap().load(), 1.5);
}

orbital! {
    /// Counted by every worker.
    pub Hits: i64 = 0, "macro.hits"
    pub(crate) Visitors: u64 = 10, "macro.visitors", kept
}

fn open_declared<D: Declared>(orbitals: &Orbitals) -> D {
    D::open(orbitals).unwrap()
}

#[test]
fn the_macro_declares_types() {
    let orbitals = orbitals();
    let hits = Hits::open(&orbitals).unwrap();
    hits.fetch_add(2);
    let same: Hits = open_declared(&orbitals);
    assert_eq!(same.load(), 2);
    assert_eq!(same.id(), orbitals.declare("macro.hits", 0_i64).unwrap().id());

    assert_eq!(<Visitors as Declared>::NAME, "macro.visitors");
    assert_eq!(<Visitors as Declared>::INITIAL, 10);
    assert_eq!(<Visitors as Declared>::LIFETIME, Lifetime::Kept);
    Visitors::open(&orbitals).unwrap().fetch_add(1);
    orbitals.reset_all().unwrap();
    assert_eq!(Visitors::open(&orbitals).unwrap().load(), 11);
    assert_eq!(Hits::open(&orbitals).unwrap().load(), 0);
}

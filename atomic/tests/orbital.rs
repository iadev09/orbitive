//! The address contract, over an in-memory table.

use std::sync::Arc;

use orbit_atomic::{ATOMIC_CAPACITY, Error, Kind, OrbitalId, Orbitals};
use orbit_core::Fleet;

fn orbitals() -> Orbitals {
    Orbitals::new(Arc::new(Fleet::join("example", 1).unwrap())).unwrap()
}

#[test]
fn an_id_round_trips_and_refuses_a_foreign_kind() {
    let orbitals = orbitals();
    let value = orbitals.allocate(-3_i64).unwrap();
    let id = value.id();
    assert_eq!(id.kind(), Kind::I64);
    assert_eq!(OrbitalId::from_bytes(id.to_bytes()).unwrap(), id);
    assert_eq!(id.to_string().parse::<OrbitalId>().unwrap(), id);
    assert_eq!(orbitals.open::<i64>(id).unwrap().load(), -3);

    let mut foreign = id.to_bytes();
    foreign[0] = 0x7f;
    assert!(matches!(
        OrbitalId::from_bytes(foreign),
        Err(Error::UnknownKind(0x7f))
    ));
    assert!(matches!(
        orbitals.open::<u64>(id),
        Err(Error::KindMismatch { expected: Kind::U64, .. })
    ));
}

#[test]
fn an_id_from_before_a_reset_is_stale() {
    let orbitals = orbitals();
    let before = orbitals.allocate(1_u64).unwrap().id();
    orbitals.reset_all().unwrap();
    // The same slot and kind, in the next epoch.
    let after = orbitals.allocate(2_u64).unwrap().id();
    assert_ne!(before, after);
    assert!(matches!(orbitals.open::<u64>(before), Err(Error::Stale(_))));
    assert_eq!(orbitals.open::<u64>(after).unwrap().load(), 2);
}

#[test]
fn a_full_table_refuses() {
    let orbitals = orbitals();
    let held: Vec<_> = (0..ATOMIC_CAPACITY)
        .map(|index| orbitals.allocate(index as u64).unwrap())
        .collect();
    for _ in 0..2 {
        assert!(matches!(
            orbitals.allocate(0_u64),
            Err(Error::Full { capacity: ATOMIC_CAPACITY })
        ));
    }
    let last = held.last().unwrap();
    assert_eq!(last.load(), ATOMIC_CAPACITY as u64 - 1);
}

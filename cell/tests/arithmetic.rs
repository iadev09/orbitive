use std::sync::Arc;

use orbit_cell::{Cells, Claim, Error};
use orbit_core::Fleet;

fn cells() -> Cells {
    Cells::new(Arc::new(Fleet::join("example", 1).unwrap())).unwrap()
}

#[test]
fn signed_subtraction_accepts_min_and_preserves_overflowed_values() {
    let cell = cells().allocate(0_i64).unwrap();
    let cases = [
        (i64::MIN, i64::MIN, Some(0)),
        (-1, i64::MIN, Some(i64::MAX)),
        (-2, i64::MIN, Some(i64::MAX - 1)),
        (i64::MIN + 1, i64::MIN, Some(1)),
        (i64::MAX, i64::MAX, Some(0)),
        (i64::MIN + 1, 1, Some(i64::MIN)),
        (0, i64::MAX, Some(-i64::MAX)),
        (0, -i64::MAX, Some(i64::MAX)),
        (i64::MIN, -1, Some(i64::MIN + 1)),
        (i64::MIN, 0, Some(i64::MIN)),
        (i64::MAX, 0, Some(i64::MAX)),
        (i64::MAX, -1, None),
        (i64::MIN, 1, None),
        (0, i64::MIN, None),
        (i64::MAX, i64::MIN, None),
        (i64::MIN, i64::MAX, None),
    ];
    for (initial, by, expected) in cases {
        cell.store(initial).unwrap();
        let version = cell.version().unwrap();
        let result = cell.fetch_sub(by);
        if let Some(value) = expected {
            assert_eq!(result.unwrap(), value, "{initial} - {by}");
            assert_eq!(cell.load().unwrap(), value);
            assert_eq!(cell.version().unwrap(), version + 1);
        } else {
            assert!(matches!(result, Err(Error::Overflow)), "{initial} - {by}");
            assert_eq!(cell.load().unwrap(), initial);
            assert_eq!(cell.version().unwrap(), version);
        }
    }
}

#[test]
fn bounded_signed_claims_handle_extreme_distances_and_rejected_steps() {
    use Claim::{Exhausted, Took};

    let cell = cells().allocate(0_i64).unwrap();
    let cases = [
        (i64::MIN, 1, i64::MAX, Some(Took { taken: 1, after: i64::MIN + 1 })),
        (
            i64::MIN,
            i64::MAX,
            i64::MAX,
            Some(Took { taken: i64::MAX, after: -1 }),
        ),
        (i64::MIN, i64::MAX, 0, Some(Took { taken: i64::MAX, after: -1 })),
        (
            i64::MIN,
            i64::MAX,
            i64::MIN + 7,
            Some(Took { taken: 7, after: i64::MIN + 7 }),
        ),
        (
            -1,
            i64::MAX,
            i64::MAX,
            Some(Took { taken: i64::MAX, after: i64::MAX - 1 }),
        ),
        (-10, 4, -7, Some(Took { taken: 3, after: -7 })),
        (-10, 4, 10, Some(Took { taken: 4, after: -6 })),
        (
            i64::MAX - 1,
            i64::MAX,
            i64::MAX,
            Some(Took { taken: 1, after: i64::MAX }),
        ),
        (i64::MAX, 1, i64::MAX, Some(Exhausted)),
        (i64::MIN, 1, i64::MIN, Some(Exhausted)),
        (7, 1, 3, Some(Exhausted)),
        (-1, 1, i64::MIN, Some(Exhausted)),
        (0, 0, i64::MAX, None),
        (i64::MIN, i64::MIN, i64::MAX, None),
        (i64::MIN, -1, i64::MAX, None),
        (i64::MAX, 0, i64::MAX, None),
    ];
    for (initial, by, limit, expected) in cases {
        cell.store(initial).unwrap();
        let version = cell.version().unwrap();
        let result = cell.add_until(by, limit);
        match expected {
            Some(Took { taken, after }) => {
                assert_eq!(result.unwrap(), Took { taken, after });
                assert!(taken > 0 && taken <= by);
                assert!(after > initial && after <= limit);
                assert_eq!(
                    i128::from(after) - i128::from(initial),
                    i128::from(taken)
                );
                assert_eq!(cell.load().unwrap(), after);
                assert_eq!(cell.version().unwrap(), version + 1);
            }
            Some(Exhausted) => {
                assert_eq!(result.unwrap(), Exhausted);
                assert_eq!(cell.load().unwrap(), initial);
                assert_eq!(cell.version().unwrap(), version);
            }
            None => {
                assert!(matches!(result, Err(Error::Overflow)));
                assert_eq!(cell.load().unwrap(), initial);
                assert_eq!(cell.version().unwrap(), version);
            }
        }
    }
}

#[test]
fn bounded_claims_cross_the_entire_signed_range_without_overshooting() {
    let cell = cells().allocate(i64::MIN).unwrap();
    let mut total = 0_i128;
    for (taken, after) in
        [(i64::MAX, -1), (i64::MAX, i64::MAX - 1), (1, i64::MAX)]
    {
        assert_eq!(
            cell.add_until(i64::MAX, i64::MAX).unwrap(),
            Claim::Took { taken, after }
        );
        total += i128::from(taken);
        assert_eq!(cell.load().unwrap(), after);
    }
    assert_eq!(total, i128::from(u64::MAX));
    let version = cell.version().unwrap();
    assert_eq!(cell.add_until(i64::MAX, i64::MAX).unwrap(), Claim::Exhausted);
    assert_eq!(cell.version().unwrap(), version);
    assert_eq!(cell.load().unwrap(), i64::MAX);
}

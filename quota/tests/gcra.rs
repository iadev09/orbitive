use super::*;

#[derive(Debug, Eq, PartialEq)]
enum Outcome {
    Allowed { tat: u128 },
    Limited { retry_after: Duration },
}

// Resolve the production phases for the independent token-bucket comparison;
// there is still only one implementation of the GCRA transition calculation.
trait EvaluateForTest {
    fn evaluate(
        &self,
        now: u128,
        previous: Option<u128>,
        cost: NonZeroU64,
    ) -> Result<Outcome, Error>;
}

impl EvaluateForTest for Gcra {
    fn evaluate(
        &self,
        now: u128,
        previous: Option<u128>,
        cost: NonZeroU64,
    ) -> Result<Outcome, Error> {
        match self.prepare(cost)?.evaluate(now, previous)? {
            super::Outcome::Allowed { tat } => Ok(Outcome::Allowed { tat }),
            super::Outcome::Limited { debt } => {
                Ok(Outcome::Limited { retry_after: self.retry_after(debt)? })
            }
        }
    }
}

fn count(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn algorithm(
    units: u64,
    nanos: u64,
    burst: u64,
) -> Gcra {
    Gcra::new(
        Policy::new(count(units), Duration::from_nanos(nanos), count(burst))
            .unwrap(),
    )
    .unwrap()
}

#[test]
fn fractional_intervals_are_exact_and_retry_rounds_up() {
    let gcra = algorithm(3, 10, 2);
    assert_eq!(
        gcra.evaluate(0, None, count(2)).unwrap(),
        Outcome::Allowed { tat: 20 }
    );
    assert_eq!(
        gcra.evaluate(0, Some(20), count(1)).unwrap(),
        Outcome::Limited { retry_after: Duration::from_nanos(4) }
    );
    assert_eq!(
        gcra.evaluate(gcra.scale(3).unwrap(), Some(20), count(1)).unwrap(),
        Outcome::Limited { retry_after: Duration::from_nanos(1) }
    );
    assert_eq!(
        gcra.evaluate(gcra.scale(4).unwrap(), Some(20), count(1)).unwrap(),
        Outcome::Allowed { tat: 30 }
    );
}

#[test]
fn weighted_cost_is_all_or_nothing_and_idle_time_cannot_bank_extra_burst() {
    let gcra = algorithm(2, 100, 5);
    assert_eq!(
        gcra.evaluate(0, None, count(4)).unwrap(),
        Outcome::Allowed { tat: 400 }
    );
    assert_eq!(
        gcra.evaluate(0, Some(400), count(2)).unwrap(),
        Outcome::Limited { retry_after: Duration::from_nanos(50) }
    );
    assert_eq!(
        gcra.evaluate(0, Some(400), count(1)).unwrap(),
        Outcome::Allowed { tat: 500 }
    );
    assert_eq!(
        gcra.evaluate(10_000, Some(500), count(5)).unwrap(),
        Outcome::Allowed { tat: 10_500 }
    );
    assert!(matches!(
        gcra.evaluate(0, None, count(6)),
        Err(Error::CostExceedsBurst)
    ));
}

#[test]
fn overflow_never_returns_an_admission() {
    let policy = Policy::new(count(1), Duration::MAX, count(u64::MAX)).unwrap();
    assert!(matches!(Gcra::new(policy), Err(Error::ArithmeticOverflow)));
    let gcra = algorithm(2, 100, 5);
    assert!(matches!(gcra.scale(u128::MAX), Err(Error::ArithmeticOverflow)));
    assert!(matches!(
        gcra.evaluate(u128::MAX, None, count(1)),
        Err(Error::ArithmeticOverflow)
    ));
    assert!(matches!(
        gcra.evaluate(0, Some(u128::MAX), count(1)),
        Err(Error::ArithmeticOverflow)
    ));
}

#[test]
fn gcra_matches_an_independent_integer_token_bucket() {
    for units in 1..=7 {
        for period in 1..=11 {
            let gcra = algorithm(units, period, 5);
            let mut tat = None;
            let capacity = u128::from(period) * 5;
            let mut credit = capacity;
            let mut now = 0;
            for step in 0..1_000_u64 {
                let elapsed = step % 4;
                now += u128::from(elapsed);
                credit = (credit + u128::from(elapsed * units)).min(capacity);
                let cost = step % 5 + 1;
                let charge = u128::from(cost * period);
                let outcome = gcra
                    .evaluate(gcra.scale(now).unwrap(), tat, count(cost))
                    .unwrap();
                if credit >= charge {
                    credit -= charge;
                    let Outcome::Allowed { tat: next } = outcome else {
                        panic!("available credit was rejected")
                    };
                    tat = Some(next);
                } else {
                    let nanos = (charge - credit).div_ceil(u128::from(units));
                    assert_eq!(
                        outcome,
                        Outcome::Limited {
                            retry_after: Duration::from_nanos(nanos as u64)
                        }
                    );
                }
            }
        }
    }
}

#[test]
fn prepared_cost_is_reusable_and_denial_resolution_keeps_its_snapshot() {
    let gcra = algorithm(3, 10, 2);
    let prepared = gcra.prepare(count(1)).unwrap();
    assert!(matches!(gcra.prepare(count(3)), Err(Error::CostExceedsBurst)));
    let super::Outcome::Limited { debt } =
        prepared.evaluate(0, Some(20)).unwrap()
    else {
        panic!("expected a denied snapshot");
    };
    assert_eq!(debt, 10);
    assert_eq!(
        prepared.evaluate(9, Some(20)).unwrap(),
        super::Outcome::Limited { debt: 1 }
    );
    assert_eq!(gcra.retry_after(debt).unwrap(), Duration::from_nanos(4));
    assert_eq!(
        prepared.evaluate(10, Some(20)).unwrap(),
        super::Outcome::Allowed { tat: 30 }
    );
}

#[test]
fn unrepresentable_retry_is_an_error_without_changing_the_transition() {
    let gcra =
        Gcra::new(Policy::new(count(1), Duration::MAX, count(2)).unwrap())
            .unwrap();
    let prepared = gcra.prepare(count(2)).unwrap();
    let tat = Duration::MAX.as_nanos() * 2;
    assert_eq!(
        prepared.evaluate(0, Some(tat)).unwrap(),
        super::Outcome::Limited { debt: tat }
    );
    assert!(matches!(gcra.retry_after(tat), Err(Error::ArithmeticOverflow)));
    assert_eq!(
        prepared.evaluate(0, Some(tat)).unwrap(),
        super::Outcome::Limited { debt: tat }
    );
}

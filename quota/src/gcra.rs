use std::num::NonZeroU64;
use std::time::Duration;

use crate::{Admission, Decision, Error, Policy, Snapshot};

/// Internal static dispatch keeps ordinary admission free of snapshot math.
pub(crate) trait AdmissionResult {
    fn allowed(
        now: u128,
        tat: u128,
    ) -> Self;
    fn limited(retry_after: Duration) -> Self;
}

impl AdmissionResult for Decision {
    fn allowed(
        _: u128,
        _: u128,
    ) -> Self {
        Self::Allowed
    }
    fn limited(retry_after: Duration) -> Self {
        Self::Limited { retry_after }
    }
}

/// Capture only local scalars under exclusion; resolve after guards are gone.
pub(crate) enum Captured {
    Allowed { now: u128, tat: u128 },
    Limited { retry_after: Duration },
}

impl AdmissionResult for Captured {
    fn allowed(
        now: u128,
        tat: u128,
    ) -> Self {
        Self::Allowed { now, tat }
    }
    fn limited(retry_after: Duration) -> Self {
        Self::Limited { retry_after }
    }
}

impl Captured {
    pub(crate) fn resolve(
        self,
        algorithm: &Gcra,
    ) -> Admission {
        match self {
            Self::Allowed { now, tat } => Admission {
                decision: Decision::Allowed,
                snapshot: Some(algorithm.snapshot(now, tat)),
            },
            Self::Limited { retry_after } => Admission {
                decision: Decision::Limited { retry_after },
                snapshot: None,
            },
        }
    }
}

/// Time is measured in nanoseconds multiplied by the sustained unit count.
/// A unit therefore costs exactly period_ns, without rounding the interval.
pub(crate) struct Gcra {
    policy: Policy,
    period: u128,
    units: u128,
    capacity: u128,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    Allowed { tat: u128 },
    Limited { debt: u128 },
}

/// Validated request cost bound to its immutable policy. Preparation does not
/// read a clock or access storage and can precede the shard hold.
pub(crate) struct Prepared<'a> {
    algorithm: &'a Gcra,
    charge: u128,
}

impl Gcra {
    /// Called only for a successfully committed evaluate() result. Its checked
    /// ceiling proves debt <= capacity. Calculation is infallible after charge;
    /// floor division never advertises a fractional unit as available.
    fn snapshot(
        &self,
        now: u128,
        tat: u128,
    ) -> Snapshot {
        let remaining = (self.capacity - (tat - now)) / self.period;
        Snapshot {
            limit: self.policy.burst().get(),
            remaining: remaining as u64,
        }
    }
    pub(crate) fn new(policy: Policy) -> Result<Self, Error> {
        let period = policy.period().as_nanos();
        let capacity = period
            .checked_mul(u128::from(policy.burst().get()))
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(Self {
            policy,
            period,
            units: u128::from(policy.units().get()),
            capacity,
        })
    }

    pub(crate) fn prepare(
        &self,
        cost: NonZeroU64,
    ) -> Result<Prepared<'_>, Error> {
        if cost > self.policy.burst() {
            return Err(Error::CostExceedsBurst);
        }
        let charge = self
            .period
            .checked_mul(u128::from(cost.get()))
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(Prepared { algorithm: self, charge })
    }

    pub(crate) fn scale(
        &self,
        now: u128,
    ) -> Result<u128, Error> {
        now.checked_mul(self.units).ok_or(Error::ArithmeticOverflow)
    }

    /// Resolve a denied snapshot after releasing storage exclusion. This does
    /// not re-read or change the record; another request may extend its debt.
    pub(crate) fn retry_after(
        &self,
        debt: u128,
    ) -> Result<Duration, Error> {
        let nanos = debt.div_ceil(self.units);
        let seconds = u64::try_from(nanos / 1_000_000_000)
            .map_err(|_| Error::ArithmeticOverflow)?;
        let subsecond = (nanos % 1_000_000_000) as u32;
        Ok(Duration::new(seconds, subsecond))
    }
}

impl Prepared<'_> {
    /// Pure calculation under backend exclusion: no cost preparation or retry
    /// division. The backend commits TAT only on Allowed.
    pub(crate) fn evaluate(
        &self,
        now: u128,
        previous: Option<u128>,
    ) -> Result<Outcome, Error> {
        let tat = previous
            .unwrap_or(now)
            .max(now)
            .checked_add(self.charge)
            .ok_or(Error::ArithmeticOverflow)?;
        let ceiling = now
            .checked_add(self.algorithm.capacity)
            .ok_or(Error::ArithmeticOverflow)?;
        if tat <= ceiling {
            return Ok(Outcome::Allowed { tat });
        }
        Ok(Outcome::Limited { debt: tat - ceiling })
    }
}

#[cfg(test)]
#[path = "../tests/gcra.rs"]
mod tests;

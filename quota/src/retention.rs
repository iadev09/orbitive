use portable_atomic::AtomicU128;
use std::sync::atomic::Ordering;

use crate::Error;
use crate::budget::WorkBudget;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Retention {
    Strict,
    Guard,
}

#[derive(Clone, Copy)]
pub(crate) struct Observation {
    pub tat: u128,
    pub seen: u128,
}

#[derive(Clone, Copy)]
pub(crate) struct Candidate {
    pub index: usize,
    pub generation: u64,
    // Present only when a guard proposes discarding live debt. Revalidate both
    // fields after exclusive reservation; a newly touched candidate is Busy.
    pub observed: Option<Observation>,
}

/// Candidate selection covers only the configured probe window, not a global
/// LRU list. Empty/replenished slots always take precedence over live debt.
pub(crate) struct Selection {
    retention: Retention,
    replenished: Option<Candidate>,
    cold: Option<Candidate>,
}

impl Selection {
    pub fn new(retention: Retention) -> Self {
        Self { retention, replenished: None, cold: None }
    }

    pub fn consider(
        &mut self,
        index: usize,
        generation: u64,
        replenished: bool,
        observation: Observation,
    ) {
        if replenished {
            if self.replenished.is_none() {
                self.replenished =
                    Some(Candidate { index, generation, observed: None });
            }
        } else if self.retention == Retention::Guard
            && self.cold.is_none_or(|candidate| {
                observation.seen < candidate.observed.unwrap().seen
            })
        {
            self.cold = Some(Candidate {
                index,
                generation,
                observed: Some(observation),
            });
        }
    }

    pub fn finish(self) -> Result<Candidate, Error> {
        self.replenished.or(self.cold).ok_or(Error::CapacityFull)
    }
}

/// Recency uses the already sampled scaled monotonic time. No timer, global
/// sequence, allocation or independent retry budget is introduced. Touches
/// precede the decision, so no fallible work follows a successful charge.
pub(crate) fn touch(
    last_seen: &AtomicU128,
    now: u128,
    budget: &mut WorkBudget,
) -> Result<(), Error> {
    let mut previous = last_seen.load(Ordering::Acquire);
    while previous < now {
        budget.take()?;
        match last_seen.compare_exchange_weak(
            previous,
            now,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Ok(()),
            Err(current) => previous = current,
        }
    }
    Ok(())
}

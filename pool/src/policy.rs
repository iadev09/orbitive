//! The decision: given what the fleet has for a key, what should this
//! caller do? The pool runs the loop (snapshot, decide, reserve or claim,
//! retry on a lost race) and the policy makes the choice. No cost model is
//! built in; a policy that wants one brings its own numbers.

use crate::{Candidate, Key, ResourceId, State};

/// Caller-supplied bounds for one acquisition.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Fleet-wide ceiling on resources for the key, live plus being made.
    pub max_live: u32,
    /// Reservation attempts before the pool gives the decision back as
    /// [`Plan::Wait`]; a lost race (a candidate turned out busy) costs one.
    pub attempts: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_live: 1,
            attempts: 4,
        }
    }
}

/// What the policy wants done next.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    /// Reserve capacity on this candidate.
    Reuse(ResourceId),
    /// Claim creation budget and make a new resource.
    Create,
    /// Nothing usable now; the caller waits for the key to change.
    Wait,
    /// Nothing usable and waiting is not the answer.
    Reject(Reason),
}

/// Why a policy would not serve the request, for logs and metrics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reason {
    /// The key has no resources and creation is not allowed.
    NothingToUse,
    /// Every resource is at capacity and the caller asked not to wait.
    Saturated,
    /// The policy's own rule.
    Policy(&'static str),
}

pub trait Policy: Send + Sync {
    /// `candidates` is a snapshot; `budget` is `(live, creating)` for the
    /// key. Called again after a lost race with a fresh snapshot.
    fn decide(
        &self,
        key: Key,
        candidates: &[Candidate],
        budget: (u32, u32),
        limits: &Limits,
    ) -> Decision;
}

/// The starting policy: a local resource with room, else a remote one with
/// room (least loaded first), else create within the budget, else wait.
/// No number from any benchmark is built in; whether remote reuse beats
/// creation is a measurement the embedder makes and expresses in its own
/// policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalFirst;

impl Policy for LocalFirst {
    fn decide(
        &self,
        _key: Key,
        candidates: &[Candidate],
        budget: (u32, u32),
        limits: &Limits,
    ) -> Decision {
        let usable =
            |candidate: &&Candidate| candidate.state == State::Live && candidate.free() > 0;
        let least_loaded = |a: &&Candidate, b: &&Candidate| {
            (a.reserved + a.active, a.last_reserve_ms)
                .cmp(&(b.reserved + b.active, b.last_reserve_ms))
        };
        if let Some(local) = candidates
            .iter()
            .filter(|candidate| candidate.local)
            .filter(usable)
            .min_by(least_loaded)
        {
            return Decision::Reuse(local.id);
        }
        if let Some(remote) = candidates
            .iter()
            .filter(|candidate| !candidate.local)
            .filter(usable)
            .min_by(least_loaded)
        {
            return Decision::Reuse(remote.id);
        }
        let (live, creating) = budget;
        if live + creating < limits.max_live {
            return Decision::Create;
        }
        Decision::Wait
    }
}

/// Never leaves this process: local reuse, else create, else wait. What a
/// standalone runtime or a `LocalOnly` profile uses; remote candidates are
/// invisible to it even when they exist.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalOnly;

impl Policy for LocalOnly {
    fn decide(
        &self,
        _key: Key,
        candidates: &[Candidate],
        budget: (u32, u32),
        limits: &Limits,
    ) -> Decision {
        if let Some(local) = candidates
            .iter()
            .filter(|candidate| {
                candidate.local && candidate.state == State::Live && candidate.free() > 0
            })
            .min_by_key(|candidate| candidate.reserved + candidate.active)
        {
            return Decision::Reuse(local.id);
        }
        let (live, creating) = budget;
        if live + creating < limits.max_live {
            return Decision::Create;
        }
        Decision::Wait
    }
}

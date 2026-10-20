use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

/// Total admission CAS attempts per call, including successful attempts.
/// Pin, clock watermark, writer reservation and TAT updates share this budget.
/// Cleanup operations always run and do not consume admission attempts.
/// This bounds atomic API calls, not CPU instruction retries or scheduling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CasBudget(NonZeroUsize);

impl CasBudget {
    pub const fn new(attempts: NonZeroUsize) -> Self {
        Self(attempts)
    }

    pub const fn attempts(self) -> NonZeroUsize {
        self.0
    }
}

/// Immutable geometry supplied when opening either backing. No defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Geometry {
    slots: NonZeroUsize,
    shards: NonZeroUsize,
    key_bytes: NonZeroUsize,
    probes: NonZeroUsize,
}

impl Geometry {
    pub fn new(
        slots: NonZeroUsize,
        shards: NonZeroUsize,
        key_bytes: NonZeroUsize,
        probes: NonZeroUsize,
    ) -> Result<Self, Error> {
        if !slots.get().is_multiple_of(shards.get())
            || probes.get() > slots.get() / shards.get()
        {
            return Err(Error::InvalidGeometry);
        }
        Ok(Self { slots, shards, key_bytes, probes })
    }

    pub const fn slots(self) -> NonZeroUsize {
        self.slots
    }
    pub const fn shards(self) -> NonZeroUsize {
        self.shards
    }
    /// Maximum combined namespace and subject bytes, not just subject bytes.
    pub const fn key_bytes(self) -> NonZeroUsize {
        self.key_bytes
    }
    /// Slots inspected per lookup pass. Both backends use at most two passes on a
    /// miss (optimistic lookup, then serialized installation lookup).
    pub const fn probes(self) -> NonZeroUsize {
        self.probes
    }
}

/// Sustained units per explicit period, plus total immediately available burst.
/// Opening a concrete backend must also validate precision and numeric range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Policy {
    units: NonZeroU64,
    period: Duration,
    burst: NonZeroU64,
}

impl Policy {
    pub fn new(
        units: NonZeroU64,
        period: Duration,
        burst: NonZeroU64,
    ) -> Result<Self, Error> {
        if period.is_zero() {
            return Err(Error::InvalidPeriod);
        }
        Ok(Self { units, period, burst })
    }

    pub const fn units(self) -> NonZeroU64 {
        self.units
    }
    pub const fn period(self) -> Duration {
        self.period
    }
    pub const fn burst(self) -> NonZeroU64 {
        self.burst
    }
}

/// Borrowed opaque identity. The store bounds and compares both components;
/// namespace and subject boundaries must survive hashing and serialization.
/// Do not use a raw secret as the subject. Debug deliberately omits its bytes.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Key<'a> {
    namespace: &'a [u8],
    subject: &'a [u8],
}

impl<'a> Key<'a> {
    pub fn new(
        namespace: &'a [u8],
        subject: &'a [u8],
    ) -> Result<Self, Error> {
        if namespace.is_empty() || subject.is_empty() {
            return Err(Error::EmptyKey);
        }
        Ok(Self { namespace, subject })
    }

    pub const fn namespace(self) -> &'a [u8] {
        self.namespace
    }
    pub const fn subject(self) -> &'a [u8] {
        self.subject
    }
}

impl fmt::Debug for Key<'_> {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("Key")
            .field("namespace_len", &self.namespace.len())
            .field("subject_len", &self.subject.len())
            .finish()
    }
}

/// An authoritative admission result, not an observational counter snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    /// The complete cost was committed. It is not refunded on caller failure.
    Allowed,
    /// No cost was consumed. Another request may extend this retry horizon.
    Limited { retry_after: Duration },
}

/// The common contract for memory and SHM stores.
///
/// Every call has a bounded work budget and never waits for an owner or budget.
/// An error grants no permission. The policy and geometry are immutable for
/// this store; time comes from its trusted clock, not request data. Implementors
/// must atomically bind key identity, policy, time state and full consumption.
/// Neither a multi-policy transaction nor idempotent retries are promised.
pub trait QuotaStore: Send + Sync {
    fn policy(&self) -> Policy;
    fn geometry(&self) -> Geometry;
    fn try_consume(
        &self,
        key: Key<'_>,
        cost: NonZeroU64,
    ) -> Result<Decision, Error>;
}

/// Explicit failure, never an implicit empty bucket or permission to proceed.
/// Future backends may add causes; consumers should preserve unknown failures.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    InvalidGeometry,
    InvalidPeriod,
    EmptyKey,
    KeyTooLarge,
    CostExceedsBurst,
    Busy,
    CapacityFull,
    IncompatibleState,
    CorruptState,
    ArithmeticOverflow,
    ClockUnavailable,
    RandomUnavailable,
    ClockWentBackwards,
    AllocationFailed,
    UnsupportedAtomics,
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        let message = match self {
            Self::InvalidGeometry => "invalid quota table geometry",
            Self::InvalidPeriod => "quota period must be positive",
            Self::EmptyKey => "quota namespace and subject must not be empty",
            Self::KeyTooLarge => "quota key exceeds the declared byte budget",
            Self::CostExceedsBurst => "quota cost exceeds burst capacity",
            Self::Busy => {
                "quota admission is busy or its CAS budget is exhausted"
            }
            Self::CapacityFull => {
                "quota key cannot fit within the admission budget"
            }
            Self::IncompatibleState => "incompatible quota state",
            Self::CorruptState => "corrupt quota state",
            Self::ArithmeticOverflow => "quota arithmetic overflow",
            Self::ClockUnavailable => "quota clock is unavailable",
            Self::RandomUnavailable => "quota hash seed source is unavailable",
            Self::ClockWentBackwards => "quota clock moved backwards",
            Self::AllocationFailed => "quota backing allocation failed",
            Self::UnsupportedAtomics => {
                "quota requires lock-free 128-bit atomics"
            }
            Self::Io(error) => return write!(f, "quota IO error: {error}"),
        };
        f.write_str(message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "../tests/contract.rs"]
mod tests;

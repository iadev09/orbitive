//! Tick and epoch helpers for Orbit frame versions.
//!
//! Ring counters answer "where in this ring did the write land?".
//! An [`OrbitEpoch`] answers "when did this sample claim to be
//! captured?". Keeping that distinction explicit lets semantic layers
//! use `Frame::ver` consistently without making the ring itself know
//! about clocks.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch, carried in `Frame::ver` by
/// time-sensitive Orbit primitives.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct OrbitEpoch(u64);

impl OrbitEpoch {
    pub const ZERO: Self = Self(0);

    pub fn now() -> Self {
        Self::from_system_time(SystemTime::now())
    }

    pub fn from_system_time(time: SystemTime) -> Self {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        Self(millis)
    }

    pub const fn from_unix_ms(epoch_ms: u64) -> Self {
        Self(epoch_ms)
    }

    pub const fn as_unix_ms(self) -> u64 {
        self.0
    }

    pub fn age_at(self, now: Self) -> Duration {
        Duration::from_millis(now.0.saturating_sub(self.0))
    }

    pub fn is_fresh_at(self, now: Self, max_age: Duration) -> bool {
        self.age_at(now) <= max_age
    }
}

impl From<u64> for OrbitEpoch {
    fn from(value: u64) -> Self {
        Self::from_unix_ms(value)
    }
}

impl From<OrbitEpoch> for u64 {
    fn from(value: OrbitEpoch) -> Self {
        value.as_unix_ms()
    }
}

impl std::fmt::Display for OrbitEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

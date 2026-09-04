//! Resource-side fencing for lock tenures.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic identity of one lock tenure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FenceToken(u64);

impl FenceToken {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for FenceToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fence:{}", self.0)
    }
}

/// Atomic resource-side high-water mark for fencing stale holders.
#[derive(Debug, Default)]
pub struct Fence {
    high_water: AtomicU64,
}

impl Fence {
    pub const fn new() -> Self {
        Self {
            high_water: AtomicU64::new(0),
        }
    }

    pub const fn with_high_water(token: u64) -> Self {
        Self {
            high_water: AtomicU64::new(token),
        }
    }

    /// Admit this tenure unless a newer tenure was already observed.
    pub fn admit(&self, token: FenceToken) -> bool {
        let previous = self.high_water.fetch_max(token.get(), Ordering::AcqRel);
        token.get() >= previous
    }

    pub fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::{Fence, FenceToken};

    #[test]
    fn admits_current_and_newer_tenures_but_rejects_stale_ones() {
        let fence = Fence::new();
        assert!(fence.admit(FenceToken::new(5)));
        assert!(fence.admit(FenceToken::new(5)));
        assert!(fence.admit(FenceToken::new(9)));
        assert!(!fence.admit(FenceToken::new(7)));
        assert_eq!(fence.high_water(), 9);
    }

    #[test]
    fn restored_high_water_keeps_rejecting_older_tenures() {
        let fence = Fence::with_high_water(42);
        assert!(!fence.admit(FenceToken::new(41)));
        assert!(fence.admit(FenceToken::new(42)));
        assert!(fence.admit(FenceToken::new(100)));
    }
}

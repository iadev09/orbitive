//! Keyed signed counters, as kept `i64` values of the fleet's [`Orbitals`].
//!
//! A counter key is an Orbital name. `increment("hits", 1)` and a value
//! declared elsewhere as `orbital! { pub Hits: i64 = 0, "hits", kept }` are
//! the same 64 bits, so every language that counts by key and every typed
//! declaration meet in one place. The table, its capacity, its name bound
//! and its lifecycle are `orbit-atomic`'s; this crate adds checked
//! arithmetic and keys chosen at run time.
//!
//! [`Counter`] resolves the key on every call. [`Counter::open`] resolves it
//! once and returns a [`CounterHandle`], whose operations touch the value
//! word and nothing else.

use std::fmt;
use std::ops::Deref;

use orbit_atomic::{Orbital, Orbitals};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Additions are expected: a cause discovered later lands here rather
/// than in a new major version, so a caller matches what it handles and
/// leaves the rest to a catch-all.
#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    KeyEmpty,
    /// Longer than [`ATOMIC_NAME_MAX`](orbit_atomic::ATOMIC_NAME_MAX).
    KeyTooLarge {
        len: usize,
        max: usize,
    },
    /// The key names an Orbital that is not a counter: one of another type,
    /// or an `i64` not declared kept. Holds the table's
    /// `KindMismatch` or `LifetimeMismatch`, which carries the value's id.
    NotCounter(orbit_atomic::Error),
    NegativeAmount(i64),
    /// The update would leave the signed 64-bit range; the value is kept.
    Overflow,
    /// The Orbitals table has handed out every slot.
    Full {
        capacity: usize,
    },
    /// Any other failure of the table, such as an I/O error.
    Atomic(orbit_atomic::Error),
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::KeyEmpty => f.write_str("counter key must not be empty"),
            Self::KeyTooLarge { len, max } => {
                write!(f, "counter key is too large: len={len} max={max}")
            }
            Self::NotCounter(error) => {
                write!(f, "counter key names another value: {error}")
            }
            Self::NegativeAmount(value) => {
                write!(f, "counter amount must not be negative: {value}")
            }
            Self::Overflow => {
                f.write_str("counter value is outside the signed 64-bit range")
            }
            Self::Full { capacity } => {
                write!(f, "orbital table is full: capacity={capacity}")
            }
            Self::Atomic(error) => write!(f, "counter: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotCounter(error) | Self::Atomic(error) => Some(error),
            _ => None,
        }
    }
}

impl From<orbit_atomic::Error> for Error {
    fn from(error: orbit_atomic::Error) -> Self {
        use orbit_atomic::Error as Atomic;
        match error {
            Atomic::NameEmpty => Self::KeyEmpty,
            Atomic::NameTooLong { len, max } => Self::KeyTooLarge { len, max },
            Atomic::KindMismatch { .. } | Atomic::LifetimeMismatch { .. } => {
                Self::NotCounter(error)
            }
            Atomic::Full { capacity } => Self::Full { capacity },
            other => Self::Atomic(other),
        }
    }
}

/// The fleet's counters, reached by key. Cheap to clone.
///
/// A missing key reads as `None` and is not installed by reading;
/// `increment`, `decrement`, `reset` and `open` install it at zero. Each
/// call hashes and looks up its key; a caller that counts under one key
/// repeatedly holds a [`CounterHandle`] instead.
#[derive(Clone)]
pub struct Counter {
    orbitals: Orbitals,
}

impl Counter {
    /// Counters in `orbitals`, the table the fleet's other declared values
    /// live in. Its owner's reset keeps them: counters are declared kept.
    pub fn new(orbitals: Orbitals) -> Self {
        Self { orbitals }
    }

    /// The counter under `key`, installed at zero when missing. The key is
    /// resolved here and never again.
    pub fn open(
        &self,
        key: &str,
    ) -> Result<CounterHandle> {
        Ok(CounterHandle(self.orbitals.declare_kept(key, 0_i64)?))
    }

    pub fn get(
        &self,
        key: &str,
    ) -> Result<Option<i64>> {
        Ok(self.orbitals.find_kept::<i64>(key)?.map(|value| value.load()))
    }

    /// Add `by` and return the value after. A negative amount is refused
    /// before the key is installed.
    pub fn increment(
        &self,
        key: &str,
        by: i64,
    ) -> Result<i64> {
        check_amount(by)?;
        self.open(key)?.add(by)
    }

    /// Subtract `by` and return the value after.
    pub fn decrement(
        &self,
        key: &str,
        by: i64,
    ) -> Result<i64> {
        check_amount(by)?;
        self.open(key)?.sub(by)
    }

    /// Set a key to zero, installing it when necessary.
    pub fn reset(
        &self,
        key: &str,
    ) -> Result<()> {
        self.open(key)?.reset();
        Ok(())
    }
}

/// One counter, its key resolved. Cheap to clone, and the same value as
/// every handle any process opens under the key.
///
/// Derefs to the [`Orbital`], so a caller can `wait` for the counter to
/// change or `notify` its waiters. Writing it through the Orbital bypasses
/// the checks here: `fetch_add` wraps.
///
/// Like any Orbital handle, it belongs to the table's epoch. After the
/// owner's reset the counter keeps its value under a new id; open it again.
#[derive(Clone, Debug)]
pub struct CounterHandle(Orbital<i64>);

impl CounterHandle {
    pub fn get(&self) -> i64 {
        self.0.load()
    }

    /// Add `by` and return the value after.
    pub fn increment(
        &self,
        by: i64,
    ) -> Result<i64> {
        check_amount(by)?;
        self.add(by)
    }

    /// Subtract `by` and return the value after.
    pub fn decrement(
        &self,
        by: i64,
    ) -> Result<i64> {
        check_amount(by)?;
        self.sub(by)
    }

    pub fn reset(&self) {
        self.0.store(0);
    }

    fn add(
        &self,
        by: i64,
    ) -> Result<i64> {
        self.update(|value| value.checked_add(by))
    }

    fn sub(
        &self,
        by: i64,
    ) -> Result<i64> {
        self.update(|value| value.checked_sub(by))
    }

    /// Compare and exchange until the update lands or leaves the range.
    /// A refused update writes nothing and wakes nobody.
    fn update(
        &self,
        next: impl Fn(i64) -> Option<i64>,
    ) -> Result<i64> {
        let mut current = self.0.load();
        loop {
            let updated = next(current).ok_or(Error::Overflow)?;
            match self.0.compare_exchange(current, updated) {
                Ok(_) => return Ok(updated),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Deref for CounterHandle {
    type Target = Orbital<i64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn check_amount(by: i64) -> Result<()> {
    if by < 0 {
        return Err(Error::NegativeAmount(by));
    }
    Ok(())
}

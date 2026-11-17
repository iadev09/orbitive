//! What can fail: allocating, declaring, opening and waiting. Value
//! operations cannot.

use std::fmt;

use crate::{Kind, Lifetime, OrbitalId};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Additions are expected: a cause discovered later lands here rather than
/// in a new major version, so a caller matches what it handles and leaves
/// the rest to a catch-all.
#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    /// The id names nothing in this table: it was minted before the table
    /// was reset, or it was never minted here.
    Stale(OrbitalId),
    /// The id names a value of another type than the one asked for.
    KindMismatch {
        id: OrbitalId,
        expected: Kind,
    },
    /// The name was declared with the other [`Lifetime`].
    LifetimeMismatch {
        id: OrbitalId,
        expected: Lifetime,
    },
    /// A declaration needs a name.
    NameEmpty,
    /// The name is longer than [`ATOMIC_NAME_MAX`](crate::ATOMIC_NAME_MAX)
    /// bytes.
    NameTooLong {
        len: usize,
        max: usize,
    },
    /// The leading byte of an encoded id is not a known kind.
    UnknownKind(u8),
    /// The text is not an id.
    Malformed(Box<str>),
    /// Every slot of the table has been handed out.
    Full {
        capacity: usize,
    },
    /// The table cannot start another epoch.
    EpochExhausted,
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Stale(id) => write!(f, "orbital {id} is not in this table"),
            Self::KindMismatch { id, expected } => {
                write!(f, "orbital {id} is not {expected}")
            }
            Self::LifetimeMismatch { id, expected } => {
                write!(f, "orbital {id} is not declared {expected}")
            }
            Self::NameEmpty => f.write_str("orbital name is empty"),
            Self::NameTooLong { len, max } => {
                write!(f, "orbital name is {len} bytes, longer than {max}")
            }
            Self::UnknownKind(kind) => {
                write!(f, "unknown orbital kind byte {kind:#04x}")
            }
            Self::Malformed(text) => write!(f, "not an orbital id: {text:?}"),
            Self::Full { capacity } => {
                write!(f, "orbital table is full: capacity={capacity}")
            }
            Self::EpochExhausted => {
                f.write_str("orbital table epoch is exhausted")
            }
            Self::Io(error) => write!(f, "orbital table io error: {error}"),
        }
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

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

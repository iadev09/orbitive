//! The address of a shared value, and the types a value can have.

use std::fmt;
use std::str::FromStr;

use crate::{Error, Result};

/// What the 64 bits of a value spell.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(u8)]
pub enum Kind {
    I64 = 1,
    U64 = 2,
    F64 = 3,
    Bool = 4,
}

impl Kind {
    /// The name used in the text form of an id.
    pub const fn name(self) -> &'static str {
        match self {
            Self::I64 => "i64",
            Self::U64 => "u64",
            Self::F64 => "f64",
            Self::Bool => "bool",
        }
    }

    pub(crate) const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::I64),
            2 => Some(Self::U64),
            3 => Some(Self::F64),
            4 => Some(Self::Bool),
            _ => None,
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        [Self::I64, Self::U64, Self::F64, Self::Bool]
            .into_iter()
            .find(|kind| kind.name() == name)
    }
}

impl fmt::Display for Kind {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.write_str(self.name())
    }
}

pub(crate) const SLOT_BITS: u32 = 24;
const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;

/// The address of one shared value: `kind:8 | slot:24 | epoch:32`.
///
/// It is the only thing that travels between processes. Eight big-endian
/// bytes through [`Self::to_bytes`], or `<kind>:<slot>:<epoch>` as text,
/// such as `i64:12:3`. An id is valid in the table that minted it until
/// that table is reset.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct OrbitalId(u64);

impl OrbitalId {
    pub(crate) const fn new(
        kind: Kind,
        slot: u32,
        epoch: u32,
    ) -> Self {
        Self(
            ((kind as u64) << 56)
                | ((slot as u64 & SLOT_MASK) << 32)
                | epoch as u64,
        )
    }

    pub const fn kind(self) -> Kind {
        // Every constructor refuses an unknown kind byte.
        match Kind::from_tag((self.0 >> 56) as u8) {
            Some(kind) => kind,
            None => unreachable!(),
        }
    }

    pub(crate) const fn slot(self) -> u32 {
        ((self.0 >> 32) & SLOT_MASK) as u32
    }

    pub(crate) const fn epoch(self) -> u32 {
        self.0 as u32
    }

    /// The 64 bits as they are kept in shared memory.
    pub(crate) const fn to_word(self) -> u64 {
        self.0
    }

    /// Big-endian: the kind is the first byte.
    pub const fn to_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// The inverse of [`Self::to_bytes`]. An unknown kind byte is refused.
    pub fn from_bytes(bytes: [u8; 8]) -> Result<Self> {
        match Kind::from_tag(bytes[0]) {
            Some(_) => Ok(Self(u64::from_be_bytes(bytes))),
            None => Err(Error::UnknownKind(bytes[0])),
        }
    }
}

impl fmt::Display for OrbitalId {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "{}:{}:{}", self.kind(), self.slot(), self.epoch())
    }
}

impl FromStr for OrbitalId {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        let parse = || {
            let mut parts = text.splitn(3, ':');
            let kind = Kind::from_name(parts.next()?)?;
            let slot: u32 = parts.next()?.parse().ok()?;
            let epoch: u32 = parts.next()?.parse().ok()?;
            (u64::from(slot) <= SLOT_MASK).then(|| Self::new(kind, slot, epoch))
        };
        parse().ok_or_else(|| Error::Malformed(text.into()))
    }
}

mod sealed {
    pub trait Sealed {}
}

/// A type one shared value can hold: `i64`, `u64`, `f64` or `bool`.
///
/// Sealed: the table stores 64 bits and a kind, and every write keeps
/// those bits in the one form its kind allows.
pub trait Primitive: sealed::Sealed + Copy + Send + Sync + 'static {
    const KIND: Kind;

    #[doc(hidden)]
    fn into_word(self) -> u64;
    #[doc(hidden)]
    fn from_word(word: u64) -> Self;
}

impl sealed::Sealed for i64 {}
impl Primitive for i64 {
    const KIND: Kind = Kind::I64;

    #[inline]
    fn into_word(self) -> u64 {
        self as u64
    }

    #[inline]
    fn from_word(word: u64) -> Self {
        word as i64
    }
}

impl sealed::Sealed for u64 {}
impl Primitive for u64 {
    const KIND: Kind = Kind::U64;

    #[inline]
    fn into_word(self) -> u64 {
        self
    }

    #[inline]
    fn from_word(word: u64) -> Self {
        word
    }
}

impl sealed::Sealed for f64 {}
impl Primitive for f64 {
    const KIND: Kind = Kind::F64;

    #[inline]
    fn into_word(self) -> u64 {
        self.to_bits()
    }

    #[inline]
    fn from_word(word: u64) -> Self {
        f64::from_bits(word)
    }
}

impl sealed::Sealed for bool {}
impl Primitive for bool {
    const KIND: Kind = Kind::Bool;

    #[inline]
    fn into_word(self) -> u64 {
        u64::from(self)
    }

    #[inline]
    fn from_word(word: u64) -> Self {
        word != 0
    }
}

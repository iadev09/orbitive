//! The fence between a backing read and the fill that keeps its result.
//!
//! Both storage strategies order tag invalidations on a clock: a replicated
//! store on its mutation ring's sequence, a shared store on its tag table's
//! clock. A [`FillToken`] is a position on that clock, read before the backing
//! read; a fill carrying it is refused when an invalidation of one of its tags
//! took a later position.

/// A position on a store's invalidation clock, read before a backing read.
///
/// Take it with `fill_token` immediately before reading the authoritative
/// source, and pass it with the result to `fill_tagged`. Every invalidation
/// whose clock position is at or below the token advanced the clock before
/// the token was read, and its writer changed the source before that, so the
/// backing read sees it. Any invalidation of one of the fill's tags above the
/// token may be newer than what the read returned, and refuses the fill.
///
/// The token names its clock as well as its position. A store refuses, as
/// [`Fill::Invalidated`], a token read from another clock: the other mode, or
/// a shared tag table recreated since. Replicated stores on one fleet share
/// one clock, and shared stores on one table share another, so a token taken
/// from one store is valid for every store on the same clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FillToken {
    source: u64,
    clock: u64,
}

impl FillToken {
    /// The source of every replicated store's tokens: the mutation ring has
    /// no identity of its own. A shared table's instance is never zero.
    pub(crate) const REPLICATED: u64 = 0;

    /// A token from the parts [`FillToken::source`] and [`FillToken::clock`]
    /// returned, for a layer that carries it without naming this type. Parts
    /// that were not read together from one token can admit a stale fill.
    pub const fn from_parts(
        source: u64,
        clock: u64,
    ) -> Self {
        Self { source, clock }
    }

    /// The clock this token was read from.
    pub const fn source(self) -> u64 {
        self.source
    }

    /// The position on that clock.
    pub const fn clock(self) -> u64 {
        self.clock
    }
}

/// What a fill did with its value. Every variant but [`Fill::Kept`] leaves
/// the store as it was; none is an error, and the value the caller read is
/// still the right answer for the read that produced it.
#[must_use = "a refused fill is not an error, but nothing was stored"]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fill {
    /// Stored. Reads may return it until a newer write of its key, an
    /// invalidation of one of its tags newer than its token, its expiry, or
    /// eviction.
    Kept,
    /// Refused by the fence: an invalidation of one of its tags may be newer
    /// than the token. That includes a tag sharing a hash bucket or a shared
    /// table set with an invalidated one, and a token from another clock.
    Invalidated,
    /// Refused by its key: a replicated view holds or has fenced a write of
    /// this key newer than the token. A shared store never answers this.
    Superseded,
    /// Not kept, for a reason that says nothing about staleness: the value
    /// had already expired, a replicated view could not make room for it, or
    /// the view requires resync.
    Skipped,
}

impl Fill {
    pub const fn is_kept(self) -> bool {
        matches!(self, Self::Kept)
    }
}

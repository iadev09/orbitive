//! # `orbit-core` — fleet-aware shared memory
//!
//! ## What this crate is
//!
//! Fleet-shared same-host runtime storage between workers.
//!
//! Every process in the fleet is an equal member. There is no master,
//! no worker — only peers. Whatever role distinction matters to the
//! embedder is the embedder's concern; not Orbit's.
//!
//! ## Status — first light (V0)
//!
//! V0 ships fixed-capacity append logs and current-state tables with POSIX
//! shared-memory backing on Unix. Semantic layers choose the shape that
//! matches their data: history belongs in rings; current leases do not.

#[doc(hidden)]
pub mod compile;
pub mod epoch;
pub mod error;
pub mod fleet;
pub mod ring;
#[cfg(unix)]
pub mod shm;

pub mod id {
    //! Re-export of the standalone `netid64` primitive.

    pub use netid64::{NetId64, ParseNetId64Error};
}

#[cfg(unix)]
pub mod ring_shm {
    //! Compatibility module for the pre-`ring::shm` layout.
    pub use crate::ring::shm::*;
}

pub use epoch::OrbitEpoch;
pub use error::{Error, Result};
pub use fleet::{Fleet, NodeId};
pub use id::{NetId64, ParseNetId64Error};
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
pub use ring::RingEventFd;
pub use ring::cursor::{RingCursor, RingFrameSource, RingLoss, RingPoll, RingRead, poll_ring};
pub use ring::{Frame, Ring, RingSpec, RingTopology};

/// Marker for a type that has a stable wire identity across the fleet.
///
/// Required bounds:
/// - `Clone` — values may be delivered to multiple subscribers / nodes.
/// - `Send + Sync + 'static` — values cross thread / process boundaries.
pub trait OrbitTyped: Clone + Send + Sync + 'static {
    /// Stable wire identifier. Hand-picked in V0; build.rs-generated later.
    const KIND: u8;

    /// Ring layout and retention policy for this wire kind.
    ///
    /// This is part of the cross-process wire contract. Reusing a KIND
    /// with a different spec is an error.
    const RING_SPEC: RingSpec;
}

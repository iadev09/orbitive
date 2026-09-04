//! Feature-gated facade for Orbit runtime primitives.
//!
//! Common core types are available at the crate root. The complete low-level
//! API is under [`core`], while optional semantic crates keep separate module
//! namespaces.

/// Low-level Orbit primitives.
pub mod core {
    pub use orbit_core::*;
}

pub use orbit_core::{
    Error, Fleet, Frame, NetId64, NodeId, OrbitEpoch, OrbitTyped, Result, RingSpec,
};

pub mod ring {
    pub use orbit_core::ring::*;
}

#[cfg(unix)]
pub mod shm {
    pub use orbit_core::shm::*;
}

pub mod fleet {
    pub use orbit_core::fleet::*;
}

#[cfg(feature = "cache")]
pub mod cache {
    pub use orbit_cache::*;
}

#[cfg(feature = "events")]
pub mod events {
    pub use orbit_events::*;
}

#[cfg(feature = "metrics")]
pub mod metrics {
    pub use orbit_metrics::*;
}

#[cfg(feature = "lock")]
pub mod lock {
    pub use orbit_lock::*;
}

#[cfg(any(feature = "rustls", feature = "rustls_0_23", feature = "rustls_0_24"))]
pub mod rustls {
    pub use orbit_rustls::*;
}

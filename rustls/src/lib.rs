//! Fleet-shared rustls state over Orbit shared memory.
//!
//! The public adapter implements the enabled rustls version's
//! `server::StoresServerSessions` trait. The underlying table is connected to
//! rustls only through version-specific adapters: it imports no rustls types
//! and treats rustls-generated keys and encoded values as opaque, sensitive
//! bytes. rustls owns generation, encoding, and validation; this crate owns
//! bounded storage, TTL, and atomic single-use `take` across fleet processes.
//!
//! This crate is not an application or web-session store. Its table is lossy,
//! fixed-capacity, and allowed to turn a missing entry into a full TLS
//! handshake. Application sessions normally require reusable reads, explicit
//! durability, and different eviction guarantees.

#[cfg(not(unix))]
compile_error!("orbit-rustls currently requires a Unix target");

#[cfg(unix)]
mod session;

#[cfg(unix)]
pub use session::{
    DEFAULT_SESSION_TTL, FleetServerSessions, MAX_SESSION_TTL, OrbitSessionStorage, SessionDomain,
};

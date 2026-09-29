//! Fleet-shared rustls state over Orbit shared memory.
//!
//! The public adapters implement the enabled rustls version's
//! `server::StoresServerSessions` trait and, with rustls 0.24,
//! `client::ClientSessionStore`. The underlying tables are connected to rustls
//! only through version-specific adapters: they import no rustls types and
//! treat rustls-generated keys and encoded values as opaque, sensitive bytes.
//! rustls owns generation, encoding, and validation; this crate owns bounded
//! storage, TTL, and atomic single-use `take` across fleet processes.
//!
//! This crate is not an application or web-session store. Its table is lossy,
//! fixed-capacity, and allowed to turn a missing entry into a full TLS
//! handshake. Application sessions normally require reusable reads, explicit
//! durability, and different eviction guarantees.

#[cfg(not(unix))]
compile_error!("orbit-rustls currently requires a Unix target");

#[cfg(unix)]
mod session;

#[cfg(all(unix, feature = "rustls_0_24"))]
pub use session::{CLIENT_SESSION_STATE_KIND, FleetClientSessions, OrbitClientSessionStorage};
#[cfg(unix)]
pub use session::{
    DEFAULT_SESSION_TTL, FleetServerSessions, MAX_SESSION_TTL, OrbitServerSessionStorage,
    OrbitSessionStorage, SERVER_SESSION_STATE_KIND, SessionDomain
};

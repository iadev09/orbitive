use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{fmt, io};

use orbit_core::Fleet;

use self::store::{DOMAIN_MAX, SessionPrimitive};

mod store;
#[cfg(feature = "rustls_0_23")]
mod v23;
#[cfg(feature = "rustls_0_24")]
mod v24;

/// Default retention for opaque rustls server-session values.
///
/// This is no longer than rustls 0.23's stateful TLS 1.3 ticket lifetime.
pub const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);
pub const MAX_SESSION_TTL: Duration = DEFAULT_SESSION_TTL;

/// Stable isolation boundary for one equivalent set of rustls server configs.
///
/// Transport family, authentication policy, and any compatibility epoch must
/// be reflected in this value. Public TLS and mTLS must not share a domain.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SessionDomain(Arc<[u8]>);

impl SessionDomain {
    pub fn new(value: impl AsRef<[u8]>) -> io::Result<Self> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rustls session domain must not be empty",
            ));
        }
        if value.len() > DOMAIN_MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "rustls session domain is too long: {} bytes (maximum {DOMAIN_MAX})",
                    value.len()
                ),
            ));
        }
        Ok(Self(Arc::from(value)))
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SessionDomain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SessionDomain")
            .field(&String::from_utf8_lossy(&self.0))
            .finish()
    }
}

/// One fleet-wide session table from which isolated rustls views are made.
#[derive(Clone)]
pub struct FleetServerSessions {
    primitive: Arc<SessionPrimitive>,
    ttl_ms: Arc<AtomicU64>,
}

impl FleetServerSessions {
    pub fn open(fleet: Arc<Fleet>) -> io::Result<Self> {
        Self::with_ttl(fleet, DEFAULT_SESSION_TTL)
    }

    pub fn with_ttl(fleet: Arc<Fleet>, ttl: Duration) -> io::Result<Self> {
        let ttl_ms = validate_ttl(ttl)?;
        Ok(Self {
            primitive: Arc::new(SessionPrimitive::open(&fleet)?),
            ttl_ms: Arc::new(AtomicU64::new(ttl_ms)),
        })
    }

    pub fn set_ttl(&self, ttl: Duration) -> io::Result<()> {
        self.ttl_ms.store(validate_ttl(ttl)?, Ordering::Release);
        Ok(())
    }

    pub fn ttl(&self) -> Duration {
        Duration::from_millis(self.ttl_ms.load(Ordering::Acquire))
    }

    /// Create a rustls storage view isolated by `domain`.
    pub fn storage(&self, domain: SessionDomain) -> Arc<OrbitSessionStorage> {
        Arc::new(OrbitSessionStorage {
            primitive: self.primitive.clone(),
            domain,
            ttl_ms: self.ttl_ms.clone(),
        })
    }

    /// Clear every domain. Call only during owner-controlled quiescent boot.
    pub fn reset(&self) -> io::Result<()> {
        self.primitive.reset()
    }

    /// Remove the SHM name and companion lock file.
    ///
    /// Existing mappings remain valid. This is intended for fleet teardown
    /// and tests, not runtime cache invalidation.
    pub fn unlink(&self) -> io::Result<()> {
        self.primitive.unlink()
    }
}

impl fmt::Debug for FleetServerSessions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FleetServerSessions")
            .field("ttl", &self.ttl())
            .finish_non_exhaustive()
    }
}

pub struct OrbitSessionStorage {
    primitive: Arc<SessionPrimitive>,
    domain: SessionDomain,
    ttl_ms: Arc<AtomicU64>,
}

impl fmt::Debug for OrbitSessionStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrbitSessionStorage")
            .field("domain", &self.domain)
            .field(
                "ttl",
                &Duration::from_millis(self.ttl_ms.load(Ordering::Acquire)),
            )
            .finish_non_exhaustive()
    }
}

impl OrbitSessionStorage {
    fn put_bytes(&self, key: &[u8], value: &[u8]) -> bool {
        let ttl = Duration::from_millis(self.ttl_ms.load(Ordering::Acquire));
        match self.primitive.put(self.domain.as_bytes(), key, value, ttl) {
            Ok(cached) => {
                tracing::trace!(
                    target: "orbit_rustls::session_cache",
                    operation = "put",
                    domain = ?self.domain,
                    key_len = key.len(),
                    value_len = value.len(),
                    ?ttl,
                    cached,
                    "rustls session cache write"
                );
                cached
            }
            Err(error) => {
                tracing::trace!(
                    target: "orbit_rustls::session_cache",
                    operation = "put",
                    domain = ?self.domain,
                    key_len = key.len(),
                    value_len = value.len(),
                    ?ttl,
                    %error,
                    "rustls session cache write failed"
                );
                false
            }
        }
    }

    fn get_bytes(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.primitive.get(self.domain.as_bytes(), key) {
            Ok(value) => {
                tracing::trace!(
                    target: "orbit_rustls::session_cache",
                    operation = "get",
                    domain = ?self.domain,
                    key_len = key.len(),
                    value_len = value.as_ref().map_or(0, Vec::len),
                    hit = value.is_some(),
                    "rustls session cache read"
                );
                value
            }
            Err(error) => {
                tracing::trace!(
                    target: "orbit_rustls::session_cache",
                    operation = "get",
                    domain = ?self.domain,
                    key_len = key.len(),
                    %error,
                    "rustls session cache read failed"
                );
                None
            }
        }
    }

    fn take_bytes(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.primitive.take(self.domain.as_bytes(), key) {
            Ok(value) => {
                tracing::trace!(
                    target: "orbit_rustls::session_cache",
                    operation = "take",
                    domain = ?self.domain,
                    key_len = key.len(),
                    value_len = value.as_ref().map_or(0, Vec::len),
                    hit = value.is_some(),
                    "rustls session cache read and consume"
                );
                value
            }
            Err(error) => {
                tracing::trace!(
                    target: "orbit_rustls::session_cache",
                    operation = "take",
                    domain = ?self.domain,
                    key_len = key.len(),
                    %error,
                    "rustls session cache read and consume failed"
                );
                None
            }
        }
    }
}

fn validate_ttl(ttl: Duration) -> io::Result<u64> {
    if ttl.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rustls session TTL must be greater than zero",
        ));
    }
    if ttl > MAX_SESSION_TTL {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "rustls session TTL {ttl:?} exceeds rustls' stateful lifetime {MAX_SESSION_TTL:?}"
            ),
        ));
    }
    u64::try_from(ttl.as_millis().max(1)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "rustls session TTL does not fit milliseconds",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sessions() -> FleetServerSessions {
        let fleet = Arc::new(Fleet::join("rustls-session-unit", 1).expect("fleet"));
        FleetServerSessions::open(fleet).expect("sessions")
    }

    #[test]
    fn rustls_view_put_get_and_take() {
        let sessions = sessions();
        let storage = sessions.storage(SessionDomain::new("tcp-public").expect("domain"));

        assert!(storage.put_bytes(b"ticket", b"secret"));
        assert_eq!(storage.get_bytes(b"ticket"), Some(b"secret".to_vec()));
        assert_eq!(storage.take_bytes(b"ticket"), Some(b"secret".to_vec()));
        assert_eq!(storage.take_bytes(b"ticket"), None);
    }

    #[test]
    fn domains_are_isolated() {
        let sessions = sessions();
        let tcp = sessions.storage(SessionDomain::new("tcp-public").expect("domain"));
        let quic = sessions.storage(SessionDomain::new("quic-public").expect("domain"));

        assert!(tcp.put_bytes(b"same-ticket", b"tcp"));
        assert_eq!(quic.take_bytes(b"same-ticket"), None);
        assert_eq!(tcp.take_bytes(b"same-ticket"), Some(b"tcp".to_vec()));
    }

    #[test]
    fn existing_views_observe_ttl_updates() {
        let sessions = sessions();
        let storage = sessions.storage(SessionDomain::new("tcp-public").expect("domain"));
        sessions
            .set_ttl(Duration::from_millis(1))
            .expect("TTL update");

        assert!(storage.put_bytes(b"short", b"secret"));
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(storage.get_bytes(b"short"), None);
    }

    #[test]
    fn ttl_cannot_outlive_rustls_stateful_tickets() {
        let fleet = Arc::new(Fleet::join("rustls-session-ttl", 1).expect("fleet"));

        assert!(
            FleetServerSessions::with_ttl(fleet, MAX_SESSION_TTL + Duration::from_millis(1))
                .is_err()
        );
    }
}

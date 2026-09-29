//! Token strings arrive here AFTER protocol-specific extraction in the adapter.
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use orbit_auth::{
    Audience, Authority, Blake3State, Capability, Claims, FleetAuth, Keyring, Purpose, Result,
    SecretKey, Validation
};
use orbit_core::Fleet;

fn main() -> Result<()> {
    // Demo provisioning only. Real services load the same keys from a secret manager.
    let authority = Authority::new(
        "shared-prod",
        "identity-service",
        Keyring::new(1, [(1, SecretKey::generate()?)])?
    )?;
    // The host supplies its Fleet; use join_shm_as for cross-process sharing.
    let fleet = Arc::new(Fleet::join("auth-example", 1).unwrap());
    let cache = FleetAuth::new(
        fleet,
        &authority,
        SecretKey::generate()?,
        NonZeroUsize::new(128).unwrap(),
        Blake3State
    )?;
    let now = 1_800_000_000;
    let session_audience = Audience::new("session-service")?;
    let session_state = cache.create_session("user:42", now, now + 3600)?;
    let session = authority.issue_session(
        Claims {
            subject: "user:42".into(),
            audience: session_audience.clone(),
            purpose: Purpose::Session,
            capabilities: BTreeSet::from([Capability::new("profile:read")?]),
            issued_at: now,
            not_before: now,
            expires_at: now + 300
        },
        &session_state
    )?;

    // session.example.com: adapter extracts its own secure session cookie and
    // enforces CSRF/Origin policy before this token-exchange endpoint is reached.
    let session_validator = authority.validator(Validation {
        audience: session_audience,
        purpose: Purpose::Session,
        required_capabilities: BTreeSet::new()
    });
    let signed_in = session_validator.validate_cached(
        session.expose(),
        now,
        &cache,
        Duration::from_secs(30),
        &cache
    )?;

    for (audience, purpose) in [
        ("public-api", Purpose::Access), // api.other-domain.com: Bearer header
        ("realtime", Purpose::WebSocket), // ws.third-domain.net: first AUTH frame
        ("profile-worker", Purpose::Internal)  // Orbit adapter: initial control envelope
    ] {
        let audience = Audience::new(audience)?;
        // The issuer's application policy must authorize each target and the
        // delegation. This demo grants only the already-authenticated read permission.
        let read = Capability::new("profile:read")?;
        if !signed_in.has_capability(&read) {
            return Err(orbit_auth::Error::MissingCapability);
        }
        let token = authority.issue_session(
            Claims {
                subject: signed_in.subject().into(),
                audience: audience.clone(),
                purpose,
                capabilities: BTreeSet::from([read.clone()]),
                issued_at: now,
                not_before: now,
                expires_at: now + 60
            },
            &session_state
        )?;
        let validator = authority.validator(Validation {
            audience,
            purpose,
            required_capabilities: BTreeSet::from([read])
        });
        let principal = validator.validate_cached(
            token.expose(),
            now,
            &cache,
            Duration::from_secs(30),
            &cache
        )?;
        assert_eq!(principal.subject(), signed_in.subject());
        // WS/Orbit adapters may bind this Principal to an authenticated connection.
        // They continue to enforce expiry and per-operation capabilities themselves.
    }
    // One refresh generation is consumed across every peer sharing this fleet.
    let refresh = authority.issue_session(
        Claims {
            subject: session_state.subject().into(),
            audience: Audience::new("session-service")?,
            purpose: Purpose::Refresh,
            capabilities: BTreeSet::new(),
            issued_at: now,
            not_before: now,
            expires_at: now + 300
        },
        &session_state
    )?;
    let refresh_validator = authority.validator(Validation {
        audience: Audience::new("session-service")?,
        purpose: Purpose::Refresh,
        required_capabilities: BTreeSet::new()
    });
    let renewed = cache.refresh(&refresh_validator, refresh.expose(), now, now + 3600)?;
    assert_eq!(renewed.id(), session_state.id());
    cache.revoke_session(renewed.id(), now)?;
    Ok(())
}

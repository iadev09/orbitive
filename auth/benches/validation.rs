use std::collections::BTreeSet;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use orbit_auth::*;
use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::NodeId;

fn validation(c: &mut Criterion) {
    mode(c, "auth", Blake3State);
    mode(c, "auth_encrypted", EncryptedState);
    mode(c, "auth_unprotected", UnprotectedState);
}
fn mode<P: StateProtection>(
    c: &mut Criterion,
    label: &str,
    protection: P
) {
    // Benchmark-only credentials and lifetimes; no live application namespace.
    let authority = Authority::new(
        "bench",
        "issuer",
        Keyring::new(1, [(1, SecretKey::from_bytes([7; 32]))]).unwrap()
    )
    .unwrap();
    let mut random = [0; 4];
    getrandom::fill(&mut random).unwrap();
    let name = format!("ab{:x}", u32::from_le_bytes(random));
    #[cfg(unix)]
    let fleet = Arc::new(Fleet::join_shm_as(&name, 1, NodeId::ZERO).unwrap());
    #[cfg(not(unix))]
    let fleet = Arc::new(Fleet::join(&name, 1).unwrap());
    let auth = FleetAuth::new(
        fleet,
        &authority,
        SecretKey::from_bytes([9; 32]),
        NonZeroUsize::new(128).unwrap(),
        protection
    )
    .unwrap();
    let session = auth.create_session("subject", 100, 200).unwrap();
    let audience = Audience::new("api").unwrap();
    let token = authority
        .issue_session(
            Claims {
                subject: "subject".into(),
                audience: audience.clone(),
                purpose: Purpose::Access,
                capabilities: BTreeSet::new(),
                issued_at: 100,
                not_before: 100,
                expires_at: 200
            },
            &session
        )
        .unwrap();
    eprintln!("fixture {label}: opaque token {} bytes", token.expose().len());
    let validator = authority.validator(Validation {
        audience,
        purpose: Purpose::Access,
        required_capabilities: BTreeSet::new()
    });
    let ttl = Duration::from_secs(50); // fixed fixture lifetime, not a runtime default
    let principal = validator.validate_cached(token.expose(), 110, &auth, ttl, &auth).unwrap();
    c.bench_function(&format!("{label}/token_decrypt_decode"), |b| {
        b.iter(|| {
            black_box(validator.validate(black_box(token.expose()), 110, &AllowReusable).unwrap())
        })
    });
    c.bench_function(&format!("{label}/live_session_check"), |b| {
        b.iter(|| {
            auth.check(black_box(&principal), 110).unwrap();
            black_box(())
        })
    });
    c.bench_function(&format!("{label}/cached_request_with_session"), |b| {
        b.iter(|| {
            black_box(
                validator
                    .validate_cached(black_box(token.expose()), 110, &auth, ttl, &auth)
                    .unwrap()
            )
        })
    });
    c.bench_function(&format!("{label}/cold_session_check"), |b| {
        b.iter_batched(
            || auth.clear_local(),
            |()| {
                auth.check(black_box(&principal), 110).unwrap();
                black_box(())
            },
            BatchSize::PerIteration
        )
    });
    drop(auth);
    #[cfg(unix)]
    orbit_core::shm::ShmRegion::open_or_create_locked(
        &orbit_core::shm::ring_segment_name(&name, AUTH_STATE_KIND),
        64
    )
    .unwrap()
    .0
    .unlink()
    .unwrap();
}
criterion_group!(benches, validation);
criterion_main!(benches);

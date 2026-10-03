use std::collections::{BTreeSet, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use orbit_auth::*;

fn authority(
    realm: &str,
    issuer: &str,
    active: u32,
    keys: &[(u32, u8)]
) -> Authority {
    Authority::new(
        realm,
        issuer,
        Keyring::new(
            active,
            keys.iter().map(|(id, byte)| (*id, SecretKey::from_bytes([*byte; 32])))
        )
        .unwrap()
    )
    .unwrap()
}
fn standard() -> Authority {
    authority("prod", "login", 1, &[(1, 7)])
}
fn policy(
    audience: &str,
    purpose: Purpose
) -> Validation {
    Validation {
        audience: Audience::new(audience).unwrap(),
        purpose,
        required_capabilities: BTreeSet::new()
    }
}
fn claims() -> Claims {
    Claims {
        subject: "user:42".into(),
        audience: Audience::new("api").unwrap(),
        purpose: Purpose::Access,
        capabilities: BTreeSet::from([Capability::new("read").unwrap()]),
        issued_at: 100,
        not_before: 110,
        expires_at: 200
    }
}
fn cache() -> MemoryCache {
    MemoryCache::new(NonZeroUsize::new(8).unwrap())
}

#[test]
fn roundtrip_all_purposes_and_randomness() {
    let issuer = standard();
    let peer = standard();
    for purpose in
        [Purpose::Session, Purpose::Refresh, Purpose::Access, Purpose::WebSocket, Purpose::Internal]
    {
        let mut claims = claims();
        claims.purpose = purpose;
        let first = issuer.issue(claims.clone()).unwrap();
        let second = issuer.issue(claims.clone()).unwrap();
        assert_ne!(first.expose(), second.expose());
        let validator = peer.validator(policy("api", purpose));
        let principal = validator.validate(first.expose(), 110, &AllowReusable).unwrap();
        assert_eq!(principal.claims(), &claims);
        assert_eq!(principal.subject(), "user:42");
        assert_eq!(principal.realm(), "prod");
        assert_eq!(principal.issuer(), "login");
        assert!(principal.has_capability(&Capability::new("read").unwrap()));
        assert_ne!(
            principal.jti(),
            validator.validate(second.expose(), 110, &AllowReusable).unwrap().jti()
        );
        assert!(!first.expose().contains("user:42"));
        assert_eq!(format!("{first:?}"), "Token([REDACTED])");
    }
    assert_eq!(format!("{:?}", SecretKey::from_bytes([7; 32])), "SecretKey([REDACTED])");
}

#[test]
fn time_and_capabilities_are_enforced() {
    let authority = standard();
    let token = authority.issue(claims()).unwrap();
    let validator = authority.validator(policy("api", Purpose::Access));
    assert_eq!(validator.validate(token.expose(), 109, &AllowReusable), Err(Error::NotYetValid));
    assert!(validator.validate(token.expose(), 110, &AllowReusable).is_ok());
    assert!(validator.validate(token.expose(), 199, &AllowReusable).is_ok());
    assert_eq!(validator.validate(token.expose(), 200, &AllowReusable), Err(Error::Expired));
    let mut required = policy("api", Purpose::Access);
    required.required_capabilities.insert(Capability::new("write").unwrap());
    assert_eq!(
        authority.validator(required).validate(token.expose(), 110, &AllowReusable),
        Err(Error::MissingCapability)
    );
}

#[test]
fn lapsed_validation_requires_an_explicit_hook_opt_in() {
    let authority = standard();
    let token = authority.issue(claims()).unwrap();
    let validator = authority.validator(policy("api", Purpose::Access));

    assert_eq!(validator.validate_lapsed(token.expose(), 200, &AllowReusable), Err(Error::Expired));
}

#[test]
fn tampering_every_wire_byte_and_malformed_tokens_fail() {
    let authority = standard();
    let token = authority.issue(claims()).unwrap();
    let validator = authority.validator(policy("api", Purpose::Access));
    let wire = URL_SAFE_NO_PAD.decode(token.expose().strip_prefix("oa1.").unwrap()).unwrap();
    for index in 0..wire.len() {
        let mut corrupted = wire.clone();
        corrupted[index] ^= 1;
        let changed = format!("oa1.{}", URL_SAFE_NO_PAD.encode(corrupted));
        assert_eq!(validator.validate(&changed, 110, &AllowReusable), Err(Error::InvalidToken));
    }
    for malformed in ["", "oa2.AAAA", "oa1.", "oa1.***", "oa1.AAA=", "oa1.AAAA", "bearer abc"] {
        assert_eq!(validator.validate(malformed, 110, &AllowReusable), Err(Error::InvalidToken));
    }
    for length in 0..wire.len() {
        let truncated = format!("oa1.{}", URL_SAFE_NO_PAD.encode(&wire[..length]));
        assert!(validator.validate(&truncated, 110, &AllowReusable).is_err());
    }
    assert_eq!(
        validator.validate(&"a".repeat(MAX_TOKEN_LEN + 1), 110, &AllowReusable),
        Err(Error::TokenTooLarge)
    );
}

#[test]
fn trust_contexts_cannot_share_tokens_or_warm_cache_entries() {
    let original = standard();
    let token = original.issue(claims()).unwrap();
    let cache = cache();
    let validator = original.validator(policy("api", Purpose::Access));
    let ttl = Duration::from_secs(90);
    validator.validate_cached(token.expose(), 110, &cache, ttl, &AllowReusable).unwrap();
    let original_key = validator.cache_key(token.expose()).unwrap();
    for other in [
        authority("stage", "login", 1, &[(1, 7)]),
        authority("prod", "other-issuer", 1, &[(1, 7)]),
        authority("prod", "login", 1, &[(1, 8)])
    ] {
        let v = other.validator(policy("api", Purpose::Access));
        assert_ne!(original_key, v.cache_key(token.expose()).unwrap());
        assert_eq!(
            v.validate_cached(token.expose(), 110, &cache, ttl, &AllowReusable),
            Err(Error::InvalidToken)
        );
    }
    for different in [policy("other-api", Purpose::Access), policy("api", Purpose::Internal)] {
        let v = original.validator(different);
        assert_ne!(original_key, v.cache_key(token.expose()).unwrap());
        assert_eq!(
            v.validate_cached(token.expose(), 110, &cache, ttl, &AllowReusable),
            Err(Error::InvalidToken)
        );
    }
    let mut different = policy("api", Purpose::Access);
    different.required_capabilities.insert(Capability::new("write").unwrap());
    let v = original.validator(different);
    assert_ne!(original_key, v.cache_key(token.expose()).unwrap());
    assert_eq!(
        v.validate_cached(token.expose(), 110, &cache, ttl, &AllowReusable),
        Err(Error::MissingCapability)
    );
}

#[test]
fn cache_reuses_decoded_principal_and_obeys_ttl_expiry_and_clock_rollback() {
    let authority = standard();
    let token = authority.issue(claims()).unwrap();
    let validator = authority.validator(policy("api", Purpose::Access));
    let cache = cache();
    let ttl = Duration::from_secs(30);
    let first =
        validator.validate_cached(token.expose(), 110, &cache, ttl, &AllowReusable).unwrap();
    let second =
        validator.validate_cached(token.expose(), 139, &cache, ttl, &AllowReusable).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    let after_ttl =
        validator.validate_cached(token.expose(), 140, &cache, ttl, &AllowReusable).unwrap();
    assert!(!Arc::ptr_eq(&first, &after_ttl));
    let shorter = validator
        .validate_cached(token.expose(), 145, &cache, Duration::from_secs(5), &AllowReusable)
        .unwrap();
    assert!(!Arc::ptr_eq(&after_ttl, &shorter));
    let rollback =
        validator.validate_cached(token.expose(), 144, &cache, ttl, &AllowReusable).unwrap();
    assert!(!Arc::ptr_eq(&shorter, &rollback));
    assert_eq!(
        validator.validate_cached(token.expose(), 109, &cache, ttl, &AllowReusable),
        Err(Error::NotYetValid)
    );
    cache.clear();
    validator.validate_cached(token.expose(), 190, &cache, Duration::MAX, &AllowReusable).unwrap();
    let entry = cache.get(&validator.cache_key(token.expose()).unwrap()).unwrap();
    assert_eq!(entry.valid_until(), 200);
    assert_eq!(
        validator.validate_cached(token.expose(), 200, &cache, ttl, &AllowReusable),
        Err(Error::Expired)
    );
}

#[test]
fn rotation_retains_old_tokens_but_removal_invalidates_warm_cache() {
    let old = standard();
    let rotated = authority("prod", "login", 2, &[(1, 7), (2, 9)]);
    let removed = authority("prod", "login", 2, &[(2, 9)]);
    let token = old.issue(claims()).unwrap();
    let rotated_validator = rotated.validator(policy("api", Purpose::Access));
    let cache = cache();
    let ttl = Duration::from_secs(80);
    rotated_validator.validate_cached(token.expose(), 110, &cache, ttl, &AllowReusable).unwrap();
    let removed_validator = removed.validator(policy("api", Purpose::Access));
    assert_ne!(
        rotated_validator.cache_key(token.expose()).unwrap(),
        removed_validator.cache_key(token.expose()).unwrap()
    );
    assert_eq!(
        removed_validator.validate_cached(token.expose(), 110, &cache, ttl, &AllowReusable),
        Err(Error::InvalidToken)
    );
    let fresh = rotated.issue(claims()).unwrap();
    assert!(removed_validator.validate(fresh.expose(), 110, &AllowReusable).is_ok());
    assert_eq!(
        old.validator(policy("api", Purpose::Access)).validate(fresh.expose(), 110, &AllowReusable),
        Err(Error::InvalidToken)
    );
}

struct Once(Mutex<HashSet<(String, String, TokenId)>>);
impl ValidationHook for Once {
    fn check(
        &self,
        p: &Principal,
        _: u64
    ) -> Result<()> {
        if self.0.lock().unwrap().insert((p.realm().into(), p.issuer().into(), p.jti())) {
            Ok(())
        } else {
            Err(Error::Replayed)
        }
    }
}
struct Reject(Error);
impl ValidationHook for Reject {
    fn check(
        &self,
        _: &Principal,
        _: u64
    ) -> Result<()> {
        match self.0 {
            Error::Revoked => Err(Error::Revoked),
            _ => Err(Error::PolicyUnavailable)
        }
    }
}

#[test]
fn replay_revocation_and_store_failures_run_on_cache_hits() {
    let authority = standard();
    let validator = authority.validator(policy("api", Purpose::Access));
    let token = authority.issue(claims()).unwrap();
    let cache = cache();
    let ttl = Duration::from_secs(90);
    let once = Once(Mutex::new(HashSet::new()));
    validator.validate_cached(token.expose(), 110, &cache, ttl, &once).unwrap();
    assert!(cache.get(&validator.cache_key(token.expose()).unwrap()).is_some());
    assert_eq!(
        validator.validate_cached(token.expose(), 110, &cache, ttl, &once),
        Err(Error::Replayed)
    );
    assert_eq!(
        validator.validate_cached(token.expose(), 110, &cache, ttl, &Reject(Error::Revoked)),
        Err(Error::Revoked)
    );
    assert_eq!(
        validator.validate_cached(
            token.expose(),
            110,
            &cache,
            ttl,
            &Reject(Error::PolicyUnavailable)
        ),
        Err(Error::PolicyUnavailable)
    );
    cache.clear();
    assert!(
        validator
            .validate_cached(token.expose(), 110, &cache, ttl, &Reject(Error::Revoked))
            .is_err()
    );
    assert!(cache.get(&validator.cache_key(token.expose()).unwrap()).is_none());
}

#[test]
fn atomic_hook_allows_only_one_concurrent_authentication() {
    let authority = standard();
    let validator = authority.validator(policy("api", Purpose::Access));
    let token = authority.issue(claims()).unwrap();
    let cache = cache();
    let once = Once(Mutex::new(HashSet::new()));
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    validator.validate_cached(
                        token.expose(),
                        110,
                        &cache,
                        Duration::from_secs(30),
                        &once
                    )
                })
            })
            .collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|r| **r == Err(Error::Replayed)).count(), 7);
    });
}

#[test]
fn bounds_bad_configuration_and_invalid_claims() {
    assert!(matches!(Keyring::new(1, []), Err(Error::MissingActiveKey)));
    assert!(matches!(
        Keyring::new(1, [(1, SecretKey::from_bytes([0; 32])), (1, SecretKey::from_bytes([1; 32]))]),
        Err(Error::DuplicateKey)
    ));
    for label in ["", "bad\nlabel", &"x".repeat(MAX_LABEL_LEN + 1)] {
        assert!(Audience::new(label).is_err());
        assert!(Capability::new(label).is_err());
        assert!(
            Authority::new(
                label,
                "issuer",
                Keyring::new(1, [(1, SecretKey::from_bytes([1; 32]))]).unwrap()
            )
            .is_err()
        );
    }
    let authority = standard();
    let mut invalid = claims();
    invalid.expires_at = invalid.not_before;
    assert!(matches!(authority.issue(invalid), Err(Error::InvalidInput)));
    let mut invalid = claims();
    invalid.issued_at = invalid.not_before + 1;
    assert!(matches!(authority.issue(invalid), Err(Error::InvalidInput)));
    let mut invalid = claims();
    invalid.capabilities =
        (0..=MAX_CAPABILITIES).map(|n| Capability::new(n.to_string()).unwrap()).collect();
    assert!(matches!(authority.issue(invalid), Err(Error::InvalidInput)));
    let mut huge = claims();
    huge.capabilities = (0..MAX_CAPABILITIES)
        .map(|n| Capability::new(format!("{n:03}{}", "x".repeat(250))).unwrap())
        .collect();
    assert!(matches!(authority.issue(huge), Err(Error::TokenTooLarge)));
}

#[test]
fn bounded_eviction_zero_ttl_and_wrong_cached_entry_are_safe() {
    let authority = standard();
    let validator = authority.validator(policy("api", Purpose::Access));
    let first = authority.issue(claims()).unwrap();
    let second = authority.issue(claims()).unwrap();
    let cache = MemoryCache::new(NonZeroUsize::new(1).unwrap());
    let ttl = Duration::from_secs(30);
    let principal =
        validator.validate_cached(first.expose(), 110, &cache, ttl, &AllowReusable).unwrap();
    let key = validator.cache_key(first.expose()).unwrap();
    let entry = cache.get(&key).unwrap();
    validator.validate_cached(second.expose(), 110, &cache, ttl, &AllowReusable).unwrap();
    assert!(cache.get(&key).is_none());
    let second_key = validator.cache_key(second.expose()).unwrap();
    cache.insert(second_key, entry);
    let correct =
        validator.validate_cached(second.expose(), 110, &cache, ttl, &AllowReusable).unwrap();
    assert_ne!(principal.jti(), correct.jti());
    let uncached = validator
        .validate_cached(second.expose(), 110, &cache, Duration::ZERO, &AllowReusable)
        .unwrap();
    assert!(!Arc::ptr_eq(&correct, &uncached));
}

#[test]
fn owned_validator_outlives_authority_and_preserves_warm_cache_and_hooks() {
    fn assert_service_owned<T: Send + Sync + 'static>(_: &T) {}
    struct Reject;
    impl ValidationHook for Reject {
        fn check(
            &self,
            _: &Principal,
            _: u64
        ) -> Result<()> {
            Err(Error::Revoked)
        }
    }
    let authority = standard();
    let token = authority.issue(claims()).unwrap();
    let validator = authority.validator(policy("api", Purpose::Access));
    let cache = cache();
    let ttl = Duration::from_secs(90);
    let key = validator.cache_key(token.expose()).unwrap();
    let cached =
        validator.validate_cached(token.expose(), 110, &cache, ttl, &AllowReusable).unwrap();
    let owned: OwnedValidator = validator.into_owned();
    assert_service_owned(&owned);
    drop(authority);
    assert_eq!(owned.cache_key(token.expose()).unwrap(), key);
    let hit = owned.validate_cached(token.expose(), 111, &cache, ttl, &AllowReusable).unwrap();
    assert!(Arc::ptr_eq(&cached, &hit));
    assert_eq!(
        owned.validate_cached(token.expose(), 112, &cache, ttl, &Reject),
        Err(Error::Revoked)
    );
    assert_eq!(
        owned.validate_cached(token.expose(), 200, &cache, ttl, &AllowReusable),
        Err(Error::Expired)
    );
    // Cold verification also works without the root keyring.
    assert_eq!(*owned.validate(token.expose(), 110, &AllowReusable).unwrap(), *cached);
}

#[test]
fn owned_validator_keeps_domain_capability_and_key_rotation_boundaries() {
    let authority = standard();
    let token = authority.issue(claims()).unwrap();
    let owned = authority.validator(policy("api", Purpose::Access)).into_owned();
    let wrong_audience = authority.validator(policy("other", Purpose::Access)).into_owned();
    let wrong_purpose = authority.validator(policy("api", Purpose::Internal)).into_owned();
    let mut restricted = policy("api", Purpose::Access);
    restricted.required_capabilities.insert(Capability::new("write").unwrap());
    let restricted = authority.validator(restricted).into_owned();
    drop(authority);
    assert_eq!(
        wrong_audience.validate(token.expose(), 110, &AllowReusable),
        Err(Error::InvalidToken)
    );
    assert_eq!(
        wrong_purpose.validate(token.expose(), 110, &AllowReusable),
        Err(Error::InvalidToken)
    );
    assert_eq!(
        restricted.validate(token.expose(), 110, &AllowReusable),
        Err(Error::MissingCapability)
    );
    let replacement = super_authority_for_rotation();
    let replacement = replacement.validator(policy("api", Purpose::Access)).into_owned();
    assert_ne!(
        owned.cache_key(token.expose()).unwrap(),
        replacement.cache_key(token.expose()).unwrap()
    );
    assert!(owned.validate(token.expose(), 110, &AllowReusable).is_ok());
    assert!(replacement.validate(token.expose(), 110, &AllowReusable).is_err());
}

fn super_authority_for_rotation() -> Authority {
    authority("prod", "login", 1, &[(1, 8)])
}

#[test]
fn release_052_wire_fixture() {
    let authority = standard();
    // Minted with the pre-upgrade crypto dependencies and the public fixture key [7; 32].
    let token = "oa1.AAAAAVicZqEAo3aL0otDA0krivP-SnqjUSHwYR9egHOAbXtmVClzb1QgovRS3FIR5G2uEFGDtbD4-4H9uGIJ_1hVPt5V-64hK6vtu6-RaGh7w0lkSXwfC4hq_0z83VLS_WWeA1mevwz9D64ppy0Cv0M1OP0zG-86oiECuPjXiSbxY5_zvLlnvDLsG1WiLVwztZRskqI7TxziGBzAl8lSmVKVUVe1cWdaSE7zDQb8pp_Ru99YhkRnYfWNVEzxgMdapCKgDhdqi-ftk7bXZwL_5Pl9Wb4XTkhgptFLn9-m3EWj3SUuMNQgj3nM38TUG0EQiq8wbehbqohwwAiGE2OuP5nWCr7tqHjbWQ74gC7uzTk52hQ9EoDvEhyAAGHUpfY";
    let validator = authority.validator(policy("api", Purpose::Access));
    assert_eq!(validator.validate(token, 110, &AllowReusable).unwrap().claims(), &claims());
    assert_eq!(
        format!("{:?}", validator.cache_key(token).unwrap()),
        "CacheKey([39, 175, 110, 218, 106, 44, 114, 92, 173, 125, 12, 222, 207, 221, 189, 198, 163, 26, 162, 201, 167, 175, 226, 212, 53, 102, 184, 167, 144, 252, 159, 175])"
    );
}

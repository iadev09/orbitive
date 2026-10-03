#![deny(unsafe_op_in_unsafe_fn)]
#![doc = include_str!("../README.md")]

mod cache;
mod error;
mod fleet;
mod keyring;
mod principal;
mod protection;
mod table;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
pub use cache::{CacheKey, CachedPrincipal, MemoryCache, PrincipalCache};
use chacha20poly1305::aead::{Aead, Payload};
use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};
pub use error::{Error, Result};
pub use fleet::{FleetAuth, ReplayGuard, Session, SessionId};
use hmac::{Hmac, Mac};
pub use keyring::{KeyId, Keyring, SecretKey};
use keyring::{context, derive};
pub use principal::{
    AllowReusable, Audience, Capability, Claims, MAX_CAPABILITIES, MAX_LABEL_LEN, Principal,
    Purpose, TokenId, Validation, ValidationHook
};
pub use protection::{Blake3State, EncryptedState, StateProtection, UnprotectedState};
use sha2::{Digest, Sha256};
pub use table::{AUTH_STATE_CAPACITY, AUTH_STATE_KIND};
use zeroize::Zeroizing;

/// Protocol allocation bound. Transport adapters may impose smaller limits (e.g. cookies).
pub const MAX_TOKEN_LEN: usize = 16 * 1024;
const PREFIX: &str = "oa1.";
const HEADER_LEN: usize = 4 + 24;
const TAG_LEN: usize = 16;

/// Opaque bearer credential. Debug output is redacted and storage is zeroized on drop.
pub struct Token(Zeroizing<String>);

impl Token {
    /// Expose only for transport; do not log or use as a cache key.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Token {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>
    ) -> fmt::Result {
        f.write_str("Token([REDACTED])")
    }
}

/// Shared symmetric trust realm, independent of hostnames and protocols.
/// Every key holder can mint tokens; provision keys only to equally trusted services.
pub struct Authority {
    realm: String,
    issuer: String,
    keyring: Keyring,
    cache_seed: Zeroizing<[u8; 32]>
}

impl Authority {
    pub fn new(
        realm: impl Into<String>,
        issuer: impl Into<String>,
        keyring: Keyring
    ) -> Result<Self> {
        let realm = realm.into();
        let issuer = issuer.into();
        if !principal::valid_label(&realm) || !principal::valid_label(&issuer) {
            return Err(Error::InvalidInput);
        }
        let mut hash = Sha256::new();
        hash.update(b"orbit-auth/v1/cache-keyring");
        for (id, key) in &keyring.keys {
            let info = context(&[b"cache", realm.as_bytes(), issuer.as_bytes(), &id.to_be_bytes()]);
            let cache_key = derive(key, &info);
            hash.update(id.to_be_bytes());
            hash.update(cache_key.as_ref());
        }
        let cache_seed = Zeroizing::new(hash.finalize().into());
        Ok(Self { realm, issuer, keyring, cache_seed })
    }

    /// Caller must authorize subject and capabilities before issuance.
    pub fn issue(
        &self,
        claims: Claims
    ) -> Result<Token> {
        self.issue_bound(claims, None)
    }

    /// Issue a credential bound to shared session state. The caller authorizes
    /// audience/capability delegation; validation must use the FleetAuth hook.
    pub fn issue_session(
        &self,
        claims: Claims,
        session: &Session
    ) -> Result<Token> {
        session.check_issuance(self, &claims)?;
        self.issue_bound(claims, Some(session.binding()))
    }

    fn issue_bound(
        &self,
        claims: Claims,
        session: Option<fleet::SessionBinding>
    ) -> Result<Token> {
        claims.check_shape()?;
        let mut jti = [0; 16];
        let mut nonce = [0; 24];
        getrandom::fill(&mut jti).map_err(|_| Error::Randomness)?;
        getrandom::fill(&mut nonce).map_err(|_| Error::Randomness)?;
        let id = self.keyring.active;
        let info = self.scope(&claims.audience, claims.purpose, id);
        let key = derive(&self.keyring.keys[&id], &info);
        let principal = Principal {
            realm: self.realm.clone(),
            issuer: self.issuer.clone(),
            jti: TokenId(jti),
            session,
            claims
        };
        let plaintext =
            Zeroizing::new(serde_json::to_vec(&principal).map_err(|_| Error::InvalidInput)?);
        let wire_len = PREFIX.len() + ((HEADER_LEN + TAG_LEN + plaintext.len()) * 4).div_ceil(3);
        if wire_len > MAX_TOKEN_LEN {
            return Err(Error::TokenTooLarge);
        }
        let cipher = XChaCha20Poly1305::new_from_slice(key.as_ref()).expect("256-bit key");
        let ciphertext = cipher
            .encrypt(XNonce::from_slice(&nonce), Payload { msg: &plaintext, aad: &info })
            .map_err(|_| Error::InvalidToken)?;
        let mut wire = Vec::with_capacity(HEADER_LEN + ciphertext.len());
        wire.extend_from_slice(&id.to_be_bytes());
        wire.extend_from_slice(&nonce);
        wire.extend_from_slice(&ciphertext);
        Ok(Token(Zeroizing::new(format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(wire)))))
    }

    /// Build once per trusted recipient/use policy and reuse across requests.
    pub fn validator(
        &self,
        validation: Validation
    ) -> Validator<'_> {
        let keys = self
            .keyring
            .keys
            .iter()
            .map(|(id, key)| {
                let info = self.scope(&validation.audience, validation.purpose, *id);
                (*id, derive(key, &info))
            })
            .collect();
        let mut mac = <Hmac<Sha256> as hmac::KeyInit>::new_from_slice(self.cache_seed.as_ref())
            .expect("HMAC key");
        mac.update(&context(&[
            b"orbit-auth/v1/cache-policy",
            validation.audience.as_str().as_bytes(),
            validation.purpose.as_bytes()
        ]));
        for capability in &validation.required_capabilities {
            mac.update(&context(&[capability.as_str().as_bytes()]));
        }
        let cache_secret = Zeroizing::new(mac.finalize().into_bytes().into());
        Validator {
            realm: Cow::Borrowed(&self.realm),
            issuer: Cow::Borrowed(&self.issuer),
            validation,
            keys,
            cache_secret
        }
    }

    fn scope(
        &self,
        audience: &Audience,
        purpose: Purpose,
        id: KeyId
    ) -> Vec<u8> {
        token_scope(&self.realm, &self.issuer, audience, purpose, id)
    }
}

/// Prepared validator with mandatory audience/purpose and optional exact capabilities.
pub struct Validator<'a> {
    realm: Cow<'a, str>,
    issuer: Cow<'a, str>,
    validation: Validation,
    keys: BTreeMap<KeyId, Zeroizing<[u8; 32]>>,
    cache_secret: Zeroizing<[u8; 32]>
}

/// A prepared validator independent of the issuing Authority's lifetime.
/// Retains policy-scoped derived secrets, not the authority's root keyring.
pub type OwnedValidator = Validator<'static>;

impl Validator<'_> {
    /// Take ownership of the realm/issuer labels and move the prepared policy and
    /// derived keys unchanged. No key derivation or cache namespace change occurs.
    /// The result may outlive the Authority and be stored in a long-lived service.
    /// Like any prepared validator, it retains its trust snapshot until replaced.
    pub fn into_owned(self) -> OwnedValidator {
        Validator {
            realm: Cow::Owned(self.realm.into_owned()),
            issuer: Cow::Owned(self.issuer.into_owned()),
            validation: self.validation,
            keys: self.keys,
            cache_secret: self.cache_secret
        }
    }

    pub fn validate(
        &self,
        token: &str,
        now: u64,
        hook: &dyn ValidationHook
    ) -> Result<Arc<Principal>> {
        let principal = self.decrypt(token)?;
        self.check(&principal, now)?;
        hook.check(&principal, now)?;
        Ok(Arc::new(principal))
    }

    /// A credential past its own expiry whose session may still be live:
    /// the envelope is authenticated and every check but the credential's
    /// expiry runs, then the hook decides from the shared state (a revoked or
    /// expired session still refuses). For reissuing a carrier whose session
    /// outlived it, never for admitting work on the lapsed credential itself.
    /// Not cached: a lapsed credential is answered once, with a new one.
    pub fn validate_lapsed(
        &self,
        token: &str,
        now: u64,
        hook: &dyn ValidationHook
    ) -> Result<Arc<Principal>> {
        let principal = self.decrypt(token)?;
        self.check_lapsed(&principal, now)?;
        hook.check_lapsed(&principal, now)?;
        Ok(Arc::new(principal))
    }

    /// No token decryption, base64 or claims decoding on a warm cache hit.
    /// Hooks still consult current policy; FleetAuth checks its authenticated shared state.
    ///
    /// TTL is caller-owned and never exceeds token expiry. Zero/subsecond TTL disables
    /// caching. Static checks and the hook run every time. Callers supply current Unix
    /// seconds from a trusted clock, never from client input.
    pub fn validate_cached(
        &self,
        token: &str,
        now: u64,
        cache: &dyn PrincipalCache,
        ttl: Duration,
        hook: &dyn ValidationHook
    ) -> Result<Arc<Principal>> {
        let ttl = ttl.as_secs();
        if ttl == 0 {
            return self.validate(token, now, hook);
        }
        let key = self.cache_key(token)?;
        if let Some(entry) = cache.get(&key) {
            // Reject swapped entries, clock rollback, and a caller-shortened TTL.
            if entry.key == key
                && entry.cached_at <= now
                && now < entry.valid_until
                && now < entry.cached_at.saturating_add(ttl)
            {
                self.check(&entry.principal, now)?;
                hook.check(&entry.principal, now)?;
                return Ok(entry.principal);
            }
        }
        let principal = self.validate(token, now, hook)?;
        let valid_until = now.saturating_add(ttl).min(principal.claims.expires_at);
        cache.insert(
            key,
            CachedPrincipal { key, principal: principal.clone(), cached_at: now, valid_until }
        );
        Ok(principal)
    }

    /// Fingerprint calculation does not authenticate the token or expose its claims.
    pub fn cache_key(
        &self,
        token: &str
    ) -> Result<CacheKey> {
        check_size(token)?;
        let mut mac = <Hmac<Sha256> as hmac::KeyInit>::new_from_slice(self.cache_secret.as_ref())
            .expect("HMAC key");
        mac.update(token.as_bytes());
        Ok(CacheKey(mac.finalize().into_bytes().into()))
    }

    fn decrypt(
        &self,
        token: &str
    ) -> Result<Principal> {
        check_size(token)?;
        let encoded = token.strip_prefix(PREFIX).ok_or(Error::InvalidToken)?;
        let wire = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| Error::InvalidToken)?;
        if wire.len() < HEADER_LEN + TAG_LEN {
            return Err(Error::InvalidToken);
        }
        let id = KeyId::from_be_bytes(wire[..4].try_into().expect("checked header length"));
        let key = self.keys.get(&id).ok_or(Error::InvalidToken)?;
        let info = token_scope(
            &self.realm,
            &self.issuer,
            &self.validation.audience,
            self.validation.purpose,
            id
        );
        let cipher = XChaCha20Poly1305::new_from_slice(key.as_ref()).expect("256-bit key");
        let plaintext = Zeroizing::new(
            cipher
                .decrypt(
                    XNonce::from_slice(&wire[4..HEADER_LEN]),
                    Payload { msg: &wire[HEADER_LEN..], aad: &info }
                )
                .map_err(|_| Error::InvalidToken)?
        );
        serde_json::from_slice(&plaintext).map_err(|_| Error::InvalidToken)
    }

    fn check(
        &self,
        principal: &Principal,
        now: u64
    ) -> Result<()> {
        self.check_lapsed(principal, now)?;
        if now >= principal.claims.expires_at {
            return Err(Error::Expired);
        }
        Ok(())
    }

    /// Every static check but the credential's own expiry.
    fn check_lapsed(
        &self,
        principal: &Principal,
        now: u64
    ) -> Result<()> {
        let claims = &principal.claims;
        claims.check_shape().map_err(|_| Error::InvalidToken)?;
        if principal.realm != self.realm
            || principal.issuer != self.issuer
            || claims.audience != self.validation.audience
            || claims.purpose != self.validation.purpose
        {
            return Err(Error::InvalidToken);
        }
        if now < claims.not_before || now < claims.issued_at {
            return Err(Error::NotYetValid);
        }
        if !self.validation.required_capabilities.is_subset(&claims.capabilities) {
            return Err(Error::MissingCapability);
        }
        Ok(())
    }
}

fn check_size(token: &str) -> Result<()> {
    if token.len() > MAX_TOKEN_LEN {
        return Err(Error::TokenTooLarge);
    }
    if token.is_empty() {
        return Err(Error::InvalidToken);
    }
    Ok(())
}

fn token_scope(
    realm: &str,
    issuer: &str,
    audience: &Audience,
    purpose: Purpose,
    id: KeyId
) -> Vec<u8> {
    context(&[
        b"orbit-auth/v1/xchacha20poly1305",
        realm.as_bytes(),
        issuer.as_bytes(),
        audience.as_str().as_bytes(),
        purpose.as_bytes(),
        &id.to_be_bytes()
    ])
}

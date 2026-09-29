use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use hmac::{Hmac, Mac};
use lru::LruCache;
use orbit_core::Fleet;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::keyring::{context, derive};
use crate::table::{
    CONSUMED, REVOKED, Record, RecordProtection, SESSION, Snapshot, StoreKey, Table, find, insert
};
use crate::{
    Authority, CacheKey, CachedPrincipal, Claims, Error, MemoryCache, Principal, PrincipalCache,
    Purpose, Result, SecretKey, StateProtection, ValidationHook, Validator
};

/// Random session identity. It is not a bearer credential.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub [u8; 16]);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionBinding {
    pub(crate) id: SessionId,
    generation: u64
}

/// Issuer-side handle, never serialized into SHM. Live validation checks state
/// even when a caller retained a handle predating refresh or revocation.
pub struct Session {
    binding: SessionBinding,
    subject: String,
    created_at: u64,
    expires_at: u64,
    realm: String,
    issuer: String
}
impl Session {
    pub fn id(&self) -> SessionId {
        self.binding.id
    }
    pub fn subject(&self) -> &str {
        &self.subject
    }
    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }
    pub(crate) fn binding(&self) -> SessionBinding {
        self.binding.clone()
    }
    pub(crate) fn check_issuance(
        &self,
        authority: &Authority,
        claims: &Claims
    ) -> Result<()> {
        if self.realm != authority.realm
            || self.issuer != authority.issuer
            || claims.subject != self.subject
            || claims.issued_at < self.created_at
            || claims.expires_at > self.expires_at
        {
            return Err(Error::InvalidInput);
        }
        Ok(())
    }
}

struct SessionView {
    snapshot: Snapshot,
    // Compared once against the shared keyed subject tag, then checked locally.
    subject: String
}

/// Local decoded Principals plus fleet-native live sessions and replay state.
/// Backing follows the supplied Fleet. SHM survives handle drop and restart;
/// memory state lives as long as a handle retains it. No background driver.
///
/// Shared records contain only keyed identifiers and fixed control metadata.
/// Owner-only OS permissions restrict access. `Blake3State` verifies
/// integrity; `EncryptedState` also hides metadata. `UnprotectedState` explicitly
/// trusts every writer. Protected modes still permit deletion/rollback attacks.
/// `state_key` stays stable across token-key rotations and is never stored in SHM.
pub struct FleetAuth<P: StateProtection> {
    _fleet: Arc<Fleet>,
    table: Table,
    local: MemoryCache,
    sessions: Mutex<LruCache<SessionId, SessionView>>,
    state_key: Zeroizing<[u8; 32]>,
    protection: RecordProtection<P>,
    realm: String,
    issuer: String
}
impl<P: StateProtection> FleetAuth<P> {
    /// Construct with an explicitly selected shared-state protection policy.
    /// There is no default policy.
    ///
    /// `local_capacity` bounds each local Principal/session-view LRU. A cached
    /// protected view is reused only while every shared byte matches its local copy.
    /// UnprotectedState trusts the commit revision instead.
    /// Peers sharing a physical table must select the same policy. Token crypto
    /// and live session/replay checks are enabled for every policy.
    pub fn new(
        fleet: Arc<Fleet>,
        authority: &Authority,
        state_key: SecretKey,
        local_capacity: NonZeroUsize,
        _policy: P
    ) -> Result<Self> {
        let info = context(&[
            b"orbit-auth/v2/fleet-state",
            fleet.name().as_bytes(),
            authority.realm.as_bytes(),
            authority.issuer.as_bytes()
        ]);
        Ok(Self {
            table: Table::new(fleet.clone(), P::ID)?,
            // The physical table may contain multiple realm/issuer namespaces.
            // Its protection key is fleet-scoped; record lookup identifiers below bind
            // the realm/issuer. All users of one table share this state secret.
            protection: RecordProtection::new(derive(
                &state_key,
                &context(&[
                    b"orbit-auth/v4/fleet-state-protection",
                    fleet.name().as_bytes(),
                    &P::ID.to_le_bytes()
                ])
            )),
            _fleet: fleet,
            local: MemoryCache::new(local_capacity),
            sessions: Mutex::new(LruCache::new(local_capacity)),
            state_key: derive(&state_key, &info),
            realm: authority.realm.clone(),
            issuer: authority.issuer.clone()
        })
    }

    /// Clear this handle's decoded/validated views; shared authority is preserved.
    pub fn clear_local(&self) {
        self.local.clear();
        self.sessions.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    pub fn create_session(
        &self,
        subject: impl Into<String>,
        now: u64,
        expires_at: u64
    ) -> Result<Session> {
        let subject = subject.into();
        if !crate::principal::valid_label(&subject) || now >= expires_at {
            return Err(Error::InvalidInput);
        }
        let mut id = [0; 16];
        getrandom::fill(&mut id).map_err(|_| Error::Randomness)?;
        let id = SessionId(id);
        let record = Record {
            key: self.session_key(id),
            subject: self.subject_tag(&subject),
            created_at: now,
            expires_at,
            generation: 0,
            status: SESSION
        };
        self.table.write(|slots| {
            if find(slots, record.key, &self.protection)?.is_some() {
                return Err(Error::InvalidInput);
            }
            insert(slots, record, now, &self.protection)
        })?;
        Ok(self.session(id, subject, record))
    }

    /// Atomically publish revocation. Every subsequent validation rechecks the
    /// authenticated record, including warm local cache hits. Already admitted work is
    /// not cancelled; no notification or polling interval delays the next check.
    pub fn revoke_session(
        &self,
        id: SessionId,
        now: u64
    ) -> Result<()> {
        let key = self.session_key(id);
        self.table.write(|slots| {
            let snapshot = find(slots, key, &self.protection)?.ok_or(Error::Revoked)?;
            let mut record = snapshot.record;
            if !matches!(record.status, SESSION | REVOKED) {
                return Err(Error::PolicyUnavailable);
            }
            if now < record.created_at {
                return Err(Error::NotYetValid);
            }
            record.status = REVOKED;
            slots[snapshot.index].store(record, snapshot.index, &self.protection)?;
            Ok(())
        })
    }

    /// Validate and consume the current refresh generation atomically. Exactly
    /// one concurrent caller succeeds. The explicit renewed expiry may preserve
    /// or extend the previous horizon. The returned handle issues new credentials.
    /// The application owns renewal/delegation limits. If a response is lost,
    /// the consumed credential is not retried; reauthentication is required.
    pub fn refresh(
        &self,
        validator: &Validator<'_>,
        token: &str,
        now: u64,
        expires_at: u64
    ) -> Result<Session> {
        if validator.validation.purpose != Purpose::Refresh {
            return Err(Error::InvalidInput);
        }
        let principal = validator.validate(token, now, self)?;
        let binding = principal.session.as_ref().ok_or(Error::InvalidToken)?;
        let key = self.session_key(binding.id);
        let subject = self.subject_tag(principal.subject());
        self.table.write(|slots| {
            let snapshot = find(slots, key, &self.protection)?.ok_or(Error::Revoked)?;
            let mut record = snapshot.record;
            self.check_record(record, subject, &principal, now)?;
            if expires_at < record.expires_at {
                return Err(Error::InvalidInput);
            }
            record.generation = record.generation.checked_add(1).ok_or(Error::PolicyUnavailable)?;
            record.expires_at = expires_at;
            slots[snapshot.index].store(record, snapshot.index, &self.protection)?;
            Ok(self.session(binding.id, principal.subject().to_owned(), record))
        })
    }

    /// One-use hook. Unexpired consumed IDs never evict; full/unavailable denies.
    pub fn replay_guard(&self) -> ReplayGuard<'_, P> {
        ReplayGuard(self)
    }

    fn session(
        &self,
        id: SessionId,
        subject: String,
        record: Record
    ) -> Session {
        Session {
            binding: SessionBinding { id, generation: record.generation },
            subject,
            created_at: record.created_at,
            expires_at: record.expires_at,
            realm: self.realm.clone(),
            issuer: self.issuer.clone()
        }
    }
    fn session_key(
        &self,
        id: SessionId
    ) -> StoreKey {
        StoreKey(mac(self.state_key.as_ref(), &context(&[b"session", &id.0])))
    }
    fn subject_tag(
        &self,
        subject: &str
    ) -> [u64; 4] {
        mac(self.state_key.as_ref(), &context(&[b"subject", subject.as_bytes()]))
    }
    fn check_scope(
        &self,
        principal: &Principal,
        now: u64
    ) -> Result<()> {
        if principal.realm != self.realm || principal.issuer != self.issuer {
            return Err(Error::InvalidToken);
        }
        if now >= principal.claims.expires_at {
            return Err(Error::Expired);
        }
        if now < principal.claims.not_before || now < principal.claims.issued_at {
            return Err(Error::NotYetValid);
        }
        Ok(())
    }
    fn check_record(
        &self,
        record: Record,
        subject: [u64; 4],
        principal: &Principal,
        now: u64
    ) -> Result<()> {
        if record.subject != subject {
            return Err(Error::InvalidToken);
        }
        self.check_metadata(record, principal, now)
    }
    fn check_metadata(
        &self,
        record: Record,
        principal: &Principal,
        now: u64
    ) -> Result<()> {
        match record.status {
            REVOKED => return Err(Error::Revoked),
            SESSION => (),
            _ => return Err(Error::PolicyUnavailable)
        }
        if now >= record.expires_at {
            return Err(Error::Expired);
        }
        if now < record.created_at {
            return Err(Error::NotYetValid);
        }
        if principal.claims.issued_at < record.created_at
            || principal.claims.expires_at > record.expires_at
        {
            return Err(Error::InvalidToken);
        }
        let binding = principal.session.as_ref().ok_or(Error::InvalidToken)?;
        if principal.claims.purpose == Purpose::Refresh && binding.generation != record.generation {
            return Err(Error::Replayed);
        }
        Ok(())
    }
}

impl<P: StateProtection> PrincipalCache for FleetAuth<P> {
    fn get(
        &self,
        key: &CacheKey
    ) -> Option<CachedPrincipal> {
        self.local.get(key)
    }
    fn insert(
        &self,
        key: CacheKey,
        entry: CachedPrincipal
    ) {
        if key == entry.key {
            self.local.insert(key, entry);
        }
    }
}

impl<P: StateProtection> ValidationHook for FleetAuth<P> {
    fn check(
        &self,
        principal: &Principal,
        now: u64
    ) -> Result<()> {
        self.check_scope(principal, now)?;
        let binding = principal.session.as_ref().ok_or(Error::InvalidToken)?;
        {
            let mut views = self.sessions.lock().map_err(|_| Error::PolicyUnavailable)?;
            if let Some(view) = views.get(&binding.id) {
                if self.table.unchanged::<P>(&view.snapshot)? {
                    if view.subject != principal.subject() {
                        return Err(Error::InvalidToken);
                    }
                    return self.check_metadata(view.snapshot.record, principal, now);
                }
                views.pop(&binding.id);
            }
        }
        let key = self.session_key(binding.id);
        let snapshot = self.table.lookup(key, &self.protection)?.ok_or(Error::Revoked)?;
        self.check_record(snapshot.record, self.subject_tag(principal.subject()), principal, now)?;
        self.sessions
            .lock()
            .map_err(|_| Error::PolicyUnavailable)?
            .put(binding.id, SessionView { snapshot, subject: principal.subject().to_owned() });
        Ok(())
    }
}

pub struct ReplayGuard<'a, P: StateProtection>(&'a FleetAuth<P>);
impl<P: StateProtection> ValidationHook for ReplayGuard<'_, P> {
    fn check(
        &self,
        principal: &Principal,
        now: u64
    ) -> Result<()> {
        let auth = self.0;
        auth.check_scope(principal, now)?;
        let key = StoreKey(mac(auth.state_key.as_ref(), &context(&[b"replay", &principal.jti.0])));
        // Replay admission and live session check are one locked operation.
        auth.table.write(|slots| {
            if let Some(binding) = &principal.session {
                let snapshot = find(slots, auth.session_key(binding.id), &auth.protection)?
                    .ok_or(Error::Revoked)?;
                auth.check_record(
                    snapshot.record,
                    auth.subject_tag(principal.subject()),
                    principal,
                    now
                )?;
            }
            if find(slots, key, &auth.protection)?.is_some() {
                return Err(Error::Replayed);
            }
            insert(
                slots,
                Record {
                    key,
                    subject: [0; 4],
                    created_at: now,
                    expires_at: principal.claims.expires_at,
                    generation: 0,
                    status: CONSUMED
                },
                now,
                &auth.protection
            )
        })
    }
}

fn mac(
    key: &[u8],
    value: &[u8]
) -> [u64; 4] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC key");
    mac.update(value);
    let bytes = mac.finalize().into_bytes();
    std::array::from_fn(|i| {
        u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().expect("eight bytes"))
    })
}

#[cfg(test)]
#[path = "../tests/unit/fleet.rs"]
mod tests;

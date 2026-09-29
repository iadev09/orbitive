use std::collections::BTreeSet;

use super::*;
use crate::{Audience, Blake3State, Keyring, Validation};

fn setup() -> (Authority, FleetAuth<Blake3State>) {
    let authority = Authority::new(
        "prod",
        "login",
        Keyring::new(1, [(1, SecretKey::from_bytes([1; 32]))]).unwrap()
    )
    .unwrap();
    let auth = FleetAuth::new(
        Arc::new(Fleet::join("security", 1).unwrap()),
        &authority,
        SecretKey::from_bytes([2; 32]),
        NonZeroUsize::new(2).unwrap(),
        Blake3State
    )
    .unwrap();
    (authority, auth)
}
fn principal(
    authority: &Authority,
    auth: &FleetAuth<Blake3State>
) -> Arc<Principal> {
    let session = auth.create_session("sensitive-subject", 100, 300).unwrap();
    let audience = Audience::new("api").unwrap();
    let token = authority
        .issue_session(
            Claims {
                subject: session.subject().into(),
                audience: audience.clone(),
                purpose: Purpose::Access,
                capabilities: BTreeSet::new(),
                issued_at: 100,
                not_before: 100,
                expires_at: 250
            },
            &session
        )
        .unwrap();
    authority
        .validator(Validation {
            audience,
            purpose: Purpose::Access,
            required_capabilities: BTreeSet::new()
        })
        .validate(token.expose(), 110, auth)
        .unwrap()
}

#[test]
fn warm_checks_compare_authenticated_bytes_even_while_writer_lock_is_held() {
    let (authority, auth) = setup();
    let principal = principal(&authority, &auth);
    let id = principal.session_id().unwrap();
    auth.table
        .write(|slots| {
            // Would deadlock if a warm check tried to reacquire the state lock.
            auth.check(&principal, 111)?;
            let snapshot = find(slots, auth.session_key(id), &auth.protection)?.unwrap();
            let mut record = snapshot.record;
            record.status = REVOKED;
            slots[snapshot.index].store(record, snapshot.index, &auth.protection)?;
            // A changed revision reloads atomics without decrypting or acquiring
            // the writer lock when the committed snapshot is stable.
            assert_eq!(auth.check(&principal, 112), Err(Error::Revoked));
            Ok(())
        })
        .unwrap();
}

#[test]
fn cached_session_view_never_skips_subject_time_or_scope_checks() {
    let (authority, auth) = setup();
    let principal = principal(&authority, &auth);
    let mut changed = (*principal).clone();
    changed.claims.subject = "other-user".into();
    assert_eq!(auth.check(&changed, 111), Err(Error::InvalidToken));
    changed = (*principal).clone();
    changed.realm = "other-realm".into();
    assert_eq!(auth.check(&changed, 111), Err(Error::InvalidToken));
    assert_eq!(auth.check(&principal, 250), Err(Error::Expired));
    assert_eq!(auth.check(&principal, 99), Err(Error::NotYetValid));
}

#[test]
fn reused_slot_cannot_authorize_the_old_cached_session() {
    let (authority, auth) = setup();
    let principal = principal(&authority, &auth);
    let key = auth.session_key(principal.session_id().unwrap());
    auth.table
        .write(|slots| {
            let snapshot = find(slots, key, &auth.protection)?.unwrap();
            let mut other = snapshot.record;
            other.key = StoreKey([8; 4]);
            slots[snapshot.index].store(other, snapshot.index, &auth.protection)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(auth.check(&principal, 111), Err(Error::Revoked));
}

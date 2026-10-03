use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use orbit_auth::*;
use orbit_core::Fleet;

fn authority() -> Authority {
    Authority::new("prod", "login", Keyring::new(1, [(1, SecretKey::from_bytes([7; 32]))]).unwrap())
        .unwrap()
}
fn auth_with<P: StateProtection>(
    fleet: Arc<Fleet>,
    authority: &Authority,
    protection: P
) -> FleetAuth<P> {
    FleetAuth::new(
        fleet,
        authority,
        SecretKey::from_bytes([9; 32]),
        NonZeroUsize::new(8).unwrap(),
        protection
    )
    .unwrap()
}
fn auth(
    fleet: Arc<Fleet>,
    authority: &Authority
) -> FleetAuth<Blake3State> {
    auth_with(fleet, authority, Blake3State)
}
fn policy(purpose: Purpose) -> Validation {
    Validation {
        audience: Audience::new("services").unwrap(),
        purpose,
        required_capabilities: BTreeSet::new()
    }
}
fn claims(purpose: Purpose) -> Claims {
    Claims {
        subject: "user:42".into(),
        audience: Audience::new("services").unwrap(),
        purpose,
        capabilities: BTreeSet::new(),
        issued_at: 100,
        not_before: 100,
        expires_at: 200
    }
}

#[test]
fn memory_backing_shares_only_the_supplied_fleet_and_checks_live_sessions() {
    let authority = authority();
    let fleet = Arc::new(Fleet::join("auth-memory", 1).unwrap());
    let first = auth(fleet.clone(), &authority);
    let peer = auth(fleet, &authority);
    let isolated = auth(Arc::new(Fleet::join("auth-memory", 1).unwrap()), &authority);
    let session = first.create_session("user:42", 100, 250).unwrap();
    let token = authority.issue_session(claims(Purpose::Access), &session).unwrap();
    let validator = authority.validator(policy(Purpose::Access));
    let principal = validator
        .validate_cached(token.expose(), 110, &first, Duration::from_secs(90), &first)
        .unwrap();
    assert!(Arc::ptr_eq(
        &principal,
        &validator
            .validate_cached(token.expose(), 111, &first, Duration::from_secs(90), &first)
            .unwrap()
    ));
    let key = validator.cache_key(token.expose()).unwrap();
    assert!(peer.get(&key).is_none(), "Principals are local; peers validate their token once");
    assert!(isolated.get(&key).is_none());
    assert_eq!(validator.validate(token.expose(), 111, &isolated), Err(Error::Revoked));
    peer.revoke_session(session.id(), 112).unwrap();
    assert_eq!(
        validator.validate_cached(token.expose(), 113, &first, Duration::from_secs(90), &first),
        Err(Error::Revoked)
    );
}

#[test]
fn refresh_rotation_preserves_access_and_revocation_survives_token_key_rotation() {
    let authority = authority();
    let fleet = Arc::new(Fleet::join("auth-refresh", 1).unwrap());
    let state = auth(fleet.clone(), &authority);
    let session = state.create_session("user:42", 100, 250).unwrap();
    let refresh = authority.issue_session(claims(Purpose::Refresh), &session).unwrap();
    let access = authority.issue_session(claims(Purpose::Access), &session).unwrap();
    let validator = authority.validator(policy(Purpose::Refresh));
    let next = state.refresh(&validator, refresh.expose(), 120, 300).unwrap();
    assert_eq!(next.id(), session.id());
    assert!(matches!(state.refresh(&validator, refresh.expose(), 121, 300), Err(Error::Replayed)));
    assert!(
        authority.validator(policy(Purpose::Access)).validate(access.expose(), 121, &state).is_ok()
    );
    let next_refresh = authority.issue_session(claims(Purpose::Refresh), &next).unwrap();
    assert!(state.refresh(&validator, next_refresh.expose(), 122, 300).is_ok());
    let rotated = Authority::new(
        "prod",
        "login",
        Keyring::new(2, [(1, SecretKey::from_bytes([7; 32])), (2, SecretKey::from_bytes([8; 32]))])
            .unwrap()
    )
    .unwrap();
    let peer = auth(fleet, &rotated);
    peer.revoke_session(session.id(), 123).unwrap();
    assert_eq!(
        authority.validator(policy(Purpose::Access)).validate(access.expose(), 124, &state),
        Err(Error::Revoked)
    );
    assert_eq!(
        rotated.validator(policy(Purpose::Access)).validate(access.expose(), 124, &peer),
        Err(Error::Revoked)
    );
}

#[test]
fn activity_extends_the_horizon_a_peer_already_cached_and_new_credentials_reach_it() {
    let authority = authority();
    let fleet = Arc::new(Fleet::join("auth-extend", 1).unwrap());
    let state = auth(fleet.clone(), &authority);
    let peer = auth(fleet, &authority);
    let validator = authority.validator(policy(Purpose::Access));
    let session = state.create_session("user:42", 100, 200).unwrap();
    let first = authority.issue_session(claims(Purpose::Access), &session).unwrap();
    // The peer holds a warm view of the record before it changes.
    assert!(validator.validate(first.expose(), 110, &peer).is_ok());
    assert_eq!(
        state.extend_session(session.id(), "user:41", 150, 300).err(),
        Some(Error::InvalidInput)
    );
    assert_eq!(
        state.extend_session(session.id(), "user:42", 150, 150).err(),
        Some(Error::InvalidInput)
    );
    let extended = state.extend_session(session.id(), "user:42", 150, 300).unwrap();
    assert_eq!((extended.id(), extended.expires_at()), (session.id(), 300));
    // A peer reads the extended session by id and issues from the handle.
    let read = peer.session(session.id(), "user:42", 160).unwrap();
    assert_eq!((read.created_at(), read.expires_at()), (100, 300));
    assert_eq!(peer.session(session.id(), "user:41", 160).err(), Some(Error::InvalidInput));
    // Moving back is not an extension; the same horizon is a no-op.
    assert_eq!(
        state.extend_session(session.id(), "user:42", 151, 250).err(),
        Some(Error::InvalidInput)
    );
    assert_eq!(state.extend_session(session.id(), "user:42", 151, 300).unwrap().expires_at(), 300);
    // The credential issued before keeps its own expiry; a credential reaching
    // the new horizon has to come from the extended handle.
    assert_eq!(validator.validate(first.expose(), 200, &state), Err(Error::Expired));
    assert!(
        authority
            .issue_session(Claims { expires_at: 300, ..claims(Purpose::Access) }, &session)
            .is_err()
    );
    let second = authority
        .issue_session(
            Claims { issued_at: 150, not_before: 150, expires_at: 300, ..claims(Purpose::Access) },
            &extended
        )
        .unwrap();
    assert!(validator.validate(second.expose(), 250, &peer).is_ok());
    // The first credential lapsed while its session lived on: the lapsed
    // path accepts it (a new carrier may be issued), the normal path does not.
    assert!(validator.validate_lapsed(first.expose(), 250, &peer).is_ok());
    assert_eq!(validator.validate(first.expose(), 250, &peer), Err(Error::Expired));
    assert_eq!(validator.validate_lapsed(first.expose(), 300, &peer), Err(Error::Expired));
    peer.revoke_session(session.id(), 260).unwrap();
    assert_eq!(validator.validate_lapsed(first.expose(), 261, &peer), Err(Error::Revoked));
    assert_eq!(state.extend_session(session.id(), "user:42", 261, 400).err(), Some(Error::Revoked));
    let other = state.create_session("user:7", 100, 200).unwrap();
    assert_eq!(state.extend_session(other.id(), "user:7", 200, 400).err(), Some(Error::Expired));
}

#[test]
fn replay_guard_is_authoritative_across_handles_and_cache_hits() {
    let authority = authority();
    let fleet = Arc::new(Fleet::join("auth-replay", 1).unwrap());
    let first = auth(fleet.clone(), &authority);
    let peer = auth(fleet, &authority);
    let token = authority.issue(claims(Purpose::Internal)).unwrap();
    let validator = authority.validator(policy(Purpose::Internal));
    assert!(
        validator
            .validate_cached(
                token.expose(),
                110,
                &first,
                Duration::from_secs(90),
                &first.replay_guard()
            )
            .is_ok()
    );
    assert_eq!(
        validator.validate_cached(
            token.expose(),
            111,
            &peer,
            Duration::from_secs(90),
            &peer.replay_guard()
        ),
        Err(Error::Replayed)
    );
}

#[cfg(unix)]
mod shm {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};

    use orbit_core::NodeId;
    use orbit_core::shm::{ShmRegion, ring_segment_name};

    use super::*;

    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            // Tests own this unique namespace; all child processes have exited.
            ShmRegion::open_or_create_locked(&ring_segment_name(&self.0, AUTH_STATE_KIND), 64)
                .unwrap()
                .0
                .unlink()
                .unwrap();
        }
    }
    fn fleet() -> (Arc<Fleet>, Cleanup) {
        let name = format!("oa{:x}{:x}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
        (Arc::new(Fleet::join_shm_as(&name, 4, NodeId::ZERO).unwrap()), Cleanup(name))
    }
    fn child(
        name: &str,
        action: &str,
        node: u16,
        mode: u64
    ) -> std::process::Child {
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "shm::shm_child", "--nocapture"])
            .env("ORBIT_AUTH_TEST_FLEET", name)
            .env("ORBIT_AUTH_TEST_ACTION", action)
            .env("ORBIT_AUTH_TEST_NODE", node.to_string())
            .env("ORBIT_AUTH_TEST_MODE", mode.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
    }
    #[test]
    fn shm_child() {
        let Ok(name) = std::env::var("ORBIT_AUTH_TEST_FLEET") else {
            return;
        };
        let action = std::env::var("ORBIT_AUTH_TEST_ACTION").unwrap();
        if action == "tamper" {
            // This process has no Authority, FleetAuth or secret key. Model a
            // same-UID program modifying the public SHM layout directly.
            let (region, _lock) = ShmRegion::open_or_create_locked(
                &ring_segment_name(&name, AUTH_STATE_KIND),
                64 + AUTH_STATE_CAPACITY * 320
            )
            .unwrap();
            for index in 0..AUTH_STATE_CAPACITY {
                // OAUTH004: 64-byte header, 320-byte slots, one revision and
                // two 136-byte banks. Mapping alignment permits atomic u64 IO.
                let slot = unsafe { region.as_ptr().add(64 + index * 320).cast::<AtomicU64>() };
                let revision = unsafe { &*slot }.load(Ordering::SeqCst);
                if revision != 0 {
                    let expiry = unsafe { &*slot.add(1 + (revision as usize & 1) * 17 + 9) };
                    expiry.fetch_xor(0x1000, Ordering::SeqCst);
                    return;
                }
            }
            panic!("expected a live session to tamper with");
        }
        match std::env::var("ORBIT_AUTH_TEST_MODE").unwrap().as_str() {
            "1" => child_run(name, action, Blake3State),
            "2" => child_run(name, action, EncryptedState),
            "3" => child_run(name, action, UnprotectedState),
            _ => panic!("unknown test policy")
        }
    }
    fn child_run<P: StateProtection>(
        name: String,
        action: String,
        protection: P
    ) {
        let node: u16 = std::env::var("ORBIT_AUTH_TEST_NODE").unwrap().parse().unwrap();
        let fleet = Arc::new(Fleet::join_shm_as(&name, 4, NodeId::new(node)).unwrap());
        let authority = authority();
        let state = auth_with(fleet, &authority, protection);
        if action == "refresh" {
            println!("READY");
            std::io::stdout().flush().unwrap();
        }
        let mut token = String::new();
        std::io::stdin().read_to_string(&mut token).unwrap();
        if action == "refresh" {
            let result =
                state.refresh(&authority.validator(policy(Purpose::Refresh)), &token, 120, 300);
            match result {
                Ok(_) => (),
                Err(Error::Replayed) => std::process::exit(42),
                Err(e) => panic!("{e}")
            }
        } else {
            let validator = authority.validator(policy(Purpose::Access));
            let key = validator.cache_key(&token).unwrap();
            assert!(
                state.get(&key).is_none(),
                "a late process has no decoded Principal until its first request"
            );
            let principal = validator
                .validate_cached(&token, 111, &state, Duration::from_secs(90), &state)
                .unwrap();
            state.revoke_session(principal.session_id().unwrap(), 112).unwrap();
        }
    }
    #[test]
    fn keyless_process_cannot_modify_state_even_when_the_reader_has_a_warm_cache() {
        keyless_process_cannot_modify_state_even_when_the_reader_has_a_warm_cache_with(Blake3State);
        keyless_process_cannot_modify_state_even_when_the_reader_has_a_warm_cache_with(
            EncryptedState
        );
    }
    fn keyless_process_cannot_modify_state_even_when_the_reader_has_a_warm_cache_with<
        P: StateProtection
    >(
        protection: P
    ) {
        let (fleet, cleanup) = fleet();
        let authority = authority();
        let state = auth_with(fleet, &authority, protection);
        let session = state.create_session("user:42", 100, 250).unwrap();
        let token = authority.issue_session(claims(Purpose::Access), &session).unwrap();
        let validator = authority.validator(policy(Purpose::Access));
        validator
            .validate_cached(token.expose(), 110, &state, Duration::from_secs(90), &state)
            .unwrap();
        let status = child(&cleanup.0, "tamper", 1, P::ID).wait().unwrap();
        assert!(status.success(), "tampering child exited with {status}");
        assert_eq!(
            validator.validate_cached(token.expose(), 111, &state, Duration::from_secs(90), &state),
            Err(Error::PolicyUnavailable)
        );
        state.clear_local();
        assert_eq!(validator.validate(token.expose(), 111, &state), Err(Error::PolicyUnavailable));
        assert_eq!(state.revoke_session(session.id(), 111), Err(Error::PolicyUnavailable));
    }
    #[test]
    fn late_process_reads_session_and_logout_invalidates_parent_hot_path() {
        late_process_reads_session_and_logout_invalidates_parent_hot_path_with(Blake3State);
        late_process_reads_session_and_logout_invalidates_parent_hot_path_with(EncryptedState);
        late_process_reads_session_and_logout_invalidates_parent_hot_path_with(UnprotectedState);
    }
    fn late_process_reads_session_and_logout_invalidates_parent_hot_path_with<
        P: StateProtection + Copy
    >(
        protection: P
    ) {
        let (fleet, cleanup) = fleet();
        let authority = authority();
        let state = auth_with(fleet.clone(), &authority, protection);
        let session = state.create_session("user:42", 100, 250).unwrap();
        let token = authority.issue_session(claims(Purpose::Access), &session).unwrap();
        let validator = authority.validator(policy(Purpose::Access));
        validator
            .validate_cached(token.expose(), 110, &state, Duration::from_secs(90), &state)
            .unwrap();
        let mut child = child(fleet.name(), "cache-revoke", 1, P::ID);
        child.stdin.take().unwrap().write_all(token.expose().as_bytes()).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stdout));
        assert_eq!(
            validator.validate_cached(token.expose(), 113, &state, Duration::from_secs(90), &state),
            Err(Error::Revoked)
        );
        // Reopen the retained segment after all semantic handles are gone.
        drop(state);
        let reopened = FleetAuth::<P>::new(
            fleet.clone(),
            &authority,
            SecretKey::from_bytes([9; 32]),
            NonZeroUsize::new(8).unwrap(),
            protection
        )
        .unwrap();
        assert_eq!(validator.validate(token.expose(), 114, &reopened), Err(Error::Revoked));
        drop(reopened);
        drop(fleet);
        drop(cleanup);
    }
    #[test]
    fn two_processes_cannot_refresh_the_same_generation() {
        two_processes_cannot_refresh_the_same_generation_with(Blake3State);
        two_processes_cannot_refresh_the_same_generation_with(EncryptedState);
        two_processes_cannot_refresh_the_same_generation_with(UnprotectedState);
    }
    fn two_processes_cannot_refresh_the_same_generation_with<P: StateProtection>(protection: P) {
        let (fleet, cleanup) = fleet();
        let authority = authority();
        let state = auth_with(fleet.clone(), &authority, protection);
        let session = state.create_session("user:42", 100, 250).unwrap();
        let token = authority.issue_session(claims(Purpose::Refresh), &session).unwrap();
        let mut children: Vec<_> =
            (1..=2).map(|node| child(fleet.name(), "refresh", node, P::ID)).collect();
        for child in &mut children {
            let mut reader = BufReader::new(child.stdout.take().unwrap());
            let mut line = String::new();
            loop {
                assert!(reader.read_line(&mut line).unwrap() > 0, "child exited before READY");
                if line.trim_end().ends_with("READY") {
                    break;
                }
                line.clear();
            }
            // Retain stdout so the test runner can finish printing after the gate.
            child.stdout = Some(reader.into_inner());
        }
        for child in &mut children {
            child.stdin.take().unwrap().write_all(token.expose().as_bytes()).unwrap();
        }
        let mut codes: Vec<_> = children
            .into_iter()
            .map(|child| child.wait_with_output().unwrap().status.code().unwrap())
            .collect();
        codes.sort();
        assert_eq!(codes, [0, 42]);
        assert_eq!(
            authority.validator(policy(Purpose::Refresh)).validate(token.expose(), 121, &state),
            Err(Error::Replayed)
        );
        drop(state);
        drop(fleet);
        drop(cleanup);
    }
}

#[test]
fn memory_rejects_a_different_policy_on_the_same_table() {
    let authority = authority();
    let fleet = Arc::new(Fleet::join("policy-conflict", 1).unwrap());
    let first = auth(fleet.clone(), &authority);
    let session = first.create_session("user:42", 100, 250).unwrap();
    assert!(matches!(
        FleetAuth::new(
            fleet.clone(),
            &authority,
            SecretKey::from_bytes([9; 32]),
            NonZeroUsize::new(8).unwrap(),
            EncryptedState
        ),
        Err(Error::IncompatibleProtection)
    ));
    assert!(matches!(
        FleetAuth::new(
            fleet,
            &authority,
            SecretKey::from_bytes([9; 32]),
            NonZeroUsize::new(8).unwrap(),
            UnprotectedState
        ),
        Err(Error::IncompatibleProtection)
    ));
    first.revoke_session(session.id(), 110).unwrap();
}

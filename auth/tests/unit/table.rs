use super::*;

fn mac() -> RecordProtection<Blake3State> {
    RecordProtection::<Blake3State>::new(Zeroizing::new([7; 32]))
}

fn record(
    key: u64,
    generation: u64
) -> Record {
    Record {
        key: StoreKey([key; 4]),
        subject: [generation; 4],
        created_at: generation,
        expires_at: generation + 100,
        generation,
        status: SESSION
    }
}
fn value(slot: &Slot) -> (u64, Record) {
    match slot.read_once().unwrap() {
        Read::Value(revision, wire) => {
            let record = mac().decode(0, revision, &wire).unwrap();
            (revision, record)
        }
        _ => panic!("expected committed value")
    }
}

#[test]
fn interrupted_write_preserves_previous_commit_and_revision_never_aba() {
    let slot = Slot::empty();
    let first = record(1, 1);
    assert_eq!(slot.store(first, 0, &mac()).unwrap(), 1);
    for word in &slot.banks[0][..8] {
        word.store(99, Ordering::SeqCst);
    }
    assert_eq!(value(&slot), (1, first));
    slot.store(record(1, 2), 0, &mac()).unwrap();
    slot.store(record(1, 3), 0, &mac()).unwrap();
    assert_eq!(value(&slot).0, 3, "returning to the same bank never reuses revision 1");
    slot.revision.store(u64::MAX, Ordering::SeqCst);
    assert_eq!(slot.store(record(1, 4), 0, &mac()), Err(Error::PolicyUnavailable));
}

#[test]
fn malformed_metadata_is_rejected_and_full_tables_keep_live_state() {
    let slots = [Slot::empty()];
    insert(&slots, record(1, 10), 10, &mac()).unwrap();
    assert_eq!(insert(&slots, record(2, 20), 109, &mac()), Err(Error::StateFull));
    assert_eq!(find(&slots, StoreKey([1; 4]), &mac()).unwrap().unwrap().record, record(1, 10));
    insert(&slots, record(2, 20), 110, &mac()).unwrap();
    assert!(find(&slots, StoreKey([1; 4]), &mac()).unwrap().is_none());
    slots[0].banks[0][11].store(99, Ordering::SeqCst);
    assert!(matches!(find(&slots, StoreKey([2; 4]), &mac()), Err(Error::PolicyUnavailable)));
}

#[test]
fn hash_collisions_and_expired_slot_reuse_preserve_probe_chains() {
    let slots = [Slot::empty(), Slot::empty(), Slot::empty()];
    for key in [0, 3, 6] {
        insert(&slots, record(key, key), 6, &mac()).unwrap();
    }
    for key in [0, 3, 6] {
        assert!(find(&slots, StoreKey([key; 4]), &mac()).unwrap().is_some());
    }
    insert(&slots, record(9, 100), 101, &mac()).unwrap();
    assert!(find(&slots, StoreKey([0; 4]), &mac()).unwrap().is_none());
    for key in [3, 6, 9] {
        assert!(find(&slots, StoreKey([key; 4]), &mac()).unwrap().is_some());
    }
}

#[test]
fn concurrent_bank_read_never_accepts_mixed_fields() {
    let slot = Arc::new(Slot::empty());
    slot.store(record(1, 1), 0, &mac()).unwrap();
    let gate = Arc::new(std::sync::Barrier::new(2));
    let writer = {
        let slot = slot.clone();
        let gate = gate.clone();
        std::thread::spawn(move || {
            gate.wait();
            for n in 2..1002 {
                slot.store(record(1, n), 0, &mac()).unwrap();
            }
        })
    };
    gate.wait();
    for _ in 0..1000 {
        if let Read::Value(revision, wire) = slot.read_once().unwrap() {
            let value = mac().decode(0, revision, &wire).unwrap();
            assert_eq!(value, record(1, value.generation));
        }
    }
    writer.join().unwrap();
}

#[cfg(unix)]
#[test]
fn writer_exits_with_lock_held() {
    let Ok(name) = std::env::var("ORBIT_AUTH_CRASH_NAME") else {
        return;
    };
    let table = Table::shm(&name, Blake3State::ID).unwrap();
    table
        .write::<()>(|slots| {
            assert_eq!(slots[0].revision.load(Ordering::SeqCst), 1);
            for word in &slots[0].banks[0] {
                word.store(91, Ordering::SeqCst);
            }
            std::process::exit(37);
        })
        .unwrap();
}

#[cfg(unix)]
#[test]
fn peer_recovers_from_dead_writer_and_refuses_old_geometry() {
    let name = ring_segment_name(&format!("oc{:x}", std::process::id()), AUTH_STATE_KIND);
    let table = Table::shm(&name, Blake3State::ID).unwrap();
    table.write(|slots| slots[0].store(record(0, 1), 0, &mac())).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "table::tests::writer_exits_with_lock_held"])
        .env("ORBIT_AUTH_CRASH_NAME", &name)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(37));
    assert_eq!(table.lookup(StoreKey([0; 4]), &mac()).unwrap().unwrap().record, record(0, 1));
    table.write(|slots| slots[0].store(record(0, 2), 0, &mac())).unwrap();
    let Table::Shm(region) = &table else { unreachable!() };
    {
        let _lock = region.lock_exclusive().unwrap();
        // This test alone owns the segment; model the prototype v1 header.
        unsafe {
            region.as_ptr().add(7).write(b'1');
        }
    }
    assert!(matches!(Table::shm(&name, Blake3State::ID), Err(Error::IncompatibleLayout)));
    assert_eq!(table.lookup(StoreKey([0; 4]), &mac()).unwrap().unwrap().record, record(0, 2));
    region.unlink().unwrap();
}

#[test]
fn every_record_field_and_tag_is_authenticated_even_without_a_revision_change() {
    let slots = [Slot::empty()];
    let first = record(1, 10);
    slots[0].store(first, 0, &mac()).unwrap();
    let original: [u64; BANK_WORDS] =
        std::array::from_fn(|i| slots[0].banks[1][i].load(Ordering::SeqCst));
    for i in 0..BANK_WORDS {
        slots[0].banks[1][i].store(original[i] ^ 1, Ordering::SeqCst);
        assert!(
            matches!(find(&slots, first.key, &mac()), Err(Error::PolicyUnavailable)),
            "word {i}"
        );
        slots[0].banks[1][i].store(original[i], Ordering::SeqCst);
    }
    // Same bank, forged commit revision: the tag must cover the revision too.
    slots[0].revision.store(3, Ordering::SeqCst);
    assert!(matches!(find(&slots, first.key, &mac()), Err(Error::PolicyUnavailable)));
    slots[0].revision.store(1, Ordering::SeqCst);
    let other_key = RecordProtection::<Blake3State>::new(Zeroizing::new([8; 32]));
    assert!(matches!(find(&slots, first.key, &other_key), Err(Error::PolicyUnavailable)));
}

#[test]
fn authenticated_records_cannot_be_moved_to_another_slot_or_forged_for_reclamation() {
    let slots = [Slot::empty(), Slot::empty()];
    let first = record(0, 10);
    slots[0].store(first, 0, &mac()).unwrap();
    for (dst, src) in slots[1].banks[1].iter().zip(&slots[0].banks[1]) {
        dst.store(src.load(Ordering::SeqCst), Ordering::SeqCst);
    }
    slots[1].revision.store(1, Ordering::SeqCst);
    assert!(matches!(find(&slots, StoreKey([1; 4]), &mac()), Err(Error::PolicyUnavailable)));
    // A valid-looking earlier expiry is not permission to overwrite live state.
    slots[0].banks[1][9].store(11, Ordering::SeqCst);
    assert_eq!(insert(&slots, record(2, 20), 20, &mac()), Err(Error::PolicyUnavailable));
}

#[test]
fn warm_snapshot_comparison_checks_all_bytes_and_rollback_is_not_freshness_proof() {
    let fleet = Arc::new(Fleet::join("auth-mac-snapshot", 1).unwrap());
    let table = Table::new(fleet, Blake3State::ID).unwrap();
    let first = record(0, 10);
    table.write(|slots| slots[0].store(first, 0, &mac())).unwrap();
    let snapshot = table.lookup(first.key, &mac()).unwrap().unwrap();
    assert!(table.unchanged::<Blake3State>(&snapshot).unwrap());
    let slot = &table.slots()[0];
    let original: [u64; BANK_WORDS] =
        std::array::from_fn(|i| slot.banks[1][i].load(Ordering::SeqCst));
    for i in 0..BANK_WORDS {
        slot.banks[1][i].store(original[i] ^ 1, Ordering::SeqCst);
        assert!(!table.unchanged::<Blake3State>(&snapshot).unwrap_or(false), "word {i}");
        slot.banks[1][i].store(original[i], Ordering::SeqCst);
    }
    let mut revoked = first;
    revoked.status = REVOKED;
    table.write(|slots| slots[0].store(revoked, 0, &mac())).unwrap();
    assert!(!table.unchanged::<Blake3State>(&snapshot).unwrap());
    // A hostile writer can still restore a prior genuine state. This documents
    // the exact boundary: a valid MAC authenticates content, not currentness.
    slot.revision.store(1, Ordering::SeqCst);
    assert!(table.unchanged::<Blake3State>(&snapshot).unwrap());
}

fn protected_wire<P: StateProtection>() {
    let protection = RecordProtection::<P>::new(Zeroizing::new([7; 32]));
    let first = record(0, 10);
    let wire = protection.encode(3, 9, first).unwrap();
    assert_eq!(protection.decode(3, 9, &wire).unwrap(), first);
    for i in 0..BANK_WORDS {
        let mut tampered = wire;
        tampered[i] ^= 1;
        assert_eq!(protection.decode(3, 9, &tampered), Err(Error::PolicyUnavailable));
    }
    for (index, revision) in [(4, 9), (3, 11)] {
        assert_eq!(protection.decode(index, revision, &wire), Err(Error::PolicyUnavailable));
    }
    let wrong = RecordProtection::<P>::new(Zeroizing::new([8; 32]));
    assert_eq!(wrong.decode(3, 9, &wire), Err(Error::PolicyUnavailable));
}
#[test]
fn blake3_and_encrypted_records_authenticate_every_word_and_context() {
    protected_wire::<Blake3State>();
    protected_wire::<EncryptedState>();
    let encrypted = RecordProtection::<EncryptedState>::new(Zeroizing::new([7; 32]));
    let first = record(0, 10);
    let a = encrypted.encode(3, 9, first).unwrap();
    let b = encrypted.encode(3, 9, first).unwrap();
    assert_ne!(a, b, "each store uses a fresh random nonce");
    assert_ne!(&a[3..15], &first.words(), "encrypted metadata is not plaintext");
    let blake = RecordProtection::<Blake3State>::new(Zeroizing::new([7; 32]));
    assert_eq!(blake.decode(3, 9, &a), Err(Error::PolicyUnavailable));
}
#[test]
fn unprotected_mode_explicitly_accepts_well_formed_keyless_state_changes() {
    let protection = RecordProtection::<UnprotectedState>::new(Zeroizing::new([7; 32]));
    let mut original = record(0, 10);
    let mut wire = protection.encode(3, 9, original).unwrap();
    original.status = REVOKED;
    wire[11] = REVOKED;
    assert_eq!(protection.decode(3, 9, &wire).unwrap(), original);
    wire[11] = 99;
    assert_eq!(protection.decode(3, 9, &wire), Err(Error::PolicyUnavailable));
}
#[test]
fn encrypted_snapshot_checks_all_ciphertext_bytes_and_preserves_rollback_boundary() {
    let fleet = Arc::new(Fleet::join("auth-encrypted-snapshot", 1).unwrap());
    let table = Table::new(fleet, EncryptedState::ID).unwrap();
    let protection = RecordProtection::<EncryptedState>::new(Zeroizing::new([7; 32]));
    let first = record(0, 10);
    table.write(|slots| slots[0].store(first, 0, &protection)).unwrap();
    let snapshot = table.lookup(first.key, &protection).unwrap().unwrap();
    let slot = &table.slots()[0];
    for word in &slot.banks[1] {
        word.fetch_xor(1, Ordering::SeqCst);
        assert!(!table.unchanged::<EncryptedState>(&snapshot).unwrap());
        assert!(matches!(table.lookup(first.key, &protection), Err(Error::PolicyUnavailable)));
        word.fetch_xor(1, Ordering::SeqCst);
    }
    let mut revoked = first;
    revoked.status = REVOKED;
    table.write(|slots| slots[0].store(revoked, 0, &protection)).unwrap();
    assert!(!table.unchanged::<EncryptedState>(&snapshot).unwrap());
    slot.revision.store(1, Ordering::SeqCst);
    assert!(table.unchanged::<EncryptedState>(&snapshot).unwrap());
}
#[cfg(unix)]
#[test]
fn reopening_with_different_protection_refuses_without_changing_state() {
    let name = ring_segment_name(&format!("op{:x}", std::process::id()), AUTH_STATE_KIND);
    let table = Table::shm(&name, EncryptedState::ID).unwrap();
    let protection = RecordProtection::<EncryptedState>::new(Zeroizing::new([7; 32]));
    let first = record(0, 10);
    table.write(|slots| slots[0].store(first, 0, &protection)).unwrap();
    for mode in [Blake3State::ID, UnprotectedState::ID] {
        assert!(matches!(Table::shm(&name, mode), Err(Error::IncompatibleProtection)));
    }
    assert_eq!(table.lookup(first.key, &protection).unwrap().unwrap().record, first);
    let Table::Shm(region) = &table else { unreachable!() };
    region.unlink().unwrap();
}

#[test]
fn unprotected_warm_views_trust_writer_revisions_but_observe_committed_changes() {
    let table =
        Table::new(Arc::new(Fleet::join("unprotected-warm", 1).unwrap()), UnprotectedState::ID)
            .unwrap();
    let protection = RecordProtection::<UnprotectedState>::new(Zeroizing::new([7; 32]));
    let first = record(0, 10);
    table.write(|slots| slots[0].store(first, 0, &protection)).unwrap();
    let snapshot = table.lookup(first.key, &protection).unwrap().unwrap();
    table.slots()[0].banks[1][11].store(REVOKED, Ordering::SeqCst);
    assert!(
        table.unchanged::<UnprotectedState>(&snapshot).unwrap(),
        "this mode deliberately does not detect protocol-violating writers"
    );
    let mut revoked = first;
    revoked.status = REVOKED;
    table.write(|slots| slots[0].store(revoked, 0, &protection)).unwrap();
    assert!(!table.unchanged::<UnprotectedState>(&snapshot).unwrap());
}

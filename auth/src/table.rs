//! Bounded current auth state with typed protection. Reads copy fixed atomic
//! metadata; writes commit one inactive bank under a kernel-released lock.
//! Authentication does not prevent deletion, rollback or a compromised key holder.
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use chacha20poly1305::aead::AeadInPlace;
use chacha20poly1305::{KeyInit, Tag, XChaCha20Poly1305, XNonce};
use orbit_core::Fleet;
#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};
use zeroize::Zeroizing;

use crate::{Blake3State, EncryptedState, Error, Result, StateProtection, UnprotectedState};

/// Current session/replay state. Principal values live only in process memory.
pub const AUTH_STATE_KIND: u8 = 198;
pub const AUTH_STATE_CAPACITY: usize =
    orbit_core::compile::usize_from_env(option_env!("ORBIT_AUTH_STATE_CAPACITY"), 1024);
const WORDS: usize = 12;
// Largest encoding: nonce (24), record (96), AEAD tag (16).
const BANK_WORDS: usize = 17;
type Wire = [u64; BANK_WORDS];
#[cfg(unix)]
const HEADER: usize = 64;
#[cfg(unix)]
const MAGIC: &[u8; 8] = b"OAUTH004";

/// Separate fleet-scoped protection key; never stored in the shared mapping.
pub(crate) struct RecordProtection<P: StateProtection> {
    key: Zeroizing<[u8; 32]>,
    policy: PhantomData<P>
}
impl<P: StateProtection> RecordProtection<P> {
    pub(crate) fn new(key: Zeroizing<[u8; 32]>) -> Self {
        Self { key, policy: PhantomData }
    }
    fn context(
        index: usize,
        revision: u64
    ) -> [u8; 51] {
        let mut aad = [0; 51];
        aad[..26].copy_from_slice(b"orbit-auth/v4/state-record");
        aad[26] = AUTH_STATE_KIND;
        aad[27..35].copy_from_slice(&P::ID.to_le_bytes());
        aad[35..43].copy_from_slice(&(index as u64).to_le_bytes());
        aad[43..].copy_from_slice(&revision.to_le_bytes());
        aad
    }
    fn tag(
        &self,
        aad: &[u8],
        record: &[u8]
    ) -> blake3::Hash {
        let mut mac = Zeroizing::new(blake3::Hasher::new_keyed(&self.key));
        mac.update(aad);
        mac.update(record);
        mac.finalize()
    }
    fn encode(
        &self,
        index: usize,
        revision: u64,
        record: Record
    ) -> Result<Wire> {
        let mut bytes = Zeroizing::new([0; BANK_WORDS * 8]);
        let mut plaintext = Zeroizing::new([0; WORDS * 8]);
        for (dst, word) in plaintext.as_chunks_mut::<8>().0.iter_mut().zip(record.words()) {
            dst.copy_from_slice(&word.to_le_bytes());
        }
        let aad = Self::context(index, revision);
        match P::ID {
            Blake3State::ID => {
                bytes[..96].copy_from_slice(plaintext.as_ref());
                bytes[96..128].copy_from_slice(self.tag(&aad, plaintext.as_ref()).as_bytes());
            }
            EncryptedState::ID => {
                let mut nonce = [0; 24];
                getrandom::fill(&mut nonce).map_err(|_| Error::Randomness)?;
                let cipher = XChaCha20Poly1305::new((&*self.key).into());
                let tag = cipher
                    .encrypt_in_place_detached(XNonce::from_slice(&nonce), &aad, plaintext.as_mut())
                    .map_err(|_| Error::PolicyUnavailable)?;
                bytes[..24].copy_from_slice(&nonce);
                bytes[24..120].copy_from_slice(plaintext.as_ref());
                bytes[120..].copy_from_slice(&tag);
            }
            UnprotectedState::ID => bytes[..96].copy_from_slice(plaintext.as_ref()),
            _ => unreachable!("sealed policy")
        }
        Ok(std::array::from_fn(|i| u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap())))
    }
    fn decode(
        &self,
        index: usize,
        revision: u64,
        wire: &Wire
    ) -> Result<Record> {
        let mut bytes = Zeroizing::new([0; BANK_WORDS * 8]);
        for (dst, word) in bytes.as_chunks_mut::<8>().0.iter_mut().zip(wire) {
            dst.copy_from_slice(&word.to_le_bytes());
        }
        let aad = Self::context(index, revision);
        let mut plaintext = Zeroizing::new([0; WORDS * 8]);
        match P::ID {
            Blake3State::ID => {
                let expected = self.tag(&aad, &bytes[..96]);
                let actual = blake3::Hash::from_bytes(bytes[96..128].try_into().unwrap());
                // Hash::eq is constant-time; do not compare raw tag byte arrays.
                if expected != actual || bytes[128..].iter().any(|&v| v != 0) {
                    return Err(Error::PolicyUnavailable);
                }
                plaintext.copy_from_slice(&bytes[..96]);
            }
            EncryptedState::ID => {
                plaintext.copy_from_slice(&bytes[24..120]);
                let cipher = XChaCha20Poly1305::new((&*self.key).into());
                cipher
                    .decrypt_in_place_detached(
                        XNonce::from_slice(&bytes[..24]),
                        &aad,
                        plaintext.as_mut(),
                        Tag::from_slice(&bytes[120..])
                    )
                    .map_err(|_| Error::PolicyUnavailable)?;
            }
            UnprotectedState::ID => {
                if bytes[96..].iter().any(|&v| v != 0) {
                    return Err(Error::PolicyUnavailable);
                }
                plaintext.copy_from_slice(&bytes[..96]);
            }
            _ => unreachable!("sealed policy")
        }
        Record::from_words(std::array::from_fn(|i| {
            u64::from_le_bytes(plaintext[i * 8..i * 8 + 8].try_into().unwrap())
        }))
    }
}

pub(crate) const SESSION: u64 = 1;
pub(crate) const REVOKED: u64 = 2;
pub(crate) const CONSUMED: u64 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StoreKey(pub [u64; 4]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) key: StoreKey,
    pub(crate) subject: [u64; 4],
    pub(crate) created_at: u64,
    pub(crate) expires_at: u64,
    pub(crate) generation: u64,
    pub(crate) status: u64
}
impl Record {
    fn words(self) -> [u64; WORDS] {
        [
            self.key.0[0],
            self.key.0[1],
            self.key.0[2],
            self.key.0[3],
            self.subject[0],
            self.subject[1],
            self.subject[2],
            self.subject[3],
            self.created_at,
            self.expires_at,
            self.generation,
            self.status
        ]
    }
    fn from_words(words: [u64; WORDS]) -> Result<Self> {
        let record = Self {
            key: StoreKey(words[..4].try_into().expect("four words")),
            subject: words[4..8].try_into().expect("four words"),
            created_at: words[8],
            expires_at: words[9],
            generation: words[10],
            status: words[11]
        };
        if !matches!(record.status, SESSION | REVOKED | CONSUMED)
            || record.created_at >= record.expires_at
            || (record.status == CONSUMED && (record.subject != [0; 4] || record.generation != 0))
        {
            return Err(Error::PolicyUnavailable);
        }
        Ok(record)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Snapshot {
    pub(crate) index: usize,
    pub(crate) revision: u64,
    pub(crate) record: Record,
    wire: Wire
}

enum Read {
    Empty,
    Value(u64, Wire),
    Changed
}

#[repr(C, align(64))]
pub(crate) struct Slot {
    // Never wraps or resets: cached slot references cannot survive reuse (ABA).
    revision: AtomicU64,
    banks: [[AtomicU64; BANK_WORDS]; 2]
}
impl Slot {
    fn empty() -> Self {
        Self {
            revision: AtomicU64::new(0),
            banks: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0)))
        }
    }

    fn read_once(&self) -> Result<Read> {
        let before = self.revision.load(Ordering::SeqCst);
        if before == 0 {
            return Ok(Read::Empty);
        }
        let words: [u64; BANK_WORDS] =
            std::array::from_fn(|i| self.banks[(before & 1) as usize][i].load(Ordering::SeqCst));
        // Every bank word and revision operation participates in the SC total
        // order. Even if two writers cycle back to this bank during the read,
        // the non-wrapping revision changes and the partial copy is discarded.
        if self.revision.load(Ordering::SeqCst) != before {
            return Ok(Read::Changed);
        }
        Ok(Read::Value(before, words))
    }

    /// Caller holds Table::write's exclusive writer lock.
    pub(crate) fn store<P: StateProtection>(
        &self,
        record: Record,
        index: usize,
        mac: &RecordProtection<P>
    ) -> Result<u64> {
        Record::from_words(record.words())?;
        let revision =
            self.revision.load(Ordering::SeqCst).checked_add(1).ok_or(Error::PolicyUnavailable)?;
        let wire = mac.encode(index, revision, record)?;
        let bank = &self.banks[(revision & 1) as usize];
        for (dst, value) in bank.iter().zip(wire) {
            dst.store(value, Ordering::SeqCst);
        }
        self.revision.store(revision, Ordering::SeqCst);
        Ok(revision)
    }
}

pub(crate) struct Memory {
    _fleet: Arc<Fleet>,
    protection: u64,
    slots: Vec<Slot>,
    writer: Mutex<()>
}
type MemoryRegistry = HashMap<usize, Weak<Memory>>;
static MEMORY: LazyLock<Mutex<MemoryRegistry>> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) enum Table {
    Memory(Arc<Memory>),
    #[cfg(unix)]
    Shm(ShmRegion)
}
impl Table {
    pub(crate) fn new(
        fleet: Arc<Fleet>,
        protection: u64
    ) -> Result<Self> {
        if fleet.is_shm() {
            #[cfg(unix)]
            {
                return Self::shm(&ring_segment_name(fleet.name(), AUTH_STATE_KIND), protection);
            }
            #[cfg(not(unix))]
            {
                return Err(Error::PolicyUnavailable);
            }
        }
        let mut registry = MEMORY.lock().map_err(|_| Error::PolicyUnavailable)?;
        registry.retain(|_, value| value.strong_count() > 0);
        let table = registry.entry(Arc::as_ptr(&fleet) as usize).or_default();
        if let Some(memory) = table.upgrade() {
            if memory.protection != protection {
                return Err(Error::IncompatibleProtection);
            }
            return Ok(Self::Memory(memory));
        }
        let memory = Arc::new(Memory {
            _fleet: fleet,
            protection,
            slots: (0..AUTH_STATE_CAPACITY).map(|_| Slot::empty()).collect(),
            writer: Mutex::new(())
        });
        *table = Arc::downgrade(&memory);
        Ok(Self::Memory(memory))
    }

    #[cfg(unix)]
    fn shm(
        name: &str,
        protection: u64
    ) -> Result<Self> {
        let size = HEADER + AUTH_STATE_CAPACITY * size_of::<Slot>();
        let (region, _initialization) =
            ShmRegion::open_or_create_locked(name, size).map_err(|_| Error::PolicyUnavailable)?;
        let mut expected = [0u8; HEADER];
        expected[..8].copy_from_slice(MAGIC);
        for (i, value) in [
            AUTH_STATE_CAPACITY,
            size_of::<Slot>(),
            BANK_WORDS,
            usize::from(cfg!(target_endian = "little"))
        ]
        .into_iter()
        .enumerate()
        {
            expected[8 + i * 8..16 + i * 8].copy_from_slice(&(value as u64).to_le_bytes());
        }
        expected[40..48].copy_from_slice(&protection.to_le_bytes());
        // SAFETY: mmap is page aligned and covers all slots. The initialization
        // lock excludes openers; published fields are thereafter only atomic.
        unsafe {
            if region.created() {
                std::ptr::write_bytes(region.as_ptr(), 0, size);
                std::ptr::copy_nonoverlapping(expected.as_ptr(), region.as_ptr(), HEADER);
            } else {
                let actual = std::slice::from_raw_parts(region.as_ptr(), HEADER);
                if actual[..40] != expected[..40] || actual[48..] != expected[48..] {
                    return Err(Error::IncompatibleLayout);
                }
                if actual[40..48] != expected[40..48] {
                    return Err(Error::IncompatibleProtection);
                }
            }
        }
        Ok(Self::Shm(region))
    }

    fn slots(&self) -> &[Slot] {
        match self {
            Self::Memory(memory) => &memory.slots,
            #[cfg(unix)]
            Self::Shm(region) => {
                // SAFETY: validated fixed geometry and alignment; region lives
                // as long as self. All peer-visible fields are atomic and there
                // are no mutable Rust references into the shared mapping.
                unsafe {
                    std::slice::from_raw_parts(
                        region.as_ptr().add(HEADER).cast::<Slot>(),
                        AUTH_STATE_CAPACITY
                    )
                }
            }
        }
    }

    /// Protected policies compare all bytes with the verified local copy. The
    /// unprotected policy trusts writers to publish a revision for every change.
    pub(crate) fn unchanged<P: StateProtection>(
        &self,
        snapshot: &Snapshot
    ) -> Result<bool> {
        let Some(slot) = self.slots().get(snapshot.index) else { return Ok(false) };
        if P::ID == UnprotectedState::ID {
            return Ok(slot.revision.load(Ordering::SeqCst) == snapshot.revision);
        }
        Ok(matches!(slot.read_once()?, Read::Value(revision, wire)
            if revision == snapshot.revision && wire == snapshot.wire))
    }

    pub(crate) fn lookup<P: StateProtection>(
        &self,
        key: StoreKey,
        mac: &RecordProtection<P>
    ) -> Result<Option<Snapshot>> {
        let result = lookup(self.slots(), key, mac)?;
        if result.raced {
            // A raced bank copy takes one synchronized read. No spin/poll loop,
            // no deadline, and no acceptance based on a torn snapshot.
            self.write(|slots| find(slots, key, mac))
        } else {
            Ok(result.snapshot)
        }
    }

    pub(crate) fn write<T>(
        &self,
        f: impl FnOnce(&[Slot]) -> Result<T>
    ) -> Result<T> {
        match self {
            Self::Memory(memory) => {
                let _lock = memory.writer.lock().map_err(|_| Error::PolicyUnavailable)?;
                f(&memory.slots)
            }
            #[cfg(unix)]
            Self::Shm(region) => {
                let _lock = region.lock_exclusive().map_err(|_| Error::PolicyUnavailable)?;
                f(self.slots())
            }
        }
    }
}

struct Lookup {
    snapshot: Option<Snapshot>,
    raced: bool
}
impl Lookup {
    fn stable(snapshot: Option<Snapshot>) -> Self {
        Self { snapshot, raced: false }
    }
    fn changed() -> Self {
        Self { snapshot: None, raced: true }
    }
}
fn lookup<P: StateProtection>(
    slots: &[Slot],
    key: StoreKey,
    mac: &RecordProtection<P>
) -> Result<Lookup> {
    for offset in 0..slots.len() {
        let index = (key.0[0] as usize).wrapping_add(offset) % slots.len();
        match slots[index].read_once()? {
            Read::Empty => return Ok(Lookup::stable(None)),
            Read::Value(revision, wire) => {
                let record = mac.decode(index, revision, &wire)?;
                if record.key == key {
                    return Ok(Lookup::stable(Some(Snapshot { index, revision, record, wire })));
                }
            }
            Read::Changed => return Ok(Lookup::changed())
        }
    }
    Ok(Lookup::stable(None))
}

/// Used only while the writer lock is held; no bank can change here.
pub(crate) fn find<P: StateProtection>(
    slots: &[Slot],
    key: StoreKey,
    mac: &RecordProtection<P>
) -> Result<Option<Snapshot>> {
    let result = lookup(slots, key, mac)?;
    if result.raced { Err(Error::PolicyUnavailable) } else { Ok(result.snapshot) }
}

/// Caller has checked absence and holds the writer lock. Reclaims only expired
/// state; no tombstone is removed early to admit another record.
pub(crate) fn insert<P: StateProtection>(
    slots: &[Slot],
    record: Record,
    now: u64,
    mac: &RecordProtection<P>
) -> Result<()> {
    for offset in 0..slots.len() {
        let index = (record.key.0[0] as usize).wrapping_add(offset) % slots.len();
        let reusable = match slots[index].read_once()? {
            Read::Empty => true,
            Read::Value(revision, wire) => {
                let old = mac.decode(index, revision, &wire)?;
                now >= old.expires_at
            }
            Read::Changed => return Err(Error::PolicyUnavailable)
        };
        if reusable {
            slots[index].store(record, index, mac)?;
            return Ok(());
        }
    }
    Err(Error::StateFull)
}

const _: () = assert!(
    AUTH_STATE_CAPACITY > 0
        && AUTH_STATE_CAPACITY <= (isize::MAX as usize - 64) / size_of::<Slot>()
);
const _: () = assert!(size_of::<Slot>() == 320 && align_of::<Slot>() == 64);

#[cfg(test)]
#[path = "../tests/unit/table.rs"]
mod tests;

//! Orbit-backed opaque server-session table.
//!
//! This module does not import rustls. Its relationship to rustls is indirect:
//! the parent `session` module implements rustls' storage trait and passes
//! rustls-owned opaque bytes into this fixed-capacity current-state table.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use std::{io, ptr};

use orbit_core::Fleet;
use orbit_core::shm::{ShmRegion, ring_segment_name};

pub(super) const DOMAIN_MAX: usize = 64;
pub(super) const KEY_MAX: usize = 64;
pub(super) const VALUE_MAX: usize = 16 * 1024;

const SESSION_STATE_KIND: u8 = 231;
const SET_COUNT: usize = 256;
const WAYS: usize = 8;
const CAPACITY: usize = SET_COUNT * WAYS;
const MAGIC: u32 = 0x4F_54_53_53; // "OTSS"
const VERSION: u16 = 1;
const SLOT_EMPTY: u8 = 0;
const SLOT_OCCUPIED: u8 = 1;
const SLOT_WRITING: u8 = 2;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub(super) struct SessionPrimitive {
    backing: Backing,
}

enum Backing {
    Memory(Mutex<MemoryTable>),
    Shm(ShmTable),
}

impl SessionPrimitive {
    pub(super) fn open(fleet: &Arc<Fleet>) -> io::Result<Self> {
        let backing = if fleet.is_shm() {
            Backing::Shm(ShmTable::open_or_create(&ring_segment_name(
                fleet.name(),
                SESSION_STATE_KIND,
            ))?)
        } else {
            Backing::Memory(Mutex::new(MemoryTable::new()))
        };
        Ok(Self { backing })
    }

    pub(super) fn put(
        &self,
        domain: &[u8],
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> io::Result<bool> {
        if !entry_fits(domain, key, value) || ttl.is_zero() {
            return Ok(false);
        }
        let now_ms = monotonic_ms()?;
        let ttl_ms = u64::try_from(ttl.as_millis().max(1)).unwrap_or(u64::MAX);
        let expires_at_ms = now_ms.saturating_add(ttl_ms);
        self.with_slots(|slots| {
            let hash = entry_hash(domain, key);
            let set = set_slots_mut(slots, hash);
            let mut reusable = None;
            let mut oldest = (0, u64::MAX);

            for (index, slot) in set.iter_mut().enumerate() {
                if slot.matches(hash, domain, key) {
                    slot.write(hash, domain, key, value, now_ms, expires_at_ms);
                    return true;
                }
                if slot.state() != SLOT_OCCUPIED || slot.expires_at_ms <= now_ms {
                    reusable = Some(index);
                    break;
                }
                if slot.inserted_at_ms < oldest.1 {
                    oldest = (index, slot.inserted_at_ms);
                }
            }

            let candidate = reusable.unwrap_or(oldest.0);
            set[candidate].write(hash, domain, key, value, now_ms, expires_at_ms);
            true
        })
    }

    pub(super) fn get(&self, domain: &[u8], key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.read(domain, key, false)
    }

    pub(super) fn take(&self, domain: &[u8], key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.read(domain, key, true)
    }

    fn read(&self, domain: &[u8], key: &[u8], consume: bool) -> io::Result<Option<Vec<u8>>> {
        if domain.is_empty() || domain.len() > DOMAIN_MAX || key.is_empty() || key.len() > KEY_MAX {
            return Ok(None);
        }
        let now_ms = monotonic_ms()?;
        self.with_slots(|slots| {
            let hash = entry_hash(domain, key);
            for slot in set_slots_mut(slots, hash) {
                if !slot.matches(hash, domain, key) {
                    continue;
                }
                if slot.expires_at_ms <= now_ms {
                    slot.clear();
                    return None;
                }
                let value = slot.value().to_vec();
                if consume {
                    slot.clear();
                }
                return Some(value);
            }
            None
        })
    }

    pub(super) fn reset(&self) -> io::Result<()> {
        self.with_slots(|slots| {
            for slot in slots {
                slot.clear();
            }
        })
    }

    pub(super) fn unlink(&self) -> io::Result<()> {
        match &self.backing {
            Backing::Memory(_) => self.reset(),
            Backing::Shm(table) => table.region.unlink(),
        }
    }

    fn with_slots<T>(&self, operation: impl FnOnce(&mut [SessionSlot]) -> T) -> io::Result<T> {
        match &self.backing {
            Backing::Memory(table) => {
                let mut table = lock_unpoisoned(table);
                Ok(operation(&mut table.slots))
            }
            Backing::Shm(table) => table.with_slots(operation),
        }
    }
}

struct MemoryTable {
    slots: Vec<SessionSlot>,
}

impl MemoryTable {
    fn new() -> Self {
        Self {
            slots: (0..CAPACITY).map(|_| SessionSlot::empty()).collect(),
        }
    }
}

struct ShmTable {
    region: ShmRegion,
    local_lock: Mutex<()>,
}

impl ShmTable {
    fn open_or_create(name: &str) -> io::Result<Self> {
        let (region, _initialization_lock) =
            ShmRegion::open_or_create_locked(name, segment_size())?;
        if region.created() {
            unsafe {
                ptr::write(
                    region.as_ptr().cast::<SessionHeader>(),
                    SessionHeader::new(),
                );
                let slots = region.as_ptr().add(std::mem::size_of::<SessionHeader>());
                ptr::write_bytes(slots, 0, CAPACITY * std::mem::size_of::<SessionSlot>());
            }
        } else {
            validate_header(name, unsafe { &*region.as_ptr().cast::<SessionHeader>() })?;
        }
        Ok(Self {
            region,
            local_lock: Mutex::new(()),
        })
    }

    fn with_slots<T>(&self, operation: impl FnOnce(&mut [SessionSlot]) -> T) -> io::Result<T> {
        let _local = lock_unpoisoned(&self.local_lock);
        let _process = self.region.lock_exclusive()?;
        let slots = unsafe {
            std::slice::from_raw_parts_mut(
                self.region
                    .as_ptr()
                    .add(std::mem::size_of::<SessionHeader>())
                    .cast::<SessionSlot>(),
                CAPACITY,
            )
        };
        Ok(operation(slots))
    }
}

#[repr(C, align(64))]
struct SessionHeader {
    magic: u32,
    version: u16,
    header_size: u16,
    capacity: u32,
    slot_size: u32,
    set_count: u16,
    ways: u16,
    domain_max: u16,
    key_max: u16,
    value_max: u32,
    _reserved: [u8; 36],
}

impl SessionHeader {
    fn new() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            header_size: std::mem::size_of::<Self>() as u16,
            capacity: CAPACITY as u32,
            slot_size: std::mem::size_of::<SessionSlot>() as u32,
            set_count: SET_COUNT as u16,
            ways: WAYS as u16,
            domain_max: DOMAIN_MAX as u16,
            key_max: KEY_MAX as u16,
            value_max: VALUE_MAX as u32,
            _reserved: [0; 36],
        }
    }
}

#[repr(C, align(64))]
struct SessionSlot {
    state: AtomicU8,
    domain_len: u8,
    key_len: u8,
    _reserved: u8,
    value_len: u32,
    key_hash: u64,
    inserted_at_ms: u64,
    expires_at_ms: u64,
    domain: [u8; DOMAIN_MAX],
    key: [u8; KEY_MAX],
    value: [u8; VALUE_MAX],
}

impl SessionSlot {
    fn empty() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            domain_len: 0,
            key_len: 0,
            _reserved: 0,
            value_len: 0,
            key_hash: 0,
            inserted_at_ms: 0,
            expires_at_ms: 0,
            domain: [0; DOMAIN_MAX],
            key: [0; KEY_MAX],
            value: [0; VALUE_MAX],
        }
    }

    fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    fn matches(&self, hash: u64, domain: &[u8], key: &[u8]) -> bool {
        self.state() == SLOT_OCCUPIED
            && self.key_hash == hash
            && self.domain_len as usize == domain.len()
            && self.key_len as usize == key.len()
            && &self.domain[..domain.len()] == domain
            && &self.key[..key.len()] == key
    }

    fn value(&self) -> &[u8] {
        &self.value[..self.value_len as usize]
    }

    fn write(
        &mut self,
        hash: u64,
        domain: &[u8],
        key: &[u8],
        value: &[u8],
        inserted_at_ms: u64,
        expires_at_ms: u64,
    ) {
        self.clear();
        self.state.store(SLOT_WRITING, Ordering::Release);
        self.domain_len = domain.len() as u8;
        self.key_len = key.len() as u8;
        self.value_len = value.len() as u32;
        self.key_hash = hash;
        self.inserted_at_ms = inserted_at_ms;
        self.expires_at_ms = expires_at_ms;
        self.domain[..domain.len()].copy_from_slice(domain);
        self.key[..key.len()].copy_from_slice(key);
        self.value[..value.len()].copy_from_slice(value);
        self.state.store(SLOT_OCCUPIED, Ordering::Release);
    }

    fn clear(&mut self) {
        self.state.store(SLOT_WRITING, Ordering::Release);
        self.domain_len = 0;
        self.key_len = 0;
        self.value_len = 0;
        self.key_hash = 0;
        self.inserted_at_ms = 0;
        self.expires_at_ms = 0;
        self.domain.fill(0);
        self.key.fill(0);
        self.value.fill(0);
        self.state.store(SLOT_EMPTY, Ordering::Release);
    }
}

fn validate_header(name: &str, header: &SessionHeader) -> io::Result<()> {
    if header.magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "SHM segment {name} has wrong magic 0x{:08X} (expected 0x{MAGIC:08X})",
                header.magic
            ),
        ));
    }
    if header.version != VERSION
        || usize::from(header.header_size) != std::mem::size_of::<SessionHeader>()
        || header.capacity as usize != CAPACITY
        || header.slot_size as usize != std::mem::size_of::<SessionSlot>()
        || header.set_count as usize != SET_COUNT
        || header.ways as usize != WAYS
        || header.domain_max as usize != DOMAIN_MAX
        || header.key_max as usize != KEY_MAX
        || header.value_max as usize != VALUE_MAX
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("SHM segment {name} has an incompatible TLS-session layout"),
        ));
    }
    Ok(())
}

fn entry_fits(domain: &[u8], key: &[u8], value: &[u8]) -> bool {
    !domain.is_empty()
        && domain.len() <= DOMAIN_MAX
        && !key.is_empty()
        && key.len() <= KEY_MAX
        && !value.is_empty()
        && value.len() <= VALUE_MAX
}

fn entry_hash(domain: &[u8], key: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in domain.iter().chain(key) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn set_slots_mut(slots: &mut [SessionSlot], hash: u64) -> &mut [SessionSlot] {
    let start = set_index(hash) * WAYS;
    &mut slots[start..start + WAYS]
}

fn set_index(hash: u64) -> usize {
    (hash as usize) & (SET_COUNT - 1)
}

fn segment_size() -> usize {
    std::mem::size_of::<SessionHeader>() + CAPACITY * std::mem::size_of::<SessionSlot>()
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn monotonic_ms() -> io::Result<u64> {
    let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, value.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    let value = unsafe { value.assume_init() };
    let seconds = u64::try_from(value.tv_sec)
        .map_err(|_| io::Error::other("negative monotonic clock seconds"))?;
    let nanos = u64::try_from(value.tv_nsec)
        .map_err(|_| io::Error::other("negative monotonic clock nanoseconds"))?;
    Ok(seconds
        .saturating_mul(1_000)
        .saturating_add(nanos / 1_000_000))
}

const _: () = assert!(SET_COUNT.is_power_of_two());
const _: () = assert!(CAPACITY == 2_048);
const _: () = assert!(std::mem::size_of::<SessionHeader>() == 64);

#[cfg(test)]
mod tests {
    use super::*;

    fn primitive() -> SessionPrimitive {
        let fleet = Arc::new(Fleet::join("tls-primitive-unit", 1).expect("fleet"));
        SessionPrimitive::open(&fleet).expect("primitive")
    }

    #[test]
    fn expiry_bounds_and_zeroing_are_fail_closed() {
        let primitive = primitive();
        let domain = b"tcp-public";

        assert!(
            !primitive
                .put(domain, &[7; KEY_MAX + 1], b"secret", Duration::from_secs(1))
                .expect("oversized key")
        );
        assert!(
            !primitive
                .put(
                    domain,
                    b"oversized-value",
                    &vec![7; VALUE_MAX + 1],
                    Duration::from_secs(1)
                )
                .expect("oversized value")
        );

        primitive
            .put(domain, b"overwrite", b"old-secret", Duration::from_secs(1))
            .expect("first put");
        primitive
            .put(domain, b"overwrite", b"new-secret", Duration::from_secs(1))
            .expect("overwrite");
        let hash = entry_hash(domain, b"overwrite");
        let slot_index = primitive
            .with_slots(|slots| {
                let set_start = set_index(hash) * WAYS;
                set_slots_mut(slots, hash)
                    .iter()
                    .position(|slot| slot.matches(hash, domain, b"overwrite"))
                    .map(|offset| set_start + offset)
                    .expect("occupied slot")
            })
            .expect("find slot");
        assert_eq!(
            primitive.take(domain, b"overwrite").expect("take"),
            Some(b"new-secret".to_vec())
        );
        primitive
            .with_slots(|slots| {
                let slot = &slots[slot_index];
                assert_eq!(slot.state(), SLOT_EMPTY);
                assert!(slot.domain.iter().all(|byte| *byte == 0));
                assert!(slot.key.iter().all(|byte| *byte == 0));
                assert!(slot.value.iter().all(|byte| *byte == 0));
            })
            .expect("inspect cleared slot");

        primitive
            .put(domain, b"short-lived", b"secret", Duration::from_millis(1))
            .expect("short put");
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(
            primitive.get(domain, b"short-lived").expect("expired get"),
            None
        );
    }

    #[test]
    fn a_full_set_evicts_only_its_oldest_entry() {
        let primitive = primitive();
        let domain = b"quic-public";
        let mut keys = Vec::with_capacity(WAYS + 1);
        let mut candidate = 0_u64;
        let target_set = set_index(entry_hash(domain, &candidate.to_le_bytes()));
        while keys.len() < WAYS + 1 {
            let key = candidate.to_le_bytes();
            if set_index(entry_hash(domain, &key)) == target_set {
                keys.push(key);
            }
            candidate += 1;
        }

        for key in &keys[..WAYS] {
            assert!(
                primitive
                    .put(domain, key, b"secret", Duration::from_secs(1))
                    .expect("put")
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(
            primitive
                .put(domain, &keys[WAYS], b"new", Duration::from_secs(1))
                .expect("evicting put")
        );

        assert_eq!(primitive.get(domain, &keys[0]).expect("oldest get"), None);
        for key in &keys[1..WAYS] {
            assert_eq!(
                primitive.get(domain, key).expect("retained get"),
                Some(b"secret".to_vec())
            );
        }
        assert_eq!(
            primitive.get(domain, &keys[WAYS]).expect("new get"),
            Some(b"new".to_vec())
        );
    }
}

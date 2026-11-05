//! Orbit-backed opaque rustls session tables.
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

use self::claim::ClaimTable;

mod claim;

pub(super) const DOMAIN_MAX: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_RUSTLS_SESSION_DOMAIN_CAPACITY"),
    64,
);
pub(super) const SERVER_KEY_MAX: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_RUSTLS_SESSION_KEY_CAPACITY"),
    64,
);
#[cfg(feature = "rustls_0_24")]
pub(super) const CLIENT_KEY_MAX: usize = u8::MAX as usize;
pub(super) const VALUE_MAX: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_RUSTLS_SESSION_VALUE_CAPACITY"),
    16 * 1024,
);

/// The server table at layout version 2. Version 1 was kind 231, which this
/// crate no longer opens; a segment of that kind left by an earlier build is
/// neither read nor removed here.
pub(super) const SERVER_SESSION_STATE_KIND: u8 = 185;
#[cfg(feature = "rustls_0_24")]
pub(super) const CLIENT_SESSION_STATE_KIND: u8 = 254;
const SET_COUNT: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_RUSTLS_SESSION_SET_COUNT"),
    256,
);
const WAYS: usize = orbit_core::compile::usize_from_env(
    option_env!("ORBIT_RUSTLS_SESSION_WAYS"),
    8,
);
#[cfg(feature = "rustls_0_24")]
pub(super) const CLIENT_TLS13_TICKETS_PER_SERVER: usize = WAYS;
const CAPACITY: usize = SET_COUNT * WAYS;
const MAGIC: u32 = 0x4f_54_53_53; // "OTSS"
/// One lock around the whole table.
const VERSION: u16 = 1;
const SLOT_EMPTY: u8 = 0;
const SLOT_OCCUPIED: u8 = 1;
const SLOT_WRITING: u8 = 2;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub(super) type ServerSessionPrimitive =
    SessionPrimitive<SERVER_SESSION_STATE_KIND, SERVER_KEY_MAX>;
#[cfg(feature = "rustls_0_24")]
pub(super) type ClientSessionPrimitive =
    SessionPrimitive<CLIENT_SESSION_STATE_KIND, CLIENT_KEY_MAX>;

pub(super) struct SessionPrimitive<const KIND: u8, const KEY_CAPACITY: usize> {
    backing: Backing<KEY_CAPACITY>,
    /// Membership of the fleet the table belongs to, kept for as long as the
    /// table is open. Declared after the backing, so it is given up last.
    _fleet: Arc<Fleet>,
}

/// What one operation keeps every other operation out of.
///
/// It is also the shared layout: the two are different segments under
/// different kinds, and a table is only ever opened the way it was created.
#[derive(Clone, Copy)]
pub(super) enum Exclusion {
    /// The whole table, behind one lock that an operation waits for.
    #[cfg_attr(not(feature = "rustls_0_24"), allow(dead_code))]
    Table,
    /// The set its key hashes to, by a claim that is never waited for.
    Set,
}

enum Backing<const KEY_CAPACITY: usize> {
    Memory(Mutex<MemoryTable<KEY_CAPACITY>>),
    Locked(ShmTable<KEY_CAPACITY>),
    Claimed(ClaimTable<KEY_CAPACITY>),
}

impl<const KIND: u8, const KEY_CAPACITY: usize>
    SessionPrimitive<KIND, KEY_CAPACITY>
{
    pub(super) fn open(
        fleet: &Arc<Fleet>,
        exclusion: Exclusion,
    ) -> io::Result<Self> {
        if KEY_CAPACITY == 0 || KEY_CAPACITY > u8::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rustls session key capacity must fit in one byte",
            ));
        }
        let backing = if fleet.is_shm() {
            let name = ring_segment_name(fleet.name(), KIND);
            match exclusion {
                Exclusion::Table => {
                    Backing::Locked(ShmTable::open_or_create(&name)?)
                }
                Exclusion::Set => {
                    Backing::Claimed(ClaimTable::open_or_create(&name)?)
                }
            }
        } else {
            Backing::Memory(Mutex::new(MemoryTable::new()))
        };
        Ok(Self { backing, _fleet: fleet.clone() })
    }

    pub(super) fn put(
        &self,
        domain: &[u8],
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> io::Result<bool> {
        if !entry_fits::<KEY_CAPACITY>(domain, key, value) || ttl.is_zero() {
            return Ok(false);
        }
        let now_ms = monotonic_ms()?;
        let ttl_ms = u64::try_from(ttl.as_millis().max(1)).unwrap_or(u64::MAX);
        let expires_at_ms = now_ms.saturating_add(ttl_ms);
        let hash = entry_hash(domain, key);
        let stored = self.with_set(hash, true, |set| {
            let mut reusable = None;
            let mut oldest = (0, u64::MAX);

            for (index, slot) in set.iter_mut().enumerate() {
                if slot.matches(hash, domain, key) {
                    slot.write(hash, domain, key, value, now_ms, expires_at_ms);
                    return;
                }
                if slot.state() != SLOT_OCCUPIED || slot.expires_at_ms <= now_ms
                {
                    reusable = Some(index);
                    break;
                }
                if slot.inserted_at_ms < oldest.1 {
                    oldest = (index, slot.inserted_at_ms);
                }
            }

            let candidate = reusable.unwrap_or(oldest.0);
            set[candidate].write(
                hash,
                domain,
                key,
                value,
                now_ms,
                expires_at_ms,
            );
        })?;
        Ok(stored.is_some())
    }

    /// Add another value for the same key without replacing earlier values.
    ///
    /// TLS 1.3 servers may issue multiple independent tickets for one server.
    /// `max_values` bounds that queue inside the key's set.
    #[cfg(feature = "rustls_0_24")]
    pub(super) fn push(
        &self,
        domain: &[u8],
        key: &[u8],
        value: &[u8],
        ttl: Duration,
        max_values: usize,
    ) -> io::Result<bool> {
        if !entry_fits::<KEY_CAPACITY>(domain, key, value)
            || ttl.is_zero()
            || max_values == 0
        {
            return Ok(false);
        }
        let now_ms = monotonic_ms()?;
        let ttl_ms = u64::try_from(ttl.as_millis().max(1)).unwrap_or(u64::MAX);
        let expires_at_ms = now_ms.saturating_add(ttl_ms);
        let hash = entry_hash(domain, key);
        let stored = self.with_set(hash, true, |set| {
            let mut matching = 0_usize;
            let mut reusable = None;
            let mut oldest_match = None;
            let mut oldest = (0, u64::MAX);

            for (index, slot) in set.iter_mut().enumerate() {
                if slot.state() != SLOT_OCCUPIED || slot.expires_at_ms <= now_ms
                {
                    slot.release();
                    reusable.get_or_insert(index);
                    continue;
                }
                if slot.matches(hash, domain, key) {
                    matching += 1;
                    if oldest_match.is_none_or(|(_, inserted)| {
                        slot.inserted_at_ms < inserted
                    }) {
                        oldest_match = Some((index, slot.inserted_at_ms));
                    }
                }
                if slot.inserted_at_ms < oldest.1 {
                    oldest = (index, slot.inserted_at_ms);
                }
            }

            let candidate = if matching >= max_values {
                oldest_match.map(|(index, _)| index).unwrap_or(oldest.0)
            } else {
                reusable.unwrap_or(oldest.0)
            };
            set[candidate].write(
                hash,
                domain,
                key,
                value,
                now_ms,
                expires_at_ms,
            );
        })?;
        Ok(stored.is_some())
    }

    pub(super) fn get(
        &self,
        domain: &[u8],
        key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        self.read(domain, key, false)
    }

    pub(super) fn take(
        &self,
        domain: &[u8],
        key: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        self.read(domain, key, true)
    }

    fn read(
        &self,
        domain: &[u8],
        key: &[u8],
        consume: bool,
    ) -> io::Result<Option<Vec<u8>>> {
        if domain.is_empty()
            || domain.len() > DOMAIN_MAX
            || key.is_empty()
            || key.len() > KEY_CAPACITY
        {
            return Ok(None);
        }
        let now_ms = monotonic_ms()?;
        let hash = entry_hash(domain, key);
        let found = self.with_set(hash, false, |set| {
            for slot in set {
                if !slot.matches(hash, domain, key) {
                    continue;
                }
                if slot.expires_at_ms <= now_ms {
                    slot.release();
                    return None;
                }
                let Some(value) = slot.value() else {
                    // Retained slot metadata can be corrupt even when the
                    // table header is valid. Repair only this locked slot.
                    slot.clear();
                    return None;
                };
                let value = value.to_vec();
                if consume {
                    slot.release();
                }
                return Some(value);
            }
            None
        })?;
        Ok(found.flatten())
    }

    /// Erase every slot.
    ///
    /// A claimed table refuses while any of its sets is held by an owner that
    /// is alive; the sets before that one have been erased by then.
    pub(super) fn reset(&self) -> io::Result<()> {
        match &self.backing {
            Backing::Memory(table) => {
                lock_unpoisoned(table)
                    .slots
                    .iter_mut()
                    .for_each(SessionSlot::clear);
                Ok(())
            }
            Backing::Locked(table) => table.with_slots(|slots| {
                slots.iter_mut().for_each(SessionSlot::clear);
            }),
            Backing::Claimed(table) => table.reset(),
        }
    }

    pub(super) fn unlink(&self) -> io::Result<()> {
        match &self.backing {
            Backing::Memory(_) => self.reset(),
            Backing::Locked(table) => table.region.unlink(),
            Backing::Claimed(table) => table.unlink(),
        }
    }

    /// Run `operation` on the set `hash` selects, as that set's only user.
    ///
    /// `None` means another operation holds the set and this one did not wait
    /// for it. Only a claimed table answers so; the others wait their turn.
    /// `recover` lets the operation take the set back from an owner that is
    /// gone, which asks the kernel and is therefore left to stores.
    fn with_set<T>(
        &self,
        hash: u64,
        recover: bool,
        operation: impl FnOnce(&mut [SessionSlot<KEY_CAPACITY>]) -> T,
    ) -> io::Result<Option<T>> {
        match &self.backing {
            Backing::Memory(table) => {
                let mut table = lock_unpoisoned(table);
                Ok(Some(operation(set_slots_mut(&mut table.slots, hash))))
            }
            Backing::Locked(table) => table
                .with_slots(|slots| operation(set_slots_mut(slots, hash)))
                .map(Some),
            Backing::Claimed(table) => {
                table.with_set(set_index(hash), recover, operation)
            }
        }
    }

    /// The whole table, for a test that is its only user.
    #[cfg(test)]
    fn with_slots<T>(
        &self,
        operation: impl FnOnce(&mut [SessionSlot<KEY_CAPACITY>]) -> T,
    ) -> io::Result<T> {
        match &self.backing {
            Backing::Memory(table) => {
                let mut table = lock_unpoisoned(table);
                Ok(operation(&mut table.slots))
            }
            Backing::Locked(table) => table.with_slots(operation),
            Backing::Claimed(table) => Ok(table.with_slots(operation)),
        }
    }
}

struct MemoryTable<const KEY_CAPACITY: usize> {
    slots: Vec<SessionSlot<KEY_CAPACITY>>,
}

impl<const KEY_CAPACITY: usize> MemoryTable<KEY_CAPACITY> {
    fn new() -> Self {
        Self { slots: (0..CAPACITY).map(|_| SessionSlot::empty()).collect() }
    }
}

struct ShmTable<const KEY_CAPACITY: usize> {
    region: ShmRegion,
    local_lock: Mutex<()>,
}

impl<const KEY_CAPACITY: usize> ShmTable<KEY_CAPACITY> {
    fn open_or_create(name: &str) -> io::Result<Self> {
        let (region, _initialization_lock) = ShmRegion::open_or_create_locked(
            name,
            segment_size::<KEY_CAPACITY>(),
        )?;
        if region.created() {
            unsafe {
                ptr::write(
                    region.as_ptr().cast::<SessionHeader>(),
                    SessionHeader::new::<KEY_CAPACITY>(VERSION, 0),
                );
                let slots =
                    region.as_ptr().add(std::mem::size_of::<SessionHeader>());
                ptr::write_bytes(
                    slots,
                    0,
                    CAPACITY * std::mem::size_of::<SessionSlot<KEY_CAPACITY>>(),
                );
            }
        } else {
            validate_header::<KEY_CAPACITY>(
                name,
                unsafe { &*region.as_ptr().cast::<SessionHeader>() },
                VERSION,
                0,
            )?;
        }
        Ok(Self { region, local_lock: Mutex::new(()) })
    }

    fn with_slots<T>(
        &self,
        operation: impl FnOnce(&mut [SessionSlot<KEY_CAPACITY>]) -> T,
    ) -> io::Result<T> {
        let _local = lock_unpoisoned(&self.local_lock);
        let _process = self.region.lock_exclusive()?;
        let slots = unsafe {
            std::slice::from_raw_parts_mut(
                self.region
                    .as_ptr()
                    .add(std::mem::size_of::<SessionHeader>())
                    .cast::<SessionSlot<KEY_CAPACITY>>(),
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
    /// Owner registrations a claimed table has; zero in a locked one, where
    /// these bytes were reserved and written as zero.
    owners: u16,
    _reserved: [u8; 34],
}

impl SessionHeader {
    fn new<const KEY_CAPACITY: usize>(
        version: u16,
        owners: usize,
    ) -> Self {
        Self {
            magic: MAGIC,
            version,
            header_size: std::mem::size_of::<Self>() as u16,
            capacity: CAPACITY as u32,
            slot_size: std::mem::size_of::<SessionSlot<KEY_CAPACITY>>() as u32,
            set_count: SET_COUNT as u16,
            ways: WAYS as u16,
            domain_max: DOMAIN_MAX as u16,
            key_max: KEY_CAPACITY as u16,
            value_max: VALUE_MAX as u32,
            owners: owners as u16,
            _reserved: [0; 34],
        }
    }
}

#[repr(C, align(64))]
struct SessionSlot<const KEY_CAPACITY: usize> {
    state: AtomicU8,
    domain_len: u8,
    key_len: u8,
    _reserved: u8,
    value_len: u32,
    key_hash: u64,
    inserted_at_ms: u64,
    expires_at_ms: u64,
    domain: [u8; DOMAIN_MAX],
    key: [u8; KEY_CAPACITY],
    value: [u8; VALUE_MAX],
}

impl<const KEY_CAPACITY: usize> SessionSlot<KEY_CAPACITY> {
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
            key: [0; KEY_CAPACITY],
            value: [0; VALUE_MAX],
        }
    }

    fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    fn matches(
        &self,
        hash: u64,
        domain: &[u8],
        key: &[u8],
    ) -> bool {
        self.state() == SLOT_OCCUPIED
            && self.key_hash == hash
            && self.domain_len as usize == domain.len()
            && self.key_len as usize == key.len()
            && &self.domain[..domain.len()] == domain
            && &self.key[..key.len()] == key
    }

    fn value(&self) -> Option<&[u8]> {
        if self.value_len == 0 {
            return None;
        }
        self.value.get(..self.value_len as usize)
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
        self.release();
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

    /// The extents a committed entry wrote, when all three are plausible.
    ///
    /// Only an occupied slot has committed lengths, and an entry never has an
    /// empty domain, key or value. Anything else is not a committed entry.
    fn extents(&self) -> Option<(usize, usize, usize)> {
        if self.state() != SLOT_OCCUPIED {
            return None;
        }
        let extents = (
            self.domain_len as usize,
            self.key_len as usize,
            self.value_len as usize,
        );
        let (domain, key, value) = extents;
        ((1..=DOMAIN_MAX).contains(&domain)
            && (1..=KEY_CAPACITY).contains(&key)
            && (1..=VALUE_MAX).contains(&value))
        .then_some(extents)
    }

    /// Give the slot up, leaving no byte of what it held.
    ///
    /// Every byte outside a committed entry's extents is already clear: slots
    /// start zeroed and are only ever emptied by this function or by
    /// [`clear`](Self::clear). A committed entry is therefore erased over the
    /// extents it wrote and an empty slot needs nothing. A slot in any other
    /// state, or with implausible lengths, is an abandoned write or damaged
    /// metadata, and is erased whole.
    fn release(&mut self) {
        if self.state() == SLOT_EMPTY {
            return;
        }
        let Some((domain, key, value)) = self.extents() else {
            return self.clear();
        };
        // An interrupted erasure stays marked as an incomplete write, so it
        // is erased whole by whoever finds it.
        self.state.store(SLOT_WRITING, Ordering::Release);
        erase(&mut self.domain[..domain]);
        erase(&mut self.key[..key]);
        erase(&mut self.value[..value]);
        self.forget();
        self.state.store(SLOT_EMPTY, Ordering::Release);
    }

    /// Erase the whole slot, trusting nothing stored in it.
    fn clear(&mut self) {
        self.state.store(SLOT_WRITING, Ordering::Release);
        erase(&mut self.domain);
        erase(&mut self.key);
        erase(&mut self.value);
        self.forget();
        self.state.store(SLOT_EMPTY, Ordering::Release);
    }

    fn forget(&mut self) {
        self.domain_len = 0;
        self.key_len = 0;
        self.value_len = 0;
        self.key_hash = 0;
        self.inserted_at_ms = 0;
        self.expires_at_ms = 0;
    }
}

/// Zero bytes that held session secrets, with stores the compiler may not
/// remove on the grounds that nothing reads them afterwards.
fn erase(bytes: &mut [u8]) {
    // SAFETY: any bit pattern is a valid `u64`, and the three parts are
    // disjoint views of `bytes`.
    let (head, words, tail) = unsafe { bytes.align_to_mut::<u64>() };
    for byte in head.iter_mut().chain(tail) {
        // SAFETY: `byte` is a valid exclusive reference.
        unsafe { ptr::write_volatile(byte, 0) };
    }
    for word in words {
        // SAFETY: `word` is a valid exclusive reference.
        unsafe { ptr::write_volatile(word, 0) };
    }
    std::sync::atomic::compiler_fence(Ordering::SeqCst);
}

fn validate_header<const KEY_CAPACITY: usize>(
    name: &str,
    header: &SessionHeader,
    version: u16,
    owners: usize,
) -> io::Result<()> {
    if header.magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "SHM segment {name} has wrong magic 0x{:08X} (expected 0x{MAGIC:08X})",
                header.magic
            ),
        ));
    }
    if header.version != version
        || usize::from(header.owners) != owners
        || usize::from(header.header_size)
            != std::mem::size_of::<SessionHeader>()
        || header.capacity as usize != CAPACITY
        || header.slot_size as usize
            != std::mem::size_of::<SessionSlot<KEY_CAPACITY>>()
        || header.set_count as usize != SET_COUNT
        || header.ways as usize != WAYS
        || header.domain_max as usize != DOMAIN_MAX
        || header.key_max as usize != KEY_CAPACITY
        || header.value_max as usize != VALUE_MAX
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "SHM segment {name} has an incompatible TLS-session layout"
            ),
        ));
    }
    Ok(())
}

fn entry_fits<const KEY_CAPACITY: usize>(
    domain: &[u8],
    key: &[u8],
    value: &[u8],
) -> bool {
    !domain.is_empty()
        && domain.len() <= DOMAIN_MAX
        && !key.is_empty()
        && key.len() <= KEY_CAPACITY
        && !value.is_empty()
        && value.len() <= VALUE_MAX
}

fn entry_hash(
    domain: &[u8],
    key: &[u8],
) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in domain.iter().chain(key) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn set_slots_mut<const KEY_CAPACITY: usize>(
    slots: &mut [SessionSlot<KEY_CAPACITY>],
    hash: u64,
) -> &mut [SessionSlot<KEY_CAPACITY>] {
    let start = set_index(hash) * WAYS;
    &mut slots[start..start + WAYS]
}

fn set_index(hash: u64) -> usize {
    (hash as usize) & (SET_COUNT - 1)
}

fn segment_size<const KEY_CAPACITY: usize>() -> usize {
    std::mem::size_of::<SessionHeader>()
        + CAPACITY * std::mem::size_of::<SessionSlot<KEY_CAPACITY>>()
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn monotonic_ms() -> io::Result<u64> {
    let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let result = unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, value.as_mut_ptr())
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    let value = unsafe { value.assume_init() };
    let seconds = u64::try_from(value.tv_sec)
        .map_err(|_| io::Error::other("negative monotonic clock seconds"))?;
    let nanos = u64::try_from(value.tv_nsec).map_err(|_| {
        io::Error::other("negative monotonic clock nanoseconds")
    })?;
    Ok(seconds.saturating_mul(1_000).saturating_add(nanos / 1_000_000))
}

const _: () = assert!(SET_COUNT.is_power_of_two());
const _: () = assert!(SET_COUNT <= u16::MAX as usize);
const _: () = assert!(WAYS > 0 && WAYS <= u16::MAX as usize);
const _: () = assert!(CAPACITY <= u32::MAX as usize);
const _: () = assert!(DOMAIN_MAX > 0 && DOMAIN_MAX <= u8::MAX as usize);
const _: () = assert!(SERVER_KEY_MAX > 0 && SERVER_KEY_MAX <= u8::MAX as usize);
#[cfg(feature = "rustls_0_24")]
const _: () = assert!(CLIENT_KEY_MAX > 0 && CLIENT_KEY_MAX <= u8::MAX as usize);
#[cfg(feature = "rustls_0_24")]
const _: () = assert!(DOMAIN_MAX >= 33);
const _: () = assert!(VALUE_MAX > 0 && VALUE_MAX <= u32::MAX as usize);
const _: () = assert!(std::mem::size_of::<SessionHeader>() == 64);
const _: () = assert!(
    std::mem::size_of::<SessionSlot<SERVER_KEY_MAX>>() <= u32::MAX as usize
);
#[cfg(feature = "rustls_0_24")]
const _: () = assert!(
    std::mem::size_of::<SessionSlot<CLIENT_KEY_MAX>>() <= u32::MAX as usize
);

#[cfg(test)]
mod tests {
    use super::*;

    fn fleet() -> Arc<Fleet> {
        Arc::new(Fleet::join("tls-primitive-unit", 1).expect("fleet"))
    }

    fn primitive() -> ServerSessionPrimitive {
        SessionPrimitive::open(&fleet(), Exclusion::Set).expect("primitive")
    }

    #[test]
    fn malformed_retained_session_get_is_a_miss() {
        malformed_retained_session(false, Exclusion::Table);
        malformed_retained_session(false, Exclusion::Set);
    }

    #[test]
    fn malformed_retained_session_take_is_a_miss() {
        malformed_retained_session(true, Exclusion::Table);
        malformed_retained_session(true, Exclusion::Set);
    }

    fn malformed_retained_session(
        consume: bool,
        exclusion: Exclusion,
    ) {
        use std::sync::atomic::AtomicU64;

        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!(
            "/rtls-{:x}-{:x}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let open = || ServerSessionPrimitive {
            backing: match exclusion {
                Exclusion::Table => Backing::Locked(
                    ShmTable::open_or_create(&name).expect("table"),
                ),
                Exclusion::Set => Backing::Claimed(
                    ClaimTable::open_or_create(&name).expect("table"),
                ),
            },
            _fleet: fleet(),
        };
        struct Cleanup(ServerSessionPrimitive);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = self.0.unlink();
            }
        }
        let _cleanup = Cleanup(open());
        let domain = b"public";
        let key = 0_u64.to_le_bytes();
        let hash = entry_hash(domain, &key);
        let other = (1_u64..)
            .map(u64::to_le_bytes)
            .find(|candidate| {
                set_index(entry_hash(domain, candidate)) == set_index(hash)
            })
            .expect("another key in the same set");
        let ttl = super::super::DEFAULT_SESSION_TTL;

        for length in [0, VALUE_MAX as u32 + 1, u32::MAX] {
            let primitive = open();
            assert!(primitive.put(domain, &key, b"damaged", ttl).unwrap());
            assert!(primitive.put(domain, &other, b"preserved", ttl).unwrap());
            primitive
                .with_slots(|slots| {
                    let slot = set_slots_mut(slots, hash)
                        .iter_mut()
                        .find(|slot| slot.matches(hash, domain, &key))
                        .expect("stored slot");
                    slot.value_len = length;
                })
                .unwrap();
            drop(primitive);

            let primitive = open();
            let result = if consume {
                primitive.take(domain, &key)
            } else {
                primitive.get(domain, &key)
            };
            assert_eq!(result.unwrap(), None, "malformed length {length}");
            assert_eq!(primitive.get(domain, &key).unwrap(), None);
            assert_eq!(
                primitive.get(domain, &other).unwrap(),
                Some(b"preserved".to_vec())
            );
            let replacement = vec![7; VALUE_MAX];
            assert!(primitive.put(domain, &key, &replacement, ttl).unwrap());
            assert_eq!(
                primitive.get(domain, &key).unwrap(),
                Some(replacement.clone())
            );
            assert_eq!(
                primitive.take(domain, &key).unwrap(),
                Some(replacement)
            );
            assert_eq!(primitive.take(domain, &key).unwrap(), None);
            assert_eq!(
                primitive.take(domain, &other).unwrap(),
                Some(b"preserved".to_vec())
            );
        }
    }

    /// The slot holding `key`, by its index in the table.
    fn slot_of(
        primitive: &ServerSessionPrimitive,
        domain: &[u8],
        key: &[u8],
    ) -> usize {
        let hash = entry_hash(domain, key);
        primitive
            .with_slots(|slots| {
                let set_start = set_index(hash) * WAYS;
                set_slots_mut(slots, hash)
                    .iter()
                    .position(|slot| slot.matches(hash, domain, key))
                    .map(|offset| set_start + offset)
                    .expect("occupied slot")
            })
            .expect("find slot")
    }

    #[test]
    fn a_shorter_replacement_and_an_abandoned_write_leave_nothing_behind() {
        let primitive = primitive();
        let domain = b"erasure";
        let ttl = super::super::DEFAULT_SESSION_TTL;

        // The tail of a longer entry does not outlive a shorter one.
        primitive.put(domain, b"entry", &[0xaa; 1000], ttl).unwrap();
        primitive.put(domain, b"entry", &[0xbb; 10], ttl).unwrap();
        let index = slot_of(&primitive, domain, b"entry");
        primitive
            .with_slots(|slots| {
                let slot = &mut slots[index];
                assert!(slot.value[..10].iter().all(|byte| *byte == 0xbb));
                assert!(slot.value[10..].iter().all(|byte| *byte == 0));
                // A writer died here: bytes beyond any committed length, and
                // a state that says the write never finished.
                slot.value[5000..5010].fill(0xcc);
                slot.state.store(SLOT_WRITING, Ordering::Release);
            })
            .unwrap();

        // Its slot is the first one free in the set; reusing it erases all
        // of it, not the extent of an entry that was never committed.
        primitive.put(domain, b"entry", &[0xdd; 10], ttl).unwrap();
        assert_eq!(slot_of(&primitive, domain, b"entry"), index);
        primitive
            .with_slots(|slots| {
                let slot = &slots[index];
                assert!(slot.value[..10].iter().all(|byte| *byte == 0xdd));
                assert!(slot.value[10..].iter().all(|byte| *byte == 0));
            })
            .unwrap();
    }

    #[test]
    fn expiry_bounds_and_zeroing_are_fail_closed() {
        let primitive = primitive();
        let domain = b"tcp-public";

        assert!(
            !primitive
                .put(
                    domain,
                    &[7; SERVER_KEY_MAX + 1],
                    b"secret",
                    Duration::from_secs(1)
                )
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
        let target_set =
            set_index(entry_hash(domain, &candidate.to_le_bytes()));
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

    #[cfg(feature = "rustls_0_24")]
    #[test]
    fn client_ticket_queue_is_bounded_and_single_use() {
        let fleet = Arc::new(
            Fleet::join("tls-client-primitive-unit", 1).expect("fleet"),
        );
        let primitive = ClientSessionPrimitive::open(&fleet, Exclusion::Table)
            .expect("primitive");
        let domain = &[9; 33];
        let key = vec![7; CLIENT_KEY_MAX];

        for ticket in 0_u8..=CLIENT_TLS13_TICKETS_PER_SERVER as u8 {
            assert!(
                primitive
                    .push(
                        domain,
                        &key,
                        &[ticket],
                        Duration::from_secs(1),
                        CLIENT_TLS13_TICKETS_PER_SERVER
                    )
                    .expect("push ticket")
            );
        }

        let mut taken = Vec::new();
        while let Some(ticket) =
            primitive.take(domain, &key).expect("take ticket")
        {
            taken.push(ticket[0]);
        }
        assert_eq!(taken.len(), CLIENT_TLS13_TICKETS_PER_SERVER);
        assert!(!taken.contains(&0), "oldest ticket should be evicted");
        assert_eq!(primitive.take(domain, &key).expect("empty take"), None);
    }
}

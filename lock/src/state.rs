//! Fixed-capacity authoritative lock state.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};

#[cfg(unix)]
use orbit_core::shm::{ShmRegion, ring_segment_name};
use orbit_core::{Fleet, NetId64, NodeId};

use crate::layout::LockLayout;
use crate::{Error, LockAcquire, LockKey, LockLease, LockOwner, LockTransition, Result};

pub const LOCK_STATE_CAPACITY: usize = 256;
pub const LOCK_STATE_PAYLOAD_MAX: usize = 960;

const LOCK_COUNTER_MAX: u64 = 0xFF_FFFF_FFFF;
const SLOT_EMPTY: u8 = 0;
const SLOT_OCCUPIED: u8 = 1;
const SLOT_TOMBSTONE: u8 = 2;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[cfg(unix)]
const STATE_MAGIC: u32 = 0x4F_4C_53_54; // "OLST"
#[cfg(unix)]
const STATE_VERSION: u16 = 2;

#[derive(Clone)]
pub(crate) struct LockStateStore<L: LockLayout> {
    backend: StateBacking,
    state_kind: u8,
    node_id: NodeId,
    _layout: PhantomData<L>,
}

#[derive(Clone)]
enum StateBacking {
    InMemory(Arc<Mutex<MemoryTable>>),
    #[cfg(unix)]
    Shm(Arc<ShmStateStore>),
}

impl<L: LockLayout> LockStateStore<L> {
    pub(crate) fn new(fleet: &Arc<Fleet>) -> Self {
        let backend = if fleet.is_shm() {
            #[cfg(unix)]
            {
                StateBacking::Shm(Arc::new(ShmStateStore::new(&ring_segment_name(
                    fleet.name(),
                    L::STATE_KIND,
                ))))
            }
            #[cfg(not(unix))]
            unreachable!("non-Unix fleets cannot use POSIX SHM")
        } else {
            StateBacking::InMemory(memory_table(fleet, L::STATE_KIND))
        };
        Self {
            backend,
            state_kind: L::STATE_KIND,
            node_id: fleet.node_id(),
            _layout: PhantomData,
        }
    }

    pub(crate) fn acquire(
        &self,
        key: &LockKey,
        owner: &LockOwner,
        now_ms: u64,
        expires_at_ms: u64,
        publish: impl FnOnce(&LockTransition) -> Result<()>,
    ) -> Result<LockAcquire> {
        validate_entry(key, owner)?;
        self.with_table(|header, slots| {
            let hash = lock_hash(key);
            let mut insertion = None;
            for offset in 0..slots.len() {
                let index = probe_index(hash, offset, slots.len());
                let slot = &mut slots[index];
                match slot.state() {
                    SLOT_EMPTY => {
                        insertion.get_or_insert(index);
                        break;
                    }
                    SLOT_TOMBSTONE => {
                        insertion.get_or_insert(index);
                    }
                    SLOT_OCCUPIED if slot.expires_at_ms <= now_ms => {
                        slot.tombstone();
                        insertion.get_or_insert(index);
                    }
                    SLOT_OCCUPIED if slot.matches(hash, key) => {
                        return Ok(LockAcquire::Occupied(slot.lease()));
                    }
                    _ => {}
                }
            }

            let index = insertion.ok_or(Error::StateFull {
                capacity: slots.len(),
            })?;
            let lease = LockLease {
                lock_id: mint_lock_id(header, self.state_kind, self.node_id)?,
                key: key.clone(),
                owner: owner.clone(),
                acquired_at_ms: now_ms,
                expires_at_ms,
                state_revision: mint_revision(header)?,
            };
            publish(&LockTransition::Acquired(lease.clone()))?;
            slots[index].write(hash, &lease);
            Ok(LockAcquire::Acquired(lease))
        })
    }

    pub(crate) fn renew_id(
        &self,
        lease: &LockLease,
        now_ms: u64,
        requested_expires_at_ms: u64,
        publish: impl FnOnce(&LockTransition) -> Result<()>,
    ) -> Result<Option<LockLease>> {
        validate_entry(&lease.key, &lease.owner)?;
        self.with_table(|header, slots| {
            let Some(slot) = live_slot(slots, &lease.key, now_ms) else {
                return Ok(None);
            };
            if slot.lock_id != lease.lock_id.raw() {
                return Ok(None);
            }
            renew_slot(header, slot, requested_expires_at_ms, publish)
        })
    }

    pub(crate) fn renew_owner(
        &self,
        key: &LockKey,
        owner: &LockOwner,
        now_ms: u64,
        requested_expires_at_ms: u64,
        publish: impl FnOnce(&LockTransition) -> Result<()>,
    ) -> Result<Option<LockLease>> {
        validate_entry(key, owner)?;
        self.with_table(|header, slots| {
            let Some(slot) = live_slot(slots, key, now_ms) else {
                return Ok(None);
            };
            if slot.owner() != owner.as_bytes() {
                return Ok(None);
            }
            renew_slot(header, slot, requested_expires_at_ms, publish)
        })
    }

    pub(crate) fn release_id(
        &self,
        lease: &LockLease,
        now_ms: u64,
        publish: impl FnOnce(&LockTransition) -> Result<()>,
    ) -> Result<bool> {
        validate_entry(&lease.key, &lease.owner)?;
        self.with_table(|header, slots| {
            let Some(slot) = live_slot(slots, &lease.key, now_ms) else {
                return Ok(false);
            };
            if slot.lock_id != lease.lock_id.raw() {
                return Ok(false);
            }
            release_slot(header, slot, publish)
        })
    }

    pub(crate) fn release_owner(
        &self,
        key: &LockKey,
        owner: &LockOwner,
        now_ms: u64,
        publish: impl FnOnce(&LockTransition) -> Result<()>,
    ) -> Result<bool> {
        validate_entry(key, owner)?;
        self.with_table(|header, slots| {
            let Some(slot) = live_slot(slots, key, now_ms) else {
                return Ok(false);
            };
            if slot.owner() != owner.as_bytes() {
                return Ok(false);
            }
            release_slot(header, slot, publish)
        })
    }

    pub(crate) fn force_release(
        &self,
        key: &LockKey,
        now_ms: u64,
        publish: impl FnOnce(&LockTransition) -> Result<()>,
    ) -> Result<bool> {
        validate_key(key)?;
        self.with_table(|header, slots| {
            let Some(index) = find_slot(slots, lock_hash(key), key) else {
                return Ok(false);
            };
            let slot = &mut slots[index];
            if slot.expires_at_ms <= now_ms {
                slot.tombstone();
                return Ok(false);
            }
            release_slot(header, slot, publish)
        })
    }

    pub(crate) fn current(&self, key: &LockKey, now_ms: u64) -> Result<Option<LockLease>> {
        validate_key(key)?;
        self.with_table(|_header, slots| Ok(live_slot(slots, key, now_ms).map(|slot| slot.lease())))
    }

    pub(crate) fn reset(&self) -> Result<()> {
        self.with_table(|header, slots| {
            for slot in slots {
                slot.clear();
            }
            header.next_lock.store(0, Ordering::Relaxed);
            header.next_revision.store(0, Ordering::Relaxed);
            Ok(())
        })
    }

    #[cfg(unix)]
    pub(crate) fn unlink(&self) -> Result<()> {
        match &self.backend {
            StateBacking::InMemory(_) => self.reset(),
            StateBacking::Shm(store) => store.open()?.unlink().map_err(Error::Io),
        }
    }

    fn with_table<T>(
        &self,
        operation: impl FnOnce(&LockStateHeader, &mut [LockStateSlot]) -> Result<T>,
    ) -> Result<T> {
        match &self.backend {
            StateBacking::InMemory(table) => {
                let mut table = lock_unpoisoned(table);
                let MemoryTable { header, slots } = &mut *table;
                operation(header, slots)
            }
            #[cfg(unix)]
            StateBacking::Shm(store) => store.open()?.with_table(operation).map_err(Error::Io)?,
        }
    }
}

fn renew_slot(
    header: &LockStateHeader,
    slot: &mut LockStateSlot,
    requested_expires_at_ms: u64,
    publish: impl FnOnce(&LockTransition) -> Result<()>,
) -> Result<Option<LockLease>> {
    let mut lease = slot.lease();
    lease.expires_at_ms = lease.expires_at_ms.max(requested_expires_at_ms);
    lease.state_revision = mint_revision(header)?;
    publish(&LockTransition::Renewed(lease.clone()))?;
    let hash = slot.key_hash;
    slot.write(hash, &lease);
    Ok(Some(lease))
}

fn release_slot(
    header: &LockStateHeader,
    slot: &mut LockStateSlot,
    publish: impl FnOnce(&LockTransition) -> Result<()>,
) -> Result<bool> {
    let lease = slot.lease();
    let state_revision = mint_revision(header)?;
    publish(&LockTransition::Released {
        lock_id: lease.lock_id,
        key: lease.key,
        owner: lease.owner,
        state_revision,
    })?;
    slot.tombstone();
    Ok(true)
}

fn live_slot<'a>(
    slots: &'a mut [LockStateSlot],
    key: &LockKey,
    now_ms: u64,
) -> Option<&'a mut LockStateSlot> {
    let hash = lock_hash(key);
    let index = find_slot(slots, hash, key)?;
    let slot = &mut slots[index];
    if slot.expires_at_ms <= now_ms {
        slot.tombstone();
        None
    } else {
        Some(slot)
    }
}

fn find_slot(slots: &[LockStateSlot], hash: u64, key: &LockKey) -> Option<usize> {
    for offset in 0..slots.len() {
        let index = probe_index(hash, offset, slots.len());
        let slot = &slots[index];
        match slot.state() {
            SLOT_EMPTY => return None,
            SLOT_OCCUPIED if slot.matches(hash, key) => return Some(index),
            _ => {}
        }
    }
    None
}

fn validate_key(key: &LockKey) -> Result<()> {
    if key.namespace().is_empty() {
        return Err(Error::NamespaceEmpty);
    }
    if key.label().is_empty() {
        return Err(Error::KeyEmpty);
    }
    if key.namespace().len() > u16::MAX as usize
        || key.label().len() > u16::MAX as usize
        || key.namespace().len().saturating_add(key.label().len()) > LOCK_STATE_PAYLOAD_MAX
    {
        return Err(Error::EntryTooLarge {
            namespace_len: key.namespace().len(),
            key_len: key.label().len(),
            owner_len: 0,
            max_payload: LOCK_STATE_PAYLOAD_MAX,
        });
    }
    Ok(())
}

fn validate_entry(key: &LockKey, owner: &LockOwner) -> Result<()> {
    validate_key(key)?;
    if owner.as_bytes().is_empty() {
        return Err(Error::OwnerEmpty);
    }
    if key.namespace().len() > u16::MAX as usize
        || key.label().len() > u16::MAX as usize
        || owner.as_bytes().len() > u16::MAX as usize
        || key
            .namespace()
            .len()
            .saturating_add(key.label().len())
            .saturating_add(owner.as_bytes().len())
            > LOCK_STATE_PAYLOAD_MAX
    {
        return Err(Error::EntryTooLarge {
            namespace_len: key.namespace().len(),
            key_len: key.label().len(),
            owner_len: owner.as_bytes().len(),
            max_payload: LOCK_STATE_PAYLOAD_MAX,
        });
    }
    Ok(())
}

fn mint_lock_id(header: &LockStateHeader, state_kind: u8, node_id: NodeId) -> Result<NetId64> {
    let current = header.next_lock.load(Ordering::Relaxed);
    let counter = current.checked_add(1).ok_or(Error::IdExhausted)?;
    if counter > LOCK_COUNTER_MAX {
        return Err(Error::IdExhausted);
    }
    header.next_lock.store(counter, Ordering::Relaxed);
    Ok(NetId64::make(state_kind, node_id.get(), counter))
}

fn mint_revision(header: &LockStateHeader) -> Result<u64> {
    let current = header.next_revision.load(Ordering::Relaxed);
    let revision = current.checked_add(1).ok_or(Error::IdExhausted)?;
    header.next_revision.store(revision, Ordering::Relaxed);
    Ok(revision)
}

fn lock_hash(key: &LockKey) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in key.namespace().iter().chain(key.label()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn probe_index(hash: u64, offset: usize, capacity: usize) -> usize {
    debug_assert!(capacity.is_power_of_two());
    (hash as usize).wrapping_add(offset) & (capacity - 1)
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

struct MemoryTable {
    header: LockStateHeader,
    slots: Vec<LockStateSlot>,
}

impl MemoryTable {
    fn new() -> Self {
        Self {
            header: LockStateHeader::new(),
            slots: (0..LOCK_STATE_CAPACITY)
                .map(|_| LockStateSlot::empty())
                .collect(),
        }
    }
}

type MemoryRegistry = HashMap<(usize, u8), Weak<Mutex<MemoryTable>>>;
static MEMORY_TABLES: LazyLock<Mutex<MemoryRegistry>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn memory_table(fleet: &Arc<Fleet>, state_kind: u8) -> Arc<Mutex<MemoryTable>> {
    let fleet_identity = Arc::as_ptr(fleet) as usize;
    let mut tables = lock_unpoisoned(&MEMORY_TABLES);
    tables.retain(|_, table| table.strong_count() > 0);
    if let Some(table) = tables
        .get(&(fleet_identity, state_kind))
        .and_then(Weak::upgrade)
    {
        return table;
    }
    let table = Arc::new(Mutex::new(MemoryTable::new()));
    tables.insert((fleet_identity, state_kind), Arc::downgrade(&table));
    table
}

#[repr(C, align(64))]
struct LockStateHeader {
    #[cfg(unix)]
    magic: u32,
    #[cfg(not(unix))]
    _magic: u32,
    #[cfg(unix)]
    version: u16,
    #[cfg(not(unix))]
    _version: u16,
    header_size: u16,
    capacity: u32,
    slot_size: u32,
    next_lock: AtomicU64,
    next_revision: AtomicU64,
    _reserved: [u8; 32],
}

impl LockStateHeader {
    fn new() -> Self {
        Self {
            #[cfg(unix)]
            magic: STATE_MAGIC,
            #[cfg(not(unix))]
            _magic: 0,
            #[cfg(unix)]
            version: STATE_VERSION,
            #[cfg(not(unix))]
            _version: 0,
            header_size: std::mem::size_of::<Self>() as u16,
            capacity: LOCK_STATE_CAPACITY as u32,
            slot_size: std::mem::size_of::<LockStateSlot>() as u32,
            next_lock: AtomicU64::new(0),
            next_revision: AtomicU64::new(0),
            _reserved: [0; 32],
        }
    }
}

#[repr(C, align(64))]
struct LockStateSlot {
    state: AtomicU8,
    _reserved: u8,
    namespace_len: u16,
    key_len: u16,
    owner_len: u16,
    key_hash: u64,
    lock_id: u64,
    state_revision: u64,
    acquired_at_ms: u64,
    expires_at_ms: u64,
    payload: [u8; LOCK_STATE_PAYLOAD_MAX],
}

impl LockStateSlot {
    fn empty() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            _reserved: 0,
            namespace_len: 0,
            key_len: 0,
            owner_len: 0,
            key_hash: 0,
            lock_id: 0,
            state_revision: 0,
            acquired_at_ms: 0,
            expires_at_ms: 0,
            payload: [0; LOCK_STATE_PAYLOAD_MAX],
        }
    }

    fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    fn namespace_bytes(&self) -> &[u8] {
        &self.payload[..usize::from(self.namespace_len)]
    }

    fn key_bytes(&self) -> &[u8] {
        let start = usize::from(self.namespace_len);
        let end = start + usize::from(self.key_len);
        &self.payload[start..end]
    }

    fn owner(&self) -> &[u8] {
        let start = usize::from(self.namespace_len) + usize::from(self.key_len);
        let end = start + usize::from(self.owner_len);
        &self.payload[start..end]
    }

    fn matches(&self, hash: u64, key: &LockKey) -> bool {
        self.state() == SLOT_OCCUPIED
            && self.key_hash == hash
            && self.namespace_bytes() == key.namespace()
            && self.key_bytes() == key.label()
    }

    fn lease(&self) -> LockLease {
        LockLease {
            lock_id: NetId64::from_raw(self.lock_id),
            key: LockKey::from_parts(
                bytes::Bytes::copy_from_slice(self.namespace_bytes()),
                bytes::Bytes::copy_from_slice(self.key_bytes()),
            ),
            owner: LockOwner::from(bytes::Bytes::copy_from_slice(self.owner())),
            acquired_at_ms: self.acquired_at_ms,
            expires_at_ms: self.expires_at_ms,
            state_revision: self.state_revision,
        }
    }

    fn write(&mut self, hash: u64, lease: &LockLease) {
        self.state.store(SLOT_TOMBSTONE, Ordering::Release);
        self.namespace_len = lease.key.namespace().len() as u16;
        self.key_len = lease.key.label().len() as u16;
        self.owner_len = lease.owner.as_bytes().len() as u16;
        self.key_hash = hash;
        self.lock_id = lease.lock_id.raw();
        self.state_revision = lease.state_revision;
        self.acquired_at_ms = lease.acquired_at_ms;
        self.expires_at_ms = lease.expires_at_ms;
        self.payload.fill(0);
        let namespace_end = lease.key.namespace().len();
        self.payload[..namespace_end].copy_from_slice(lease.key.namespace());
        let key_end = namespace_end + lease.key.label().len();
        self.payload[namespace_end..key_end].copy_from_slice(lease.key.label());
        let owner_start = key_end;
        let owner_end = owner_start + lease.owner.as_bytes().len();
        self.payload[owner_start..owner_end].copy_from_slice(lease.owner.as_bytes());
        self.state.store(SLOT_OCCUPIED, Ordering::Release);
    }

    fn tombstone(&self) {
        self.state.store(SLOT_TOMBSTONE, Ordering::Release);
    }

    fn clear(&self) {
        self.state.store(SLOT_EMPTY, Ordering::Release);
    }
}

#[cfg(unix)]
struct ShmStateStore {
    name: String,
    opened: Mutex<Option<Arc<ShmLockTable>>>,
}

#[cfg(unix)]
impl ShmStateStore {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            opened: Mutex::new(None),
        }
    }

    fn open(&self) -> Result<Arc<ShmLockTable>> {
        let mut opened = lock_unpoisoned(&self.opened);
        if let Some(table) = opened.as_ref() {
            return Ok(table.clone());
        }
        let table = Arc::new(ShmLockTable::open_or_create(&self.name)?);
        *opened = Some(table.clone());
        Ok(table)
    }
}

#[cfg(unix)]
struct ShmLockTable {
    region: ShmRegion,
    local_lock: Mutex<()>,
}

#[cfg(unix)]
impl ShmLockTable {
    fn open_or_create(name: &str) -> Result<Self> {
        use std::io;
        use std::ptr;

        let (region, _initialization_lock) =
            ShmRegion::open_or_create_locked(name, shm_segment_size())?;
        if region.created() {
            unsafe {
                ptr::write(
                    region.as_ptr().cast::<LockStateHeader>(),
                    LockStateHeader::new(),
                );
                let slots = region.as_ptr().add(std::mem::size_of::<LockStateHeader>());
                ptr::write_bytes(
                    slots,
                    0,
                    LOCK_STATE_CAPACITY * std::mem::size_of::<LockStateSlot>(),
                );
            }
        } else {
            let header = unsafe { &*region.as_ptr().cast::<LockStateHeader>() };
            if header.magic != STATE_MAGIC {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {name} has wrong magic 0x{:08X} (expected 0x{STATE_MAGIC:08X})",
                        header.magic
                    ),
                )));
            }
            if header.version != STATE_VERSION
                || usize::from(header.header_size) != std::mem::size_of::<LockStateHeader>()
                || header.capacity as usize != LOCK_STATE_CAPACITY
                || header.slot_size as usize != std::mem::size_of::<LockStateSlot>()
            {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("SHM segment {name} has an incompatible lock-state layout"),
                )));
            }
        }

        Ok(Self {
            region,
            local_lock: Mutex::new(()),
        })
    }

    fn with_table<T>(
        &self,
        operation: impl FnOnce(&LockStateHeader, &mut [LockStateSlot]) -> Result<T>,
    ) -> std::io::Result<Result<T>> {
        let _local = lock_unpoisoned(&self.local_lock);
        let _process = self.region.lock_exclusive()?;
        let header = unsafe { &*self.region.as_ptr().cast::<LockStateHeader>() };
        let slots = unsafe {
            std::slice::from_raw_parts_mut(
                self.region
                    .as_ptr()
                    .add(std::mem::size_of::<LockStateHeader>())
                    .cast::<LockStateSlot>(),
                LOCK_STATE_CAPACITY,
            )
        };
        Ok(operation(header, slots))
    }

    fn unlink(&self) -> std::io::Result<()> {
        self.region.unlink()
    }
}

#[cfg(unix)]
fn shm_segment_size() -> usize {
    std::mem::size_of::<LockStateHeader>()
        + LOCK_STATE_CAPACITY * std::mem::size_of::<LockStateSlot>()
}

const _: () = assert!(LOCK_STATE_CAPACITY.is_power_of_two());
const _: () = assert!(std::mem::size_of::<LockStateHeader>() == 64);
const _: () = assert!(std::mem::size_of::<LockStateSlot>() == 1_024);

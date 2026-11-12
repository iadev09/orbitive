use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use lru::LruCache;

use crate::layout::CacheLayout;
use crate::protocol::{CacheMutation, CacheRevision};
use crate::transport::CacheTransport;

/// An immutable snapshot of the locally owned bytes and their revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    pub value: Bytes,
    pub revision: CacheRevision,
    pub expires_at_ms: Option<u64>,
}

impl CacheEntry {
    pub fn is_expired_at(
        &self,
        now_ms: u64,
    ) -> bool {
        self.expires_at_ms.is_some_and(|deadline| deadline <= now_ms)
    }
}

/// Honest result of consulting only this process' bounded L1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheRead {
    Hit(CacheEntry),
    /// The key is not currently resident. A higher layer may consult its
    /// backing store.
    Miss,
    /// Mutation history was lost or malformed. No local hit is served until
    /// a higher layer explicitly re-establishes a coherent snapshot.
    ResyncRequired,
}

/// One store's worker-local values, split by key into shards so readers of
/// different keys do not wait on one another. A key always maps to the same
/// shard, which therefore holds its revision floor: an eviction fences the
/// keys of its own shard, and a store-wide change applies to every shard.
#[derive(Clone)]
pub struct LocalCache {
    shards: Arc<[Mutex<LocalState>]>,
    /// `shards.len() - 1`; the count is a power of two.
    mask: usize,
    /// Entries this store keeps, across its shards.
    capacity: usize,
    /// Entries resident now, across its shards.
    resident: Arc<AtomicUsize>,
    /// Where the next capacity eviction looks first.
    victim: Arc<AtomicUsize>,
    budget: Arc<LocalBudget>,
    coherent: Arc<AtomicBool>,
}

struct LocalState {
    slots: LruCache<Vec<u8>, LocalSlot>,
    revision_floor: Option<u64>,
}

pub(crate) struct LocalBudget {
    pub(crate) limit: usize,
    used: AtomicUsize,
}

impl LocalBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self { limit, used: AtomicUsize::new(0) }
    }

    fn reserve(
        &self,
        len: usize,
    ) -> bool {
        // The connection serializes inserts. Readers can only lower the
        // charge as they expire a value.
        if len > self.limit - self.used.load(Ordering::Relaxed) {
            return false;
        }
        self.used.fetch_add(len, Ordering::Relaxed);
        true
    }
}

struct LocalValue {
    bytes: Bytes,
    budget: Arc<LocalBudget>,
}

impl Drop for LocalValue {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes.len(), Ordering::Relaxed);
    }
}

enum LocalSlot {
    Present {
        value: LocalValue,
        revision: CacheRevision,
        expires_at_ms: Option<u64>,
    },
    Missing {
        revision: CacheRevision,
    },
}

impl LocalSlot {
    fn revision(&self) -> CacheRevision {
        match self {
            Self::Present { revision, .. } => *revision,
            Self::Missing { revision } => *revision,
        }
    }
}

pub(crate) enum ApplyOutcome {
    Applied,
    Ignored,
    PayloadUnavailable { key: Bytes },
}

impl LocalCache {
    /// Construct a standalone local view. The arena argument is retained for
    /// source compatibility; resident values are owned by the view.
    pub fn new(
        capacity: NonZeroUsize,
        _arena: orbit_arena::Arena,
    ) -> Self {
        Self::with_connection(
            capacity,
            Arc::new(LocalBudget::new(orbit_arena::ARENA_BYTES)),
            Arc::new(AtomicBool::new(true)),
        )
    }

    pub(crate) fn with_connection(
        capacity: NonZeroUsize,
        budget: Arc<LocalBudget>,
        coherent: Arc<AtomicBool>,
    ) -> Self {
        let shards = shard_count(capacity);
        Self {
            shards: (0..shards)
                .map(|_| {
                    Mutex::new(LocalState {
                        slots: LruCache::unbounded(),
                        revision_floor: None,
                    })
                })
                .collect(),
            mask: shards - 1,
            capacity: capacity.get(),
            resident: Arc::new(AtomicUsize::new(0)),
            victim: Arc::new(AtomicUsize::new(0)),
            budget,
            coherent,
        }
    }

    fn shard_of(
        &self,
        key: &[u8],
    ) -> usize {
        if self.mask == 0 { 0 } else { key_hash(key) as usize & self.mask }
    }

    fn lock(
        &self,
        shard: usize,
    ) -> MutexGuard<'_, LocalState> {
        self.shards[shard].lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record a shard's change in resident entries.
    fn settle(
        &self,
        before: usize,
        after: usize,
    ) {
        if after > before {
            self.resident.fetch_add(after - before, Ordering::Relaxed);
        } else if before > after {
            self.resident.fetch_sub(before - after, Ordering::Relaxed);
        }
    }

    /// Evict least recently used entries, one shard at a time, while the store
    /// holds more than its capacity. It never holds two shard locks.
    fn trim(&self) {
        let mut empty = 0;
        while self.resident.load(Ordering::Relaxed) > self.capacity
            && empty < self.shards.len()
        {
            let shard = self.victim.fetch_add(1, Ordering::Relaxed) & self.mask;
            let mut state = self.lock(shard);
            let before = state.slots.len();
            match state.slots.pop_lru() {
                Some((_, evicted)) => {
                    empty = 0;
                    state.advance_floor(evicted.revision().sequence);
                }
                None => empty += 1,
            }
            let after = state.slots.len();
            drop(state);
            self.settle(before, after);
        }
    }

    /// Free value bytes in this store's shards other than `except`, for a
    /// writer that holds `except`. Only writers holding the connection's
    /// cursor call it, so two of them never wait on each other's shard.
    fn evict_sibling(
        &self,
        except: usize,
    ) -> bool {
        (0..self.shards.len()).filter(|&shard| shard != except).any(|shard| {
            let mut state = self.lock(shard);
            let before = state.slots.len();
            let evicted = state.evict_value();
            let after = state.slots.len();
            drop(state);
            self.settle(before, after);
            evicted
        })
    }

    pub fn read(
        &self,
        key: &[u8],
    ) -> CacheRead {
        self.read_with_clock(key, now_ms)
    }

    pub fn read_at(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> CacheRead {
        self.read_with_clock(key, || now_ms)
    }

    fn read_with_clock(
        &self,
        key: &[u8],
        now_ms: impl FnOnce() -> u64,
    ) -> CacheRead {
        let mut state = self.lock(self.shard_of(key));
        if !self.is_coherent() {
            return CacheRead::ResyncRequired;
        }
        let Some(slot) = state.slots.get_mut(key) else {
            return CacheRead::Miss;
        };
        match slot {
            LocalSlot::Present { value, revision, expires_at_ms } => {
                if !expires_at_ms.is_some_and(|deadline| deadline <= now_ms()) {
                    return CacheRead::Hit(CacheEntry {
                        value: value.bytes.clone(),
                        revision: *revision,
                        expires_at_ms: *expires_at_ms,
                    });
                }
                // Expiry is an honest miss; the revision tombstone
                // still prevents a delayed older Put from resurrecting it.
                *slot = LocalSlot::Missing { revision: *revision };
                CacheRead::Miss
            }
            LocalSlot::Missing { .. } => CacheRead::Miss,
        }
    }

    pub fn len(&self) -> usize {
        (0..self.shards.len())
            .map(|shard| {
                self.lock(shard)
                    .slots
                    .iter()
                    .filter(|(_, slot)| {
                        matches!(slot, LocalSlot::Present { .. })
                    })
                    .count()
            })
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_coherent(&self) -> bool {
        self.coherent.load(Ordering::Acquire)
    }

    /// Clear uncertain local state after transport loss.
    pub fn require_resync(&self) {
        self.coherent.store(false, Ordering::Release);
        // One floor for the store, as before it had shards: the newest
        // revision any shard holds.
        let newest = (0..self.shards.len())
            .filter_map(|shard| {
                self.lock(shard)
                    .slots
                    .iter()
                    .map(|(_, slot)| slot.revision())
                    .max()
            })
            .max();
        self.for_each_shard(|state| {
            if let Some(revision) = newest {
                state.advance_floor(revision.sequence);
            }
            state.slots.clear();
        });
    }

    pub(crate) fn recover_from_backing(
        &self,
        revision_floor: u64,
    ) {
        self.for_each_shard(|state| {
            state.slots.clear();
            state.revision_floor =
                Some(state.revision_floor.map_or(revision_floor, |current| {
                    current.max(revision_floor)
                }));
        });
    }

    pub(crate) fn reset_after_transport_reset(&self) {
        self.for_each_shard(|state| {
            state.slots.clear();
            state.revision_floor = None;
        });
    }

    /// Apply a store-wide change to every shard, one lock at a time.
    fn for_each_shard(
        &self,
        mut change: impl FnMut(&mut LocalState),
    ) {
        for shard in 0..self.shards.len() {
            let mut state = self.lock(shard);
            let before = state.slots.len();
            change(&mut state);
            let after = state.slots.len();
            drop(state);
            self.settle(before, after);
        }
    }

    pub(crate) fn apply<L: CacheLayout>(
        &self,
        transport: &CacheTransport<L>,
        mutation: CacheMutation,
        reclaim: impl FnMut() -> bool,
    ) -> ApplyOutcome {
        let revision = mutation.revision();
        if let CacheMutation::Reset { revision, .. } = mutation {
            if !self.is_coherent() {
                return ApplyOutcome::Ignored;
            }
            let mut applied = false;
            self.for_each_shard(|state| {
                if state.accepts(revision) {
                    state.advance_floor(revision.sequence);
                    applied = true;
                }
            });
            return if applied {
                ApplyOutcome::Applied
            } else {
                ApplyOutcome::Ignored
            };
        }
        let shard = match &mutation {
            CacheMutation::Put { key, .. }
            | CacheMutation::Delete { key, .. } => self.shard_of(key),
            CacheMutation::Reset { .. } => unreachable!("handled above"),
        };
        let outcome =
            self.apply_to(shard, transport, mutation, revision, reclaim);
        self.trim();
        outcome
    }

    fn apply_to<L: CacheLayout>(
        &self,
        shard: usize,
        transport: &CacheTransport<L>,
        mutation: CacheMutation,
        revision: CacheRevision,
        mut reclaim: impl FnMut() -> bool,
    ) -> ApplyOutcome {
        let mut state = self.lock(shard);
        if !self.is_coherent() || !state.accepts(revision) {
            return ApplyOutcome::Ignored;
        }
        let before = state.slots.len();
        let outcome = match mutation {
            CacheMutation::Put {
                store: _,
                key,
                revision,
                expires_at_ms,
                payload,
            } => {
                if !state.accepts_key(&key, revision) {
                    return ApplyOutcome::Ignored;
                }
                let entry =
                    transport.arena().get_address(payload.address).filter(
                        |entry| entry.body().len() as u64 == payload.value_len,
                    );
                let value = entry.as_ref().map(|entry| entry.body());
                let applied = state.put_value(
                    &self.budget,
                    &key,
                    value,
                    revision,
                    expires_at_ms,
                    || self.evict_sibling(shard) || reclaim(),
                );
                match (applied, entry.is_some()) {
                    (true, true) => ApplyOutcome::Applied,
                    (true, false) => ApplyOutcome::PayloadUnavailable { key },
                    (false, _) => ApplyOutcome::Ignored,
                }
            }
            CacheMutation::Delete { store: _, key, revision } => {
                if state.put_slot(&key, LocalSlot::Missing { revision }) {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::Ignored
                }
            }
            CacheMutation::Reset { .. } => unreachable!("handled by apply"),
        };
        let after = state.slots.len();
        drop(state);
        self.settle(before, after);
        outcome
    }

    pub(crate) fn install_local(
        &self,
        key: &[u8],
        value: &[u8],
        revision: CacheRevision,
        expires_at_ms: Option<u64>,
        mut reclaim: impl FnMut() -> bool,
    ) {
        let shard = self.shard_of(key);
        let mut state = self.lock(shard);
        let before = state.slots.len();
        if self.is_coherent()
            && state.accepts(revision)
            && state.accepts_key(key, revision)
        {
            state.put_value(
                &self.budget,
                key,
                Some(value),
                revision,
                expires_at_ms,
                || self.evict_sibling(shard) || reclaim(),
            );
        }
        let after = state.slots.len();
        drop(state);
        self.settle(before, after);
        self.trim();
    }

    pub(crate) fn evict_value(&self) -> bool {
        let start = self.victim.load(Ordering::Relaxed);
        (0..self.shards.len()).any(|offset| {
            let mut state = self.lock((start + offset) & self.mask);
            let before = state.slots.len();
            let evicted = state.evict_value();
            let after = state.slots.len();
            drop(state);
            self.settle(before, after);
            evicted
        })
    }

    pub(crate) fn install_missing(
        &self,
        key: &[u8],
        revision: CacheRevision,
    ) {
        let mut state = self.lock(self.shard_of(key));
        let before = state.slots.len();
        if self.is_coherent() && state.accepts(revision) {
            let _ = state.put_slot(key, LocalSlot::Missing { revision });
        }
        let after = state.slots.len();
        drop(state);
        self.settle(before, after);
        self.trim();
    }

    pub(crate) fn reset_local(
        &self,
        revision: CacheRevision,
    ) {
        if !self.is_coherent() {
            return;
        }
        self.for_each_shard(|state| {
            if state.accepts(revision) {
                state.advance_floor(revision.sequence);
            }
        });
    }
}

/// One shard per core the process may use, as a power of two, and never more
/// shards than the store holds entries. Read once, when the store opens.
fn shard_count(capacity: NonZeroUsize) -> usize {
    let cores =
        std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    let most = 1usize << (usize::BITS - 1 - capacity.get().leading_zeros());
    cores.next_power_of_two().min(most)
}

/// FNV-1a: cheap for the short keys a cache holds, and the same in every
/// process, though only this process's shards depend on it.
fn key_hash(key: &[u8]) -> u64 {
    key.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

impl LocalState {
    fn accepts(
        &self,
        revision: CacheRevision,
    ) -> bool {
        !self.revision_floor.is_some_and(|floor| revision.sequence <= floor)
    }

    /// Install `slot` under `key`. An existing key is replaced in place and
    /// keeps its allocation; only a new key is copied.
    fn put_slot(
        &mut self,
        key: &[u8],
        slot: LocalSlot,
    ) -> bool {
        if !self.accepts_key(key, slot.revision()) {
            return false;
        }
        if let Some(current) = self.slots.get_mut(key) {
            *current = slot;
            return true;
        }
        self.insert_slot(key.to_vec(), slot);
        true
    }

    /// Insert under a key known to be absent. A shard has no bound of its own;
    /// the store trims to its capacity after the insert.
    fn insert_slot(
        &mut self,
        key: Vec<u8>,
        slot: LocalSlot,
    ) {
        self.slots.push(key, slot);
    }

    fn accepts_key(
        &self,
        key: &[u8],
        revision: CacheRevision,
    ) -> bool {
        self.slots.peek(key).is_none_or(|current| current.revision() < revision)
    }

    fn put_value(
        &mut self,
        budget: &Arc<LocalBudget>,
        key: &[u8],
        value: Option<&[u8]>,
        revision: CacheRevision,
        expires_at_ms: Option<u64>,
        mut reclaim: impl FnMut() -> bool,
    ) -> bool {
        // Drop the old slot before charging its replacement, keeping its key
        // buffer. If the payload cannot be kept, record its revision as a miss.
        let key_buffer = self.slots.pop_entry(key).map(|(key, _)| key);
        let mut slot = LocalSlot::Missing { revision };
        if let Some(value) = value.filter(|value| value.len() <= budget.limit) {
            loop {
                if !self.accepts(revision) {
                    return false;
                }
                if budget.reserve(value.len()) {
                    slot = LocalSlot::Present {
                        value: LocalValue {
                            bytes: Bytes::copy_from_slice(value),
                            budget: budget.clone(),
                        },
                        revision,
                        expires_at_ms,
                    };
                    break;
                }
                if !self.evict_value() && !reclaim() {
                    break;
                }
            }
        }
        self.insert_slot(key_buffer.unwrap_or_else(|| key.to_vec()), slot);
        true
    }

    fn evict_value(&mut self) -> bool {
        let oldest = self.slots.iter().rev().find_map(|(_, slot)| match slot {
            LocalSlot::Present { revision, .. } => Some(revision.sequence),
            LocalSlot::Missing { .. } => None,
        });
        if let Some(floor) = oldest {
            self.advance_floor(floor);
        }
        oldest.is_some()
    }

    fn advance_floor(
        &mut self,
        revision: u64,
    ) {
        let floor = self
            .revision_floor
            .map_or(revision, |current| current.max(revision));
        self.revision_floor = Some(floor);
        let stale_keys = self
            .slots
            .iter()
            .filter(|(_, slot)| slot.revision().sequence <= floor)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in stale_keys {
            self.slots.pop(&key);
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_checks_the_clock_only_for_a_value_with_expiry() {
        let cache = LocalCache::with_connection(
            NonZeroUsize::new(2).unwrap(),
            Arc::new(LocalBudget::new(16)),
            Arc::new(AtomicBool::new(true)),
        );
        let revision = CacheRevision {
            sequence: 1,
            mutation_id: orbit_core::NetId64::ZERO,
        };
        cache.install_local(b"plain", b"value", revision, None, || false);
        cache.install_local(b"ttl", b"value", revision, Some(10), || false);

        let no_clock = || panic!("unexpected clock read");
        assert!(matches!(
            cache.read_with_clock(b"plain", no_clock),
            CacheRead::Hit(_)
        ));
        assert_eq!(cache.read_with_clock(b"absent", no_clock), CacheRead::Miss);
        assert!(matches!(cache.read_at(b"ttl", 9), CacheRead::Hit(_)));
        assert_eq!(cache.read_with_clock(b"ttl", || 10), CacheRead::Miss);
        assert_eq!(cache.read_with_clock(b"ttl", no_clock), CacheRead::Miss);
    }
}

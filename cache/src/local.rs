use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use lru::LruCache;

use crate::layout::CacheLayout;
use crate::protocol::{CacheMutation, CacheRevision};
use crate::transport::CacheTransport;

/// One worker-local value plus the mutation revision that installed it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    pub value: Bytes,
    pub revision: CacheRevision,
    pub expires_at_ms: Option<u64>,
}

impl CacheEntry {
    pub fn is_expired_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms
            .is_some_and(|deadline| deadline <= now_ms)
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

#[derive(Clone)]
pub struct LocalCache {
    inner: Arc<Mutex<LocalState>>,
}

struct LocalState {
    slots: LruCache<Vec<u8>, LocalSlot>,
    revision_floor: Option<u64>,
    coherent: bool,
}

#[derive(Clone)]
enum LocalSlot {
    Present(CacheEntry),
    Missing { revision: CacheRevision },
}

impl LocalSlot {
    fn revision(&self) -> CacheRevision {
        match self {
            Self::Present(entry) => entry.revision,
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
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LocalState {
                slots: LruCache::new(capacity),
                revision_floor: None,
                coherent: true,
            })),
        }
    }

    pub fn read(&self, key: &[u8]) -> CacheRead {
        self.read_at(key, now_ms())
    }

    pub fn read_at(&self, key: &[u8], now_ms: u64) -> CacheRead {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if !state.coherent {
            return CacheRead::ResyncRequired;
        }
        let Some(slot) = state.slots.get(key).cloned() else {
            return CacheRead::Miss;
        };
        match slot {
            LocalSlot::Present(entry) if !entry.is_expired_at(now_ms) => CacheRead::Hit(entry),
            LocalSlot::Present(entry) => {
                let _ = state.put_slot(
                    key.to_vec(),
                    LocalSlot::Missing {
                        revision: entry.revision,
                    },
                );
                CacheRead::Miss
            }
            LocalSlot::Missing { .. } => CacheRead::Miss,
        }
    }

    pub fn len(&self) -> usize {
        let state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state
            .slots
            .iter()
            .filter(|(_, slot)| matches!(slot, LocalSlot::Present(_)))
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_coherent(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .coherent
    }

    /// Clear uncertain local state after transport loss.
    pub fn require_resync(&self) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.advance_floor_from_slots();
        state.slots.clear();
        state.coherent = false;
    }

    pub(crate) fn recover_from_backing(&self, revision_floor: u64) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.slots.clear();
        state.revision_floor = Some(
            state
                .revision_floor
                .map_or(revision_floor, |current| current.max(revision_floor)),
        );
        state.coherent = true;
    }

    pub(crate) fn reset_after_transport_reset(&self) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.slots.clear();
        state.revision_floor = None;
        state.coherent = true;
    }

    pub(crate) fn apply<L: CacheLayout>(
        &self,
        transport: &CacheTransport<L>,
        mutation: CacheMutation,
    ) -> ApplyOutcome {
        // A value may span the complete payload lane. Copy it before taking
        // the L1 lock so local reads are not blocked on SHM traversal.
        let resolved_payload = match &mutation {
            CacheMutation::Put { payload, .. } => Some(transport.read_payload(*payload)),
            CacheMutation::Delete { .. } | CacheMutation::Reset { .. } => None,
        };
        let revision = mutation.revision();
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if !state.accepts(revision) {
            return ApplyOutcome::Ignored;
        }

        match mutation {
            CacheMutation::Put {
                store: _,
                key,
                revision,
                expires_at_ms,
                ..
            } => match resolved_payload.expect("put payload was resolved before L1 lock") {
                Some(value) => {
                    let applied = state.put_slot(
                        key.to_vec(),
                        LocalSlot::Present(CacheEntry {
                            value,
                            revision,
                            expires_at_ms,
                        }),
                    );
                    if applied {
                        ApplyOutcome::Applied
                    } else {
                        ApplyOutcome::Ignored
                    }
                }
                None => {
                    if state.put_slot(key.to_vec(), LocalSlot::Missing { revision }) {
                        ApplyOutcome::PayloadUnavailable { key }
                    } else {
                        ApplyOutcome::Ignored
                    }
                }
            },
            CacheMutation::Delete {
                store: _,
                key,
                revision,
            } => {
                if state.put_slot(key.to_vec(), LocalSlot::Missing { revision }) {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::Ignored
                }
            }
            CacheMutation::Reset { store: _, revision } => {
                state.advance_floor(revision.sequence);
                ApplyOutcome::Applied
            }
        }
    }

    pub(crate) fn install_local(
        &self,
        key: &[u8],
        value: Bytes,
        revision: CacheRevision,
        expires_at_ms: Option<u64>,
    ) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if state.accepts(revision) {
            let _ = state.put_slot(
                key.to_vec(),
                LocalSlot::Present(CacheEntry {
                    value,
                    revision,
                    expires_at_ms,
                }),
            );
        }
    }

    pub(crate) fn install_missing(&self, key: &[u8], revision: CacheRevision) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if state.accepts(revision) {
            let _ = state.put_slot(key.to_vec(), LocalSlot::Missing { revision });
        }
    }

    pub(crate) fn reset_local(&self, revision: CacheRevision) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if state.accepts(revision) {
            state.advance_floor(revision.sequence);
        }
    }
}

impl LocalState {
    fn accepts(&self, revision: CacheRevision) -> bool {
        if self
            .revision_floor
            .is_some_and(|floor| revision.sequence <= floor)
        {
            return false;
        }
        true
    }

    fn put_slot(&mut self, key: Vec<u8>, slot: LocalSlot) -> bool {
        if self
            .slots
            .peek(&key)
            .is_some_and(|current| current.revision() >= slot.revision())
        {
            return false;
        }
        let inserted_key = key.clone();
        if let Some((removed_key, removed)) = self.slots.push(key, slot)
            && removed_key != inserted_key
        {
            self.advance_floor(removed.revision().sequence);
        }
        self.slots.peek(&inserted_key).is_some()
    }

    fn advance_floor_from_slots(&mut self) {
        if let Some(revision) = self.slots.iter().map(|(_, slot)| slot.revision()).max() {
            self.advance_floor(revision.sequence);
        }
    }

    fn advance_floor(&mut self, revision: u64) {
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

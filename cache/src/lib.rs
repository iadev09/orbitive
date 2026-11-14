//! Fleet-coherent cache with worker-local values.
//!
//! One [`Cache`] is the process-local handle for one physical Orbit cache
//! connection: the fleet arena, a mutation ring, and one mutation cursor.
//! Logical [`Store`] handles share that connection while keeping independent,
//! bounded process-local L1 values. The arena carries payloads to listeners.
//! Hits clone the local value handle. Neither ring nor arena is a durable source.

mod error;
mod layout;
mod local;
#[cfg(unix)]
pub mod observer;
mod protocol;
mod shared;
mod transport;

#[cfg(test)]
#[path = "../tests/coherence/mod.rs"]
mod coherence;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use bytes::Bytes;
pub use error::{Error, Result};
pub use layout::{
    CACHE_MUTATION_RING_CAPACITY, CACHE_MUTATION_RING_KIND,
    CACHE_MUTATION_RING_PAYLOAD_CAPACITY, CACHE_MUTATION_RING_SPEC,
    CacheLayout, DefaultCacheLayout,
};
pub use local::{CacheEntry, CacheRead, LocalCache};
#[cfg(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "macos"
))]
pub use orbit_core::RingEventFd;
use orbit_core::{Fleet, RingLoss};
pub use protocol::{CacheMutation, CacheRevision, PayloadRef};
pub use shared::{SHARED_CACHE_KIND, SharedCache, SharedStore};
pub use transport::{CacheMutationCursor, CacheMutationPoll, CacheTransport};

pub const DEFAULT_L1_CAPACITY: usize = 10_000;
pub const DEFAULT_L1_BYTE_CAPACITY: usize = orbit_arena::ARENA_BYTES;

/// A logical cache entry address inside the shared transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheAddress {
    pub store: Bytes,
    pub key: Bytes,
}

/// Result of dispatching all currently visible mutations to local stores.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CachePoll {
    pub observed: usize,
    pub applied: usize,
    pub ignored: usize,
    pub payload_unavailable: Vec<CacheAddress>,
    /// Mutations for stores that this process has not registered.
    pub unknown_stores: Vec<Bytes>,
    pub loss: RingLoss,
    pub malformed: u64,
    pub resync_required: bool,
}

impl CachePoll {
    pub fn is_empty(&self) -> bool {
        self.observed == 0 && self.loss.is_empty() && self.malformed == 0
    }
}

/// One physical Orbit cache connection per process.
///
/// Clones share the transport, mutation cursor, and logical-store registry.
/// A single poll therefore drains the connection once and dispatches each
/// mutation to the addressed store.
pub struct Cache<L: CacheLayout = DefaultCacheLayout> {
    transport: CacheTransport<L>,
    stores: Arc<RwLock<HashMap<Vec<u8>, LocalCache>>>,
    cursor: Arc<Mutex<CacheMutationCursor>>,
    budget: Arc<local::LocalBudget>,
    coherent: Arc<AtomicBool>,
}

/// One named logical cache store with its own bounded process-local L1.
pub struct Store<L: CacheLayout = DefaultCacheLayout> {
    cache: Cache<L>,
    name: Bytes,
    local: LocalCache,
    /// Longest key this store's name leaves room for in a mutation frame.
    max_key_len: usize,
    /// Largest value both the arena and this connection's budget admit.
    max_value_len: usize,
}

impl<L: CacheLayout> Clone for Cache<L> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            stores: self.stores.clone(),
            cursor: self.cursor.clone(),
            budget: self.budget.clone(),
            coherent: self.coherent.clone(),
        }
    }
}

impl<L: CacheLayout> Clone for Store<L> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            name: self.name.clone(),
            local: self.local.clone(),
            max_key_len: self.max_key_len,
            max_value_len: self.max_value_len,
        }
    }
}

impl<L: CacheLayout> Cache<L> {
    /// Subscribe to future mutations on one physical cache connection.
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        Self::with_byte_capacity(fleet, DEFAULT_L1_BYTE_CAPACITY)
    }

    /// Subscribe at the current heads with a shared value-byte budget for
    /// this connection's stores. A zero budget admits only empty values.
    pub fn with_byte_capacity(
        fleet: Arc<Fleet>,
        value_bytes: usize,
    ) -> Result<Self> {
        let transport = CacheTransport::<L>::new(fleet)?;
        let cursor = transport.cursor_at_head();
        Ok(Self {
            transport,
            stores: Arc::new(RwLock::new(HashMap::new())),
            cursor: Arc::new(Mutex::new(cursor)),
            budget: Arc::new(local::LocalBudget::new(value_bytes)),
            coherent: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Replay mutation history still retained by the ring.
    ///
    /// This is useful for tests and diagnostics. Production cold starts
    /// open empty and follow future mutations.
    pub fn replay_retained(fleet: Arc<Fleet>) -> Result<Self> {
        let transport = CacheTransport::<L>::new(fleet)?;
        let cursor = transport.cursor_from_start();
        Ok(Self {
            transport,
            stores: Arc::new(RwLock::new(HashMap::new())),
            cursor: Arc::new(Mutex::new(cursor)),
            budget: Arc::new(local::LocalBudget::new(DEFAULT_L1_BYTE_CAPACITY)),
            coherent: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Open a logical store on this connection.
    ///
    /// Opening the same name more than once returns another handle to the
    /// existing local store. Its capacity is therefore fixed by the first
    /// registration in this process.
    pub fn open_store(
        &self,
        name: impl AsRef<[u8]>,
        capacity: NonZeroUsize,
    ) -> Result<Store<L>> {
        let name = name.as_ref();
        self.transport.validate_store(name)?;
        // Recovery must set the revision floor on every registered store.
        let _cursor =
            self.cursor.lock().unwrap_or_else(|error| error.into_inner());

        let local = {
            let stores =
                self.stores.read().unwrap_or_else(|error| error.into_inner());
            stores.get(name).cloned()
        }
        .unwrap_or_else(|| {
            let mut stores =
                self.stores.write().unwrap_or_else(|error| error.into_inner());
            stores
                .entry(name.to_vec())
                .or_insert_with(|| {
                    LocalCache::with_connection(
                        capacity,
                        self.budget.clone(),
                        self.coherent.clone(),
                    )
                })
                .clone()
        });

        // The name is validated above and fixed for the handle's life, so its
        // limits are settled here rather than on every operation.
        Ok(Store {
            cache: self.clone(),
            name: Bytes::copy_from_slice(name),
            local,
            max_key_len: self.transport.max_key_len(name),
            max_value_len: self
                .budget
                .limit
                .min(self.transport.max_value_len()),
        })
    }

    pub fn open_default_store(&self) -> Result<Store<L>> {
        self.open_store(
            b"default",
            NonZeroUsize::new(DEFAULT_L1_CAPACITY)
                .expect("default L1 capacity is non-zero"),
        )
    }

    /// Reset the shared transport during owner-controlled, quiescent boot and
    /// align every registered local store with the new ring generation.
    pub fn reset_transport(&self) -> Result<()> {
        let mut cursor =
            self.cursor.lock().unwrap_or_else(|error| error.into_inner());
        self.transport.reset_rings()?;
        *cursor = self.transport.cursor_at_head();
        self.for_each_local(LocalCache::reset_after_transport_reset);
        self.coherent.store(true, Ordering::Release);
        Ok(())
    }

    /// Drain every committed mutation currently visible and dispatch it to
    /// the addressed logical store.
    pub fn poll(&self) -> CachePoll {
        // Keep the cursor through application. Another drain or recovery
        // must not overtake an already-consumed batch.
        let mut cursor =
            self.cursor.lock().unwrap_or_else(|error| error.into_inner());
        let raw = self.transport.poll(&mut cursor);
        let observed = raw.mutations.len();
        if raw.requires_full_resync() {
            // Loss belongs to the shared connection. The missing mutation's
            // store is unknowable, so every registered store becomes unsafe.
            self.coherent.store(false, Ordering::Release);
            self.for_each_local(LocalCache::require_resync);
        }

        let mut result = CachePoll {
            observed,
            loss: raw.loss,
            malformed: raw.malformed,
            ..CachePoll::default()
        };
        // A registered name keeps its local store for the connection's life,
        // so a run of mutations for one store resolves it once.
        let mut resolved: Option<(Bytes, LocalCache)> = None;
        for mutation in raw.mutations {
            let store = mutation.store().clone();
            if resolved.as_ref().is_none_or(|(name, _)| *name != store) {
                resolved = self
                    .stores
                    .read()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(store.as_ref())
                    .map(|local| (store.clone(), local.clone()));
            }
            let Some((_, local)) = &resolved else {
                result.ignored += 1;
                if !result.unknown_stores.contains(&store) {
                    result.unknown_stores.push(store);
                }
                continue;
            };

            match local
                .apply(&self.transport, mutation, || self.reclaim_value(&store))
            {
                local::ApplyOutcome::Applied => result.applied += 1,
                local::ApplyOutcome::Ignored => result.ignored += 1,
                local::ApplyOutcome::PayloadUnavailable { key } => {
                    result
                        .payload_unavailable
                        .push(CacheAddress { store, key });
                }
            }
        }
        result.resync_required = !self.coherent.load(Ordering::Acquire);
        result
    }

    /// Discard uncertain local state and resume after the current mutation
    /// heads for the complete shared connection.
    ///
    /// Call this only when the embedding layer can satisfy ordinary misses
    /// from authoritative backing stores. Because the cursor is shared, loss
    /// and recovery apply to every registered logical store together.
    pub fn recover_from_backing(&self) -> Result<()> {
        let mut cursor =
            self.cursor.lock().unwrap_or_else(|error| error.into_inner());
        self.transport.with_recovery_cut(|head, revision_floor| {
            self.coherent.store(false, Ordering::Release);
            *cursor = head;
            self.for_each_local(|local| {
                local.recover_from_backing(revision_floor)
            });
            self.coherent.store(true, Ordering::Release);
        })
    }

    pub fn transport(&self) -> &CacheTransport<L> {
        &self.transport
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    pub fn event_fd(&self) -> Result<RingEventFd> {
        self.transport.event_fd()
    }

    fn for_each_local(
        &self,
        mut apply: impl FnMut(&LocalCache),
    ) {
        let locals = self
            .stores
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for local in &locals {
            apply(local);
        }
    }

    // Callers hold the connection cursor, so at most one update may lock
    // more than one local store to reclaim the shared byte budget.
    fn reclaim_value(
        &self,
        except: &[u8],
    ) -> bool {
        self.stores
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .any(|(name, local)| {
                name.as_slice() != except && local.evict_value()
            })
    }
}

impl<L: CacheLayout> Store<L> {
    pub fn name(&self) -> &Bytes {
        &self.name
    }

    pub fn read(
        &self,
        key: &[u8],
    ) -> CacheRead {
        self.local.read(key)
    }

    /// Read at an explicit Unix-millisecond instant (the same TTL boundary
    /// used by `read`). Useful to exercise expiry without sleeping.
    pub fn read_at(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> CacheRead {
        self.local.read_at(key, now_ms)
    }

    pub fn validate_put(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let max = self.max_value_len;
        if value.len() > max {
            return Err(Error::ValueTooLarge { value_len: value.len(), max });
        }
        self.validate_key(key)
    }

    pub fn validate_key(
        &self,
        key: &[u8],
    ) -> Result<()> {
        if key.is_empty() {
            return Err(Error::KeyEmpty);
        }
        let max = self.max_key_len;
        if key.len() > max {
            return Err(Error::KeyTooLarge { key_len: key.len(), max });
        }
        Ok(())
    }

    pub fn put(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<CacheRevision> {
        self.validate_put(key, value)?;
        // The local listener must not apply our own descriptor before the
        // writer can install the original bytes.
        let _cursor =
            self.cache.cursor.lock().unwrap_or_else(|error| error.into_inner());
        let (revision, expires_at_ms, _) = self
            .cache
            .transport
            .publish_validated_put(&self.name, key, value, ttl)?;
        self.local.install_local(key, value, revision, expires_at_ms, || {
            self.cache.reclaim_value(&self.name)
        });
        Ok(revision)
    }

    pub fn delete(
        &self,
        key: &[u8],
    ) -> Result<CacheRevision> {
        self.validate_key(key)?;
        let revision =
            self.cache.transport.publish_validated_delete(&self.name, key)?;
        self.local.install_missing(key, revision);
        Ok(revision)
    }

    /// Clear only this logical store.
    pub fn reset(&self) -> Result<CacheRevision> {
        let revision =
            self.cache.transport.publish_validated_reset(&self.name)?;
        self.local.reset_local(revision);
        Ok(revision)
    }

    /// The revision a backing read started now is current as of. Take it
    /// before the read and pass it to [`Store::fill`]: every mutation
    /// published up to it wrote its backing value first, and any later one is
    /// newer than the fill.
    pub fn fill_revision(&self) -> CacheRevision {
        CacheRevision {
            sequence: self.cache.transport.current_revision_sequence(),
            mutation_id: orbit_core::NetId64::ZERO,
        }
    }

    /// Keep `value`, read from a backing store after
    /// [`Store::fill_revision`], in this worker's view unless a mutation it
    /// has applied since makes it stale. A value this store could not hold,
    /// or one already expired, is not kept. Publishes nothing; returns whether
    /// the value is kept.
    pub fn fill(
        &self,
        key: &[u8],
        value: &[u8],
        revision: CacheRevision,
        expires_at_ms: Option<u64>,
    ) -> bool {
        if self.validate_put(key, value).is_err()
            || expires_at_ms
                .is_some_and(|deadline| deadline <= transport::now_ms())
        {
            return false;
        }
        self.local.install_fill(key, value, revision, expires_at_ms, || {
            self.cache.reclaim_value(&self.name)
        })
    }

    pub fn local(&self) -> &LocalCache {
        &self.local
    }
}

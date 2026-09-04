//! Fleet-coherent local cache over Orbit shared-memory rings.
//!
//! One [`Cache`] is the process-local handle for one physical Orbit cache
//! connection: a mutation ring, a payload ring, and one mutation cursor.
//! Logical [`Store`] handles share that connection while keeping independent,
//! bounded process-local L1 state. The rings transport cache mutations and
//! short-lived payload bytes; they are not the cache's durable value store.

mod error;
mod layout;
mod local;
mod protocol;
mod transport;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use bytes::Bytes;
use orbit_core::{Fleet, RingLoss};

pub use error::{Error, Result};
pub use layout::{
    CACHE_MUTATION_RING_KIND, CACHE_MUTATION_RING_SPEC, CACHE_PAYLOAD_RING_KIND,
    CACHE_PAYLOAD_RING_SPEC, CacheLayout, DefaultCacheLayout,
};
pub use local::{CacheEntry, CacheRead, LocalCache};
pub use protocol::{CacheMutation, CacheRevision, PayloadRef};
pub use transport::{CacheMutationCursor, CacheMutationPoll, CacheTransport};

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
pub use orbit_core::RingEventFd;

pub const DEFAULT_L1_CAPACITY: usize = 10_000;

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
}

/// One named logical cache store with its own bounded process-local L1.
pub struct Store<L: CacheLayout = DefaultCacheLayout> {
    cache: Cache<L>,
    name: Bytes,
    local: LocalCache,
}

impl<L: CacheLayout> Clone for Cache<L> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            stores: self.stores.clone(),
            cursor: self.cursor.clone(),
        }
    }
}

impl<L: CacheLayout> Clone for Store<L> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            name: self.name.clone(),
            local: self.local.clone(),
        }
    }
}

impl<L: CacheLayout> Cache<L> {
    /// Subscribe to future mutations on one physical cache connection.
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        let transport = CacheTransport::<L>::new(fleet)?;
        let cursor = transport.cursor_at_head();
        Ok(Self {
            transport,
            stores: Arc::new(RwLock::new(HashMap::new())),
            cursor: Arc::new(Mutex::new(cursor)),
        })
    }

    /// Replay mutation history still retained by the ring.
    ///
    /// This is useful for tests and diagnostics. Production cold starts
    /// normally hydrate registered stores from their backing stores and then
    /// follow future mutations.
    pub fn replay_retained(fleet: Arc<Fleet>) -> Result<Self> {
        let transport = CacheTransport::<L>::new(fleet)?;
        let cursor = transport.cursor_from_start();
        Ok(Self {
            transport,
            stores: Arc::new(RwLock::new(HashMap::new())),
            cursor: Arc::new(Mutex::new(cursor)),
        })
    }

    /// Open a logical store on this connection.
    ///
    /// Opening the same name more than once returns another handle to the
    /// existing local store. Its capacity is therefore fixed by the first
    /// registration in this process.
    pub fn open_store(&self, name: impl AsRef<[u8]>, capacity: NonZeroUsize) -> Result<Store<L>> {
        let name = name.as_ref();
        self.transport.validate_store(name)?;

        let local = {
            let stores = self
                .stores
                .read()
                .unwrap_or_else(|error| error.into_inner());
            stores.get(name).cloned()
        }
        .unwrap_or_else(|| {
            let mut stores = self
                .stores
                .write()
                .unwrap_or_else(|error| error.into_inner());
            stores
                .entry(name.to_vec())
                .or_insert_with(|| LocalCache::new(capacity))
                .clone()
        });

        Ok(Store {
            cache: self.clone(),
            name: Bytes::copy_from_slice(name),
            local,
        })
    }

    pub fn open_default_store(&self) -> Result<Store<L>> {
        self.open_store(
            b"default",
            NonZeroUsize::new(DEFAULT_L1_CAPACITY).expect("default L1 capacity is non-zero"),
        )
    }

    /// Reset the shared transport during owner-controlled, quiescent boot and
    /// align every registered local store with the new ring generation.
    pub fn reset_transport(&self) -> Result<()> {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.transport.reset_rings()?;
        *cursor = self.transport.cursor_at_head();
        self.for_each_local(LocalCache::reset_after_transport_reset);
        Ok(())
    }

    /// Drain every committed mutation currently visible and dispatch it to
    /// the addressed logical store.
    pub fn poll(&self) -> CachePoll {
        let raw = {
            let mut cursor = self
                .cursor
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.transport.poll(&mut cursor)
        };
        let observed = raw.mutations.len();
        if raw.requires_full_resync() {
            // Loss belongs to the shared connection. The missing mutation's
            // store is unknowable, so every registered store becomes unsafe.
            self.for_each_local(LocalCache::require_resync);
        }

        let mut result = CachePoll {
            observed,
            loss: raw.loss,
            malformed: raw.malformed,
            ..CachePoll::default()
        };
        for mutation in raw.mutations {
            let store = mutation.store().clone();
            let local = self
                .stores
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .get(store.as_ref())
                .cloned();
            let Some(local) = local else {
                result.ignored += 1;
                if !result.unknown_stores.contains(&store) {
                    result.unknown_stores.push(store);
                }
                continue;
            };

            match local.apply(&self.transport, mutation) {
                local::ApplyOutcome::Applied => result.applied += 1,
                local::ApplyOutcome::Ignored => result.ignored += 1,
                local::ApplyOutcome::PayloadUnavailable { key } => {
                    result.payload_unavailable.push(CacheAddress { store, key });
                }
            }
        }
        result.resync_required = self.any_local(|local| !local.is_coherent());
        result
    }

    /// Discard uncertain local state and resume after the current mutation
    /// heads for the complete shared connection.
    ///
    /// Call this only when the embedding layer can satisfy ordinary misses
    /// from authoritative backing stores. Because the cursor is shared, loss
    /// and recovery apply to every registered logical store together.
    pub fn recover_from_backing(&self) {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *cursor = self.transport.cursor_at_head();
        let revision_floor = self.transport.current_revision_sequence();
        self.for_each_local(|local| local.recover_from_backing(revision_floor));
    }

    pub fn transport(&self) -> &CacheTransport<L> {
        &self.transport
    }

    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    pub fn event_fd(&self) -> Result<RingEventFd> {
        self.transport.event_fd()
    }

    fn for_each_local(&self, mut apply: impl FnMut(&LocalCache)) {
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

    fn any_local(&self, mut predicate: impl FnMut(&LocalCache) -> bool) -> bool {
        self.stores
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .any(&mut predicate)
    }
}

impl<L: CacheLayout> Store<L> {
    pub fn name(&self) -> &Bytes {
        &self.name
    }

    pub fn read(&self, key: &[u8]) -> CacheRead {
        self.local.read(key)
    }

    pub fn validate_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.cache.transport.validate_put(&self.name, key, value)
    }

    pub fn validate_key(&self, key: &[u8]) -> Result<()> {
        self.cache.transport.validate_key(&self.name, key)
    }

    pub fn put(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<CacheRevision> {
        let mutation = self
            .cache
            .transport
            .publish_put(&self.name, key, value, ttl)?;
        let CacheMutation::Put {
            revision,
            expires_at_ms,
            ..
        } = mutation
        else {
            unreachable!("publish_put returned a non-put mutation")
        };
        self.local
            .install_local(key, Bytes::copy_from_slice(value), revision, expires_at_ms);
        Ok(revision)
    }

    pub fn delete(&self, key: &[u8]) -> Result<CacheRevision> {
        let mutation = self.cache.transport.publish_delete(&self.name, key)?;
        let CacheMutation::Delete { revision, .. } = mutation else {
            unreachable!("publish_delete returned a non-delete mutation")
        };
        self.local.install_missing(key, revision);
        Ok(revision)
    }

    /// Clear only this logical store.
    pub fn reset(&self) -> Result<CacheRevision> {
        let mutation = self.cache.transport.publish_reset(&self.name)?;
        let CacheMutation::Reset { revision, .. } = mutation else {
            unreachable!("publish_reset returned a non-reset mutation")
        };
        self.local.reset_local(revision);
        Ok(revision)
    }

    pub fn local(&self) -> &LocalCache {
        &self.local
    }
}

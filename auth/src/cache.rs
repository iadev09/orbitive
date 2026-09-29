use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;

use crate::Principal;

/// Opaque, keyed token fingerprint scoped to the keyring and validation context.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey(pub(crate) [u8; 32]);

impl CacheKey {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Successful authenticated snapshot, never a raw token. Constructed only by the validator.
#[derive(Clone, Debug)]
pub struct CachedPrincipal {
    pub(crate) key: CacheKey,
    pub(crate) principal: Arc<Principal>,
    pub(crate) cached_at: u64,
    pub(crate) valid_until: u64
}

impl CachedPrincipal {
    pub fn principal(&self) -> &Arc<Principal> {
        &self.principal
    }
    pub fn valid_until(&self) -> u64 {
        self.valid_until
    }
}

/// Trusted optimization only; misses and failures must return `None`.
///
/// Preserve entries as supplied and bound memory. No invalidation guarantee is required:
/// expiry and the current validation hook are checked on every hit. This interface does
/// not provide an atomic replay store. Concurrent cold misses may independently decrypt.
pub trait PrincipalCache {
    fn get(
        &self,
        key: &CacheKey
    ) -> Option<CachedPrincipal>;
    fn insert(
        &self,
        key: CacheKey,
        entry: CachedPrincipal
    );
}

/// Bounded process-local decoded-principal LRU. A hit only clones an `Arc`.
pub struct MemoryCache(Mutex<LruCache<CacheKey, CachedPrincipal>>);

impl MemoryCache {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self(Mutex::new(LruCache::new(capacity)))
    }

    pub fn clear(&self) {
        self.0.lock().unwrap_or_else(|error| error.into_inner()).clear();
    }
}

impl PrincipalCache for MemoryCache {
    fn get(
        &self,
        key: &CacheKey
    ) -> Option<CachedPrincipal> {
        self.0.lock().unwrap_or_else(|error| error.into_inner()).get(key).cloned()
    }

    fn insert(
        &self,
        key: CacheKey,
        entry: CachedPrincipal
    ) {
        self.0.lock().unwrap_or_else(|error| error.into_inner()).put(key, entry);
    }
}

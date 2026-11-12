use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use orbit_arena::{ARENA_RECORD_MAX, Arena, Key, Record};
use orbit_core::{Fleet, NetId64};

use crate::transport::{duration_ms, now_ms};
use crate::{CacheEntry, CacheRead, CacheRevision, Error, Result};

/// Dedicated keyed arena. Replicated payloads and file records use kind 193.
pub const SHARED_CACHE_KIND: u8 = 180;

/// One shared current-state table per fleet.
#[derive(Clone)]
pub struct SharedCache(Arena);

impl SharedCache {
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        Ok(Self(Arena::open_kind(fleet, SHARED_CACHE_KIND)?))
    }

    pub fn open_store(
        &self,
        name: &[u8],
    ) -> Result<SharedStore> {
        if name.is_empty() {
            return Err(Error::StoreEmpty);
        }
        let key_hash = blake3::derive_key("orbit shared cache store v1", name);
        let namespace = u64::from_le_bytes(key_hash[..8].try_into().unwrap());
        Ok(SharedStore { arena: self.0.clone(), namespace, key_hash })
    }

    /// Unlink only the dedicated cache arena during fleet teardown.
    pub fn unlink(&self) -> Result<()> {
        Ok(self.0.unlink()?)
    }
}

/// One namespace in the shared table. A read pins the exact immutable
/// generation, and may keep using it after replacement or invalidation.
#[derive(Clone)]
pub struct SharedStore {
    arena: Arena,
    namespace: u64,
    key_hash: [u8; 32],
}

impl SharedStore {
    pub fn read(
        &self,
        key: &[u8],
    ) -> CacheRead {
        self.read_with_clock(key, now_ms)
    }

    pub fn read_at(
        &self,
        key: &[u8],
        now: u64,
    ) -> CacheRead {
        self.read_with_clock(key, || now)
    }

    fn read_with_clock(
        &self,
        key: &[u8],
        now: impl FnOnce() -> u64,
    ) -> CacheRead {
        let Some(entry) = self.arena.get(self.key(key)) else {
            return CacheRead::Miss;
        };
        let expires_at_ms = (entry.stamp() != 0).then(|| entry.stamp());
        if expires_at_ms.is_some_and(|deadline| deadline <= now()) {
            return CacheRead::Miss;
        }
        CacheRead::Hit(CacheEntry {
            revision: CacheRevision {
                sequence: entry.generation(),
                mutation_id: NetId64::ZERO,
            },
            expires_at_ms,
            value: Bytes::from_owner(entry.into_body()),
        })
    }

    pub fn validate_key(
        &self,
        key: &[u8],
    ) -> Result<()> {
        if key.is_empty() {
            return Err(Error::KeyEmpty);
        }
        Ok(())
    }

    pub fn validate_put(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.validate_key(key)?;
        if value.len() > ARENA_RECORD_MAX {
            return Err(Error::ValueTooLarge {
                value_len: value.len(),
                max: ARENA_RECORD_MAX,
            });
        }
        Ok(())
    }

    pub fn put(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<()> {
        self.validate_put(key, value)?;
        let expires =
            ttl.map_or(0, |ttl| now_ms().saturating_add(duration_ms(ttl)));
        self.arena.put(
            self.key(key),
            Record::body(value).stamp(expires).extra(self.namespace),
        )?;
        Ok(())
    }

    pub fn delete(
        &self,
        key: &[u8],
    ) -> Result<()> {
        self.validate_key(key)?;
        self.arena.forget(self.key(key))?;
        Ok(())
    }

    pub fn reset(&self) -> Result<()> {
        Ok(self.arena.forget_namespace(self.namespace)?)
    }

    fn key(
        &self,
        key: &[u8],
    ) -> Key {
        let hash = blake3::keyed_hash(&self.key_hash, key);
        Key::from_bytes(hash.as_bytes()[..16].try_into().unwrap())
    }
}

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use orbit_arena::{ARENA_RECORD_MAX, Arena, Entry, Key, Record};
use orbit_core::{Fleet, NetId64};

use crate::floors::TagFloors;
pub use crate::floors::{SHARED_TAG_KIND, SHARED_TAG_SETS};
use crate::tags::TagKey;
use crate::transport::{duration_ms, now_ms};
use crate::{CacheEntry, CacheRead, CacheRevision, Error, Result};

/// Dedicated keyed arena. Replicated payloads and file records use kind 193.
/// Kind 180 held the same table before records carried tags.
pub const SHARED_CACHE_KIND: u8 = 172;

/// The arena encoding tag of a record's tag span:
/// `[table instance][clock][tag hash]...`, little-endian `u64`s.
const TAG_SPAN: u8 = 1;
const TAG_SPAN_HEAD: usize = 2 * size_of::<u64>();

/// One shared current-state table per fleet.
#[derive(Clone)]
pub struct SharedCache {
    arena: Arena,
    floors: Arc<TagFloors>,
}

impl SharedCache {
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        let floors = TagFloors::open(&fleet)?;
        Ok(Self { arena: Arena::open_kind(fleet, SHARED_CACHE_KIND)?, floors })
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
        Ok(SharedStore {
            arena: self.arena.clone(),
            floors: self.floors.clone(),
            namespace,
            key_hash,
            tag_key: TagKey::for_store(name),
        })
    }

    /// Unlink the dedicated cache arena and its tag floors during fleet
    /// teardown.
    pub fn unlink(&self) -> Result<()> {
        self.arena.unlink()?;
        #[cfg(unix)]
        self.floors.unlink()?;
        Ok(())
    }
}

/// One namespace in the shared table. A read pins the exact immutable
/// generation, and may keep using it after replacement or invalidation.
#[derive(Clone)]
pub struct SharedStore {
    arena: Arena,
    floors: Arc<TagFloors>,
    namespace: u64,
    key_hash: [u8; 32],
    tag_key: TagKey,
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
        // An untagged record has no second span; the pin loaded that.
        if entry.encoding().is_some() && !self.tags_admit(&entry) {
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

    /// Validate a tagged put: the key as [`SharedStore::validate_key`], at
    /// most [`crate::MAX_TAGS`] tags, and room for the value beside them in
    /// one arena record.
    pub fn validate_put_tagged<T: AsRef<[u8]>>(
        &self,
        key: &[u8],
        value: &[u8],
        tags: &[T],
    ) -> Result<()> {
        self.put_tags(key, value, tags).map(|_| ())
    }

    fn put_tags<T: AsRef<[u8]>>(
        &self,
        key: &[u8],
        value: &[u8],
        tags: &[T],
    ) -> Result<Box<[u64]>> {
        self.validate_key(key)?;
        let tags = self.tag_key.hash_all(tags)?;
        let span = if tags.is_empty() {
            0
        } else {
            TAG_SPAN_HEAD + tags.len() * size_of::<u64>()
        };
        let max = ARENA_RECORD_MAX - span;
        if value.len() > max {
            return Err(Error::ValueTooLarge { value_len: value.len(), max });
        }
        Ok(tags)
    }

    pub fn put(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<()> {
        self.validate_put(key, value)?;
        self.arena.put(self.key(key), self.record(value, ttl))?;
        Ok(())
    }

    /// Put a value that [`SharedStore::invalidate_tags`] of any of `tags`
    /// makes a miss for every reader. An empty set is an untagged put.
    pub fn put_tagged<T: AsRef<[u8]>>(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
        tags: &[T],
    ) -> Result<()> {
        let tags = self.put_tags(key, value, tags)?;
        if tags.is_empty() {
            return self.put(key, value, ttl);
        }
        // Read before the record is written: an invalidation that advances
        // the clock after this covers the record.
        let clock = self.floors.clock();
        let mut span = [0; TAG_SPAN_HEAD + crate::MAX_TAGS * size_of::<u64>()];
        let words = [self.floors.instance(), clock].into_iter().chain(tags);
        let mut len = 0;
        for word in words {
            span[len..len + size_of::<u64>()]
                .copy_from_slice(&word.to_le_bytes());
            len += size_of::<u64>();
        }
        let span = &span[..len];
        self.arena.put(
            self.key(key),
            self.record(value, ttl).encoded(span, TAG_SPAN),
        )?;
        Ok(())
    }

    /// Make every record carrying any of `tags`, written before this call,
    /// a miss for every reader of this store. One update of the shared tag
    /// floors; no record is visited.
    pub fn invalidate_tags<T: AsRef<[u8]>>(
        &self,
        tags: &[T],
    ) -> Result<()> {
        let tags = self.tag_key.hash_all(tags)?;
        if tags.is_empty() {
            return Err(Error::TagsEmpty);
        }
        self.floors.invalidate(&tags)
    }

    fn record<'a>(
        &self,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> Record<'a> {
        let expires =
            ttl.map_or(0, |ttl| now_ms().saturating_add(duration_ms(ttl)));
        Record::body(value).stamp(expires).extra(self.namespace)
    }

    /// Whether a tagged record is newer than every invalidation of its tags.
    /// A span this table did not write, or a table instance it was not
    /// written under, is a miss.
    fn tags_admit(
        &self,
        entry: &Entry,
    ) -> bool {
        let Some((span, TAG_SPAN)) = entry.encoded() else { return false };
        let word = |at: usize| {
            u64::from_le_bytes(
                span[at..at + size_of::<u64>()].try_into().unwrap(),
            )
        };
        span.len() >= TAG_SPAN_HEAD
            && span.len() % size_of::<u64>() == 0
            && word(0) == self.floors.instance()
            && self.floors.admits(
                word(size_of::<u64>()),
                (TAG_SPAN_HEAD..span.len()).step_by(size_of::<u64>()).map(word),
            )
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

#[cfg(all(test, unix))]
mod tests {
    use orbit_core::NodeId;

    use super::*;

    #[test]
    fn a_recreated_tag_table_never_vouches_for_records_it_did_not_judge() {
        let name = format!("ctag{}", std::process::id());
        let fleet = Arc::new(
            Fleet::join_shm_as(
                Box::leak(name.into_boxed_str()),
                1,
                NodeId::new(0),
            )
            .unwrap(),
        );
        let cache = SharedCache::new(fleet.clone()).unwrap();
        let store = cache.open_store(b"models").unwrap();
        store.put_tagged(b"tagged", b"t", None, &["users"]).unwrap();
        store.put(b"plain", b"p", None).unwrap();
        store.invalidate_tags(&["users"]).unwrap();
        store.put_tagged(b"after", b"a", None, &["users"]).unwrap();
        assert!(matches!(store.read(b"after"), CacheRead::Hit(_)));

        // The floors go while the records stay: the invalidation they held
        // is gone, so no record tagged under them may be trusted again.
        cache.floors.unlink().unwrap();
        drop((store, cache));
        let cache = SharedCache::new(fleet).unwrap();
        let store = cache.open_store(b"models").unwrap();
        assert_eq!(store.read(b"tagged"), CacheRead::Miss);
        assert_eq!(store.read(b"after"), CacheRead::Miss);
        assert!(matches!(store.read(b"plain"), CacheRead::Hit(_)));
        cache.unlink().unwrap();
    }
}

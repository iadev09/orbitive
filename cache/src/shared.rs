use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use orbit_arena::{ARENA_RECORD_MAX, Arena, Entry, Key, Record};
use orbit_core::{Fleet, NetId64};

use crate::floors::TagFloors;
pub use crate::floors::{SHARED_TAG_KIND, SHARED_TAG_SETS};
use crate::tags::{TagHashes, TagKey};
use crate::transport::{duration_ms, now_ms};
use crate::{
    CacheEntry, CacheRead, CacheRevision, Error, Fill, FillToken, Result,
};

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
    ) -> Result<TagHashes> {
        self.validate_key(key)?;
        let tags = self.tag_key.hash_set(tags)?;
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
        self.arena.put(self.key(key), self.record(value, expires(ttl)))?;
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
        self.write_tagged(key, value, expires(ttl), &tags, self.floors.clock())
    }

    /// This store's position on the tag clock, for [`SharedStore::fill_tagged`].
    /// Take it before reading the authoritative source. One atomic load;
    /// it publishes nothing.
    pub fn fill_token(&self) -> FillToken {
        FillToken::from_parts(self.floors.instance(), self.floors.clock())
    }

    /// Keep `value`, read from an authoritative source after `token` was
    /// taken, under `tags`, for every reader, unless an invalidation of one
    /// of them is newer than the token.
    ///
    /// The record is stamped with the token, not with the clock at this
    /// call, so an invalidation between the token and this call refuses it
    /// even when it lands first: it is refused here when its floor is
    /// already visible ([`Fill::Invalidated`], nothing written), and reads as
    /// a miss when the floor is raised after the record is written. One at
    /// or below the token does neither. `expires_at_ms` is a Unix-millisecond
    /// deadline; a value already past it is [`Fill::Skipped`].
    ///
    /// The fence orders the fill against tag invalidations only. Like a put,
    /// a fill replaces whatever the key holds: a concurrent `put` or `delete`
    /// of the same key is not ordered against it. A writer that changes the
    /// source invalidates a tag the fill carries. An empty set is an untagged
    /// fill, with nothing to fence.
    pub fn fill_tagged<T: AsRef<[u8]>>(
        &self,
        token: FillToken,
        key: &[u8],
        value: &[u8],
        expires_at_ms: Option<u64>,
        tags: &[T],
    ) -> Result<Fill> {
        let tags = self.put_tags(key, value, tags)?;
        let instance = self.floors.instance();
        if token.source() != instance {
            return Ok(Fill::Invalidated);
        }
        if expires_at_ms.is_some_and(|deadline| deadline <= now_ms()) {
            return Ok(Fill::Skipped);
        }
        if tags.is_empty() {
            let record = self.record(value, expires_at_ms.unwrap_or(0));
            self.arena.put(self.key(key), record)?;
            return Ok(Fill::Kept);
        }
        // Not needed for correctness, since the stamp decides at read time,
        // but a record already dead would only displace live ones.
        if !self.floors.admits(token.clock(), tags.iter().copied()) {
            return Ok(Fill::Invalidated);
        }
        self.write_tagged(
            key,
            value,
            expires_at_ms.unwrap_or(0),
            &tags,
            token.clock(),
        )?;
        Ok(Fill::Kept)
    }

    /// Write a record carrying `tags`, live while `clock` is at or above
    /// each of their floors.
    fn write_tagged(
        &self,
        key: &[u8],
        value: &[u8],
        expires: u64,
        tags: &[u64],
        clock: u64,
    ) -> Result<()> {
        let mut span = [0; TAG_SPAN_HEAD + crate::MAX_TAGS * size_of::<u64>()];
        let words = [self.floors.instance(), clock]
            .into_iter()
            .chain(tags.iter().copied());
        let mut len = 0;
        for word in words {
            span[len..len + size_of::<u64>()]
                .copy_from_slice(&word.to_le_bytes());
            len += size_of::<u64>();
        }
        let span = &span[..len];
        self.arena.put(
            self.key(key),
            self.record(value, expires).encoded(span, TAG_SPAN),
        )?;
        Ok(())
    }

    /// Make every record carrying any of `tags`, written before this call,
    /// a miss for every reader of this store. One update of the shared tag
    /// floors; no record is visited. Returns the token at the clock value
    /// this invalidation took: a fill with it is ordered after this
    /// invalidation and before any later one.
    pub fn invalidate_tags<T: AsRef<[u8]>>(
        &self,
        tags: &[T],
    ) -> Result<FillToken> {
        let tags = self.tag_key.hash_set(tags)?;
        if tags.is_empty() {
            return Err(Error::TagsEmpty);
        }
        let clock = self.floors.invalidate(&tags)?;
        Ok(FillToken::from_parts(self.floors.instance(), clock))
    }

    fn record<'a>(
        &self,
        value: &'a [u8],
        expires: u64,
    ) -> Record<'a> {
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

/// A record's expiry stamp: zero for none.
fn expires(ttl: Option<Duration>) -> u64 {
    ttl.map_or(0, |ttl| now_ms().saturating_add(duration_ms(ttl)))
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

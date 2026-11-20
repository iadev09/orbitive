//! Tag hashes, shared by both storage strategies.

use std::ops::Deref;

use crate::{Error, Result};

/// The most tags one entry carries, and one invalidation names.
pub const MAX_TAGS: usize = 16;

/// Hashes a store's tags. Keyed by the store name, so one tag in two stores
/// is two tags; fixed for the store handle's life.
#[derive(Clone)]
pub(crate) struct TagKey([u8; 32]);

/// A sorted, distinct set of tag hashes, held without allocating.
pub(crate) struct TagHashes {
    len: usize,
    hashes: [u64; MAX_TAGS],
}

impl Deref for TagHashes {
    type Target = [u64];

    fn deref(&self) -> &[u64] {
        &self.hashes[..self.len]
    }
}

impl TagKey {
    pub(crate) fn for_store(store: &[u8]) -> Self {
        Self(blake3::derive_key("orbit cache tag v1", store))
    }

    /// The sorted, distinct hashes of `tags`. A collision makes two tags
    /// one, which only invalidates more. Zero is never a hash, so a shared
    /// table can use it for an empty way.
    pub(crate) fn hash_set<T: AsRef<[u8]>>(
        &self,
        tags: &[T],
    ) -> Result<TagHashes> {
        if tags.len() > MAX_TAGS {
            return Err(Error::TooManyTags {
                count: tags.len(),
                max: MAX_TAGS,
            });
        }
        let mut hashes = [0; MAX_TAGS];
        for (slot, tag) in hashes.iter_mut().zip(tags) {
            let hash = blake3::keyed_hash(&self.0, tag.as_ref());
            *slot =
                u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap())
                    .max(1);
        }
        let held = &mut hashes[..tags.len()];
        held.sort_unstable();
        let mut len = 0;
        for at in 0..held.len() {
            if len == 0 || held[len - 1] != held[at] {
                held[len] = held[at];
                len += 1;
            }
        }
        Ok(TagHashes { len, hashes })
    }

    /// [`TagKey::hash_set`], owned, for a value that keeps its tags.
    pub(crate) fn hash_all<T: AsRef<[u8]>>(
        &self,
        tags: &[T],
    ) -> Result<Box<[u64]>> {
        Ok(self.hash_set(tags)?.into())
    }
}

impl From<TagHashes> for Box<[u64]> {
    fn from(set: TagHashes) -> Self {
        set.hashes[..set.len].into()
    }
}

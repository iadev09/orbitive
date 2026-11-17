//! Tag hashes, shared by both storage strategies.

use crate::{Error, Result};

/// The most tags one entry carries, and one invalidation names.
pub const MAX_TAGS: usize = 16;

/// Hashes a store's tags. Keyed by the store name, so one tag in two stores
/// is two tags; fixed for the store handle's life.
#[derive(Clone)]
pub(crate) struct TagKey([u8; 32]);

impl TagKey {
    pub(crate) fn for_store(store: &[u8]) -> Self {
        Self(blake3::derive_key("orbit cache tag v1", store))
    }

    /// The sorted, distinct hashes of `tags`. A collision makes two tags
    /// one, which only invalidates more. Zero is never a hash, so a shared
    /// table can use it for an empty way.
    pub(crate) fn hash_all<T: AsRef<[u8]>>(
        &self,
        tags: &[T],
    ) -> Result<Box<[u64]>> {
        if tags.len() > MAX_TAGS {
            return Err(Error::TooManyTags {
                count: tags.len(),
                max: MAX_TAGS,
            });
        }
        let mut hashes = tags
            .iter()
            .map(|tag| {
                let hash = blake3::keyed_hash(&self.0, tag.as_ref());
                u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap())
                    .max(1)
            })
            .collect::<Vec<_>>();
        hashes.sort_unstable();
        hashes.dedup();
        Ok(hashes.into_boxed_slice())
    }
}

use std::cmp::Ordering;

use bytes::{BufMut, Bytes, BytesMut};
use orbit_core::{Frame, NetId64};

use crate::layout::CacheLayout;

const PROTOCOL_VERSION: u8 = 4;
pub(crate) const FRAME_KIND_PUT: u8 = 1;
pub(crate) const FRAME_KIND_DELETE: u8 = 2;
pub(crate) const FRAME_KIND_RESET: u8 = 3;
pub(crate) const FRAME_KIND_INVALIDATE_TAGS: u8 = 4;

// version + store_len + key_len + tag_count + expires_at + arena slot
// + generation + len
pub(crate) const MIN_MUTATION_HEADER_LEN: usize = 1 + 2 + 2 + 1 + 8 + 4 + 8 + 8;
/// Bytes one tag takes in a frame.
pub(crate) const TAG_LEN: usize = size_of::<u64>();

/// Deterministic last-write-wins order for cache mutations from independent
/// node lanes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CacheRevision {
    pub sequence: u64,
    pub mutation_id: NetId64,
}

impl Ord for CacheRevision {
    fn cmp(
        &self,
        other: &Self,
    ) -> Ordering {
        self.sequence
            .cmp(&other.sequence)
            .then_with(|| self.mutation_id.raw().cmp(&other.mutation_id.raw()))
    }
}

impl PartialOrd for CacheRevision {
    fn partial_cmp(
        &self,
        other: &Self,
    ) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Exact immutable body in the fleet arena. This descriptor owns no pin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayloadRef {
    pub address: orbit_arena::Address,
    pub value_len: u64,
}

/// One cache fact decoded from the mutation ring.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheMutation {
    Put {
        store: Bytes,
        key: Bytes,
        revision: CacheRevision,
        expires_at_ms: Option<u64>,
        payload: PayloadRef,
        /// Tag hashes, sorted and distinct; empty for an untagged put.
        tags: Box<[u64]>,
    },
    Delete {
        store: Bytes,
        key: Bytes,
        revision: CacheRevision,
    },
    Reset {
        store: Bytes,
        revision: CacheRevision,
    },
    /// Every entry carrying one of these tag hashes, at or below the
    /// revision, becomes a miss.
    InvalidateTags {
        store: Bytes,
        tags: Box<[u64]>,
        revision: CacheRevision,
    },
}

impl CacheMutation {
    pub fn store(&self) -> &Bytes {
        match self {
            Self::Put { store, .. }
            | Self::Delete { store, .. }
            | Self::Reset { store, .. }
            | Self::InvalidateTags { store, .. } => store,
        }
    }

    pub fn revision(&self) -> CacheRevision {
        match self {
            Self::Put { revision, .. }
            | Self::Delete { revision, .. }
            | Self::Reset { revision, .. }
            | Self::InvalidateTags { revision, .. } => *revision,
        }
    }
}

pub(crate) fn max_store_len<L: CacheLayout>() -> usize {
    L::MUTATION_RING_SPEC
        .payload_capacity
        // Store validation is shared by Put/Delete/Reset. Reserve one byte so
        // every accepted store name can also address a non-empty key.
        .saturating_sub(MIN_MUTATION_HEADER_LEN + 1)
        .min(u16::MAX as usize)
}

pub(crate) fn max_key_len<L: CacheLayout>(store_len: usize) -> usize {
    L::MUTATION_RING_SPEC
        .payload_capacity
        .saturating_sub(MIN_MUTATION_HEADER_LEN)
        .saturating_sub(store_len)
        .min(u16::MAX as usize)
}

/// The most tags a frame for `store` and a key of `key_len` has room for,
/// within [`crate::MAX_TAGS`].
pub(crate) fn max_tags<L: CacheLayout>(
    store_len: usize,
    key_len: usize,
) -> usize {
    (max_key_len::<L>(store_len).saturating_sub(key_len) / TAG_LEN)
        .min(crate::MAX_TAGS)
}

struct Header {
    kind: u8,
    expires_at_ms: u64,
    payload: PayloadRef,
}

fn encode<L: CacheLayout>(
    header: Header,
    store: &[u8],
    key: &[u8],
    tags: &[u64],
) -> Bytes {
    debug_assert!(!store.is_empty() && store.len() <= max_store_len::<L>());
    debug_assert!(key.len() <= max_key_len::<L>(store.len()));
    debug_assert!(tags.len() <= max_tags::<L>(store.len(), key.len()));
    debug_assert_eq!(
        key.is_empty(),
        matches!(header.kind, FRAME_KIND_RESET | FRAME_KIND_INVALIDATE_TAGS)
    );
    let mut out = BytesMut::with_capacity(
        MIN_MUTATION_HEADER_LEN
            + store.len()
            + key.len()
            + tags.len() * TAG_LEN,
    );
    out.put_u8(PROTOCOL_VERSION);
    out.put_u16_le(store.len() as u16);
    out.put_u16_le(key.len() as u16);
    out.put_u8(tags.len() as u8);
    out.put_u64_le(header.expires_at_ms);
    out.put_u32_le(header.payload.address.slot);
    out.put_u64_le(header.payload.address.generation);
    out.put_u64_le(header.payload.value_len);
    out.put_slice(store);
    out.put_slice(key);
    for tag in tags {
        out.put_u64_le(*tag);
    }
    out.freeze()
}

const NO_PAYLOAD: PayloadRef = PayloadRef {
    address: orbit_arena::Address { slot: 0, generation: 0 },
    value_len: 0,
};

pub(crate) fn encode_put<L: CacheLayout>(
    store: &[u8],
    key: &[u8],
    expires_at_ms: Option<u64>,
    payload: PayloadRef,
    tags: &[u64],
) -> Bytes {
    let header = Header {
        kind: FRAME_KIND_PUT,
        expires_at_ms: expires_at_ms.unwrap_or(0),
        payload,
    };
    encode::<L>(header, store, key, tags)
}

pub(crate) fn encode_delete<L: CacheLayout>(
    store: &[u8],
    key: &[u8],
) -> Bytes {
    let header = Header {
        kind: FRAME_KIND_DELETE,
        expires_at_ms: 0,
        payload: NO_PAYLOAD,
    };
    encode::<L>(header, store, key, &[])
}

pub(crate) fn encode_reset<L: CacheLayout>(store: &[u8]) -> Bytes {
    let header = Header {
        kind: FRAME_KIND_RESET,
        expires_at_ms: 0,
        payload: NO_PAYLOAD,
    };
    encode::<L>(header, store, &[], &[])
}

pub(crate) fn encode_invalidate_tags<L: CacheLayout>(
    store: &[u8],
    tags: &[u64],
) -> Bytes {
    debug_assert!(!tags.is_empty());
    let header = Header {
        kind: FRAME_KIND_INVALIDATE_TAGS,
        expires_at_ms: 0,
        payload: NO_PAYLOAD,
    };
    encode::<L>(header, store, &[], tags)
}

pub(crate) fn decode(frame: &Frame) -> Option<CacheMutation> {
    decode_with_slot_limit::<{ orbit_arena::ARENA_SLOTS }>(frame)
}

/// Diagnostic readers don't share a producer's compiled capacity profile.
/// Their arena reader validates addresses against the actual mapped header.
pub(crate) fn decode_observed(frame: &Frame) -> Option<CacheMutation> {
    decode_with_slot_limit::<{ usize::MAX }>(frame)
}

#[inline]
fn decode_with_slot_limit<const SLOTS: usize>(
    frame: &Frame
) -> Option<CacheMutation> {
    if frame.payload.len() < MIN_MUTATION_HEADER_LEN
        || frame.payload[0] != PROTOCOL_VERSION
    {
        return None;
    }

    let field = |at: usize| -> Option<u64> {
        Some(u64::from_le_bytes(
            frame.payload.get(at..at + 8)?.try_into().ok()?,
        ))
    };
    let store_len =
        u16::from_le_bytes(frame.payload[1..3].try_into().ok()?) as usize;
    let key_len =
        u16::from_le_bytes(frame.payload[3..5].try_into().ok()?) as usize;
    let tag_count = usize::from(frame.payload[5]);
    let expires_at_ms = field(6)?;
    let slot = u32::from_le_bytes(frame.payload[14..18].try_into().ok()?);
    let generation = field(18)?;
    let value_len = field(26)?;
    let store_end = MIN_MUTATION_HEADER_LEN.checked_add(store_len)?;
    let key_end = store_end.checked_add(key_len)?;
    let tags_end = key_end.checked_add(tag_count.checked_mul(TAG_LEN)?)?;
    if tags_end != frame.payload.len() || tag_count > crate::MAX_TAGS {
        return None;
    }

    let revision = CacheRevision { sequence: frame.ver, mutation_id: frame.id };
    let store = frame.payload.slice(MIN_MUTATION_HEADER_LEN..store_end);
    let key = frame.payload.slice(store_end..key_end);
    let tags = || -> Option<Box<[u64]>> {
        let tags = frame.payload[key_end..tags_end]
            .as_chunks::<TAG_LEN>()
            .0
            .iter()
            .map(|tag| u64::from_le_bytes(*tag))
            .collect::<Box<[u64]>>();
        // Encoders send a sorted, distinct set; anything else is not one.
        tags.windows(2).all(|pair| pair[0] < pair[1]).then_some(tags)
    };
    let bare =
        expires_at_ms == 0 && slot == 0 && generation == 0 && value_len == 0;
    match frame.kind {
        FRAME_KIND_PUT
            if store_len > 0
                && key_len > 0
                && generation > 0
                && (slot as usize) < SLOTS
                && value_len <= usize::MAX as u64 =>
        {
            Some(CacheMutation::Put {
                store,
                key,
                revision,
                expires_at_ms: (expires_at_ms != 0).then_some(expires_at_ms),
                payload: PayloadRef {
                    address: orbit_arena::Address { slot, generation },
                    value_len,
                },
                tags: tags()?,
            })
        }
        FRAME_KIND_DELETE
            if store_len > 0 && key_len > 0 && tag_count == 0 && bare =>
        {
            Some(CacheMutation::Delete { store, key, revision })
        }
        FRAME_KIND_RESET
            if store_len > 0 && key_len == 0 && tag_count == 0 && bare =>
        {
            Some(CacheMutation::Reset { store, revision })
        }
        FRAME_KIND_INVALIDATE_TAGS
            if store_len > 0 && key_len == 0 && tag_count > 0 && bare =>
        {
            Some(CacheMutation::InvalidateTags {
                store,
                tags: tags()?,
                revision,
            })
        }
        _ => None,
    }
}

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use orbit_core::fleet::FleetLaneCursor;
use orbit_core::{Fleet, NetId64, RingLoss};

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use orbit_core::RingEventFd;

use crate::layout::{CacheLayout, MutationRecord, PayloadRecord};
use crate::protocol::{
    self, CacheMutation, CacheRevision, FRAME_KIND_DELETE, FRAME_KIND_PAYLOAD, FRAME_KIND_PUT,
    FRAME_KIND_RESET, PayloadRef,
};
use crate::{Error, Result};

/// Caller-owned read positions for every cache-mutation writer lane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheMutationCursor {
    inner: FleetLaneCursor,
}

/// Decoded result of advancing one cache-mutation cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheMutationPoll {
    pub mutations: Vec<CacheMutation>,
    pub loss: RingLoss,
    pub malformed: u64,
}

impl CacheMutationPoll {
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty() && self.loss.total() == 0 && self.malformed == 0
    }

    pub fn requires_full_resync(&self) -> bool {
        self.loss.total() != 0 || self.malformed != 0
    }
}

/// Raw mutation and payload transport for one cache layout.
pub struct CacheTransport<L: CacheLayout = crate::DefaultCacheLayout> {
    fleet: Arc<Fleet>,
    _layout: PhantomData<L>,
}

impl<L: CacheLayout> Clone for CacheTransport<L> {
    fn clone(&self) -> Self {
        Self {
            fleet: self.fleet.clone(),
            _layout: PhantomData,
        }
    }
}

impl<L: CacheLayout> CacheTransport<L> {
    pub fn new(fleet: Arc<Fleet>) -> Result<Self> {
        crate::layout::validate::<L>()?;
        Ok(Self {
            fleet,
            _layout: PhantomData,
        })
    }

    pub fn fleet(&self) -> &Arc<Fleet> {
        &self.fleet
    }

    pub fn cursor_at_head(&self) -> CacheMutationCursor {
        CacheMutationCursor {
            inner: self.fleet.lane_cursor_at_head::<MutationRecord<L>>(),
        }
    }

    pub fn cursor_from_start(&self) -> CacheMutationCursor {
        CacheMutationCursor {
            inner: self.fleet.lane_cursor_from_start::<MutationRecord<L>>(),
        }
    }

    /// Publish value bytes first, then the `Put` mutation that makes those
    /// bytes visible to cache consumers.
    pub fn publish_put(
        &self,
        store: &[u8],
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<CacheMutation> {
        self.validate_put(store, key, value)?;

        let payload_version = self.fleet.next_ring_version::<PayloadRecord<L>>();
        let chunks = split_payload::<L>(value);
        let ids = self.fleet.publish_batch::<PayloadRecord<L>>(
            FRAME_KIND_PAYLOAD,
            payload_version,
            chunks,
        );
        let payload = PayloadRef {
            first_id: ids[0],
            payload_version,
            chunk_count: ids.len() as u32,
            value_len: value.len() as u64,
        };
        let expires_at_ms = ttl.map(|ttl| now_ms().saturating_add(duration_ms(ttl)));
        let encoded = protocol::encode_put::<L>(store, key, expires_at_ms, payload)?;
        let sequence = self.fleet.next_ring_version::<MutationRecord<L>>();
        let mutation_id = self.publish_mutation(FRAME_KIND_PUT, sequence, encoded)?;

        Ok(CacheMutation::Put {
            store: Bytes::copy_from_slice(store),
            key: Bytes::copy_from_slice(key),
            revision: CacheRevision {
                sequence,
                mutation_id,
            },
            expires_at_ms,
            payload,
        })
    }

    pub fn publish_delete(&self, store: &[u8], key: &[u8]) -> Result<CacheMutation> {
        self.validate_key(store, key)?;
        let encoded = protocol::encode_delete::<L>(store, key)?;
        let sequence = self.fleet.next_ring_version::<MutationRecord<L>>();
        let mutation_id = self.publish_mutation(FRAME_KIND_DELETE, sequence, encoded)?;
        Ok(CacheMutation::Delete {
            store: Bytes::copy_from_slice(store),
            key: Bytes::copy_from_slice(key),
            revision: CacheRevision {
                sequence,
                mutation_id,
            },
        })
    }

    pub fn publish_reset(&self, store: &[u8]) -> Result<CacheMutation> {
        self.validate_store(store)?;
        let sequence = self.fleet.next_ring_version::<MutationRecord<L>>();
        let mutation_id = self.publish_mutation(
            FRAME_KIND_RESET,
            sequence,
            protocol::encode_reset::<L>(store)?,
        )?;
        Ok(CacheMutation::Reset {
            store: Bytes::copy_from_slice(store),
            revision: CacheRevision {
                sequence,
                mutation_id,
            },
        })
    }

    pub fn poll(&self, cursor: &mut CacheMutationCursor) -> CacheMutationPoll {
        let raw = self
            .fleet
            .poll_lanes::<MutationRecord<L>>(&mut cursor.inner);
        let mut mutations = Vec::with_capacity(raw.frames.len());
        let mut malformed = 0u64;
        for frame in raw.frames {
            match protocol::decode(&frame) {
                Some(mutation) => mutations.push(mutation),
                None => malformed = malformed.saturating_add(1),
            }
        }
        mutations.sort_by_key(CacheMutation::revision);
        CacheMutationPoll {
            mutations,
            loss: raw.loss,
            malformed,
        }
    }

    /// Resolve a `Put` descriptor through exact payload-ring ids.
    ///
    /// `None` means at least one slot was overwritten, malformed, or did not
    /// belong to this cache layout. It never returns bytes from a replacement
    /// frame that merely occupies the same physical slot.
    pub fn read_payload(&self, payload: PayloadRef) -> Option<Bytes> {
        if payload.first_id.kind() != L::PAYLOAD_RING_KIND
            || payload.chunk_count == 0
            || payload.chunk_count as usize > L::PAYLOAD_RING_SPEC.capacity
            || payload.value_len > self.max_value_len() as u64
        {
            return None;
        }

        let expected_len = usize::try_from(payload.value_len).ok()?;
        let mut value = Vec::with_capacity(expected_len);
        for offset in 0..u64::from(payload.chunk_count) {
            let counter = payload.first_id.counter().checked_add(offset)?;
            let id = NetId64::make(L::PAYLOAD_RING_KIND, payload.first_id.node(), counter);
            let frame = self.fleet.read(id)?;
            if frame.kind != FRAME_KIND_PAYLOAD || frame.ver != payload.payload_version {
                return None;
            }
            value.extend_from_slice(&frame.payload);
            if value.len() > expected_len {
                return None;
            }
        }
        (value.len() == expected_len).then(|| Bytes::from(value))
    }

    pub fn max_store_len(&self) -> usize {
        protocol::max_store_len::<L>()
    }

    pub fn max_key_len(&self, store: &[u8]) -> usize {
        protocol::max_key_len::<L>(store.len())
    }

    pub fn max_value_len(&self) -> usize {
        L::PAYLOAD_RING_SPEC
            .capacity
            .saturating_mul(L::PAYLOAD_RING_SPEC.payload_capacity)
    }

    pub fn validate_store(&self, store: &[u8]) -> Result<()> {
        if store.is_empty() {
            return Err(Error::StoreEmpty);
        }
        let max = self.max_store_len();
        if store.len() > max {
            return Err(Error::StoreTooLarge {
                store_len: store.len(),
                max,
            });
        }
        Ok(())
    }

    pub fn validate_key(&self, store: &[u8], key: &[u8]) -> Result<()> {
        self.validate_store(store)?;
        if key.is_empty() {
            return Err(Error::KeyEmpty);
        }
        let max = self.max_key_len(store);
        if key.len() > max {
            return Err(Error::KeyTooLarge {
                key_len: key.len(),
                max,
            });
        }
        Ok(())
    }

    pub fn validate_put(&self, store: &[u8], key: &[u8], value: &[u8]) -> Result<()> {
        self.validate_key(store, key)?;
        let max = self.max_value_len();
        if value.len() > max {
            return Err(Error::ValueTooLarge {
                value_len: value.len(),
                max,
            });
        }
        Ok(())
    }

    pub fn current_revision_sequence(&self) -> u64 {
        self.fleet.current_ring_version::<MutationRecord<L>>()
    }

    /// Reset both cache rings during an owner-controlled, quiescent boot.
    pub fn reset_rings(&self) -> Result<()> {
        self.fleet.reset_ring::<PayloadRecord<L>>()?;
        self.fleet.reset_ring::<MutationRecord<L>>()?;
        Ok(())
    }

    /// Unlink both SHM objects owned by this cache layout.
    #[cfg(unix)]
    pub fn unlink_rings(&self) -> Result<()> {
        let payload = self.fleet.shm_ring::<PayloadRecord<L>>()?;
        let mutation = self.fleet.shm_ring::<MutationRecord<L>>()?;
        let payload_result = payload.unlink();
        let mutation_result = mutation.unlink();
        payload_result?;
        mutation_result?;
        Ok(())
    }

    /// Create this process' readiness bridge for the cache mutation ring.
    /// Payload writes deliberately do not have a listener.
    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    pub fn event_fd(&self) -> Result<RingEventFd> {
        self.fleet
            .ring_event_fd::<MutationRecord<L>>()
            .map_err(Error::Io)
    }

    fn publish_mutation(&self, kind: u8, version: u64, payload: Bytes) -> Result<NetId64> {
        #[cfg(any(target_os = "linux", target_os = "freebsd"))]
        {
            self.fleet
                .publish_notified::<MutationRecord<L>>(kind, version, payload)
                .map_err(Error::Io)
        }
        #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
        {
            Ok(self
                .fleet
                .publish::<MutationRecord<L>>(kind, version, payload))
        }
    }
}

fn split_payload<L: CacheLayout>(value: &[u8]) -> Vec<Bytes> {
    let chunk_len = L::PAYLOAD_RING_SPEC.payload_capacity;
    if value.is_empty() {
        return vec![Bytes::new()];
    }
    value
        .chunks(chunk_len)
        .map(Bytes::copy_from_slice)
        .collect()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().max(1).min(u128::from(u64::MAX)) as u64
}

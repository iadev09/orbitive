//! Read-only inspection of retained mutation history, not a full cache index.

use std::collections::BTreeMap;
use std::io;

use bytes::Bytes;
use orbit_core::{
    FleetObserver, RingCursor, RingLoss, RingTopology, ShmRingView, poll_ring,
};

use crate::{
    CACHE_MUTATION_RING_KIND, CacheMutation, CacheRevision, PayloadRef,
    protocol,
};

/// A Put candidate still represented in the bounded mutation history.
/// It may no longer be resident in any worker's L1 or in the arena.
#[derive(Clone, Debug)]
pub struct RetainedEntry {
    pub key: Bytes,
    pub revision: CacheRevision,
    pub expires_at_ms: Option<u64>,
    pub payload: PayloadRef,
}

#[derive(Debug, Default)]
pub struct RetainedSnapshot {
    pub stores: BTreeMap<Bytes, BTreeMap<Bytes, RetainedEntry>>,
    pub loss: RingLoss,
    pub malformed: u64,
    pub pending: u64,
    pub changed_during_scan: bool,
}

/// Maps only an already existing cache mutation ring. No fleet membership,
/// writer lane, reader pin, new SHM object or cache mutation is introduced.
pub struct CacheObserver {
    ring: ShmRingView,
}

impl CacheObserver {
    pub fn attach_existing(fleet: &FleetObserver) -> io::Result<Self> {
        let ring = fleet.ring(CACHE_MUTATION_RING_KIND)?;
        if ring.metadata().spec.topology != RingTopology::PerNode {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cache mutations require per-node lanes",
            ));
        }
        Ok(Self { ring })
    }

    /// Collect at most one retained window per lane. Missing counters and
    /// concurrent changes are reported, never turned into cache misses.
    /// Even a loss-free scan is not an authoritative L1 snapshot.
    pub fn snapshot(&self) -> io::Result<RetainedSnapshot> {
        let version = self.ring.current_version();
        let heads = (0..self.ring.metadata().lane_count)
            .map(|index| self.ring.lane(index).map(|lane| lane.head()))
            .collect::<io::Result<Vec<_>>>()?;
        let mut snapshot = RetainedSnapshot::default();
        let mut mutations = Vec::new();
        for (index, head) in heads.iter().enumerate() {
            let lane = self.ring.lane(index)?;
            let mut cursor = RingCursor::from_start();
            let poll = poll_ring(&lane, &mut cursor);
            snapshot.loss.overwritten =
                snapshot.loss.overwritten.saturating_add(poll.loss.overwritten);
            snapshot.loss.unavailable =
                snapshot.loss.unavailable.saturating_add(poll.loss.unavailable);
            snapshot.pending = snapshot
                .pending
                .saturating_add(head.saturating_sub(poll.to_counter));
            for frame in poll.frames {
                match protocol::decode_observed(&frame) {
                    Some(mutation) => mutations.push(mutation),
                    None => {
                        snapshot.malformed =
                            snapshot.malformed.saturating_add(1)
                    }
                }
            }
        }
        snapshot.changed_during_scan = version != self.ring.current_version();
        for (index, head) in heads.iter().enumerate() {
            snapshot.changed_during_scan |=
                self.ring.lane(index)?.head() != *head;
        }
        apply(&mut snapshot, mutations);
        Ok(snapshot)
    }
}

fn apply(
    snapshot: &mut RetainedSnapshot,
    mut mutations: Vec<CacheMutation>,
) {
    mutations.sort_by_key(CacheMutation::revision);
    for mutation in mutations {
        match mutation {
            CacheMutation::Put {
                store,
                key,
                revision,
                expires_at_ms,
                payload,
            } => {
                snapshot.stores.entry(store).or_default().insert(
                    key.clone(),
                    RetainedEntry { key, revision, expires_at_ms, payload },
                );
            }
            CacheMutation::Delete { store, key, .. } => {
                snapshot.stores.entry(store).or_default().remove(&key);
            }
            CacheMutation::Reset { store, .. } => {
                snapshot.stores.entry(store).or_default().clear();
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/observer/mod.rs"]
mod tests;

//! Metrics snapshot families over `orbit-core` rings.
//!
//! Metrics are not cache entries. They are periodic, worker-local
//! snapshots where readers want the latest value per node and can
//! discard stale samples. This crate keeps that semantic layer out of
//! `orbit-core` while staying independent of any application runtime.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use orbit_core::{Fleet, NetId64, NodeId};
pub use orbit_core::{OrbitTyped, RingSpec, RingTopology};

/// Default ring capacity for one current snapshot per fleet node.
///
/// This is a scan window, not a history-retention promise. At the default
/// one-second publish cadence it leaves ample room for fleet-sized bursts
/// while keeping scalar metric rings small.
pub const METRIC_SNAPSHOT_RING_CAPACITY: usize = 1_024;

/// Default ring capacity for ordinary keyed metric rows.
///
/// Keyed collectors may need to scan far enough back to find the latest row
/// for every active key, so their default is larger than node snapshots.
/// High-cardinality domains should still choose and document their own bound.
pub const KEYED_METRIC_RING_CAPACITY: usize = 4_096;

/// A periodic metrics snapshot carried by an Orbit ring.
///
/// Implement this on a compact snapshot type. Hot paths should update
/// local atomics; a background task captures a snapshot and publishes it
/// through [`OrbitMetricFamily`].
pub trait OrbitMetricSnapshot: OrbitTyped + Sized {
    /// Human-readable family name for diagnostics.
    const FAMILY: &'static str;

    /// Logical node this snapshot describes.
    fn node_id(&self) -> u16;

    /// Unix timestamp in seconds when the snapshot was captured.
    fn captured_at_unix_secs(&self) -> u64;

    /// Encode the snapshot into a ring payload.
    fn encode(&self) -> Result<Vec<u8>, String>;

    /// Decode the snapshot from a ring payload.
    fn decode(bytes: &[u8]) -> Result<Self, String>;
}

/// Optional key projection for row-like metric families.
///
/// Worker-control metrics usually use `node_id`, but label-preserving
/// families often need "latest row per logical key" where the key is not
/// the producing process id.
pub trait OrbitMetricKeyedSnapshot: OrbitMetricSnapshot {
    type Key: Eq + Hash;

    fn metric_key(&self) -> Self::Key;
}

/// One decoded metrics sample plus the Orbit id that carried it.
#[derive(Clone, Debug)]
pub struct OrbitMetricSample<T> {
    pub id: NetId64,
    pub snapshot: T,
}

impl<T: OrbitMetricSnapshot> OrbitMetricSample<T> {
    pub fn node_id(&self) -> u16 {
        self.snapshot.node_id()
    }

    pub fn captured_at_unix_secs(&self) -> u64 {
        self.snapshot.captured_at_unix_secs()
    }

    pub fn age_secs(&self, now_unix_secs: u64) -> u64 {
        now_unix_secs.saturating_sub(self.captured_at_unix_secs())
    }

    pub fn is_fresh(&self, now_unix_secs: u64, max_age_secs: u64) -> bool {
        self.age_secs(now_unix_secs) <= max_age_secs
    }
}

/// Ring-backed metrics family.
#[derive(Clone)]
pub struct OrbitMetricFamily<T: OrbitMetricSnapshot> {
    fleet: Arc<Fleet>,
    _t: std::marker::PhantomData<T>,
}

impl<T: OrbitMetricSnapshot> OrbitMetricFamily<T> {
    pub fn new(fleet: Arc<Fleet>) -> Self {
        Self {
            fleet,
            _t: std::marker::PhantomData,
        }
    }

    pub fn publisher(&self) -> OrbitMetricPublisher<T> {
        OrbitMetricPublisher {
            family: self.clone(),
        }
    }

    pub fn collector(&self) -> OrbitMetricCollector<T> {
        OrbitMetricCollector {
            family: self.clone(),
        }
    }
}

/// Write-side handle for one metrics family. Usually lives in the
/// worker/background publisher task.
#[derive(Clone)]
pub struct OrbitMetricPublisher<T: OrbitMetricSnapshot> {
    family: OrbitMetricFamily<T>,
}

impl<T: OrbitMetricSnapshot> OrbitMetricPublisher<T> {
    pub fn new(fleet: Arc<Fleet>) -> Self {
        Self {
            family: OrbitMetricFamily::new(fleet),
        }
    }

    /// Publish one captured snapshot. The ring frame version mirrors
    /// the snapshot timestamp so low-level tools can inspect freshness
    /// without decoding.
    pub fn publish(&self, snapshot: &T) -> Result<NetId64, String> {
        let payload = snapshot.encode()?;
        if payload.len() > T::RING_SPEC.payload_capacity {
            return Err(format!(
                "orbit metrics payload too large for {}: {} > {}",
                T::FAMILY,
                payload.len(),
                T::RING_SPEC.payload_capacity
            ));
        }
        Ok(self.family.fleet.publish::<T>(
            0,
            snapshot.captured_at_unix_secs(),
            Bytes::from(payload),
        ))
    }
}

/// Read-side handle for one metrics family. Usually lives in the
/// master/aggregator path.
#[derive(Clone)]
pub struct OrbitMetricCollector<T: OrbitMetricSnapshot> {
    family: OrbitMetricFamily<T>,
}

impl<T: OrbitMetricSnapshot> OrbitMetricCollector<T> {
    pub fn new(fleet: Arc<Fleet>) -> Self {
        Self {
            family: OrbitMetricFamily::new(fleet),
        }
    }

    /// Walk the ring backwards and return the newest decodable sample
    /// for each node. Malformed frames are ignored; a newer valid frame
    /// for a node wins because the walk starts at the ring head.
    pub fn latest_by_node(&self) -> HashMap<u16, OrbitMetricSample<T>> {
        if T::RING_SPEC.topology == RingTopology::PerNode {
            return self.latest_by_node_lanes();
        }
        let head = self.family.fleet.head::<T>();
        if head == 0 {
            return HashMap::new();
        }

        let capacity = self.family.fleet.ring_capacity::<T>() as u64;
        let walk_count = head.min(capacity);
        let mut samples = HashMap::new();
        let expected_nodes = usize::from(self.family.fleet.fleet_capacity());

        for i in 0..walk_count {
            let counter = head - 1 - i;
            let Some(frame) = self.family.fleet.read_at::<T>(counter) else {
                if counter == 0 {
                    break;
                }
                continue;
            };
            let Ok(snapshot) = T::decode(&frame.payload) else {
                if counter == 0 {
                    break;
                }
                continue;
            };
            samples
                .entry(snapshot.node_id())
                .or_insert(OrbitMetricSample {
                    id: frame.id,
                    snapshot,
                });
            if expected_nodes > 0 && samples.len() >= expected_nodes {
                break;
            }
            if counter == 0 {
                break;
            }
        }

        samples
    }

    /// Walk the ring backwards and return the newest decodable sample
    /// for each logical metric key.
    pub fn latest_by_key<K>(&self) -> HashMap<K, OrbitMetricSample<T>>
    where
        T: OrbitMetricKeyedSnapshot<Key = K>,
        K: Eq + Hash,
    {
        if T::RING_SPEC.topology == RingTopology::PerNode {
            return self.latest_by_key_lanes();
        }
        let head = self.family.fleet.head::<T>();
        if head == 0 {
            return HashMap::new();
        }

        let capacity = self.family.fleet.ring_capacity::<T>() as u64;
        let walk_count = head.min(capacity);
        let mut samples = HashMap::new();

        for i in 0..walk_count {
            let counter = head - 1 - i;
            let Some(frame) = self.family.fleet.read_at::<T>(counter) else {
                if counter == 0 {
                    break;
                }
                continue;
            };
            let Ok(snapshot) = T::decode(&frame.payload) else {
                if counter == 0 {
                    break;
                }
                continue;
            };
            samples
                .entry(snapshot.metric_key())
                .or_insert(OrbitMetricSample {
                    id: frame.id,
                    snapshot,
                });
            if counter == 0 {
                break;
            }
        }

        samples
    }

    fn latest_by_node_lanes(&self) -> HashMap<u16, OrbitMetricSample<T>> {
        let capacity = self.family.fleet.ring_capacity::<T>() as u64;
        let mut samples = HashMap::new();

        for node in 0..self.family.fleet.fleet_capacity() {
            let node_id = NodeId::new(node);
            let head = self.family.fleet.lane_head::<T>(node_id);
            let walk_count = head.min(capacity);
            for offset in 0..walk_count {
                let counter = head - 1 - offset;
                let Some(frame) = self.family.fleet.read_lane_at::<T>(node_id, counter) else {
                    continue;
                };
                let Ok(snapshot) = T::decode(&frame.payload) else {
                    continue;
                };
                if snapshot.node_id() != u16::from(node) {
                    continue;
                }
                samples.insert(
                    snapshot.node_id(),
                    OrbitMetricSample {
                        id: frame.id,
                        snapshot,
                    },
                );
                break;
            }
        }

        samples
    }

    fn latest_by_key_lanes<K>(&self) -> HashMap<K, OrbitMetricSample<T>>
    where
        T: OrbitMetricKeyedSnapshot<Key = K>,
        K: Eq + Hash,
    {
        let capacity = self.family.fleet.ring_capacity::<T>() as u64;
        let mut samples = HashMap::new();

        for node in 0..self.family.fleet.fleet_capacity() {
            let node_id = NodeId::new(node);
            let head = self.family.fleet.lane_head::<T>(node_id);
            let walk_count = head.min(capacity);
            for offset in 0..walk_count {
                let counter = head - 1 - offset;
                let Some(frame) = self.family.fleet.read_lane_at::<T>(node_id, counter) else {
                    continue;
                };
                let Ok(snapshot) = T::decode(&frame.payload) else {
                    continue;
                };
                let key = snapshot.metric_key();
                let candidate = OrbitMetricSample {
                    id: frame.id,
                    snapshot,
                };
                match samples.entry(key) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(candidate);
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry)
                        if sample_is_newer(&candidate, entry.get()) =>
                    {
                        entry.insert(candidate);
                    }
                    std::collections::hash_map::Entry::Occupied(_) => {}
                }
            }
        }

        samples
    }

    /// Return latest keyed samples whose captured timestamp is within
    /// `max_age_secs` of `now_unix_secs`.
    pub fn fresh_by_key<K>(
        &self,
        now_unix_secs: u64,
        max_age_secs: u64,
    ) -> HashMap<K, OrbitMetricSample<T>>
    where
        T: OrbitMetricKeyedSnapshot<Key = K>,
        K: Eq + Hash,
    {
        self.latest_by_key()
            .into_iter()
            .filter(|(_, sample)| sample.is_fresh(now_unix_secs, max_age_secs))
            .collect()
    }

    /// Return latest samples whose captured timestamp is within
    /// `max_age_secs` of `now_unix_secs`.
    pub fn fresh_by_node(
        &self,
        now_unix_secs: u64,
        max_age_secs: u64,
    ) -> HashMap<u16, OrbitMetricSample<T>> {
        self.latest_by_node()
            .into_iter()
            .filter(|(_, sample)| sample.is_fresh(now_unix_secs, max_age_secs))
            .collect()
    }
}

fn sample_is_newer<T: OrbitMetricSnapshot>(
    candidate: &OrbitMetricSample<T>,
    current: &OrbitMetricSample<T>,
) -> bool {
    (
        candidate.captured_at_unix_secs(),
        candidate.id.node(),
        candidate.id.counter(),
    ) > (
        current.captured_at_unix_secs(),
        current.id.node(),
        current.id.counter(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestSnapshot {
        node: u16,
        captured_at: u64,
        value: u64,
    }

    impl OrbitTyped for TestSnapshot {
        const KIND: u8 = 211;
        const RING_SPEC: RingSpec = RingSpec::new(METRIC_SNAPSHOT_RING_CAPACITY, 18);
    }

    impl OrbitMetricSnapshot for TestSnapshot {
        const FAMILY: &'static str = "test";

        fn node_id(&self) -> u16 {
            self.node
        }

        fn captured_at_unix_secs(&self) -> u64 {
            self.captured_at
        }

        fn encode(&self) -> Result<Vec<u8>, String> {
            let mut out = Vec::with_capacity(18);
            out.extend_from_slice(&self.node.to_le_bytes());
            out.extend_from_slice(&self.captured_at.to_le_bytes());
            out.extend_from_slice(&self.value.to_le_bytes());
            Ok(out)
        }

        fn decode(bytes: &[u8]) -> Result<Self, String> {
            if bytes.len() != 18 {
                return Err(format!("bad len {}", bytes.len()));
            }
            let node = u16::from_le_bytes(bytes[0..2].try_into().expect("node bytes"));
            let captured_at = u64::from_le_bytes(bytes[2..10].try_into().expect("time bytes"));
            let value = u64::from_le_bytes(bytes[10..18].try_into().expect("value bytes"));
            Ok(Self {
                node,
                captured_at,
                value,
            })
        }
    }

    #[test]
    fn latest_by_node_keeps_newest_sample_per_node() {
        let fleet = Arc::new(Fleet::join("metrics-test", 2).unwrap());
        let family = OrbitMetricFamily::<TestSnapshot>::new(fleet);
        let publisher = family.publisher();
        let collector = family.collector();

        publisher
            .publish(&TestSnapshot {
                node: 1,
                captured_at: 10,
                value: 100,
            })
            .unwrap();
        publisher
            .publish(&TestSnapshot {
                node: 2,
                captured_at: 11,
                value: 200,
            })
            .unwrap();
        publisher
            .publish(&TestSnapshot {
                node: 1,
                captured_at: 12,
                value: 101,
            })
            .unwrap();

        let latest = collector.latest_by_node();
        assert_eq!(latest.len(), 2);
        assert_eq!(latest[&1].snapshot.value, 101);
        assert_eq!(latest[&2].snapshot.value, 200);
    }

    #[cfg(unix)]
    #[test]
    fn per_node_family_reads_each_writer_lane() {
        #[derive(Clone, Debug, PartialEq, Eq)]
        struct PerNodeSnapshot(TestSnapshot);

        impl OrbitTyped for PerNodeSnapshot {
            const KIND: u8 = 209;
            const RING_SPEC: RingSpec = RingSpec::per_node(16, 18);
        }

        impl OrbitMetricSnapshot for PerNodeSnapshot {
            const FAMILY: &'static str = "per-node-test";

            fn node_id(&self) -> u16 {
                self.0.node_id()
            }

            fn captured_at_unix_secs(&self) -> u64 {
                self.0.captured_at_unix_secs()
            }

            fn encode(&self) -> Result<Vec<u8>, String> {
                self.0.encode()
            }

            fn decode(bytes: &[u8]) -> Result<Self, String> {
                TestSnapshot::decode(bytes).map(Self)
            }
        }

        let name = Box::leak(format!("m{:x}", std::process::id()).into_boxed_str());
        let collector_fleet = Arc::new(Fleet::join_shm_as(name, 3, NodeId::new(0)).unwrap());
        collector_fleet.reset_ring::<PerNodeSnapshot>().unwrap();
        let node_one = OrbitMetricPublisher::<PerNodeSnapshot>::new(Arc::new(
            Fleet::join_shm_as(name, 3, NodeId::new(1)).unwrap(),
        ));
        let node_two = OrbitMetricPublisher::<PerNodeSnapshot>::new(Arc::new(
            Fleet::join_shm_as(name, 3, NodeId::new(2)).unwrap(),
        ));

        node_one
            .publish(&PerNodeSnapshot(TestSnapshot {
                node: 1,
                captured_at: 10,
                value: 100,
            }))
            .unwrap();
        node_two
            .publish(&PerNodeSnapshot(TestSnapshot {
                node: 2,
                captured_at: 11,
                value: 200,
            }))
            .unwrap();
        node_one
            .publish(&PerNodeSnapshot(TestSnapshot {
                node: 1,
                captured_at: 12,
                value: 101,
            }))
            .unwrap();

        let latest =
            OrbitMetricCollector::<PerNodeSnapshot>::new(collector_fleet.clone()).latest_by_node();
        assert_eq!(latest.len(), 2);
        assert_eq!(latest[&1].snapshot.0.value, 101);
        assert_eq!(latest[&2].snapshot.0.value, 200);

        collector_fleet
            .shm_ring::<PerNodeSnapshot>()
            .unwrap()
            .unlink()
            .unwrap();
    }

    #[test]
    fn fresh_by_node_drops_stale_samples() {
        let fleet = Arc::new(Fleet::join("metrics-fresh-test", 2).unwrap());
        let family = OrbitMetricFamily::<TestSnapshot>::new(fleet);
        let publisher = family.publisher();
        let collector = family.collector();

        publisher
            .publish(&TestSnapshot {
                node: 1,
                captured_at: 10,
                value: 100,
            })
            .unwrap();
        publisher
            .publish(&TestSnapshot {
                node: 2,
                captured_at: 20,
                value: 200,
            })
            .unwrap();

        let fresh = collector.fresh_by_node(25, 10);
        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[&2].snapshot.value, 200);
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct KeyedSnapshot {
        node: u16,
        key: &'static str,
        captured_at: u64,
        value: u64,
    }

    impl OrbitTyped for KeyedSnapshot {
        const KIND: u8 = 212;
        const RING_SPEC: RingSpec = RingSpec::new(KEYED_METRIC_RING_CAPACITY, 64);
    }

    impl OrbitMetricSnapshot for KeyedSnapshot {
        const FAMILY: &'static str = "keyed-test";

        fn node_id(&self) -> u16 {
            self.node
        }

        fn captured_at_unix_secs(&self) -> u64 {
            self.captured_at
        }

        fn encode(&self) -> Result<Vec<u8>, String> {
            let mut out = Vec::with_capacity(27);
            out.extend_from_slice(&self.node.to_le_bytes());
            out.extend_from_slice(&self.captured_at.to_le_bytes());
            out.extend_from_slice(&self.value.to_le_bytes());
            let key = self.key.as_bytes();
            out.push(key.len() as u8);
            out.extend_from_slice(key);
            Ok(out)
        }

        fn decode(bytes: &[u8]) -> Result<Self, String> {
            if bytes.len() < 19 {
                return Err(format!("bad len {}", bytes.len()));
            }
            let node = u16::from_le_bytes(bytes[0..2].try_into().expect("node bytes"));
            let captured_at = u64::from_le_bytes(bytes[2..10].try_into().expect("time bytes"));
            let value = u64::from_le_bytes(bytes[10..18].try_into().expect("value bytes"));
            let key_len = usize::from(bytes[18]);
            if bytes.len() != 19 + key_len {
                return Err(format!("bad key len {}", bytes.len()));
            }
            let key = std::str::from_utf8(&bytes[19..]).map_err(|e| e.to_string())?;
            let key = match key {
                "alpha" => "alpha",
                "beta" => "beta",
                _ => return Err(format!("unknown key {key}")),
            };
            Ok(Self {
                node,
                key,
                captured_at,
                value,
            })
        }
    }

    impl OrbitMetricKeyedSnapshot for KeyedSnapshot {
        type Key = String;

        fn metric_key(&self) -> Self::Key {
            self.key.to_owned()
        }
    }

    #[test]
    fn latest_by_key_keeps_newest_sample_per_key() {
        let fleet = Arc::new(Fleet::join("metrics-keyed-test", 2).unwrap());
        let family = OrbitMetricFamily::<KeyedSnapshot>::new(fleet);
        let publisher = family.publisher();
        let collector = family.collector();

        publisher
            .publish(&KeyedSnapshot {
                node: 0,
                key: "alpha",
                captured_at: 10,
                value: 100,
            })
            .unwrap();
        publisher
            .publish(&KeyedSnapshot {
                node: 0,
                key: "beta",
                captured_at: 11,
                value: 200,
            })
            .unwrap();
        publisher
            .publish(&KeyedSnapshot {
                node: 0,
                key: "alpha",
                captured_at: 12,
                value: 101,
            })
            .unwrap();

        let latest = collector.latest_by_key();
        assert_eq!(latest.len(), 2);
        assert_eq!(latest["alpha"].snapshot.value, 101);
        assert_eq!(latest["beta"].snapshot.value, 200);
    }
}

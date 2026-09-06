//! Smoke test for orbit-core — verifies the API shape works end to end.
//!
//! V0 is in-process only, so "fleet" here is a single member. Real
//! cross-process behavior arrives with V1 + SHM.

use bytes::Bytes;
use orbit_core::{Fleet, NetId64, NodeId, OrbitTyped, RingSpec};

#[test]
fn empty_fleet_rejected() {
    let err = Fleet::join("test", 0).unwrap_err();
    assert!(matches!(err, orbit_core::Error::EmptyFleet));
}

#[test]
fn join_single_member_succeeds() {
    let fleet = Fleet::join("test", 1).unwrap();
    assert_eq!(fleet.name(), "test");
    assert_eq!(fleet.fleet_capacity(), 1);
    assert_eq!(fleet.node_id().get(), 0);
}

#[test]
fn node_must_fit_inside_the_declared_fleet() {
    let err = Fleet::join_as("test", 2, NodeId::new(2)).unwrap_err();
    assert!(matches!(
        err,
        orbit_core::Error::NodeOutsideFleet {
            node_id: 2,
            fleet_capacity: 2
        }
    ));
}

#[test]
fn capacity_accepts_the_full_claviron_node_space() {
    let fleet = Fleet::join_as("test", 256, NodeId::new(255)).unwrap();
    assert_eq!(fleet.fleet_capacity(), 256);
    assert_eq!(fleet.node_id().get(), 255);
}

// ─────────────────────────────────────────────────────────────────────
// NetId64 — fleet-aware minting
// ─────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct CurrencyRate;
impl OrbitTyped for CurrencyRate {
    const KIND: u8 = 7;
    const RING_SPEC: RingSpec = RingSpec::new(4, 256);
}

#[derive(Clone, Debug)]
struct UserSession;
impl OrbitTyped for UserSession {
    const KIND: u8 = 9;
    const RING_SPEC: RingSpec = RingSpec::new(4, 256);
}

#[test]
fn next_id_carries_kind_and_node() {
    let fleet = Fleet::join("test", 1).unwrap();
    let id = fleet.next_id::<CurrencyRate>();
    assert_eq!(id.kind(), 7);
    assert_eq!(id.node(), fleet.node_id().get());
    assert_eq!(id.counter(), 0); // first mint
}

#[test]
fn next_id_increments_counter_per_kind() {
    let fleet = Fleet::join("test", 1).unwrap();
    let a = fleet.next_id::<CurrencyRate>();
    let b = fleet.next_id::<CurrencyRate>();
    let c = fleet.next_id::<CurrencyRate>();
    assert_eq!(a.counter(), 0);
    assert_eq!(b.counter(), 1);
    assert_eq!(c.counter(), 2);
}

#[test]
fn next_id_independent_counters_per_kind() {
    let fleet = Fleet::join("test", 1).unwrap();
    let r1 = fleet.next_id::<CurrencyRate>();
    let s1 = fleet.next_id::<UserSession>();
    let r2 = fleet.next_id::<CurrencyRate>();
    let s2 = fleet.next_id::<UserSession>();
    assert_eq!(r1.counter(), 0);
    assert_eq!(s1.counter(), 0); // different kind, own counter
    assert_eq!(r2.counter(), 1);
    assert_eq!(s2.counter(), 1);
    assert_ne!(r1.kind(), s1.kind());
}

#[test]
fn orbit_id_display_and_parse_roundtrip() {
    let fleet = Fleet::join("test", 1).unwrap();
    let id = fleet.next_id::<CurrencyRate>();
    let s = id.to_string();
    let parsed: NetId64 = s.parse().unwrap();
    assert_eq!(parsed, id);
}

#[test]
fn orbit_id_be_bytes_roundtrip() {
    let fleet = Fleet::join("test", 1).unwrap();
    let id = fleet.next_id::<CurrencyRate>();
    let bytes = id.to_be_bytes();
    assert_eq!(NetId64::from_be_bytes(bytes), id);
}

// ─────────────────────────────────────────────────────────────────────
// Ring buffer — the orbit runtime substrate
// ─────────────────────────────────────────────────────────────────────

#[test]
fn publish_returns_id_with_correct_kind_and_node() {
    let fleet = Fleet::join("test", 1).unwrap();
    let id = fleet.publish::<CurrencyRate>(0, 0, Bytes::from_static(b"hello"));
    assert_eq!(id.kind(), CurrencyRate::KIND);
    assert_eq!(id.node(), fleet.node_id().get());
    assert_eq!(id.counter(), 0);
}

#[test]
fn publish_increments_counter_per_kind() {
    let fleet = Fleet::join("test", 1).unwrap();
    let a = fleet.publish::<CurrencyRate>(0, 0, Bytes::from_static(b"a"));
    let b = fleet.publish::<CurrencyRate>(0, 0, Bytes::from_static(b"b"));
    let c = fleet.publish::<CurrencyRate>(0, 0, Bytes::from_static(b"c"));
    assert_eq!(a.counter(), 0);
    assert_eq!(b.counter(), 1);
    assert_eq!(c.counter(), 2);
}

#[test]
fn read_returns_published_frame() {
    let fleet = Fleet::join("test", 1).unwrap();
    let id = fleet.publish::<CurrencyRate>(0, 7, Bytes::from_static(b"payload"));
    let frame = fleet.read(id).expect("frame should be present");
    assert_eq!(frame.id, id);
    assert_eq!(frame.kind, 0);
    assert_eq!(frame.ver, 7);
    assert_eq!(&frame.payload[..], b"payload");
}

#[test]
fn read_unknown_kind_returns_none() {
    let fleet = Fleet::join("test", 1).unwrap();
    // Synthesize an id for a kind that has never been published to.
    let phantom = NetId64::make(42, 0, 0);
    assert!(fleet.read(phantom).is_none());
}

#[test]
fn ring_wraparound_overwrites_old_frames() {
    let fleet = Fleet::join("test", 1).unwrap();
    let mut ids = Vec::new();
    for i in 0..6 {
        ids.push(fleet.publish::<CurrencyRate>(0, i as u64, Bytes::from(vec![i as u8])));
    }

    // Counters 0 and 1 have been overwritten by 4 and 5 (capacity = 4).
    assert!(fleet.read(ids[0]).is_none());
    assert!(fleet.read(ids[1]).is_none());
    // Counters 2..6 are still present.
    for id in &ids[2..] {
        assert!(fleet.read(*id).is_some(), "id {id} should still be in ring");
    }
}

#[test]
fn ring_per_kind_is_independent() {
    let fleet = Fleet::join("test", 1).unwrap();
    let r_id = fleet.publish::<CurrencyRate>(0, 0, Bytes::from_static(b"rate"));
    let s_id = fleet.publish::<UserSession>(0, 0, Bytes::from_static(b"session"));
    assert_eq!(r_id.kind(), CurrencyRate::KIND);
    assert_eq!(s_id.kind(), UserSession::KIND);
    // Both still readable from their respective rings.
    assert!(fleet.read(r_id).is_some());
    assert!(fleet.read(s_id).is_some());
}

#[test]
fn ring_head_advances_with_writes() {
    let fleet = Fleet::join("test", 1).unwrap();
    let ring = fleet.ring::<CurrencyRate>();
    assert_eq!(ring.head(), 0);
    fleet.publish::<CurrencyRate>(0, 0, Bytes::from_static(b"x"));
    assert_eq!(ring.head(), 1);
    fleet.publish::<CurrencyRate>(0, 0, Bytes::from_static(b"y"));
    assert_eq!(ring.head(), 2);
}

#[test]
fn ring_read_head_returns_latest_frame() {
    let fleet = Fleet::join("test", 1).unwrap();
    let ring = fleet.ring::<CurrencyRate>();
    assert!(ring.read_head().is_none());

    fleet.publish::<CurrencyRate>(0, 0, Bytes::from_static(b"first"));
    let last_id = fleet.publish::<CurrencyRate>(0, 0, Bytes::from_static(b"second"));

    let head_frame = ring.read_head().expect("ring has head after writes");
    assert_eq!(head_frame.id, last_id);
    assert_eq!(&head_frame.payload[..], b"second");
}

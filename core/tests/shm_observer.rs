#![cfg(unix)]

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use orbit_core::ring::cursor::{RingCursor, poll_ring};
use orbit_core::ring::shm::ShmRing;
use orbit_core::shm::{ShmRegion, ShmValidation, ring_segment_name};
use orbit_core::{FleetObserver, NodeId, OrbitTyped, RingSpec};

static NEXT_FLEET: AtomicU64 = AtomicU64::new(0);

fn fleet_name(prefix: &str) -> String {
    let id = NEXT_FLEET.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}{:x}{id:x}", std::process::id())
}

#[test]
fn missing_observation_does_not_create_a_segment() {
    let fleet = fleet_name("om");
    let kind = 246;
    let observer = FleetObserver::attach_existing(&fleet).expect("observer namespace");

    let error = observer.ring(kind).expect_err("missing ring must stay missing");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(
        ShmRegion::validate_existing(&ring_segment_name(&fleet, kind), 1).unwrap(),
        ShmValidation::Missing
    );
}

#[test]
fn observer_reads_persisted_geometry_and_each_lane() {
    let fleet = fleet_name("or");
    let kind = 247;
    let spec = RingSpec::per_node(4, 16);
    let ring = ShmRing::open_or_create_for_fleet(&fleet, kind, spec, 2).expect("create ring");
    let first =
        ring.write(NodeId::new(0), 11, 101, Bytes::from_static(b"first")).expect("write lane zero");
    let second =
        ring.write(NodeId::new(1), 12, 102, Bytes::from_static(b"second")).expect("write lane one");

    let observer = FleetObserver::attach_existing(&fleet).expect("observer namespace");
    let view = observer.ring(kind).expect("attach existing ring");
    assert_eq!(view.metadata().kind, kind);
    assert_eq!(view.metadata().spec, spec);
    assert_eq!(view.metadata().lane_count, 2);

    let lane_zero = view.lane(0).expect("lane zero");
    let lane_one = view.lane(1).expect("lane one");
    assert_eq!(lane_zero.read_head().expect("lane zero head").id, first);
    assert_eq!(lane_one.read_head().expect("lane one head").id, second);
    assert_eq!(lane_zero.retained_range(), 0..1);

    let mut cursor = RingCursor::from_start();
    let poll = poll_ring(&lane_one, &mut cursor);
    assert_eq!(poll.frames.len(), 1);
    assert_eq!(poll.frames[0].payload.as_ref(), b"second");

    drop(view);
    assert_eq!(ring.read(second).expect("writer survives observer drop").ver, 102);
    ring.unlink().expect("cleanup ring");
}

#[derive(Clone)]
struct ExpectedRing;

impl OrbitTyped for ExpectedRing {
    const KIND: u8 = 248;
    const RING_SPEC: RingSpec = RingSpec::new(8, 32);
}

#[test]
fn typed_observation_rejects_a_different_persisted_spec() {
    let fleet = fleet_name("ot");
    let ring = ShmRing::open_or_create(&fleet, ExpectedRing::KIND, RingSpec::new(4, 32))
        .expect("create mismatched ring");
    let observer = FleetObserver::attach_existing(&fleet).expect("observer namespace");

    let error = observer
        .typed_ring::<ExpectedRing>()
        .expect_err("typed observer must verify the linked contract");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("existing spec"));

    ring.unlink().expect("cleanup ring");
}

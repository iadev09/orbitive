//! ShmRing smoke tests — V1 substrate, single-process for now.
//!
//! Each test uses a unique fleet name to avoid SHM segment
//! collisions across parallel runs. Stale segments left over from a
//! crashed test can be cleaned manually with
//! `rm /dev/shm/orbit-shmtest-*-$(id -u)` (Linux) or
//! `ls /tmp/orbit-shmtest-*` plus appropriate cleanup on macOS.

#![cfg(unix)]

use bytes::Bytes;
use orbit_core::ring_shm::{ShmRing, segment_size_for_spec, segment_size_for_spec_and_fleet};
use orbit_core::{NodeId, RingSpec};

/// macOS limits POSIX SHM names to PSHMNAMLEN (31 chars). Keep
/// fleet names short — the full segment name is
/// `/orbit-{fleet}-{kind}-{uid}` and even with a 5-digit UID and a
/// 3-digit kind we only have ~14 chars budget for the fleet name.
fn fresh_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid_short = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFF;
    format!("t{pid_short:04x}{nonce:08x}{n:02x}")
}

fn spec(capacity: usize) -> RingSpec {
    RingSpec::new(capacity, 16)
}

#[test]
fn open_or_create_creates_first_then_attaches() {
    let name = fresh_name();
    let r1 = ShmRing::open_or_create(&name, 7, spec(16)).unwrap();
    assert!(r1.created(), "first opener creates");
    let r2 = ShmRing::open_or_create(&name, 7, spec(16)).unwrap();
    assert!(!r2.created(), "second opener attaches");
    let _ = r1.unlink();
}

#[test]
fn write_then_read_returns_same_frame() {
    let name = fresh_name();
    let ring = ShmRing::open_or_create(&name, 7, spec(16)).unwrap();
    let id = ring
        .write(NodeId::new(3), 0, 42, Bytes::from_static(b"hello"))
        .unwrap();
    assert_eq!(id.kind(), 7);
    assert_eq!(id.node(), 3);
    assert_eq!(id.counter(), 0);

    let frame = ring.read(id).unwrap();
    assert_eq!(frame.id, id);
    assert_eq!(frame.kind, 0);
    assert_eq!(frame.ver, 42);
    assert_eq!(&frame.payload[..], b"hello");

    let _ = ring.unlink();
}

#[test]
fn head_advances_with_writes() {
    let name = fresh_name();
    let ring = ShmRing::open_or_create(&name, 5, spec(16)).unwrap();
    assert_eq!(ring.head(), 0);
    ring.write(NodeId::new(0), 0, 0, Bytes::from_static(b"a"))
        .unwrap();
    ring.write(NodeId::new(0), 0, 0, Bytes::from_static(b"b"))
        .unwrap();
    assert_eq!(ring.head(), 2);
    let _ = ring.unlink();
}

#[test]
fn read_head_returns_latest() {
    let name = fresh_name();
    let ring = ShmRing::open_or_create(&name, 5, spec(16)).unwrap();
    assert!(ring.read_head().is_none());

    ring.write(NodeId::new(0), 0, 0, Bytes::from_static(b"first"))
        .unwrap();
    let last = ring
        .write(NodeId::new(0), 0, 0, Bytes::from_static(b"second"))
        .unwrap();

    let frame = ring.read_head().unwrap();
    assert_eq!(frame.id, last);
    assert_eq!(&frame.payload[..], b"second");
    let _ = ring.unlink();
}

#[test]
fn wraparound_overwrites_old_slots() {
    let name = fresh_name();
    let ring = ShmRing::open_or_create(&name, 5, spec(4)).unwrap();
    let mut ids = Vec::new();
    for i in 0..6 {
        let id = ring
            .write(NodeId::new(0), 0, 0, Bytes::from(vec![i]))
            .unwrap();
        ids.push(id);
    }
    // First two slots overwritten by writes 4 and 5 (capacity = 4).
    assert!(ring.read(ids[0]).is_none());
    assert!(ring.read(ids[1]).is_none());
    for id in &ids[2..] {
        assert!(ring.read(*id).is_some(), "id {id} should still be live");
    }
    let _ = ring.unlink();
}

#[test]
fn two_handles_same_segment_share_state() {
    let name = fresh_name();
    let writer = ShmRing::open_or_create(&name, 9, spec(16)).unwrap();
    let reader = ShmRing::open_or_create(&name, 9, spec(16)).unwrap();
    assert!(writer.created());
    assert!(!reader.created());

    let id = writer
        .write(NodeId::new(1), 0, 0, Bytes::from_static(b"shared"))
        .unwrap();

    // Reader sees what writer wrote — the whole point of SHM.
    let frame = reader.read(id).unwrap();
    assert_eq!(&frame.payload[..], b"shared");
    let _ = writer.unlink();
}

#[test]
fn payload_too_large_returns_error() {
    let name = fresh_name();
    let ring_spec = RingSpec::new(16, 8);
    let ring = ShmRing::open_or_create(&name, 3, ring_spec).unwrap();
    let huge = Bytes::from(vec![0u8; ring_spec.payload_capacity + 1]);
    assert!(ring.write(NodeId::new(0), 0, 0, huge).is_err());
    let _ = ring.unlink();
}

#[test]
fn segment_size_uses_the_ring_payload_capacity() {
    let small = segment_size_for_spec(RingSpec::new(4, 16)).unwrap();
    let large = segment_size_for_spec(RingSpec::new(4, 256)).unwrap();

    assert_eq!(small, 64 + 64 + 4 * 64);
    assert_eq!(large, 64 + 64 + 4 * 320);
}

#[test]
fn per_node_segment_size_multiplies_the_lane_storage() {
    let size = segment_size_for_spec_and_fleet(RingSpec::per_node(4, 16), 3).unwrap();

    assert_eq!(size, 64 + 3 * 64 + 3 * 4 * 64);
}

#[test]
fn same_kind_rejects_a_different_ring_spec() {
    let name = fresh_name();
    let original_spec = RingSpec::new(16, 32);
    let ring = ShmRing::open_or_create(&name, 14, original_spec).unwrap();

    let err = match ShmRing::open_or_create(&name, 14, RingSpec::new(16, 256)) {
        Ok(_) => panic!("same SHM path must reject a different slot layout"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    let _ = ring.unlink();
}

#[test]
fn zero_payload_ring_accepts_only_empty_frames() {
    let name = fresh_name();
    let ring = ShmRing::open_or_create(&name, 15, RingSpec::new(16, 0)).unwrap();

    ring.write(NodeId::ZERO, 1, 0, Bytes::new())
        .expect("empty heartbeat-like frame");
    assert!(
        ring.write(NodeId::ZERO, 1, 0, Bytes::from_static(b"x"))
            .is_err()
    );

    let _ = ring.unlink();
}

#[test]
fn shared_ordered_serializes_independent_writer_handles() {
    use std::sync::{Arc, Barrier};

    let name = fresh_name();
    let ring_spec = RingSpec::shared_ordered(2048, 0);
    let first = Arc::new(ShmRing::open_or_create(&name, 16, ring_spec).unwrap());
    let second = Arc::new(ShmRing::open_or_create(&name, 16, ring_spec).unwrap());
    first.reset();
    let barrier = Arc::new(Barrier::new(3));

    let spawn_writer = |ring: Arc<ShmRing>, node_id: NodeId, barrier: Arc<Barrier>| {
        std::thread::spawn(move || {
            barrier.wait();
            (0..1000)
                .map(|_| ring.write(node_id, 1, 0, Bytes::new()).unwrap().counter())
                .collect::<Vec<_>>()
        })
    };
    let writer_one = spawn_writer(first.clone(), NodeId::new(1), barrier.clone());
    let writer_two = spawn_writer(second, NodeId::new(2), barrier.clone());
    barrier.wait();

    let mut counters = writer_one.join().unwrap();
    counters.extend(writer_two.join().unwrap());
    counters.sort_unstable();

    assert_eq!(counters, (0..2000).collect::<Vec<_>>());
    assert_eq!(first.head(), 2000);
    for counter in 0..2000 {
        let frame = first.read_at(counter).expect("ordered frame retained");
        assert_eq!(frame.id.counter(), counter);
    }

    first.unlink().unwrap();
}

#[test]
fn per_node_batch_is_contiguous_and_addressable() {
    let name = fresh_name();
    let ring_spec = RingSpec::per_node(4, 8);
    let ring = ShmRing::open_or_create_for_fleet(&name, 17, ring_spec, 2).unwrap();
    let ids = ring
        .write_batch(
            NodeId::new(1),
            3,
            99,
            vec![
                Bytes::from_static(b"aa"),
                Bytes::from_static(b"bb"),
                Bytes::from_static(b"cc"),
            ],
        )
        .unwrap();

    assert_eq!(ids.len(), 3);
    assert_eq!(ids[0].node(), 1);
    assert_eq!(ids[0].counter(), 0);
    assert_eq!(ids[2].counter(), 2);
    assert_eq!(ring.lane_head(NodeId::new(1)), 3);
    assert_eq!(&ring.read(ids[1]).expect("middle frame").payload[..], b"bb");

    ring.unlink().unwrap();
}

#[test]
fn attached_handles_share_one_semantic_version_counter() {
    let name = fresh_name();
    let ring_spec = RingSpec::per_node(4, 8);
    let first = ShmRing::open_or_create_for_fleet(&name, 18, ring_spec, 2).unwrap();
    let second = ShmRing::open_or_create_for_fleet(&name, 18, ring_spec, 2).unwrap();

    assert_eq!(first.next_version(), 1);
    assert_eq!(second.next_version(), 2);
    assert_eq!(first.next_version(), 3);

    first.unlink().unwrap();
}

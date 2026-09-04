use bytes::Bytes;
use orbit_core::{
    Fleet, Frame, NetId64, NodeId, OrbitTyped, Ring, RingCursor, RingFrameSource, RingRead,
    RingSpec, poll_ring,
};

#[derive(Clone, Debug)]
struct CursorRecord;

impl OrbitTyped for CursorRecord {
    const KIND: u8 = 31;
    const RING_SPEC: RingSpec = RingSpec::new(4, 16);
}

#[derive(Clone, Debug)]
struct SmallWindowRecord;

impl OrbitTyped for SmallWindowRecord {
    const KIND: u8 = 32;
    const RING_SPEC: RingSpec = RingSpec::new(2, 16);
}

#[derive(Clone, Debug)]
struct BatchRecord;

impl OrbitTyped for BatchRecord {
    const KIND: u8 = 33;
    const RING_SPEC: RingSpec = RingSpec::per_node(4, 16);
}

#[test]
fn cursor_from_start_reads_available_history() {
    let ring = Ring::new::<CursorRecord>();
    ring.write(NodeId::ZERO, 0, 0, Bytes::from_static(b"a"));
    ring.write(NodeId::ZERO, 0, 0, Bytes::from_static(b"b"));

    let mut cursor = RingCursor::from_start();
    let poll = poll_ring(&ring, &mut cursor);

    assert_eq!(poll.loss.total(), 0);
    assert_eq!(poll.frames.len(), 2);
    assert_eq!(&poll.frames[0].payload[..], b"a");
    assert_eq!(&poll.frames[1].payload[..], b"b");
    assert_eq!(cursor.next_counter(), 2);
    assert!(poll_ring(&ring, &mut cursor).is_empty());
}

#[test]
fn cursor_at_head_only_reads_future_frames() {
    let fleet = Fleet::join("cursor_head", 1).expect("fleet");
    fleet.publish::<CursorRecord>(0, 0, Bytes::from_static(b"old"));
    let mut cursor = fleet.cursor_at_head::<CursorRecord>();
    fleet.publish::<CursorRecord>(0, 0, Bytes::from_static(b"new"));

    let poll = fleet.poll_ring::<CursorRecord>(&mut cursor);

    assert_eq!(poll.loss.total(), 0);
    assert_eq!(poll.frames.len(), 1);
    assert_eq!(&poll.frames[0].payload[..], b"new");
    assert_eq!(cursor.next_counter(), 2);
}

#[test]
fn lagged_cursor_reports_overwritten_window() {
    let fleet = Fleet::join("cursor_lag", 1).expect("fleet");
    let mut cursor = fleet.cursor_from_start::<SmallWindowRecord>();

    fleet.publish::<SmallWindowRecord>(0, 0, Bytes::from_static(b"one"));
    fleet.publish::<SmallWindowRecord>(0, 0, Bytes::from_static(b"two"));
    fleet.publish::<SmallWindowRecord>(0, 0, Bytes::from_static(b"three"));

    let poll = fleet.poll_ring::<SmallWindowRecord>(&mut cursor);

    assert_eq!(poll.loss.overwritten, 1);
    assert_eq!(poll.loss.unavailable, 0);
    assert_eq!(poll.frames.len(), 2);
    assert_eq!(&poll.frames[0].payload[..], b"two");
    assert_eq!(&poll.frames[1].payload[..], b"three");
    assert_eq!(cursor.next_counter(), 3);
}

#[test]
fn missing_or_unexpected_slots_report_unavailable() {
    struct SparseSource {
        frames: Vec<Option<Frame>>,
    }

    impl RingFrameSource for SparseSource {
        fn kind(&self) -> u8 {
            CursorRecord::KIND
        }

        fn head(&self) -> u64 {
            3
        }

        fn capacity(&self) -> usize {
            3
        }

        fn read_at(&self, counter: u64) -> Option<Frame> {
            self.frames.get(counter as usize).cloned().flatten()
        }
    }

    let source = SparseSource {
        frames: vec![
            Some(Frame {
                id: NetId64::make(CursorRecord::KIND, 0, 0),
                kind: 0,
                ver: 0,
                payload: Bytes::from_static(b"ok"),
            }),
            None,
            Some(Frame {
                id: NetId64::make(CursorRecord::KIND, 0, 99),
                kind: 0,
                ver: 0,
                payload: Bytes::from_static(b"wrapped"),
            }),
        ],
    };
    let mut cursor = RingCursor::from_start();

    let poll = poll_ring(&source, &mut cursor);

    assert_eq!(poll.loss.overwritten, 0);
    assert_eq!(poll.loss.unavailable, 2);
    assert_eq!(poll.frames.len(), 1);
    assert_eq!(&poll.frames[0].payload[..], b"ok");
    assert_eq!(cursor.next_counter(), 3);
}

#[test]
fn pending_counter_does_not_advance_cursor() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DeferredSource {
        committed: AtomicBool,
    }

    impl RingFrameSource for DeferredSource {
        fn kind(&self) -> u8 {
            CursorRecord::KIND
        }

        fn head(&self) -> u64 {
            1
        }

        fn capacity(&self) -> usize {
            4
        }

        fn read_at(&self, counter: u64) -> Option<Frame> {
            self.committed.load(Ordering::Acquire).then(|| Frame {
                id: NetId64::make(CursorRecord::KIND, 0, counter),
                kind: 0,
                ver: 0,
                payload: Bytes::from_static(b"committed"),
            })
        }

        fn read_state_at(&self, counter: u64) -> RingRead {
            if !self.committed.load(Ordering::Acquire) {
                return RingRead::Pending;
            }
            RingRead::Ready(self.read_at(counter).expect("committed frame"))
        }
    }

    let source = DeferredSource {
        committed: AtomicBool::new(false),
    };
    let mut cursor = RingCursor::from_start();

    let pending = poll_ring(&source, &mut cursor);
    assert!(pending.is_empty());
    assert_eq!(cursor.next_counter(), 0);

    source.committed.store(true, Ordering::Release);
    let committed = poll_ring(&source, &mut cursor);
    assert_eq!(committed.frames.len(), 1);
    assert_eq!(&committed.frames[0].payload[..], b"committed");
    assert_eq!(cursor.next_counter(), 1);
}

#[test]
fn batch_publish_reserves_consecutive_ids_in_one_lane() {
    let fleet = Fleet::join("cursor_batch", 1).expect("fleet");
    let ids = fleet.publish_batch::<BatchRecord>(
        7,
        42,
        vec![
            bytes::Bytes::from_static(b"one"),
            bytes::Bytes::from_static(b"two"),
            bytes::Bytes::from_static(b"three"),
        ],
    );

    assert_eq!(ids.len(), 3);
    assert_eq!(ids[0].counter(), 0);
    assert_eq!(ids[1].counter(), 1);
    assert_eq!(ids[2].counter(), 2);
    assert_eq!(fleet.lane_head::<BatchRecord>(NodeId::ZERO), 3);
    assert_eq!(
        &fleet.read(ids[0]).expect("first frame").payload[..],
        b"one"
    );
    assert_eq!(
        &fleet.read(ids[2]).expect("last frame").payload[..],
        b"three"
    );
}

#[test]
fn per_node_ring_versions_share_one_semantic_sequence() {
    let fleet = Fleet::join_as("cursor_versions", 2, NodeId::new(1)).expect("fleet");

    assert_eq!(fleet.next_ring_version::<BatchRecord>(), 1);
    assert_eq!(fleet.next_ring_version::<BatchRecord>(), 2);
    assert_eq!(fleet.current_ring_version::<BatchRecord>(), 2);
}

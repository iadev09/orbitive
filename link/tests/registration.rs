#![cfg(unix)]

use std::sync::atomic::{AtomicU64, Ordering};

use orbit_link::{Error, InboxGeometry, LinkSegment, LinkSpec};
use orbit_stream::StreamSpec;

const SPEC: LinkSpec = LinkSpec::new(
    2,
    198,
    InboxGeometry::new(8, 128),
    StreamSpec::new(199, 4, 256),
);

struct Scratch(LinkSegment);

impl Drop for Scratch {
    fn drop(&mut self) {
        self.0.unlink().unwrap();
    }
}

fn scratch() -> (String, Scratch) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let name = format!(
        "lr{:x}{:x}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let segment = Scratch(LinkSegment::open(&name, SPEC).unwrap());
    (name, segment)
}

#[test]
fn registration_pressure_reclaims_abandoned_and_withdrawn_destinations() {
    for orderly in [false, true] {
        let (name, source) = scratch();
        let live = source.0.join("example-source", "source", 1).unwrap();
        for round in 0..12 {
            let incarnation = round * 2 + 2;
            let receiver = LinkSegment::open(&name, SPEC).unwrap();
            let destination =
                receiver.join("example-client", "client", incarnation).unwrap();
            drop(receiver);
            source.0.inbox().write(live.lane, b"live queue").unwrap();
            // Every remaining registration is retained by committed work.
            // Its publisher mapping has already closed, but its reader lives.
            for _ in 1..2 * SPEC.fleet_capacity {
                let publisher = LinkSegment::open(&name, SPEC).unwrap();
                publisher.inbox().write(destination.lane, b"reply").unwrap();
            }
            if orderly {
                source.0.inbox().release_lane(destination.lane).unwrap();
            }
            assert!(matches!(
                LinkSegment::open(&name, SPEC),
                Err(Error::InboxFull)
            ));
            assert_eq!(source.0.inbox().depth(destination.lane).unwrap(), 3);

            // A live draining reader's hold prevented cleanup above. Once
            // the kernel releases it, opening must recover without a spare
            // producer record, a replacement consumer, or unlinking SHM.
            drop(destination.hold);
            let replacement = LinkSegment::open(&name, SPEC).unwrap();
            assert_eq!(replacement.inbox().depth(destination.lane).unwrap(), 0);
            assert_eq!(
                replacement.inbox().consumers(destination.lane).unwrap(),
                0
            );
            let mut frame = Vec::new();
            assert!(source.0.inbox().read(live.lane, &mut frame).unwrap());
            assert_eq!(frame, b"live queue");

            // A failed join must not consume open-time death observations.
            assert!(replacement.join("", "client", round * 2 + 3).is_err());
            let joined = replacement
                .join("example-client", "client", round * 2 + 3)
                .unwrap();
            assert!(joined.reclaimed.iter().any(|gone| {
                gone.lane == destination.lane && gone.incarnation == incarnation
            }));
            source.0.inbox().write(joined.lane, b"new reader").unwrap();
            assert!(replacement.inbox().read(joined.lane, &mut frame).unwrap());
            assert_eq!(frame, b"new reader");
            replacement.inbox().release_lane(joined.lane).unwrap();
            drop(joined.hold);
        }
        source.0.inbox().release_lane(live.lane).unwrap();
    }
}

#[test]
fn registration_pressure_preserves_unread_work_for_a_live_consumer() {
    let (name, source) = scratch();
    let live = source.0.join("example-reader", "reader", 1).unwrap();
    for payload in [b"one", b"two", b"end"] {
        let publisher = LinkSegment::open(&name, SPEC).unwrap();
        publisher.inbox().write(live.lane, payload).unwrap();
    }
    assert!(matches!(LinkSegment::open(&name, SPEC), Err(Error::InboxFull)));
    let mut frame = Vec::new();
    for payload in [b"one", b"two", b"end"] {
        assert!(source.0.inbox().read(live.lane, &mut frame).unwrap());
        assert_eq!(frame, payload);
    }
    assert!(!source.0.inbox().read(live.lane, &mut frame).unwrap());
    let next = LinkSegment::open(&name, SPEC).unwrap();
    next.inbox().write(live.lane, b"available again").unwrap();
    assert!(source.0.inbox().read(live.lane, &mut frame).unwrap());
    assert_eq!(frame, b"available again");
    source.0.inbox().release_lane(live.lane).unwrap();
}

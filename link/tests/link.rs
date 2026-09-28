#![cfg(unix)]

use std::sync::atomic::{AtomicU64, Ordering};

use orbit_link::{InboxGeometry, LinkBodies, LinkSegment, LinkSpec};
use orbit_stream::StreamSpec;

const SPEC: LinkSpec =
    LinkSpec::new(4, 198, InboxGeometry::new(8, 256), StreamSpec::new(199, 8, 4 * 1024));

fn fleet_name(tag: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("ol{tag}{:x}{:x}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}

struct Scratch {
    segment: LinkSegment,
    bodies: Option<LinkBodies>
}

impl Scratch {
    fn segment(name: &str) -> Self {
        Self { segment: LinkSegment::open(name, SPEC).unwrap(), bodies: None }
    }

    fn participant(
        name: &str,
        lane: usize,
        incarnation: u64
    ) -> Self {
        let segment = LinkSegment::open(name, SPEC).unwrap();
        let bodies = LinkBodies::for_name(name, lane, incarnation, SPEC).unwrap();
        Self { segment, bodies: Some(bodies) }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = self.segment.unlink();
        if let Some(bodies) = &self.bodies {
            let _ = bodies.unlink();
        }
    }
}

#[test]
fn a_named_inbox_is_shared_and_refuses_a_different_shape() {
    let name = fleet_name("map");
    let owner = Scratch::segment(&name);
    assert!(owner.segment.created());
    owner.segment.inbox().claim_lane(2, "checkout", "worker", 7).unwrap();

    let peer = LinkSegment::open(&name, SPEC).unwrap();
    assert!(!peer.created());
    peer.inbox().write(2, b"application frame").unwrap();

    let mut frame = Vec::new();
    assert!(owner.segment.inbox().read(2, &mut frame).unwrap());
    assert_eq!(frame, b"application frame");

    let wrong = LinkSpec::new(
        SPEC.fleet_capacity,
        SPEC.inbox_kind,
        InboxGeometry::new(16, 256),
        SPEC.streams
    );
    assert!(LinkSegment::open(&name, wrong).is_err());
}

#[test]
fn a_ticket_in_an_inbox_frame_opens_a_reusable_duplex_session() {
    let name = fleet_name("reuse");
    let origin = Scratch::participant(&name, 0, 10);
    let target = Scratch::participant(&name, 1, 11);
    let origin_bodies = origin.bodies.as_ref().unwrap();
    let target_bodies = target.bodies.as_ref().unwrap();
    origin_bodies.streams().reset_all();

    origin.segment.inbox().claim_lane(0, "edge", "edge", 10).unwrap();
    target.segment.inbox().claim_lane(1, "checkout", "worker", 11).unwrap();

    let (mut creator, ticket) = origin_bodies.create().unwrap();
    target.segment.inbox().write(1, ticket.to_string().as_bytes()).unwrap();
    let mut frame = Vec::new();
    assert!(target.segment.inbox().read(1, &mut frame).unwrap());
    let announced = String::from_utf8(frame).unwrap().parse().unwrap();
    let peer = target_bodies.accept(announced).unwrap();

    creator.blocking_write_all(b"first request").unwrap();
    creator.finish().unwrap();
    assert_eq!(peer.blocking_read_chunk(64).unwrap().as_ref(), b"first request");
    assert!(peer.blocking_read_chunk(64).unwrap().is_empty());
    drop(peer);

    origin_bodies.rearm_endpoint(&mut creator).unwrap();
    let peer = target_bodies.accept(ticket).unwrap();
    creator.blocking_write_all(b"second request").unwrap();
    assert_eq!(peer.blocking_read_chunk(64).unwrap().as_ref(), b"second request");
}

#[test]
fn link_kinds_are_explicit_and_must_be_distinct() {
    let invalid = LinkSpec::new(4, 200, InboxGeometry::new(8, 256), StreamSpec::new(200, 8, 4096));
    assert!(LinkSegment::open(&fleet_name("kinds"), invalid).is_err());
    assert!(LinkBodies::for_name(&fleet_name("lane"), 4, 1, SPEC).is_err());
}

#[test]
fn a_join_reclaims_the_exact_dead_incarnation_and_ends_its_streams() {
    let name = fleet_name("reclaim");
    let held = Scratch::segment(&name);
    let live = held.segment.join("checkout", "worker", 10).unwrap();
    let gone = held.segment.join("checkout", "worker", 20).unwrap();
    assert_eq!((live.lane, gone.lane), (0, 1));

    let live_bodies = LinkBodies::for_name(&name, live.lane, 10, SPEC).unwrap();
    live_bodies.streams().reset_all();
    let gone_bodies = LinkBodies::for_name(&name, gone.lane, 20, SPEC).unwrap();
    let (creator, ticket) = live_bodies.create().unwrap();
    let peer = gone_bodies.accept(ticket).unwrap();

    drop(gone.hold);
    let replacement = held.segment.join("checkout", "worker", 30).unwrap();
    assert_eq!(replacement.lane, 1);
    assert_eq!(replacement.reclaimed.len(), 1);
    assert_eq!(replacement.reclaimed[0].lane, 1);
    assert_eq!(replacement.reclaimed[0].incarnation, 20);

    live_bodies.reclaim(&replacement.reclaimed);
    let mut byte = [0_u8; 1];
    assert!(matches!(creator.try_read(&mut byte), Err(orbit_stream::Error::Reset)));

    drop(peer);
    held.segment.inbox().release_lane(live.lane).unwrap();
    held.segment.inbox().release_lane(replacement.lane).unwrap();
    drop((live.hold, replacement.hold));
    live_bodies.unlink().unwrap();
}

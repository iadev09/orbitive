#![cfg(unix)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use orbit_core::{Fleet, NodeId};
use orbit_link::{InboxGeometry, LinkBodies, LinkSegment, LinkSpec};
use orbit_stream::StreamSpec;

const SPEC: LinkSpec = LinkSpec::new(
    4,
    198,
    InboxGeometry::new(8, 256),
    StreamSpec::new(199, 8, 4 * 1024),
);

fn fleet_name(tag: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "ol{tag}{:x}{:x}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

struct Scratch {
    segment: LinkSegment,
    bodies: Option<LinkBodies>,
}

impl Scratch {
    fn segment(name: &str) -> Self {
        Self { segment: LinkSegment::open(name, SPEC).unwrap(), bodies: None }
    }

    fn participant(
        name: &str,
        lane: usize,
        incarnation: u64,
    ) -> Self {
        let segment = LinkSegment::open(name, SPEC).unwrap();
        let bodies =
            LinkBodies::for_name(name, lane, incarnation, SPEC).unwrap();
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
        SPEC.streams,
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
    assert_eq!(
        peer.blocking_read_chunk(64).unwrap().as_ref(),
        b"first request"
    );
    assert!(peer.blocking_read_chunk(64).unwrap().is_empty());
    drop(peer);

    origin_bodies.rearm_endpoint(&mut creator).unwrap();
    let peer = target_bodies.accept(ticket).unwrap();
    creator.blocking_write_all(b"second request").unwrap();
    assert_eq!(
        peer.blocking_read_chunk(64).unwrap().as_ref(),
        b"second request"
    );
}

#[test]
fn link_kinds_are_explicit_and_must_be_distinct() {
    let invalid = LinkSpec::new(
        4,
        200,
        InboxGeometry::new(8, 256),
        StreamSpec::new(200, 8, 4096),
    );
    assert!(LinkSegment::open(&fleet_name("kinds"), invalid).is_err());
    assert!(LinkBodies::for_name(&fleet_name("lane"), 4, 1, SPEC).is_err());

    let name = fleet_name("capacity");
    let fleet = Arc::new(Fleet::join_shm_as(&name, 3, NodeId::ZERO).unwrap());
    assert!(LinkBodies::open(fleet, 1, SPEC).is_err());
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
    assert!(matches!(
        creator.try_read(&mut byte),
        Err(orbit_stream::Error::Reset)
    ));

    drop(peer);
    held.segment.inbox().release_lane(live.lane).unwrap();
    held.segment.inbox().release_lane(replacement.lane).unwrap();
    drop((live.hold, replacement.hold));
    live_bodies.unlink().unwrap();
}

#[cfg(feature = "tokio")]
#[tokio::test(flavor = "current_thread")]
async fn a_tokio_receiver_observes_queued_and_reactor_woken_frames() {
    let name = fleet_name("async");
    let owner = Arc::new(LinkSegment::open(&name, SPEC).unwrap());
    owner.inbox().claim_lane(0, "checkout", "worker", 1).unwrap();
    let peer = LinkSegment::open(&name, SPEC).unwrap();

    peer.inbox().write(0, b"already queued").unwrap();
    let mut receiver = owner.receiver(0).unwrap();
    let mut frame = Vec::new();
    receiver.recv(&mut frame).await.unwrap();
    assert_eq!(frame, b"already queued");

    let waiting = tokio::spawn(async move {
        receiver.recv(&mut frame).await.unwrap();
        frame
    });
    tokio::task::yield_now().await;
    peer.inbox().write(0, b"reactor wake").unwrap();
    assert_eq!(waiting.await.unwrap(), b"reactor wake");

    owner.inbox().release_lane(0).unwrap();
    owner.unlink().unwrap();
}

/// A process hears another claim and release a lane through the lane it
/// already reads: no thread, registry or interval of its own.
#[cfg(feature = "tokio")]
#[tokio::test(flavor = "current_thread")]
async fn a_tokio_receiver_hears_lanes_claimed_and_released() {
    use orbit_link::Received;

    let name = fleet_name("dir");
    let owner = Arc::new(LinkSegment::open(&name, SPEC).unwrap());
    owner.inbox().claim_lane(0, "checkout", "worker", 1).unwrap();
    let peer = LinkSegment::open(&name, SPEC).unwrap();
    let mut receiver = owner.receiver(0).unwrap();

    let waiting = tokio::spawn(async move {
        let mut frame = Vec::new();
        let received = receiver.next(&mut frame).await.unwrap();
        (receiver, received)
    });
    tokio::task::yield_now().await;
    peer.inbox().claim_lane(1, "checkout-edge", "edge", 1).unwrap();
    let (mut receiver, claimed) = waiting.await.unwrap();
    assert_eq!(claimed, Received::Directory(owner.inbox().directory()));

    peer.inbox().release_lane(1).unwrap();
    let mut frame = Vec::new();
    assert_eq!(
        receiver.next(&mut frame).await.unwrap(),
        Received::Directory(owner.inbox().directory())
    );

    owner.inbox().release_lane(0).unwrap();
    owner.unlink().unwrap();
}

/// A process that holds no lane — a master — is woken by the same claims and
/// releases, through a watcher slot that never shows up in the directory.
#[cfg(feature = "tokio")]
#[tokio::test(flavor = "current_thread")]
async fn a_watcher_without_a_lane_hears_lanes_claimed_and_released() {
    let name = fleet_name("watch");
    let master = Arc::new(LinkSegment::open(&name, SPEC).unwrap());
    let worker = LinkSegment::open(&name, SPEC).unwrap();
    let mut watcher = master.watch("worker").unwrap();
    assert_eq!(
        master.inbox().watcher(watcher.slot()).unwrap(),
        Some(std::process::id())
    );

    let waiting = tokio::spawn(async move {
        let moved = watcher.next().await.unwrap();
        (watcher, moved)
    });
    tokio::task::yield_now().await;
    worker.inbox().claim_lane(0, "checkout", "worker", 1).unwrap();
    let (mut watcher, claimed) = waiting.await.unwrap();
    assert_eq!(claimed, master.inbox().directory());
    assert!(master.inbox().serving("checkout").contains(&0));

    worker.inbox().release_lane(0).unwrap();
    assert_eq!(watcher.next().await.unwrap(), master.inbox().directory());
    assert!(master.inbox().serving("checkout").is_empty());

    let slot = watcher.slot();
    drop(watcher);
    assert_eq!(master.inbox().watcher(slot).unwrap(), None);
    master.unlink().unwrap();
}

/// One event crosses to the side that wants it, once, and does not come back:
/// the edge publishes two topics, the application wants one, and the
/// application's republication is not sent back over the same segment.
#[cfg(feature = "events")]
#[test]
fn an_event_crosses_once_to_the_side_that_wants_it() {
    use orbit_events::FleetEventBus;
    use orbit_link::{LinkEvents, Routing};

    let name = fleet_name("events");
    let segment = Arc::new(LinkSegment::open(&name, SPEC).unwrap());
    segment.inbox().claim_lane(0, "web", "edge", 1).unwrap();
    segment.inbox().claim_lane(1, "shop", "php", 1).unwrap();

    let edge_bus = Arc::new(FleetEventBus::new(Arc::new(
        Fleet::join(&format!("{name}e"), 1).unwrap(),
    )));
    let app_bus = Arc::new(FleetEventBus::new(Arc::new(
        Fleet::join(&format!("{name}a"), 1).unwrap(),
    )));
    let edge = LinkEvents::for_lane(
        &segment,
        0,
        Arc::clone(&edge_bus),
        |role| role != "edge",
        Routing::Topic,
    )
    .unwrap();
    let app = LinkEvents::for_lane(
        &segment,
        1,
        Arc::clone(&app_bus),
        |role| role == "edge",
        Routing::Topic,
    )
    .unwrap();
    app.interest(&["cache.reset"]);
    edge.interest(&["cache.reset"]);

    edge_bus.publish("cache.reset", b"all").unwrap();
    edge_bus.publish("worker.booted", b"w1").unwrap();
    edge.pump().unwrap();

    let mut frame = Vec::new();
    assert!(segment.inbox().read(1, &mut frame).unwrap());
    assert_eq!(app.accept(&frame).unwrap(), ("cache.reset", &b"all"[..]));
    assert!(
        !segment.inbox().read(1, &mut frame).unwrap(),
        "only the wanted topic crossed"
    );

    let received = app_bus.poll(&mut app_bus.cursor_from_start()).events;
    assert_eq!(received.len(), 1);
    assert_eq!(
        (received[0].topic.as_str(), received[0].payload.as_slice()),
        ("cache.reset", &b"all"[..])
    );

    app.pump().unwrap();
    assert!(
        !segment.inbox().read(0, &mut frame).unwrap(),
        "the republication did not go back"
    );

    drop((edge, app));
    segment.unlink().unwrap();
}

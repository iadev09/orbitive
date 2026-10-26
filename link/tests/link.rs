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

#[cfg(target_os = "linux")]
#[test]
fn an_abstract_bell_name_must_fit_before_a_link_is_opened() {
    let name = "a".repeat(96);
    let error = orbit_link::check_name(&name, SPEC).unwrap_err();
    assert!(matches!(
        error,
        orbit_link::Error::InvalidName { reason, .. }
            if reason.contains("reactor socket address")
    ));
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
fn live_link_ownership_ignores_the_runtime_environment() {
    use std::os::unix::fs::DirBuilderExt;
    use std::process::Command;

    const CHILD: &str = "ORBIT_LINK_ENVIRONMENT_TEST";
    if let Ok(name) = std::env::var(CHILD) {
        let peer = LinkSegment::open(&name, SPEC).unwrap();
        assert!(peer.reclaim_if_gone(0).unwrap().is_none());
        let joined = peer.join("example-peer", "native", 20).unwrap();
        assert_ne!(joined.lane, 0, "a live owner's lane was stolen");
        assert!(joined.reclaimed.iter().all(|gone| gone.lane != 0));
        peer.inbox().write(0, b"still owned").unwrap();
        peer.inbox().release_lane(joined.lane).unwrap();
        return;
    }

    let name = fleet_name("env");
    let owner = Scratch::segment(&name);
    let joined = owner.segment.join("example-owner", "native", 10).unwrap();
    assert_eq!(joined.lane, 0);
    struct RuntimeDirectory(std::path::PathBuf);
    impl Drop for RuntimeDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let runtime = RuntimeDirectory(std::env::temp_dir().join(&name));
    std::fs::DirBuilder::new().mode(0o700).create(&runtime.0).unwrap();
    for configured in [true, false] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "live_link_ownership_ignores_the_runtime_environment",
                "--nocapture",
            ])
            .env(CHILD, &name);
        if configured {
            command.env("XDG_RUNTIME_DIR", &runtime.0);
        } else {
            command.env_remove("XDG_RUNTIME_DIR");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let mut frame = Vec::new();
        assert!(owner.segment.inbox().read(joined.lane, &mut frame).unwrap());
        assert_eq!(frame, b"still owned");
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

#[test]
fn process_recovery_is_fenced_and_covers_withdrawn_lanes() {
    let name = fleet_name("exit");
    let held = Scratch::segment(&name);
    let live = held.segment.join("checkout", "worker", 10).unwrap();
    let gone = held.segment.join("checkout", "worker", 20).unwrap();
    let pid = std::process::id();
    held.segment.inbox().release_lane(gone.lane).unwrap();
    assert_eq!(held.segment.inbox().owner(gone.lane).unwrap(), None);
    assert_eq!(
        held.segment.inbox().last_owner(gone.lane).unwrap(),
        Some((pid, 20))
    );
    assert!(
        held.segment
            .reclaim_process(pid, 20, |_| panic!("live hold"))
            .unwrap()
            .is_empty()
    );
    drop(gone.hold);
    assert!(
        held.segment
            .reclaim_process(pid, 21, |_| panic!("wrong incarnation"))
            .unwrap()
            .is_empty()
    );
    let dead = held
        .segment
        .reclaim_process(pid, 20, |dead| {
            // A replacement cannot steal the identity while stream cleanup runs.
            assert_eq!(dead.lane, gone.lane);
            assert!(
                held.segment
                    .reclaim_process(pid, 20, |_| panic!("reentrant recovery"))
                    .unwrap()
                    .is_empty()
            );
        })
        .unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(held.segment.inbox().consumers(live.lane).unwrap(), 1);
    let replacement = held.segment.join("checkout", "worker", 30).unwrap();
    assert_eq!(replacement.lane, gone.lane);
    assert!(
        held.segment
            .reclaim_process(pid, 20, |_| panic!("replacement"))
            .unwrap()
            .is_empty()
    );
    held.segment.inbox().release_lane(live.lane).unwrap();
    held.segment.inbox().release_lane(replacement.lane).unwrap();
    drop((live.hold, replacement.hold));
}

#[test]
fn joining_a_withdrawn_lane_recovers_before_reusing_the_same_incarnation() {
    let name = fleet_name("drain");
    let held = Scratch::segment(&name);
    let live = held.segment.join("checkout", "worker", 10).unwrap();
    let gone = held.segment.join("checkout", "worker", 20).unwrap();
    let local = LinkBodies::for_name(&name, live.lane, 10, SPEC).unwrap();
    let remote = LinkBodies::for_name(&name, gone.lane, 20, SPEC).unwrap();
    let (old, ticket) = local.create().unwrap();
    let old_remote = remote.accept(ticket).unwrap();
    held.segment.inbox().release_lane(gone.lane).unwrap();
    drop(gone.hold);

    let next = held.segment.join("checkout", "worker", 20).unwrap();
    assert_eq!(next.lane, gone.lane);
    let mut bytes = [0; 1];
    assert!(matches!(
        old.try_read(&mut bytes),
        Err(orbit_stream::Error::Reset)
    ));
    drop((old, old_remote));
    let (fresh, ticket) = local.create().unwrap();
    let fresh_remote = remote.accept(ticket).unwrap();
    fresh.blocking_write_all(b"x").unwrap();
    assert_eq!(fresh_remote.blocking_read_chunk(1).unwrap().as_ref(), b"x");
    local.unlink().unwrap();
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

//! The table lives in shared memory: a second fleet handle, as another
//! process would open it, holds the other end of the same stream.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::sync::Arc;

use orbit_core::{Fleet, NodeId};
use orbit_stream::{Error, Incarnation, STREAM_BUFFER_BYTES, StreamSpec, Streams};
use std::time::Duration;

fn fleet_name(tag: &str) -> &'static str {
    Box::leak(format!("st{tag}{:x}", std::process::id()).into_boxed_str())
}

#[test]
fn a_second_mapping_holds_the_other_end() {
    let name = fleet_name("a");
    let owner = Streams::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet")),
        Incarnation::new(10),
    )
    .expect("owner streams");
    owner.reset_all();

    let (mut a, ticket) = owner.create().expect("create");
    let text = ticket.to_string();

    let peer = Streams::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet")),
        Incarnation::new(11),
    )
    .expect("peer streams");
    let b = peer.open(text.parse().expect("ticket")).expect("open");

    let body = (0..3 * STREAM_BUFFER_BYTES + 17)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    let expected = body.clone();
    let writer = std::thread::spawn(move || {
        a.blocking_write_all(&body).expect("write");
        a.finish().expect("shutdown");
    });
    let mut out = Vec::new();
    loop {
        let chunk = b.blocking_read_chunk(4_096).expect("read");
        if chunk.is_empty() {
            break;
        }
        out.extend_from_slice(&chunk);
    }
    writer.join().unwrap();
    assert_eq!(out, expected);

    // B is still held here; A ended with the writer thread.
    assert!(matches!(owner.open(ticket), Err(Error::AlreadyClaimed(_))));
    drop(b);
    assert!(matches!(owner.open(ticket), Err(Error::Stale(_))));
    assert!(!peer.is_live(ticket.id));
    owner.unlink().expect("unlink");
}

#[test]
fn the_peer_lane_allocates_independently() {
    let name = fleet_name("b");
    let owner = Streams::new(
        Arc::new(Fleet::join_shm_as(name, 3, NodeId::ZERO).expect("owner fleet")),
        Incarnation::new(10),
    )
    .expect("owner streams");
    owner.reset_all();
    let peer = Streams::new(
        Arc::new(Fleet::join_shm_as(name, 3, NodeId::new(2)).expect("peer fleet")),
        Incarnation::new(11),
    )
    .expect("peer streams");

    let (_a, from_owner) = owner.create().expect("create");
    let (_p, from_peer) = peer.create().expect("create");
    assert_eq!(from_owner.id.node(), 0);
    assert_eq!(from_peer.id.node(), 2);
    assert!(owner.is_live(from_peer.id));
    assert!(peer.is_live(from_owner.id));
    owner.unlink().expect("unlink");
}

#[test]
fn a_dead_peer_ends_its_side_and_the_survivor_keeps_the_slot_until_it_lets_go() {
    let name = fleet_name("c");
    let owner = Streams::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet")),
        Incarnation::new(10),
    )
    .expect("owner streams");
    owner.reset_all();
    let peer = Streams::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet")),
        Incarnation::new(11),
    )
    .expect("peer streams");

    let (a, ticket) = owner.create().expect("create");
    let b = peer.open(ticket).expect("open");
    let reader = std::thread::spawn(move || {
        let mut buf = [0_u8; 8];
        let outcome = a.blocking_read(&mut buf);
        (a, outcome)
    });
    std::thread::sleep(Duration::from_millis(30));

    // A stale report about an earlier life of node 1 changes nothing.
    owner.node_dead(NodeId::new(1), Incarnation::new(3));
    assert!(owner.is_live(ticket.id));

    // Node 1's current incarnation is confirmed dead: A's reader wakes with
    // the reset, A's writes fail, nobody can claim B, and the slot stays
    // live for A until A lets go.
    owner.node_dead(NodeId::new(1), Incarnation::new(11));
    let (a, outcome) = reader.join().unwrap();
    assert!(matches!(outcome, Err(Error::Reset)));
    assert!(matches!(a.try_write(b"x"), Err(Error::PeerGone)));
    assert!(matches!(owner.open(ticket), Err(Error::AlreadyClaimed(_))));
    assert!(owner.is_live(ticket.id));
    drop(a);
    assert!(!owner.is_live(ticket.id));
    drop(b);
    owner.unlink().expect("unlink");
}

#[test]
fn an_offer_reaches_the_other_node() {
    let name = fleet_name("o");
    let owner = Streams::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet")),
        Incarnation::new(10),
    )
    .expect("owner streams");
    owner.reset_all();
    let peer = Streams::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet")),
        Incarnation::new(11),
    )
    .expect("peer streams");

    let taker = peer.clone();
    let waiter = std::thread::spawn(move || taker.blocking_take_offer());
    std::thread::sleep(Duration::from_millis(30));
    let (mut a, ticket) = owner.create().expect("create");
    owner.offer(ticket, NodeId::new(1)).expect("offer");
    let offered = waiter.join().unwrap().expect("offer arrives");
    assert_eq!(offered, ticket);
    assert!(peer.take_offer().is_none());

    let b = peer.open(offered).expect("open");
    a.blocking_write_all(b"via offer").expect("write");
    a.finish().expect("finish");
    assert_eq!(
        b.blocking_read_chunk(32).expect("read").as_ref(),
        b"via offer"
    );
    owner.unlink().expect("unlink");
}

/// Two specs are two tables. A relay carrying response bodies wants a ring
/// sized to a body; a control channel carrying short frames wants a small
/// one and a lane of its own. Both live in the same fleet at the same
/// time, and nothing crosses: not the rings, not the lanes, not the epoch.
#[test]
fn two_specs_are_two_tables_in_one_fleet_and_one_process() {
    const BODIES: StreamSpec = StreamSpec::new(230, 16, 256 * 1024);
    const CONTROL: StreamSpec = StreamSpec::new(231, 8, 4 * 1024);

    let name = fleet_name("p");
    let fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("fleet"));
    let peer_fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet"));

    let bodies = Streams::with_spec(Arc::clone(&fleet), Incarnation::new(10), BODIES).unwrap();
    bodies.reset_all();
    let control = Streams::with_spec(Arc::clone(&fleet), Incarnation::new(10), CONTROL).unwrap();
    control.reset_all();
    assert_eq!(bodies.kind(), 230);
    assert_eq!(control.kind(), 231);
    assert_eq!(BODIES.lane_bytes(), 8 * 1024 * 1024);
    assert_eq!(CONTROL.lane_bytes(), 64 * 1024);
    assert!(orbit_stream::segment_size_for(2, BODIES) > orbit_stream::segment_size_for(2, CONTROL));

    // Each table's ring is its own: the body stream takes a quarter of a
    // megabyte in one write, the control stream fills at 4 KiB.
    let peer_bodies =
        Streams::with_spec(Arc::clone(&peer_fleet), Incarnation::new(11), BODIES).unwrap();
    let (a, ticket) = bodies.create().unwrap();
    let b = peer_bodies.open(ticket).unwrap();
    let big = vec![9_u8; 256 * 1024];
    assert_eq!(a.try_write(&big).unwrap(), 256 * 1024);
    assert!(matches!(a.try_write(b"more"), Err(Error::WouldBlock)));

    let peer_control =
        Streams::with_spec(Arc::clone(&peer_fleet), Incarnation::new(11), CONTROL).unwrap();
    let (c, control_ticket) = control.create().unwrap();
    let d = peer_control.open(control_ticket).unwrap();
    assert_eq!(c.try_write(&big).unwrap(), 4 * 1024);

    // An address from one table means nothing in the other, and a new
    // epoch on one leaves the other's live streams alone.
    assert!(matches!(control.open(ticket), Err(Error::Malformed(_))));
    control.reset_all();
    assert!(bodies.is_live(ticket.id));
    let mut sink = vec![0_u8; 8];
    assert_eq!(b.try_read(&mut sink).unwrap(), 8);
    drop((a, b, c, d));

    let _ = bodies.unlink();
    let _ = control.unlink();
}

/// The same segment cannot be opened twice under two geometries, and a
/// ring that is not a power of two is refused before any segment is made.
#[test]
fn one_kind_has_one_geometry_in_a_process() {
    let name = fleet_name("q");
    let fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("fleet"));
    let first =
        Streams::with_spec(Arc::clone(&fleet), Incarnation::new(10), StreamSpec::new(232, 8, 4096))
            .expect("first table");
    let second =
        Streams::with_spec(Arc::clone(&fleet), Incarnation::new(10), StreamSpec::new(232, 8, 8192));
    assert!(matches!(second, Err(Error::Malformed(_))));
    let odd =
        Streams::with_spec(Arc::clone(&fleet), Incarnation::new(10), StreamSpec::new(233, 8, 3000));
    assert!(matches!(odd, Err(Error::Malformed(_))));
    let _ = first.unlink();
}

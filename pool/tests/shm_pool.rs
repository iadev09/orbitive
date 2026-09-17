//! The table lives in shared memory: a second fleet handle, as another
//! process would open it, sees the same resources and reserves on them.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::sync::Arc;
use std::time::Duration;

use orbit_core::{Fleet, NodeId};
use orbit_pool::{Error, Incarnation, Key, Limits, LocalFirst, Plan, Pool};

const KEY: Key = Key::new(0xBEEF);

fn fleet_name(tag: &str) -> &'static str {
    Box::leak(format!("pl{tag}{:x}", std::process::id()).into_boxed_str())
}

fn pair(tag: &str) -> (Pool, Pool) {
    let name = fleet_name(tag);
    let owner = Pool::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet")),
        Incarnation::new(10),
    )
    .expect("owner pool");
    owner.reset_all();
    let peer = Pool::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet")),
        Incarnation::new(11),
    )
    .expect("peer pool");
    (owner, peer)
}

#[test]
fn a_remote_reservation_is_accepted_and_completed_by_the_owner() {
    let (owner, peer) = pair("a");
    let id = owner.register(KEY, 1).expect("register");
    let limits = Limits {
        max_live: 1,
        attempts: 2,
    };

    let Plan::RemoteReuse(lease) = peer.acquire(KEY, &limits, &LocalFirst).expect("acquire") else {
        panic!("the peer must see the owner's resource as remote");
    };
    assert_eq!(lease.holder, NodeId::new(1));
    // The peer cannot accept what it does not own; the owner can.
    assert!(matches!(peer.accept(lease), Err(Error::NotOwner(_))));
    let execution = owner.accept(lease).expect("accept");
    assert!(matches!(peer.reserve(id), Err(Error::Busy(_))));
    assert!(matches!(
        peer.acquire(KEY, &limits, &LocalFirst).expect("acquire"),
        Plan::Wait(_)
    ));

    let since = peer.version(KEY).expect("version");
    let waiter = {
        let peer = peer.clone();
        std::thread::spawn(move || peer.wait_capacity(KEY, since))
    };
    std::thread::sleep(Duration::from_millis(30));
    execution.complete();
    assert!(waiter.join().unwrap().expect("woken") > since);
    assert!(peer.reserve(id).is_ok());
    owner.unlink().expect("unlink");
}

#[test]
fn a_dead_owner_takes_its_resources_with_it_but_not_the_peers() {
    let (owner, peer) = pair("d");
    let mine = owner.register(KEY, 1).expect("register");
    let theirs = peer.register(KEY, 1).expect("register");
    assert_eq!(peer.candidates(KEY).len(), 2);
    let lease = peer.reserve(mine).expect("reserve");

    peer.node_dead(NodeId::ZERO, Incarnation::new(3));
    assert!(peer.is_current(lease));
    peer.node_dead(NodeId::ZERO, Incarnation::new(10));
    assert!(!peer.is_current(lease));
    assert!(matches!(owner.accept(lease), Err(Error::Stale(_))));
    let left = peer.candidates(KEY);
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].id, theirs);
    owner.unlink().expect("unlink");
}

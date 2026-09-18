//! The table lives in shared memory: a second fleet handle, as another
//! process would open it, sees the same resources and reserves on them.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::sync::Arc;
use std::time::Duration;

use orbit_core::{Fleet, NodeId};
use orbit_pool::{Error, Incarnation, Key, Limits, LocalFirst, Plan, Pool, PoolSpec};

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

/// Two specs are two pools. An upstream's origin connections, an FCGI
/// client's sockets and a worker pool have neither the same shape nor the
/// same life: each names its own kind and its own capacities, and one
/// fleet carries all of them at once. Nothing crosses: not the budget,
/// not the key space, not the epoch.
#[test]
fn two_specs_are_two_pools_in_one_fleet_and_one_process() {
    const UPSTREAM: PoolSpec = PoolSpec::new(240, 64, 32);
    const WORKERS: PoolSpec = PoolSpec::new(241, 8, 4);

    let name = fleet_name("s");
    let fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("fleet"));
    let upstream =
        Pool::with_spec(Arc::clone(&fleet), Incarnation::new(10), UPSTREAM).expect("upstream pool");
    upstream.reset_all();
    let workers =
        Pool::with_spec(Arc::clone(&fleet), Incarnation::new(10), WORKERS).expect("worker pool");
    workers.reset_all();

    assert_eq!(upstream.kind(), 240);
    assert_eq!(workers.kind(), 241);
    assert!(orbit_pool::segment_size_for(2, UPSTREAM) > orbit_pool::segment_size_for(2, WORKERS));

    // The same key in both is two different keys.
    let origin = upstream.register(KEY, 2).expect("register upstream");
    let worker = workers.register(KEY, 1).expect("register worker");
    assert_eq!(origin.kind(), 240);
    assert_eq!(worker.kind(), 241);
    assert_eq!(upstream.budget(KEY), (1, 0));
    assert_eq!(workers.budget(KEY), (1, 0));

    // One pool's creation budget is not the other's.
    let permit = workers.claim_create(KEY, 2).expect("worker claim");
    assert_eq!(workers.budget(KEY), (1, 1));
    assert_eq!(upstream.budget(KEY), (1, 0));
    permit.finish();

    // Neither does an address from one pool mean anything in the other.
    assert!(matches!(upstream.reserve(worker), Err(Error::Malformed(_))));

    // And a new epoch on one leaves the other's resources alone.
    let lease = upstream.reserve(origin).expect("reserve");
    workers.reset_all();
    assert!(upstream.is_current(lease));
    assert!(upstream.accept(lease).is_ok());
    assert!(workers.reserve(worker).is_err());

    let _ = upstream.unlink();
    let _ = workers.unlink();
}

/// The same segment cannot be opened twice under two geometries: the
/// process refuses it rather than hand back a table that is not the one
/// asked for.
#[test]
fn one_kind_has_one_geometry_in_a_process() {
    let name = fleet_name("g");
    let fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("fleet"));
    let first = Pool::with_spec(Arc::clone(&fleet), Incarnation::new(10), PoolSpec::new(242, 64, 32))
        .expect("first pool");
    let second = Pool::with_spec(Arc::clone(&fleet), Incarnation::new(10), PoolSpec::new(242, 8, 8));
    assert!(matches!(second, Err(Error::Malformed(_))));
    let odd = Pool::with_spec(Arc::clone(&fleet), Incarnation::new(10), PoolSpec::new(243, 3, 8));
    assert!(matches!(odd, Err(Error::Malformed(_))));
    let _ = first.unlink();
}

/// The admission window a consumer actually has: wait this long for
/// capacity, then answer busy. The deadline is the caller's, and the
/// release that ends the wait comes from the other node.
#[test]
fn waiting_for_capacity_can_be_bounded() {
    let (owner, peer) = pair("t");
    let id = owner.register(KEY, 1).expect("register");
    let lease = peer.reserve(id).expect("reserve");
    let execution = owner.accept(lease).expect("accept");

    // Busy, and it stays busy: the wait ends at the deadline.
    let since = peer.version(KEY).expect("version");
    assert!(matches!(peer.reserve(id), Err(Error::Busy(_))));
    let started = std::time::Instant::now();
    assert!(
        peer.wait_capacity_timeout(KEY, since, Duration::from_millis(150))
            .expect("wait")
            .is_none()
    );
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(100), "gave up after {waited:?}");
    assert!(waited < Duration::from_secs(5), "waited {waited:?}");

    // The owner completes, and the same call answers before its deadline.
    let since = peer.version(KEY).expect("version");
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        execution.complete();
    });
    let version = peer
        .wait_capacity_timeout(KEY, since, Duration::from_secs(5))
        .expect("wait")
        .expect("capacity came back");
    assert!(version > since);
    assert!(peer.reserve(id).is_ok());
}

//! Tasks parked on a key wake through the process's doorbell driver when
//! the owner completes, across two fleet nodes in shared memory.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::future::poll_fn;
use std::sync::Arc;
use std::time::Duration;

use orbit_core::{Fleet, NodeId};
use orbit_pool::{Error, Incarnation, Key, Pool};

const KEY: Key = Key::new(0xFACE);

fn fleet_name(tag: &str) -> &'static str {
    Box::leak(format!("pt{tag}{:x}", std::process::id()).into_boxed_str())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_tasks_are_woken_when_the_owner_completes() {
    let name = fleet_name("w");
    let owner = Pool::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet")),
        Incarnation::new(10)
    )
    .expect("owner pool");
    owner.reset_all();
    let peer = Pool::new(
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet")),
        Incarnation::new(11)
    )
    .expect("peer pool");

    let id = owner.register(KEY, 1).expect("register");
    let execution = owner.accept(owner.reserve(id).expect("reserve")).expect("accept");
    assert!(matches!(peer.reserve(id), Err(Error::Busy(_))));

    // Several tasks on the peer node wait for the same key.
    let since = peer.version(KEY).expect("version");
    let waiters = (0..4)
        .map(|_| {
            let peer = peer.clone();
            tokio::spawn(async move {
                poll_fn(|cx| peer.poll_capacity(KEY, since, cx)).await.expect("woken")
            })
        })
        .collect::<Vec<_>>();
    tokio::time::sleep(Duration::from_millis(50)).await;
    execution.complete();
    for waiter in waiters {
        let now = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("woken in time")
            .unwrap();
        assert!(now > since);
    }
    // And the winner of the race gets the unit; the rest see busy.
    let taken = peer.reserve(id).expect("reserve");
    assert!(matches!(peer.reserve(id), Err(Error::Busy(_))));
    drop(owner.accept(taken).expect("accept"));
    owner.unlink().expect("unlink");
}

//! The loop an embedded worker actually runs.
//!
//! One poll set, two descriptors — the next request on a stream, capacity
//! in the pool — and one deadline on the `poll` call itself. This is why
//! neither crate grew a timeout of its own: a consumer that can only
//! block on one word would have to choose which of the two to wait for.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use orbit_core::{Fleet, NodeId};
use orbit_pool::{Error, Incarnation, Key, Pool, Readiness};
use orbit_stream::Streams;

const KEY: Key = Key::new(0xF00D);
const MAX_LIVE: u32 = 2;

fn fleet_name(tag: &str) -> &'static str {
    Box::leak(format!("pr{tag}{:x}", std::process::id()).into_boxed_str())
}

/// Which of the two descriptors became readable within `millis`.
fn poll_two(first: &Readiness, second: &Readiness, millis: i32) -> (bool, bool) {
    let mut watched = [
        libc::pollfd { fd: first.as_raw_fd(), events: libc::POLLIN, revents: 0 },
        libc::pollfd { fd: second.as_raw_fd(), events: libc::POLLIN, revents: 0 },
    ];
    // SAFETY: two descriptors this test owns, and a timeout.
    let ready = unsafe { libc::poll(watched.as_mut_ptr(), 2, millis) };
    if ready <= 0 {
        return (false, false);
    }
    (
        watched[0].revents & libc::POLLIN != 0,
        watched[1].revents & libc::POLLIN != 0,
    )
}

fn quieten(first: &Readiness, second: &Readiness) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (a, b) = poll_two(first, second, 50);
        if !a && !b {
            return;
        }
        if a {
            first.drain().expect("drain");
        }
        if b {
            second.drain().expect("drain");
        }
        assert!(Instant::now() < deadline, "the descriptors never fell quiet");
    }
}

#[test]
fn a_worker_waits_for_a_request_and_for_capacity_in_one_poll_set() {
    let name = fleet_name("w");
    let producer = Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("producer fleet"));
    let worker = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("worker fleet"));

    let owner_pool = Pool::new(Arc::clone(&producer), Incarnation::new(10)).expect("owner pool");
    owner_pool.reset_all();
    let owner_streams =
        Streams::new(Arc::clone(&producer), orbit_stream::Incarnation::new(10)).expect("streams");
    owner_streams.reset_all();
    let worker_pool = Pool::new(Arc::clone(&worker), Incarnation::new(11)).expect("worker pool");
    let worker_streams =
        Streams::new(Arc::clone(&worker), orbit_stream::Incarnation::new(11)).expect("streams");

    let requests = worker_streams.readiness().expect("stream descriptor");
    let capacity = worker_pool.readiness().expect("pool descriptor");
    assert!(matches!(worker_pool.readiness(), Err(Error::Malformed(_))));

    // The fleet's budget is spent: one registered resource and one
    // creation in progress, both on the other node.
    let _id = owner_pool.register(KEY, 1).expect("register");
    let permit = owner_pool.claim_create(KEY, MAX_LIVE).expect("the last unit");
    assert!(matches!(
        worker_pool.claim_create(KEY, MAX_LIVE),
        Err(Error::CreationBudget { .. })
    ));

    quieten(&requests, &capacity);

    // Nothing to do and nothing to take: the deadline is the consumer's,
    // and it passes. This is where a worker answers "busy".
    worker_pool.watch(KEY).expect("watch");
    assert_eq!(poll_two(&requests, &capacity, 100), (false, false));

    // A request arrives.
    let (endpoint, ticket) = owner_streams.create().expect("create");
    endpoint.try_write(b"work").expect("write");
    owner_streams.offer(ticket, NodeId::new(1)).expect("offer");
    let (asked, _) = poll_two(&requests, &capacity, 5_000);
    assert!(asked, "the request never reached the descriptor");
    requests.drain().expect("drain");
    let offered = worker_streams.take_offer().expect("an offer");
    let taken = worker_streams.open(offered).expect("open");
    let mut sink = [0_u8; 4];
    assert_eq!(taken.try_read(&mut sink).expect("read"), 4);
    assert_eq!(&sink, b"work");

    // And capacity opens, from the other node, on the other descriptor.
    // The loop is the worker's own: whatever is ready gets handled, and
    // the wait continues for what is not. A `poll` returns on the first
    // descriptor that speaks, which here is often the stream answering
    // for the traffic just handled.
    worker_pool.watch(KEY).expect("re-arm");
    permit.finish();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut freed = false;
    while !freed && Instant::now() < deadline {
        let (asked, capacity_ready) = poll_two(&requests, &capacity, 100);
        if asked {
            requests.drain().expect("drain");
        }
        if capacity_ready {
            capacity.drain().expect("drain");
            freed = true;
        }
    }
    assert!(freed, "the released claim never reached the descriptor");
    worker_pool
        .claim_create(KEY, MAX_LIVE)
        .expect("capacity came back")
        .finish();

    let _ = owner_pool.unlink();
    let _ = owner_streams.unlink();
}

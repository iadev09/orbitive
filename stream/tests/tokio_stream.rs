//! The halves as Tokio I/O: a copy across two fleet nodes in shared memory,
//! duplex at the same time, and a task parked on an empty ring woken by the
//! doorbell driver rather than by a timer.

#![cfg(all(
    feature = "tokio",
    any(target_os = "linux", target_os = "freebsd", target_os = "macos")
))]

use std::sync::Arc;
use std::time::Duration;

use orbit_core::{Fleet, NodeId};
use orbit_stream::{Incarnation, STREAM_BUFFER_BYTES, Streams};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn fleet_name(tag: &str) -> &'static str {
    Box::leak(format!("sk{tag}{:x}", std::process::id()).into_boxed_str())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_closed_wakes_on_reader_departure_from_an_empty_shared_ring() {
    use std::future::{Future, poll_fn};

    let name = fleet_name("close");
    let table = |node| {
        Streams::new(
            Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(node)).unwrap()),
            Incarnation::new(1),
        )
        .unwrap()
    };
    let owner = table(0);
    let peer = table(1);
    let (origin, ticket) = owner.create().unwrap();
    let remote = peer.open(ticket).unwrap();
    let (_incoming, mut outgoing) = origin.split();
    let (reader, _writer) = remote.split();
    let (parked, ready) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(async move {
        let mut closed = std::pin::pin!(outgoing.peer_closed());
        let mut parked = Some(parked);
        poll_fn(|cx| {
            let result = closed.as_mut().poll(cx);
            if result.is_pending()
                && let Some(parked) = parked.take()
            {
                parked.send(()).unwrap();
            }
            result
        })
        .await
    });
    ready.await.unwrap();
    // No bytes, FIN, reset or process death: the reader alone cancels.
    reader.stop_reading();
    waiter.await.unwrap().unwrap();
    owner.unlink().unwrap();
}

#[test]
fn peer_closed_disarms_departure_wakes_and_ignores_fin() {
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};

    #[derive(Default)]
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let streams = Streams::new(
        Arc::new(Fleet::join("close-disarm", 2).unwrap()),
        Incarnation::new(1),
    )
    .unwrap();
    let (origin, ticket) = streams.create().unwrap();
    let remote = streams.open(ticket).unwrap();
    let (reader, mut writer) = remote.split();
    let (_incoming, mut outgoing) = origin.split();
    let count = Arc::new(Count::default());
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    writer.finish().unwrap();
    {
        let mut closed = std::pin::pin!(outgoing.peer_closed());
        assert!(closed.as_mut().poll(&mut cx).is_pending());
    }
    reader.stop_reading();
    assert_eq!(count.0.load(Ordering::Relaxed), 0);
    let mut closed = std::pin::pin!(outgoing.peer_closed());
    assert!(matches!(
        closed.as_mut().poll(&mut cx),
        std::task::Poll::Ready(Ok(()))
    ));
}

#[tokio::test]
async fn reset_signal_ignores_buffered_fin_and_wakes_after_reset() {
    use std::future::poll_fn;

    let streams = Streams::new(
        Arc::new(Fleet::join("reset-signal", 2).unwrap()),
        Incarnation::new(1),
    )
    .unwrap();
    let (origin, ticket) = streams.create().unwrap();
    let mut peer = streams.open(ticket).unwrap();
    let (read, _write) = origin.split();
    let reset = read.reset_signal();
    peer.write_all(b"body before head").await.unwrap();
    peer.finish().unwrap();

    let (parked, ready) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(async move {
        let mut parked = Some(parked);
        poll_fn(|cx| {
            let result = reset.poll_reset(cx);
            if result.is_pending()
                && let Some(parked) = parked.take()
            {
                parked.send(()).unwrap();
            }
            result
        })
        .await
    });
    ready.await.unwrap();
    peer.reset();
    waiter.await.unwrap().unwrap();
    assert!(matches!(
        read.try_read(&mut [0; 1]),
        Err(orbit_stream::Error::Reset)
    ));
}

#[test]
fn reset_observer_does_not_replace_a_concurrent_body_reader_waker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};

    #[derive(Default)]
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let streams = Streams::new(
        Arc::new(Fleet::join("reset-wakers", 2).unwrap()),
        Incarnation::new(1),
    )
    .unwrap();
    let (origin, ticket) = streams.create().unwrap();
    let mut peer = streams.open(ticket).unwrap();
    let (read, _write) = origin.split();
    let reset = read.reset_signal();
    let body_count = Arc::new(Count::default());
    let reset_count = Arc::new(Count::default());
    let body_waker = Waker::from(Arc::clone(&body_count));
    let reset_waker = Waker::from(Arc::clone(&reset_count));
    assert!(
        read.poll_readable(&mut Context::from_waker(&body_waker)).is_pending()
    );
    assert!(
        reset.poll_reset(&mut Context::from_waker(&reset_waker)).is_pending()
    );
    peer.reset();
    assert!(body_count.0.load(Ordering::Relaxed) > 0);
    assert!(reset_count.0.load(Ordering::Relaxed) > 0);
}

fn pattern(
    len: usize,
    seed: u32,
) -> Vec<u8> {
    (0..len as u32)
        .map(|i| (i.wrapping_mul(2654435761).wrapping_add(seed) >> 11) as u8)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_death_wakes_a_quiet_writer_without_touching_a_sibling() {
    use std::future::poll_fn;

    let name = fleet_name("death");
    let table = |node, incarnation| {
        Streams::new(
            Arc::new(Fleet::join_shm_as(name, 3, NodeId::new(node)).unwrap()),
            Incarnation::new(incarnation),
        )
        .unwrap()
    };
    let owner = table(0, 10);
    owner.reset_all();
    let peer = table(1, 11);
    let sibling = table(2, 11);
    let (origin, ticket) = owner.create().unwrap();
    let _remote = peer.open(ticket).unwrap();
    let (_read, write) = origin.split();
    let (mut other, ticket) = owner.create().unwrap();
    let mut other_remote = sibling.open(ticket).unwrap();

    let (parked, ready) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(async move {
        let mut parked = Some(parked);
        poll_fn(|cx| {
            let result = write.poll_peer_dead(cx);
            if result.is_pending()
                && let Some(parked) = parked.take()
            {
                parked.send(()).unwrap();
            }
            result
        })
        .await
        .unwrap();
    });
    ready.await.unwrap();
    owner.node_dead(NodeId::new(1), Incarnation::new(12));
    assert!(!waiter.is_finished(), "a different incarnation is not this peer");
    assert!(
        Streams::node_dead_in_shm(
            name,
            3,
            orbit_stream::StreamSpec::DEFAULT,
            NodeId::new(1),
            Incarnation::new(11)
        )
        .unwrap()
    );
    waiter.await.unwrap();

    other.write_all(b"still live").await.unwrap();
    let mut bytes = [0; 10];
    other_remote.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"still live");
    drop((_remote, peer));
    let replacement = table(1, 12);
    let (mut fresh, ticket) = owner.create().unwrap();
    let mut fresh_remote = replacement.open(ticket).unwrap();
    owner.node_dead(NodeId::new(1), Incarnation::new(11));
    fresh.write_all(b"new").await.unwrap();
    let mut bytes = [0; 3];
    fresh_remote.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"new");
    owner.unlink().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplex_copies_across_two_nodes() {
    let name = fleet_name("d");
    let owner = Streams::new(
        Arc::new(
            Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet"),
        ),
        Incarnation::new(10),
    )
    .expect("owner streams");
    owner.reset_all();
    let peer = Streams::new(
        Arc::new(
            Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet"),
        ),
        Incarnation::new(11),
    )
    .expect("peer streams");

    let (a, ticket) = owner.create().expect("create");
    let b = peer.open(ticket).expect("open");
    let (mut a_read, mut a_write) = a.split();
    let (mut b_read, mut b_write) = b.split();

    let forward = pattern(5 * STREAM_BUFFER_BYTES + 3, 1);
    let backward = pattern(2 * STREAM_BUFFER_BYTES + 9, 2);
    let (forward_expected, backward_expected) =
        (forward.clone(), backward.clone());

    let send_forward = tokio::spawn(async move {
        a_write.write_all(&forward).await.expect("write");
        a_write.shutdown().await.expect("shutdown");
    });
    let send_backward = tokio::spawn(async move {
        b_write.write_all(&backward).await.expect("write");
        b_write.shutdown().await.expect("shutdown");
    });
    let recv_forward = tokio::spawn(async move {
        let mut out = Vec::new();
        b_read.read_to_end(&mut out).await.expect("read");
        out
    });
    let recv_backward = tokio::spawn(async move {
        let mut out = Vec::new();
        a_read.read_to_end(&mut out).await.expect("read");
        out
    });

    send_forward.await.unwrap();
    send_backward.await.unwrap();
    assert_eq!(recv_forward.await.unwrap(), forward_expected);
    assert_eq!(recv_backward.await.unwrap(), backward_expected);
    owner.unlink().expect("unlink");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parked_reader_is_woken_by_the_peer_node() {
    let name = fleet_name("w");
    let owner = Streams::new(
        Arc::new(
            Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet"),
        ),
        Incarnation::new(10),
    )
    .expect("owner streams");
    owner.reset_all();
    let peer = Streams::new(
        Arc::new(
            Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet"),
        ),
        Incarnation::new(11),
    )
    .expect("peer streams");

    let (a, ticket) = owner.create().expect("create");
    let mut b = peer.open(ticket).expect("open");
    let (mut a_read, mut a_write) = a.split();

    let reader = tokio::spawn(async move {
        let mut buf = [0_u8; 8];
        let n = a_read.read(&mut buf).await.expect("read");
        buf[..n].to_vec()
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    b.write_all(b"knock").await.expect("write");
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), reader)
            .await
            .expect("woken in time")
            .unwrap(),
        b"knock"
    );

    // A writer parked on a full ring is woken when the peer drains it.
    let filler = vec![1_u8; STREAM_BUFFER_BYTES];
    a_write.write_all(&filler).await.expect("fill");
    let writer = tokio::spawn(async move {
        a_write.write_all(b"after").await.expect("write after drain");
        a_write.shutdown().await.expect("shutdown");
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut drained = Vec::new();
    b.read_to_end(&mut drained).await.expect("drain");
    tokio::time::timeout(std::time::Duration::from_secs(5), writer)
        .await
        .expect("writer woken in time")
        .unwrap();
    assert_eq!(drained.len(), STREAM_BUFFER_BYTES + 5);
    assert_eq!(&drained[STREAM_BUFFER_BYTES..], b"after");
    owner.unlink().expect("unlink");
}

/// A future dropped while parked leaves nothing behind that could swallow
/// the next wake: a fresh read on the same half completes when bytes come,
/// and a fresh write completes when the peer drains a ring the dropped
/// write had found full.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_pending_operation_does_not_lose_the_next_wake() {
    let name = fleet_name("x");
    let owner = Streams::new(
        Arc::new(
            Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet"),
        ),
        Incarnation::new(10),
    )
    .expect("owner streams");
    owner.reset_all();
    let peer = Streams::new(
        Arc::new(
            Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet"),
        ),
        Incarnation::new(11),
    )
    .expect("peer streams");

    let (a, ticket) = owner.create().expect("create");
    let mut b = peer.open(ticket).expect("open");
    let (mut a_read, mut a_write) = a.split();

    // Park a read, then abandon it.
    let mut buf = [0_u8; 8];
    let abandoned =
        tokio::time::timeout(Duration::from_millis(20), a_read.read(&mut buf))
            .await;
    assert!(abandoned.is_err(), "nothing was written yet");
    b.write_all(b"later").await.expect("write");
    let n = tokio::time::timeout(Duration::from_secs(5), a_read.read(&mut buf))
        .await
        .expect("the fresh read was woken")
        .expect("read");
    assert_eq!(&buf[..n], b"later");

    // Fill the ring, park a write, abandon it, then drain from the peer.
    let filler = vec![9_u8; STREAM_BUFFER_BYTES];
    a_write.write_all(&filler).await.expect("fill");
    let abandoned = tokio::time::timeout(
        Duration::from_millis(20),
        a_write.write_all(b"stuck"),
    )
    .await;
    assert!(abandoned.is_err(), "the ring was full");
    let mut drained = vec![0_u8; STREAM_BUFFER_BYTES];
    b.read_exact(&mut drained).await.expect("drain");
    tokio::time::timeout(Duration::from_secs(5), a_write.write_all(b"flows"))
        .await
        .expect("the fresh write was woken")
        .expect("write");
    a_write.shutdown().await.expect("shutdown");
    let mut rest = Vec::new();
    b.read_to_end(&mut rest).await.expect("rest");
    assert_eq!(rest, b"flows");
    owner.unlink().expect("unlink");
}

/// A header write and then a body write, round after round, with the
/// reader asking for both at once. The reader takes the header, finds
/// nothing behind it and parks; the body's commit has to ring it even
/// though the ring was not empty when the writer first looked. The
/// writer's emptiness test is therefore taken after its own commit, not
/// before: taken before, this stalls within a few thousand rounds and the
/// bytes sit in the ring until some unrelated poll finds them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_body_behind_a_header_wakes_a_reader_that_parked_between_the_two() {
    const ROUNDS: usize = 5_000;
    // Big enough that the copy alone gives the reader time to take the
    // header, find nothing behind it and park.
    const BODY: usize = 60 * 1024;

    let name = fleet_name("h");
    let owner = Streams::new(
        Arc::new(
            Fleet::join_shm_as(name, 2, NodeId::ZERO).expect("owner fleet"),
        ),
        Incarnation::new(10),
    )
    .expect("owner streams");
    owner.reset_all();
    let peer = Streams::new(
        Arc::new(
            Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("peer fleet"),
        ),
        Incarnation::new(11),
    )
    .expect("peer streams");

    let (a, ticket) = owner.create().expect("create");
    let b = peer.open(ticket).expect("open");
    let (mut a_read, mut a_write) = a.split();
    let (mut b_read, mut b_write) = b.split();

    let answering = tokio::spawn(async move {
        let mut header = [0_u8; 8];
        let body = vec![7_u8; BODY - 4];
        for _ in 0..ROUNDS {
            b_read.read_exact(&mut header).await.expect("request");
            b_write.write_all(&header[..4]).await.expect("header");
            b_write.write_all(&body).await.expect("body");
        }
        b_write.finish().expect("fin");
    });
    let asking = tokio::spawn(async move {
        let mut sink = vec![0_u8; BODY];
        for round in 0..ROUNDS {
            let request = (round as u64).to_le_bytes();
            a_write.write_all(&request).await.expect("request");
            a_read.read_exact(&mut sink).await.expect("response");
            assert_eq!(sink[..4], request[..4]);
        }
    });

    tokio::time::timeout(Duration::from_secs(60), async {
        asking.await.expect("asking");
        answering.await.expect("answering");
    })
    .await
    .expect("a reader was left parked with its answer already in the ring");

    // 64 MiB of sparse segment per run is worth cleaning up, even though
    // the other tests in this file do not.
    let _ = owner.unlink();
}

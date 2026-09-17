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

fn pattern(len: usize, seed: u32) -> Vec<u8> {
    (0..len as u32)
        .map(|i| (i.wrapping_mul(2654435761).wrapping_add(seed) >> 11) as u8)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplex_copies_across_two_nodes() {
    let name = fleet_name("d");
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
    let (mut a_read, mut a_write) = a.split();
    let (mut b_read, mut b_write) = b.split();

    let forward = pattern(5 * STREAM_BUFFER_BYTES + 3, 1);
    let backward = pattern(2 * STREAM_BUFFER_BYTES + 9, 2);
    let (forward_expected, backward_expected) = (forward.clone(), backward.clone());

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
        a_write
            .write_all(b"after")
            .await
            .expect("write after drain");
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
    let mut b = peer.open(ticket).expect("open");
    let (mut a_read, mut a_write) = a.split();

    // Park a read, then abandon it.
    let mut buf = [0_u8; 8];
    let abandoned = tokio::time::timeout(Duration::from_millis(20), a_read.read(&mut buf)).await;
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
    let abandoned =
        tokio::time::timeout(Duration::from_millis(20), a_write.write_all(b"stuck")).await;
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

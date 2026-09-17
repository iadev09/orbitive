//! A real second process: the test binary re-executes itself as node 1 of
//! the fleet, and the two share only the named segment. Bytes cross in both
//! directions with the writer waiting on a full ring, the offer reaches the
//! peer through its doorbell, and a peer killed mid-stream is reported dead
//! and its side ends without taking the survivor's handles with it.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use orbit_core::{Fleet, NodeId};
use orbit_stream::{Error, Incarnation, STREAM_BUFFER_BYTES, Streams};

const PEER_INCARNATION: u64 = 11;

fn report(line: &str) {
    println!("ORBIT:{line}");
    std::io::stdout().flush().unwrap();
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len as u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 9) as u8)
        .collect()
}

/// The peer. Runs only when the parent test exec'd us with the fleet name.
#[test]
fn stream_peer() {
    let Ok(name) = std::env::var("ORBIT_STREAM_TEST_FLEET") else {
        return;
    };
    let name = Box::leak(name.into_boxed_str());
    let fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).unwrap());
    let streams = Streams::new(fleet, Incarnation::new(PEER_INCARNATION)).unwrap();
    report("ready");
    for line in std::io::stdin().lock().lines() {
        match line.unwrap().as_str() {
            // Take the offered stream, echo everything back, finish.
            "echo" => {
                let ticket = streams.blocking_take_offer().unwrap();
                let mut endpoint = streams.open(ticket).unwrap();
                report("opened");
                let mut total = 0;
                loop {
                    let chunk = endpoint.blocking_read_chunk(3_000).unwrap();
                    if chunk.is_empty() {
                        break;
                    }
                    total += chunk.len();
                    endpoint.blocking_write_all(&chunk).unwrap();
                }
                endpoint.finish().unwrap();
                report(&format!("echoed {total}"));
            }
            // Take the offered stream, say hello, then hold both ends until
            // the parent kills us.
            "hold" => {
                let ticket = streams.blocking_take_offer().unwrap();
                let endpoint = streams.open(ticket).unwrap();
                endpoint.blocking_write_all(b"hello").unwrap();
                report("holding");
                std::thread::sleep(Duration::from_secs(60));
                drop(endpoint);
            }
            "drop" => break,
            command => panic!("unknown command {command}"),
        }
    }
    report("dropped");
}

struct Peer {
    child: Child,
    lines: Receiver<String>,
}

impl Peer {
    fn spawn(name: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "stream_peer", "--nocapture"])
            .env("ORBIT_STREAM_TEST_FLEET", name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if tx.send(line.unwrap()).is_err() {
                    break;
                }
            }
        });
        let peer = Self { child, lines };
        peer.expect("ready");
        peer
    }

    fn expect(&self, message: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let line = self
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("peer failed or hung");
            if let Some(rest) = line.strip_prefix("ORBIT:")
                && rest.starts_with(message)
            {
                return rest.to_owned();
            }
        }
    }

    fn send(&mut self, command: &str) {
        writeln!(self.child.stdin.as_mut().unwrap(), "{command}").unwrap();
        self.child.stdin.as_mut().unwrap().flush().unwrap();
    }

    fn finish(&mut self) {
        self.send("drop");
        self.expect("dropped");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success());
                return;
            }
            assert!(Instant::now() < deadline, "peer did not exit");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        // Only this test's subprocess; never leave a failed test peer alive.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Owner {
    streams: Streams,
    name: &'static str,
}

impl Owner {
    fn new(tag: &str) -> Self {
        let name: &'static str =
            Box::leak(format!("sp{tag}{:x}", std::process::id()).into_boxed_str());
        let fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).unwrap());
        let streams = Streams::new(fleet, Incarnation::new(10)).unwrap();
        streams.reset_all();
        Self { streams, name }
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.streams.unlink();
    }
}

#[test]
fn bytes_cross_between_two_processes_and_the_writer_waits_on_a_full_ring() {
    let owner = Owner::new("e");
    let mut peer = Peer::spawn(owner.name);
    peer.send("echo");

    let (a, ticket) = owner.streams.create().unwrap();
    owner.streams.offer(ticket, NodeId::new(1)).unwrap();
    peer.expect("opened");

    let body = pattern(5 * STREAM_BUFFER_BYTES + 7);
    let expected = body.clone();
    let (a_read, mut a_write) = a.split();
    let writer = std::thread::spawn(move || {
        // Five rings' worth into one ring: the peer's drain is what lets
        // each write after the first proceed.
        a_write.blocking_write_all(&body).unwrap();
        a_write.finish().unwrap();
    });
    let mut echoed = Vec::new();
    loop {
        let chunk = a_read.blocking_read_chunk(4_096).unwrap();
        if chunk.is_empty() {
            break;
        }
        echoed.extend_from_slice(&chunk);
    }
    writer.join().unwrap();
    assert_eq!(echoed, expected);
    assert_eq!(peer.expect("echoed"), format!("echoed {}", expected.len()));

    // The peer released its side when the echo finished; ours is the last.
    assert!(matches!(
        owner.streams.open(ticket),
        Err(Error::AlreadyClaimed(_))
    ));
    drop(a_read);
    assert!(!owner.streams.is_live(ticket.id));
    peer.finish();
}

#[test]
fn a_killed_peer_is_reported_dead_and_the_survivor_ends_cleanly() {
    let owner = Owner::new("k");
    let mut peer = Peer::spawn(owner.name);
    peer.send("hold");

    let (a, ticket) = owner.streams.create().unwrap();
    owner.streams.offer(ticket, NodeId::new(1)).unwrap();
    peer.expect("holding");
    assert_eq!(a.blocking_read_chunk(16).unwrap().as_ref(), b"hello");

    let reader = std::thread::spawn(move || {
        let mut buf = [0_u8; 8];
        let outcome = a.blocking_read(&mut buf);
        (a, outcome)
    });
    std::thread::sleep(Duration::from_millis(50));
    peer.kill();

    // Death alone changes nothing in the segment: the reader is still
    // parked. The supervisor's report is what ends the peer's side.
    assert!(owner.streams.is_live(ticket.id));
    owner
        .streams
        .node_dead(NodeId::new(1), Incarnation::new(PEER_INCARNATION));
    let (a, outcome) = reader.join().unwrap();
    assert!(matches!(outcome, Err(Error::Reset)), "{outcome:?}");
    assert!(matches!(a.try_write(b"x"), Err(Error::PeerGone)));
    assert!(matches!(
        owner.streams.open(ticket),
        Err(Error::AlreadyClaimed(_))
    ));
    assert!(owner.streams.is_live(ticket.id));
    drop(a);
    assert!(!owner.streams.is_live(ticket.id));
}

/// The same echo with the parent on Tokio: the doorbell driver, not a
/// blocking wait, is what wakes the parent's tasks when the peer drains and
/// writes.
#[cfg(feature = "tokio")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_halves_cross_between_two_processes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let owner = Owner::new("t");
    let mut peer = Peer::spawn(owner.name);
    peer.send("echo");

    let (a, ticket) = owner.streams.create().unwrap();
    owner.streams.offer(ticket, NodeId::new(1)).unwrap();
    peer.expect("opened");

    let body = pattern(4 * STREAM_BUFFER_BYTES + 1);
    let expected = body.clone();
    let (mut a_read, mut a_write) = a.split();
    let writer = tokio::spawn(async move {
        a_write.write_all(&body).await.unwrap();
        a_write.shutdown().await.unwrap();
    });
    let mut echoed = Vec::new();
    tokio::time::timeout(Duration::from_secs(20), a_read.read_to_end(&mut echoed))
        .await
        .expect("echo did not complete")
        .unwrap();
    writer.await.unwrap();
    assert_eq!(echoed, expected);
    peer.expect("echoed");
    peer.finish();
}

//! Exec-based peers share only the named ring, never a readiness descriptor.
#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use bytes::Bytes;
use orbit_core::{Fleet, NodeId, OrbitTyped, RingEventFd, RingSpec};

#[derive(Clone)]
struct Record;

impl OrbitTyped for Record {
    const KIND: u8 = 81;
    const RING_SPEC: RingSpec = RingSpec::new(64, 8);
}

fn readable(fd: &RingEventFd, timeout: i32) -> bool {
    let mut poll = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let result = unsafe { libc::poll(&mut poll, 1, timeout) };
    assert!(result >= 0, "poll: {}", std::io::Error::last_os_error());
    result > 0 && poll.revents & libc::POLLIN != 0
}

fn report(line: &str) {
    println!("ORBIT:{line}");
    std::io::stdout().flush().unwrap();
}

#[test]
fn readiness_peer() {
    let Ok(name) = std::env::var("ORBIT_READINESS_TEST_FLEET") else {
        return;
    };
    let name = Box::leak(name.into_boxed_str());
    let fleet = Fleet::join_shm_as(name, 1, NodeId::new(0)).unwrap();
    let fd = fleet.ring_event_fd::<Record>().unwrap();
    assert_eq!(fd.drain().unwrap(), 0);
    report("ready");
    for line in std::io::stdin().lock().lines() {
        match line.unwrap().as_str() {
            "wait" => {
                assert!(readable(&fd, 5000), "native wake timed out");
                assert!(fd.drain().unwrap() > 0);
                assert!(fleet.read_head::<Record>().is_some());
                report("awake");
            }
            "drop" => break,
            command => panic!("unknown command {command}"),
        }
    }
    drop(fd);
    // Race immediate drop with driver startup, then drop an idle parked driver.
    for _ in 0..100 {
        drop(fleet.ring_event_fd::<Record>().unwrap());
    }
    let fd = fleet.ring_event_fd::<Record>().unwrap();
    std::thread::sleep(Duration::from_millis(20));
    drop(fd);
    report("dropped");
}

struct Peer {
    child: Child,
    lines: Receiver<String>,
}
impl Peer {
    fn spawn(name: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "readiness_peer", "--nocapture"])
            .env("ORBIT_READINESS_TEST_FLEET", name)
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
    fn expect(&self, message: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let line = self
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("peer failed or hung");
            if line.contains(&format!("ORBIT:{message}")) {
                return;
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
}
impl Drop for Peer {
    fn drop(&mut self) {
        // Only this test's subprocess; never leave a failed test peer alive.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct TestFleet(Fleet);
impl Drop for TestFleet {
    fn drop(&mut self) {
        self.0.shm_ring::<Record>().unwrap().unlink().unwrap();
    }
}

#[tokio::test]
async fn async_reactor_drains_and_rearms_native_fd() {
    let name = Box::leak(format!("ar{:x}", std::process::id()).into_boxed_str());
    let fleet = TestFleet(Fleet::join_shm_as(name, 1, NodeId::new(0)).unwrap());
    let fd = tokio::io::unix::AsyncFd::new(fleet.0.ring_event_fd::<Record>().unwrap()).unwrap();
    for round in 0..20 {
        let publisher = fleet.0.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(2)).await;
            publisher
                .publish_notified::<Record>(0, round, Bytes::from_static(b"async"))
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut ready = fd.readable().await.unwrap();
                let drained = fd.get_ref().drain().unwrap();
                ready.clear_ready();
                if drained > 0 {
                    break;
                }
            }
        })
        .await
        .expect("reactor did not wake");
        task.await.unwrap();
        assert_eq!(fleet.0.read_head::<Record>().unwrap().ver, round);
    }
    drop(fd);
}

#[test]
fn independent_readers_broadcast_late_join_and_drop() {
    let name = Box::leak(format!("nr{:x}", std::process::id()).into_boxed_str());
    let fleet = TestFleet(Fleet::join_shm_as(name, 1, NodeId::new(0)).unwrap());
    // A publisher with no native waiters must still succeed.
    fleet
        .0
        .publish_notified::<Record>(0, 0, Bytes::from_static(b"before"))
        .unwrap();
    let mut first = Peer::spawn(name);
    let mut second = Peer::spawn(name);
    first.send("wait");
    second.send("wait");
    std::thread::sleep(Duration::from_millis(20));
    fleet
        .0
        .publish_notified::<Record>(0, 1, Bytes::from_static(b"one"))
        .unwrap();
    first.expect("awake");
    second.expect("awake");

    // A fresh exec after earlier publications obtains its own local fd.
    let mut late = Peer::spawn(name);
    for peer in [&mut first, &mut second, &mut late] {
        peer.send("wait");
    }
    fleet
        .0
        .publish_batch_notified::<Record>(0, 2, vec![Bytes::from_static(b"two"); 32])
        .unwrap();
    for peer in [&first, &second, &late] {
        peer.expect("awake");
    }
    // Repeatedly race publication against the driver's next wait and fd drain.
    for round in 0..100 {
        for peer in [&mut first, &mut second, &mut late] {
            peer.send("wait");
        }
        fleet
            .0
            .publish_notified::<Record>(0, round, Bytes::from_static(b"race"))
            .unwrap();
        for peer in [&first, &second, &late] {
            peer.expect("awake");
        }
    }
    for peer in [&mut first, &mut second, &mut late] {
        peer.finish();
    }
}

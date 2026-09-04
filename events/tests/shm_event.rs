//! Cross-process OrbitEventBus via Fleet::join_shm.
//!
//! This is the smallest proof that Orbit events are real cross-process
//! pulses: one process publishes a topic/payload event into the SHM
//! ring, another process advances its own cursor and sees it.

#![cfg(unix)]

use std::sync::Arc;
use std::time::Duration;

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use std::io::{Read, Write};
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use std::os::fd::AsRawFd;
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use std::os::unix::net::UnixStream;

use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, fork};
use orbit_core::ring_shm::ShmRing;
use orbit_core::{Fleet, NodeId};
use orbit_events::{EVENT_RING_KIND, EVENT_RING_SPEC, OrbitEventBus};

fn fresh_name() -> &'static str {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid_short = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFF;
    let s = format!("e{pid_short:04x}{nonce:08x}{n:02x}");
    Box::leak(s.into_boxed_str())
}

fn wait_child(pid: nix::unistd::Pid) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => return code,
            Ok(WaitStatus::Signaled(_, sig, _)) => {
                panic!("child killed by signal {:?}", sig);
            }
            Ok(WaitStatus::StillAlive) => {
                if std::time::Instant::now() >= deadline {
                    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL);
                    panic!("child timed out");
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok(other) => panic!("unexpected child status: {:?}", other),
            Err(e) => panic!("waitpid failed: {e}"),
        }
    }
}

fn cleanup_event_ring(name: &str) {
    if let Ok(ring) = ShmRing::open_or_create_for_fleet(name, EVENT_RING_KIND, EVENT_RING_SPEC, 2) {
        let _ = ring.unlink();
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
fn wait_until_readable(fd: &impl AsRawFd) -> bool {
    let mut poll_fd = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let ready = unsafe { libc::poll(&mut poll_fd, 1, 2_000) };
    ready == 1 && poll_fd.revents & libc::POLLIN != 0
}

#[test]
fn two_nodes_publish_into_independent_lanes() {
    let name = fresh_name();
    let parent_fleet =
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("parent join_shm"));
    let parent_bus = OrbitEventBus::new(parent_fleet);
    let mut cursor = parent_bus.cursor_at_head();
    parent_bus
        .publish("test.parent_also", b"hello-from-parent")
        .expect("parent publish");

    match unsafe { fork() }.expect("fork") {
        ForkResult::Parent { child } => {
            let code = wait_child(child);
            assert_eq!(code, 0, "child reported failure (exit {code})");

            let poll = parent_bus.poll(&mut cursor);
            cleanup_event_ring(name);

            assert_eq!(poll.lagged, 0);
            assert_eq!(poll.events.len(), 2);
            assert_eq!(poll.events[0].id.node(), 0);
            assert_eq!(poll.events[0].id.counter(), 0);
            assert_eq!(poll.events[0].topic, "test.parent_also");
            assert_eq!(poll.events[1].id.node(), 1);
            assert_eq!(poll.events[1].id.counter(), 0);
            assert_eq!(poll.events[1].topic, "test.child_published");
            assert_eq!(poll.events[1].payload, b"hello-from-child");
        }
        ForkResult::Child => {
            let child_fleet = match Fleet::join_shm_as(name, 2, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(11),
            };
            let child_bus = OrbitEventBus::new(child_fleet);
            if child_bus
                .publish("test.child_published", b"hello-from-child")
                .is_err()
            {
                std::process::exit(12);
            }
            std::process::exit(0);
        }
    }
}

#[test]
fn parent_publishes_child_polls_event() {
    let name = fresh_name();
    let parent_fleet =
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("parent join_shm"));
    let parent_bus = OrbitEventBus::new(parent_fleet);
    let id = parent_bus
        .publish("test.parent_published", b"hello-from-parent")
        .expect("publish");

    match unsafe { fork() }.expect("fork") {
        ForkResult::Parent { child } => {
            let code = wait_child(child);
            cleanup_event_ring(name);
            assert_eq!(code, 0, "child reported failure (exit {code})");
            println!("child accepted OrbitEvent id={id} topic=test.parent_published");
        }
        ForkResult::Child => {
            let child_fleet = match Fleet::join_shm_as(name, 2, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(21),
            };
            let child_bus = OrbitEventBus::new(child_fleet);
            let mut cursor = child_bus.cursor_from_start();
            let poll = child_bus.poll(&mut cursor);
            if poll.lagged != 0 || poll.events.len() != 1 {
                std::process::exit(22);
            }
            let event = &poll.events[0];
            if event.topic != "test.parent_published" {
                std::process::exit(23);
            }
            if event.payload != b"hello-from-parent" {
                std::process::exit(24);
            }
            if event.id.node() != 0 {
                std::process::exit(25);
            }
            std::process::exit(0);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
#[test]
fn cross_process_publish_wakes_process_local_event_fd() {
    let name = fresh_name();
    let parent_fleet =
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("parent join_shm"));
    let parent_bus = OrbitEventBus::new(parent_fleet);
    parent_bus.reset_ring().expect("reset event ring");
    let (mut parent_ready, mut child_ready) = UnixStream::pair().expect("ready socket pair");

    match unsafe { fork() }.expect("fork") {
        ForkResult::Parent { child } => {
            drop(child_ready);
            let mut ready = [0u8; 1];
            parent_ready
                .read_exact(&mut ready)
                .expect("child listener ready");
            parent_bus
                .publish("test.eventfd", b"wake-child")
                .expect("publish event");

            let code = wait_child(child);
            cleanup_event_ring(name);
            assert_eq!(code, 0, "child reported failure (exit {code})");
        }
        ForkResult::Child => {
            drop(parent_ready);
            let child_fleet = match Fleet::join_shm_as(name, 2, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(31),
            };
            let child_bus = OrbitEventBus::new(child_fleet);
            let event_fd = match child_bus.event_fd() {
                Ok(event_fd) => event_fd,
                Err(_) => std::process::exit(32),
            };
            let mut cursor = child_bus.cursor_at_head();
            if child_ready.write_all(&[1]).is_err() {
                std::process::exit(33);
            }

            if !wait_until_readable(&event_fd) {
                std::process::exit(34);
            }
            if event_fd.drain().is_err() {
                std::process::exit(35);
            }
            let events = child_bus.poll(&mut cursor);
            if events.lagged != 0 || events.events.len() != 1 {
                std::process::exit(36);
            }
            let event = &events.events[0];
            if event.topic != "test.eventfd" || event.payload != b"wake-child" {
                std::process::exit(37);
            }
            std::process::exit(0);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
#[test]
fn one_publish_wakes_each_process_local_event_fd() {
    let name = fresh_name();
    let bus_a = OrbitEventBus::new(Arc::new(
        Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("node zero join_shm"),
    ));
    let bus_b = OrbitEventBus::new(Arc::new(
        Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("node one join_shm"),
    ));
    bus_a.reset_ring().expect("reset event ring");

    let event_fd_a = bus_a.event_fd().expect("node zero eventfd");
    let event_fd_b = bus_b.event_fd().expect("node one eventfd");
    bus_a
        .publish("test.broadcast", b"wake-every-listener")
        .expect("publish event");

    assert!(wait_until_readable(&event_fd_a));
    assert!(wait_until_readable(&event_fd_b));
    assert!(event_fd_a.drain().expect("drain node zero") > 0);
    assert!(event_fd_b.drain().expect("drain node one") > 0);

    drop(event_fd_a);
    drop(event_fd_b);
    cleanup_event_ring(name);
}

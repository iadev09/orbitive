//! Cross-process SHM visibility — the V1 promise verified end-to-end.
//!
//! These tests `fork()` and have the parent and child interact
//! through a POSIX SHM segment. If `ShmRing` is doing its job, what
//! the parent writes the child reads, and vice-versa, with no
//! shared memory in the Rust language sense — only the kernel-level
//! mapping bridges them.
//!
//! ## Why fork instead of subprocess
//!
//! Spawning a separate binary (`std::process::Command`) would require
//! adding a `[[bin]]` test helper, a build dance, and binary lookups
//! at test time. `fork()` is direct: we get two real OS processes
//! with separate address spaces, and the test framework's harness
//! is replaced by a clean exit in the child.
//!
//! ## Cleanup
//!
//! Each test uses a unique segment name so parallel runs don't
//! collide. The parent calls `unlink` on the way out; if a test
//! crashes mid-run, leftover segments live in `/dev/shm/` (Linux)
//! or under `/tmp/` (macOS shm location varies) and can be cleaned
//! by hand.

#![cfg(unix)]

use std::time::Duration;

use bytes::Bytes;
use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, fork};
use orbit_core::ring_shm::ShmRing;
use orbit_core::{NodeId, RingSpec};

/// macOS limits POSIX SHM names to PSHMNAMLEN (31 chars). The full
/// constructed segment name is `/orbit-{fleet}-{kind}-{uid}`, so the
/// fleet name we pass must stay short. Use a tiny hex-pid + counter
/// to fit comfortably under the limit on every Unix.
fn fresh_name(_test_label: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid_short = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFF;
    format!("x{pid_short:04x}{nonce:08x}{n:02x}")
}

fn spec() -> RingSpec {
    RingSpec::new(16, 64)
}

/// Wait for child with a generous timeout. Returns the child's
/// raw exit code (0 = success, non-zero = test failure asserted
/// inside the child).
fn wait_child(pid: nix::unistd::Pid) -> i32 {
    // Poll-with-timeout — pure waitpid blocks indefinitely if the
    // child hangs, and we want test runs to fail fast not deadlock.
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

#[test]
fn parent_writes_child_reads() {
    let name = fresh_name("parent-writes");

    // Parent creates the segment and writes BEFORE forking so the
    // mapping is already populated when the child opens it.
    let ring = ShmRing::open_or_create(&name, 7, spec()).expect("parent open_or_create");
    let id = ring
        .write(
            NodeId::new(0),
            0,
            99,
            Bytes::from_static(b"hello-from-parent"),
        )
        .expect("parent write");

    match unsafe { fork() }.expect("fork failed") {
        ForkResult::Parent { child } => {
            let code = wait_child(child);
            // Clean up the segment regardless of child outcome.
            let _ = ring.unlink();
            assert_eq!(code, 0, "child reported failure (exit code {code})");
        }
        ForkResult::Child => {
            // Child has its own address space; open the SAME named segment.
            let child_ring = match ShmRing::open_or_create(&name, 7, spec()) {
                Ok(r) => r,
                Err(_) => std::process::exit(11),
            };
            let frame = match child_ring.read(id) {
                Some(f) => f,
                None => std::process::exit(12),
            };
            if frame.id != id {
                std::process::exit(13);
            }
            if frame.ver != 99 {
                std::process::exit(14);
            }
            if &frame.payload[..] != b"hello-from-parent" {
                std::process::exit(15);
            }
            // All checks passed.
            std::process::exit(0);
        }
    }
}

#[test]
fn child_writes_parent_reads() {
    let name = fresh_name("child-writes");

    // Parent creates the segment empty, forks; child writes; parent reads.
    let parent_ring =
        ShmRing::open_or_create(&name, 11, spec()).expect("parent create empty segment");

    match unsafe { fork() }.expect("fork failed") {
        ForkResult::Parent { child } => {
            let code = wait_child(child);
            assert_eq!(code, 0, "child reported failure (exit code {code})");

            // Now read what the child wrote.
            let frame = parent_ring
                .read_head()
                .expect("parent should see child's write");
            assert_eq!(frame.kind, 0);
            assert_eq!(frame.ver, 7);
            assert_eq!(&frame.payload[..], b"hello-from-child");

            let _ = parent_ring.unlink();
        }
        ForkResult::Child => {
            let child_ring = match ShmRing::open_or_create(&name, 11, spec()) {
                Ok(r) => r,
                Err(_) => std::process::exit(21),
            };
            if child_ring.created() {
                // We expected the parent to have created the segment.
                std::process::exit(22);
            }
            let result = child_ring.write(
                NodeId::new(2),
                0,
                7,
                Bytes::from_static(b"hello-from-child"),
            );
            if result.is_err() {
                std::process::exit(23);
            }
            std::process::exit(0);
        }
    }
}

#[test]
fn ping_pong_two_writes_one_each_side() {
    let name = fresh_name("ping-pong");

    let parent_ring = ShmRing::open_or_create(&name, 13, spec()).expect("parent create");
    let parent_id = parent_ring
        .write(NodeId::new(0), 0, 1, Bytes::from_static(b"ping"))
        .expect("parent write");

    match unsafe { fork() }.expect("fork failed") {
        ForkResult::Parent { child } => {
            let code = wait_child(child);
            assert_eq!(code, 0, "child reported failure (exit code {code})");

            // Parent should now see TWO entries: its own ping, then child's pong.
            let head = parent_ring.head();
            assert_eq!(head, 2, "two writes total");
            let head_frame = parent_ring.read_head().expect("read pong");
            assert_eq!(&head_frame.payload[..], b"pong");
            // And the original ping is still readable.
            let ping_frame = parent_ring.read(parent_id).expect("read ping");
            assert_eq!(&ping_frame.payload[..], b"ping");
            assert_eq!(ping_frame.id.node(), 0);
            assert_eq!(head_frame.id.node(), 2);

            let _ = parent_ring.unlink();
        }
        ForkResult::Child => {
            let child_ring = match ShmRing::open_or_create(&name, 13, spec()) {
                Ok(r) => r,
                Err(_) => std::process::exit(31),
            };
            // Child should see parent's write.
            let frame = match child_ring.read(parent_id) {
                Some(f) => f,
                None => std::process::exit(32),
            };
            if &frame.payload[..] != b"ping" {
                std::process::exit(33);
            }
            // Child writes back.
            if child_ring
                .write(NodeId::new(2), 0, 2, Bytes::from_static(b"pong"))
                .is_err()
            {
                std::process::exit(34);
            }
            std::process::exit(0);
        }
    }
}

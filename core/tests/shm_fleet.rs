//! Cross-process typed ring access via Fleet::join_shm.
//!
//! Validates that the Fleet integration of ShmRing actually carries
//! semantic payloads across `fork()`-spawned peers.

#![cfg(unix)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, fork};
use orbit_core::{Fleet, NetId64, OrbitTyped, RingSpec};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CrossCounter(pub u64);
impl OrbitTyped for CrossCounter {
    const KIND: u8 = 41;
    const RING_SPEC: RingSpec = RingSpec::new(16, size_of::<u64>());
}

fn publish_counter(fleet: &Fleet, value: CrossCounter) -> NetId64 {
    fleet.publish::<CrossCounter>(0, 0, Bytes::copy_from_slice(&value.0.to_le_bytes()))
}

fn load_counter(fleet: &Fleet) -> Option<CrossCounter> {
    let frame = fleet.read_head::<CrossCounter>()?;
    let bytes = <[u8; size_of::<u64>()]>::try_from(frame.payload.as_ref()).ok()?;
    Some(CrossCounter(u64::from_le_bytes(bytes)))
}

fn fresh_name() -> &'static str {
    // Reuse the same short name across the test (fork sees same str).
    // Each test_fn picks a different one to avoid segment collisions
    // between parallel tests. We leak a String to get 'static.
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid_short = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFF;
    let s = format!("f{pid_short:04x}{nonce:08x}{n:02x}");
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

#[test]
fn parent_publishes_child_loads() {
    let name = fresh_name();

    let fleet = Arc::new(Fleet::join_shm(name, 4).expect("parent join_shm"));
    assert!(fleet.is_shm());

    publish_counter(&fleet, CrossCounter(0xCAFE_F00D));

    match unsafe { fork() }.expect("fork") {
        ForkResult::Parent { child } => {
            let code = wait_child(child);
            // Cleanup the SHM segment regardless of outcome.
            if let Ok(ring) = fleet.shm_ring::<CrossCounter>() {
                let _ = ring.unlink();
            }
            assert_eq!(code, 0, "child reported failure (exit {code})");
        }
        ForkResult::Child => {
            // Child process: open the same SHM-backed fleet.
            let child_fleet = match Fleet::join_shm(name, 4) {
                Ok(f) => Arc::new(f),
                Err(_) => std::process::exit(41),
            };
            match load_counter(&child_fleet) {
                Some(v) if v.0 == 0xCAFE_F00D => std::process::exit(0),
                Some(other) => {
                    eprintln!("child loaded unexpected value: {:#x}", other.0);
                    std::process::exit(42);
                }
                None => std::process::exit(43),
            }
        }
    }
}

#[test]
fn child_publishes_parent_loads() {
    let name = fresh_name();

    // Parent creates the fleet (and thus the SHM segment) before fork.
    let fleet = Arc::new(Fleet::join_shm(name, 4).expect("parent join_shm"));

    match unsafe { fork() }.expect("fork") {
        ForkResult::Parent { child } => {
            let code = wait_child(child);
            // After child writes and exits, parent reads.
            let observed = load_counter(&fleet);
            if let Ok(ring) = fleet.shm_ring::<CrossCounter>() {
                let _ = ring.unlink();
            }
            assert_eq!(code, 0, "child reported failure (exit {code})");
            assert_eq!(
                observed,
                Some(CrossCounter(0xDEAD_BEEF)),
                "parent should see child's write"
            );
        }
        ForkResult::Child => {
            let child_fleet = match Fleet::join_shm(name, 4) {
                Ok(f) => Arc::new(f),
                Err(_) => std::process::exit(51),
            };
            publish_counter(&child_fleet, CrossCounter(0xDEAD_BEEF));
            std::process::exit(0);
        }
    }
}

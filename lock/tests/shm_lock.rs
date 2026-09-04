//! Cross-process lock ownership and native transition readiness.

#![cfg(unix)]

use std::io::{Read, Write};
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, fork};
use orbit_core::{Fleet, NodeId};
use orbit_lock::{Lock, LockAcquire, LockKey, LockOwner};

fn fresh_name() -> &'static str {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFF;
    Box::leak(format!("l{pid:04x}{nonce:08x}{counter:02x}").into_boxed_str())
}

fn wait_child(pid: nix::unistd::Pid) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => return code,
            Ok(WaitStatus::Signaled(_, signal, _)) => panic!("child killed by {signal:?}"),
            Ok(WaitStatus::StillAlive) => {
                assert!(std::time::Instant::now() < deadline, "child timed out");
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(other) => panic!("unexpected child status: {other:?}"),
            Err(error) => panic!("waitpid failed: {error}"),
        }
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
fn processes_contending_for_one_key_produce_one_owner() {
    let name = fresh_name();
    let parent_fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("parent fleet"));
    let parent = Lock::new(parent_fleet).expect("parent locks");
    parent.reset_transport().expect("reset lock transport");
    let (mut parent_pipe, mut child_pipe) = UnixStream::pair().expect("socket pair");

    match unsafe { fork() }.expect("fork") {
        ForkResult::Child => {
            drop(parent_pipe);
            let mut start = [0; 1];
            if child_pipe.read_exact(&mut start).is_err() {
                std::process::exit(11);
            }
            let fleet = match Fleet::join_shm_as(name, 2, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(12),
            };
            let locks = match Lock::new(fleet) {
                Ok(locks) => locks,
                Err(_) => std::process::exit(13),
            };
            let key = LockKey::from_parts("test.pool", Bytes::from_static(b"pool:queue:1"));
            let acquired = matches!(
                locks.try_acquire(&key, &LockOwner::from("child"), Duration::from_secs(30)),
                Ok(LockAcquire::Acquired(_))
            );
            let _ = child_pipe.write_all(&[u8::from(acquired)]);
            std::process::exit(0);
        }
        ForkResult::Parent { child } => {
            drop(child_pipe);
            parent_pipe.write_all(&[1]).expect("release child");
            let key = LockKey::from_parts("test.pool", Bytes::from_static(b"pool:queue:1"));
            let parent_acquired = matches!(
                parent
                    .try_acquire(&key, &LockOwner::from("parent"), Duration::from_secs(30))
                    .expect("parent acquire"),
                LockAcquire::Acquired(_)
            );
            let mut child_result = [0; 1];
            parent_pipe
                .read_exact(&mut child_result)
                .expect("child result");
            assert_eq!(wait_child(child), 0);
            assert_eq!(
                usize::from(parent_acquired) + usize::from(child_result[0]),
                1
            );
            assert!(parent.current(&key).expect("current").is_some());
            parent.unlink().expect("unlink lock transport");
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
#[test]
fn child_transition_wakes_parent_eventfd() {
    let name = fresh_name();
    let parent_fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("parent fleet"));
    let parent = Lock::new(parent_fleet).expect("parent locks");
    parent.reset_transport().expect("reset lock transport");
    let event_fd = parent.event_fd().expect("lock eventfd");

    match unsafe { fork() }.expect("fork") {
        ForkResult::Child => {
            let fleet = match Fleet::join_shm_as(name, 2, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(21),
            };
            let locks = match Lock::new(fleet) {
                Ok(locks) => locks,
                Err(_) => std::process::exit(22),
            };
            let key = LockKey::from_parts("test.pool", Bytes::from_static(b"pool:queue:1"));
            if !matches!(
                locks.try_acquire(&key, &LockOwner::from("child"), Duration::from_secs(30)),
                Ok(LockAcquire::Acquired(_))
            ) {
                std::process::exit(23);
            }
            std::process::exit(0);
        }
        ForkResult::Parent { child } => {
            assert!(wait_until_readable(&event_fd));
            assert!(event_fd.drain().expect("drain eventfd") > 0);
            assert_eq!(wait_child(child), 0);
            let poll = parent.poll();
            assert_eq!(poll.events.len(), 1);
            parent.unlink().expect("unlink lock transport");
        }
    }
}

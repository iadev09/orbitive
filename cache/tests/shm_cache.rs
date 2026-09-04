//! Cross-process cache propagation through independent SHM fleet handles.

#![cfg(unix)]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use std::io::{Read, Write};
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;

use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, fork};
use orbit_cache::{Cache, CacheMutation, CacheRead, CacheTransport, DefaultCacheLayout};
use orbit_core::{Fleet, NodeId};

fn fresh_name() -> &'static str {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFF;
    Box::leak(format!("k{pid:04x}{nonce:08x}{counter:02x}").into_boxed_str())
}

fn wait_child(pid: nix::unistd::Pid) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => return code,
            Ok(WaitStatus::Signaled(_, signal, _)) => {
                panic!("child killed by signal {signal:?}");
            }
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
fn child_put_wakes_and_populates_parent_l1() {
    let name = fresh_name();
    let parent_fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("parent fleet"));
    let parent_cache = Cache::<DefaultCacheLayout>::new(parent_fleet).expect("parent cache");
    let parent = parent_cache.open_default_store().expect("parent store");
    parent_cache
        .transport()
        .reset_rings()
        .expect("reset cache rings");
    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    let event_fd = parent_cache.event_fd().expect("cache mutation eventfd");

    match unsafe { fork() }.expect("fork") {
        ForkResult::Child => {
            let child_fleet = match Fleet::join_shm_as(name, 2, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(11),
            };
            let child_cache = match Cache::<DefaultCacheLayout>::new(child_fleet) {
                Ok(cache) => cache,
                Err(_) => std::process::exit(12),
            };
            let child = match child_cache.open_default_store() {
                Ok(store) => store,
                Err(_) => std::process::exit(12),
            };
            let value = vec![b'x'; 5_000];
            if child.put(b"cross-process", &value, None).is_err() {
                std::process::exit(13);
            }
            std::process::exit(0);
        }
        ForkResult::Parent { child } => {
            #[cfg(any(target_os = "linux", target_os = "freebsd"))]
            {
                assert!(wait_until_readable(&event_fd));
                assert!(event_fd.drain().expect("drain eventfd") > 0);
            }

            let child_code = wait_child(child);
            let poll = parent_cache.poll();
            let observed = parent.read(b"cross-process");
            parent_cache
                .transport()
                .unlink_rings()
                .expect("unlink cache rings");

            assert_eq!(child_code, 0, "child reported failure");
            assert_eq!(poll.observed, 1);
            assert_eq!(poll.applied, 1);
            let CacheRead::Hit(entry) = observed else {
                panic!("parent L1 must contain child value");
            };
            assert_eq!(entry.value.len(), 5_000);
            assert!(entry.value.iter().all(|byte| *byte == b'x'));
        }
    }
}

#[test]
fn writers_in_different_lanes_converge_by_shared_revision() {
    let name = fresh_name();
    let parent_fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("parent fleet"));
    let parent_cache =
        Cache::<DefaultCacheLayout>::new(parent_fleet.clone()).expect("parent cache");
    let parent = parent_cache.open_default_store().expect("parent store");
    parent_cache
        .transport()
        .reset_rings()
        .expect("reset cache rings");
    let observer =
        CacheTransport::<DefaultCacheLayout>::new(parent_fleet).expect("observer transport");
    let mut observer_cursor = observer.cursor_at_head();
    let (mut parent_start, mut child_start) = UnixStream::pair().expect("start socket pair");

    match unsafe { fork() }.expect("fork") {
        ForkResult::Child => {
            drop(parent_start);
            let mut start = [0u8; 1];
            if child_start.read_exact(&mut start).is_err() {
                std::process::exit(21);
            }
            let child_fleet = match Fleet::join_shm_as(name, 2, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(22),
            };
            let child_cache = match Cache::<DefaultCacheLayout>::new(child_fleet) {
                Ok(cache) => cache,
                Err(_) => std::process::exit(23),
            };
            let child = match child_cache.open_default_store() {
                Ok(store) => store,
                Err(_) => std::process::exit(23),
            };
            if child.put(b"same-key", b"child", None).is_err() {
                std::process::exit(24);
            }
            std::process::exit(0);
        }
        ForkResult::Parent { child } => {
            drop(child_start);
            parent_start.write_all(&[1]).expect("release child writer");
            parent
                .put(b"same-key", b"parent", None)
                .expect("parent put");
            let child_code = wait_child(child);

            let observed = observer.poll(&mut observer_cursor);
            assert_eq!(observed.mutations.len(), 2);
            assert!(
                observed.mutations[0].revision().sequence
                    < observed.mutations[1].revision().sequence
            );
            let CacheMutation::Put { payload, .. } = &observed.mutations[1] else {
                panic!("newest mutation must be a put");
            };
            let expected = observer.read_payload(*payload).expect("winning payload");

            let poll = parent_cache.poll();
            let CacheRead::Hit(actual) = parent.read(b"same-key") else {
                panic!("parent must retain the winning write");
            };
            parent_cache
                .transport()
                .unlink_rings()
                .expect("unlink cache rings");

            assert_eq!(child_code, 0, "child reported failure");
            assert_eq!(poll.observed, 2);
            assert_eq!(actual.value, expected);
        }
    }
}

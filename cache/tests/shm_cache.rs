//! Independent processes sharing arena bytes and mutation metadata.
#![cfg(unix)]

use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use orbit_cache::{
    Cache, CacheMutation, CacheRead, CacheTransport, DefaultCacheLayout,
};
use orbit_core::{Fleet, NodeId};

fn fresh_name() -> &'static str {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    Box::leak(
        format!(
            "cache_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )
        .into_boxed_str(),
    )
}

#[test]
fn child_cache_writer() {
    let Ok(name) = std::env::var("ORBIT_CACHE_CHILD") else {
        return;
    };
    let mode = std::env::var("ORBIT_CACHE_CHILD_MODE").unwrap();
    std::io::stdin().read_exact(&mut [0]).unwrap();
    let fleet = Arc::new(
        Fleet::join_shm_as(Box::leak(name.into_boxed_str()), 2, NodeId::new(1))
            .unwrap(),
    );
    let cache = Cache::<DefaultCacheLayout>::new(fleet).unwrap();
    let store = cache.open_default_store().unwrap();
    if mode == "large" {
        store.put(b"cross-process", &vec![b'x'; 5_000], None).unwrap();
    } else {
        store.put(b"same-key", b"child", None).unwrap();
    }
}

fn writer(
    name: &str,
    mode: &str,
) -> Child {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_cache_writer", "--nocapture"])
        .env("ORBIT_CACHE_CHILD", name)
        .env("ORBIT_CACHE_CHILD_MODE", mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    child.stdin.as_mut().unwrap().write_all(&[1]).unwrap();
    child
}

#[test]
fn child_put_wakes_and_populates_parent_metadata() {
    let name = fresh_name();
    let fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).unwrap());
    let cache = Cache::<DefaultCacheLayout>::new(fleet).unwrap();
    let store = cache.open_default_store().unwrap();
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    let ready = cache.event_fd().unwrap();
    assert!(writer(name, "large").wait().unwrap().success());
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    assert!(ready.drain().unwrap() > 0);
    assert_eq!(cache.poll().applied, 1);
    let CacheRead::Hit(entry) = store.read(b"cross-process") else {
        panic!("hit");
    };
    assert_eq!(entry.value.len(), 5_000);
    assert!(entry.value.iter().all(|byte| *byte == b'x'));
    let same = cache
        .transport()
        .read_payload({
            let mut cursor = cache.transport().cursor_from_start();
            let mutation =
                cache.transport().poll(&mut cursor).mutations.remove(0);
            let CacheMutation::Put { payload, .. } = mutation else {
                panic!("put");
            };
            payload
        })
        .unwrap();
    assert_eq!(same.as_ptr(), entry.value.as_ptr());
    cache.transport().unlink_rings().unwrap();
    cache.transport().arena().unlink().unwrap();
}

#[test]
fn writers_in_different_lanes_converge_by_shared_revision() {
    let name = fresh_name();
    let fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).unwrap());
    let cache = Cache::<DefaultCacheLayout>::new(fleet.clone()).unwrap();
    let store = cache.open_default_store().unwrap();
    let observer = CacheTransport::<DefaultCacheLayout>::new(fleet).unwrap();
    let mut cursor = observer.cursor_at_head();
    let mut child = writer(name, "same-key");
    store.put(b"same-key", b"parent", None).unwrap();
    assert!(child.wait().unwrap().success());
    let mutations = observer.poll(&mut cursor).mutations;
    assert_eq!(mutations.len(), 2);
    assert!(
        mutations[0].revision().sequence < mutations[1].revision().sequence
    );
    let CacheMutation::Put { payload, .. } = mutations[1] else {
        panic!("put");
    };
    let expected = observer.read_payload(payload).unwrap();
    assert_eq!(cache.poll().observed, 2);
    let CacheRead::Hit(entry) = store.read(b"same-key") else {
        panic!("hit");
    };
    assert_eq!(entry.value, expected);
    cache.transport().unlink_rings().unwrap();
    cache.transport().arena().unlink().unwrap();
}

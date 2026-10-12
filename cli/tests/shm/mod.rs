use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicU8, Ordering};

use orbit_core::shm::ring_segment_name_for_uid;

use super::{
    c_name, discover, discover_fleets_in, effective_uid, fleet_hint, unlink,
};

#[test]
fn fleet_hints_accept_exact_membership_and_uid_scoped_segment_names() {
    assert_eq!(fleet_hint("orbit-web-app.fleet", 501), Some("web-app"));
    assert_eq!(fleet_hint("orbit-web-app-193-501.lock", 501), Some("web-app"));
    for name in [
        "orbit-web-app-193-502.lock",
        "orbit-web-193-0501.lock",
        "orbit-web-0193-501.lock",
        "orbit-web-256-501.lock",
        "orbit-web-193-501.lane0.lock",
        "orbit-web-193-501.lock-extra",
        "other-web.fleet",
        "orbit-.fleet",
        "orbit--193-501.lock",
    ] {
        assert_eq!(fleet_hint(name, 501), None, "{name}");
    }
}

#[test]
fn fleets_verify_shm_deduplicate_hints_and_skip_stale_locks() {
    let uid = effective_uid();
    let fleet = format!("f{:x}", std::process::id());
    let dir = std::env::temp_dir()
        .join(format!("orbit-cli-fleets-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let mut objects = Vec::new();
    for kind in [240, 241] {
        let name = ring_segment_name_for_uid(&fleet, kind, uid);
        let cname = c_name(&name).unwrap();
        // SAFETY: exclusively create an isolated test object with a valid name.
        let fd = unsafe {
            libc::shm_open(
                cname.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                0o600,
            )
        };
        assert!(fd >= 0, "{name}: {}", io_error());
        // SAFETY: the successful open transfers unique descriptor ownership.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        // SAFETY: the descriptor is valid and 4096 is a representable length.
        assert_eq!(unsafe { libc::ftruncate(fd.as_raw_fd(), 4096) }, 0);
        drop(fd);
        std::fs::write(
            dir.join(format!("{}.lock", name.trim_start_matches('/'))),
            b"",
        )
        .unwrap();
        objects.push(cname);
    }
    for filename in [
        format!("orbit-{fleet}.fleet"),
        format!("orbit-z{:x}.fleet", std::process::id()),
        "orbit-this_is_a_very_long_memory_only_fleet_name.fleet".to_owned(),
    ] {
        std::fs::write(dir.join(filename), b"").unwrap();
    }
    let fleets = discover_fleets_in(&dir, uid).unwrap();
    assert_eq!(fleets.len(), 1);
    assert_eq!(fleets[0].name, fleet);
    assert_eq!(fleets[0].segments, 2);
    let actual_size: u64 = discover(&fleet, uid, None)
        .unwrap()
        .iter()
        .map(|segment| segment.size)
        .sum();
    assert_eq!(fleets[0].size, actual_size);
    assert!(actual_size >= 8192);
    for object in objects {
        // SAFETY: only remove the exclusive objects this test created.
        assert_eq!(unsafe { libc::shm_unlink(object.as_ptr()) }, 0);
    }
    assert!(discover_fleets_in(&dir, uid).unwrap().is_empty());
    for entry in std::fs::read_dir(&dir).unwrap() {
        std::fs::remove_file(entry.unwrap().path()).unwrap();
    }
    std::fs::remove_dir(&dir).unwrap();
    assert!(discover_fleets_in(&dir, uid).unwrap().is_empty());
}

static NEXT_KIND: AtomicU8 = AtomicU8::new(240);

#[test]
fn follows_the_orbit_name_contract() {
    assert_eq!(
        ring_segment_name_for_uid("web", 231, 501),
        "/orbit-web-231-501"
    );
}

#[test]
fn discovers_and_unlinks_an_object() {
    let uid = effective_uid();
    let kind = NEXT_KIND.fetch_add(1, Ordering::Relaxed);
    let fleet = format!("t{:x}", std::process::id());
    let name = ring_segment_name_for_uid(&fleet, kind, uid);
    let c_name = c_name(&name).expect("test name must be valid");

    // SAFETY: `c_name` and flags form a valid exclusive SHM create call.
    let raw_fd = unsafe {
        libc::shm_open(
            c_name.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )
    };
    assert!(raw_fd >= 0, "failed to create {name}: {}", io_error());
    // SAFETY: `raw_fd` was returned by `shm_open` and is uniquely owned.
    let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
    // SAFETY: `fd` is valid and the requested length is representable.
    let rc = unsafe { libc::ftruncate(fd.as_raw_fd(), 4096) };
    assert_eq!(rc, 0, "failed to size {name}: {}", io_error());
    drop(fd);

    let found =
        discover(&fleet, uid, Some(kind)).expect("discovery must succeed");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].kind, kind);
    assert_eq!(found[0].name, name);
    assert!(found[0].size >= 4096);

    unlink(&found[0], uid).expect("unlink must succeed");
    assert!(
        discover(&fleet, uid, Some(kind))
            .expect("second discovery must succeed")
            .is_empty()
    );
}

#[test]
fn discovery_does_not_match_an_orbit_prefix() {
    let uid = effective_uid();
    let fleet = format!("p{:x}", std::process::id());
    let similar_name = format!("/orbit-{fleet}-231-{uid}-extra");
    let c_name = c_name(&similar_name).expect("test name must be valid");

    // SAFETY: `c_name` and flags form a valid exclusive SHM create call.
    let raw_fd = unsafe {
        libc::shm_open(
            c_name.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )
    };
    assert!(raw_fd >= 0, "failed to create {similar_name}: {}", io_error());
    // SAFETY: `raw_fd` was returned by `shm_open` and is uniquely owned.
    drop(unsafe { OwnedFd::from_raw_fd(raw_fd) });

    let found = discover(&fleet, uid, None).expect("discovery must succeed");
    assert!(found.is_empty());

    // SAFETY: `c_name` identifies the object created by this test.
    let rc = unsafe { libc::shm_unlink(c_name.as_ptr()) };
    assert_eq!(rc, 0, "failed to remove {similar_name}: {}", io_error());
}

fn io_error() -> std::io::Error {
    std::io::Error::last_os_error()
}

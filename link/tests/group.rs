#![cfg(all(unix, feature = "tokio"))]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use orbit_core::shm::{ShmAccessPolicy, ShmRegion};
use orbit_link::{GroupAccess, InboxGeometry, LinkSegment, LinkSpec, Received};
use orbit_stream::{StreamSpec, Ticket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn spec() -> LinkSpec {
    LinkSpec::new(
        2,
        240,
        InboxGeometry::new(8, 1024),
        StreamSpec::new(241, 2, 1024),
    )
}

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let path = PathBuf::from(format!("/tmp/og{}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn access(path: &Path) -> GroupAccess {
    GroupAccess::new(
        unsafe { libc::geteuid() },
        unsafe { libc::getegid() },
        path.join("ipc"),
    )
    .unwrap()
}

fn subprocess(test: &str) -> bool {
    if std::env::var_os("ORBIT_GROUP_CHILD").is_some() {
        return false;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env("ORBIT_GROUP_CHILD", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

#[test]
fn group_link_inbox_stream_wakes_and_private_resources() {
    if subprocess("group_link_inbox_stream_wakes_and_private_resources") {
        return;
    }
    // Process-wide creation masks belong only to this isolated child.
    unsafe {
        libc::umask(0o007);
    }
    let scratch = Scratch::new();
    let access = access(&scratch.0);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    runtime.block_on(async {
        let link = Arc::new(
            LinkSegment::open_group("example", spec(), access.clone()).unwrap(),
        );
        let a = link.join("example-a", "source", 1).unwrap();
        let b = link.join("example-b", "target", 2).unwrap();
        let mut rx = link.receiver(b.lane).unwrap();
        let mut frame = Vec::new();
        let (received, ()) = tokio::join!(rx.next(&mut frame), async {
            link.inbox().write(b.lane, b"hello").unwrap();
        });
        assert_eq!(received.unwrap(), Received::Frame);
        assert_eq!(frame, b"hello");
        let bodies_a = link.bodies(a.lane, 1).unwrap();
        let bodies_b = link.bodies(b.lane, 2).unwrap();
        let (mut endpoint_a, ticket) = bodies_a.create().unwrap();
        let mut endpoint_b = bodies_b.accept(ticket).unwrap();
        let mut bytes = [0; 4];
        let (read, write) = tokio::join!(
            endpoint_b.read_exact(&mut bytes),
            endpoint_a.write_all(b"ping")
        );
        read.unwrap();
        write.unwrap();
        assert_eq!(&bytes, b"ping");
        // A distinct group scope cannot reuse this table's mappings or locks.
        let second = GroupAccess::new(
            access.owner_uid(),
            access.gid(),
            scratch.0.join("other"),
        )
        .unwrap();
        let other =
            LinkSegment::open_group("example", spec(), second.clone()).unwrap();
        assert_ne!(
            access.segment_name("example", 240),
            second.segment_name("example", 240)
        );
        assert_eq!(other.inbox().consumers(0).unwrap(), 0);
        other.unlink().unwrap();
        orbit_stream::Streams::with_group(
            "example",
            2,
            orbit_core::NodeId::ZERO,
            orbit_stream::Incarnation::new(9),
            spec().streams,
            second,
        )
        .unwrap()
        .unlink()
        .unwrap();
        // Default core SHM stays 0600; group access is not a global policy.
        let private_name = orbit_core::shm::ring_segment_name(
            &format!("gp{:x}", std::process::id()),
            242,
        );
        let private = ShmRegion::open_or_create(&private_name, 4096).unwrap();
        let fd = unsafe {
            libc::shm_open(
                std::ffi::CString::new(private_name).unwrap().as_ptr(),
                libc::O_RDONLY,
                0,
            )
        };
        assert!(fd >= 0);
        use std::os::fd::FromRawFd;
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        assert_eq!(file.metadata().unwrap().mode() & 0o777, 0o600);
        private.unlink().unwrap();
        drop(endpoint_a);
        drop(endpoint_b);
        drop(rx);
        link.inbox().release_lane(a.lane).unwrap();
        link.inbox().release_lane(b.lane).unwrap();
        drop(a);
        drop(b);
        bodies_a.unlink().unwrap();
        link.unlink().unwrap();
    });
}

#[test]
fn group_access_rejects_unsafe_paths_modes_and_wrong_owner() {
    if subprocess("group_access_rejects_unsafe_paths_modes_and_wrong_owner") {
        return;
    }
    unsafe {
        libc::umask(0o007);
    }
    let scratch = Scratch::new();
    let access = access(&scratch.0);
    access.prepare().unwrap();
    std::fs::set_permissions(
        access.directory(),
        std::fs::Permissions::from_mode(0o770),
    )
    .unwrap();
    assert!(access.prepare().is_err());
    // Restore our fixture, not deployment state.
    std::fs::set_permissions(
        access.directory(),
        std::fs::Permissions::from_mode(0o750),
    )
    .unwrap();
    let wrong = GroupAccess::new(
        access.owner_uid().wrapping_add(1),
        access.gid(),
        access.directory(),
    )
    .unwrap();
    assert!(wrong.prepare().is_err());
    let wrong_gid = GroupAccess::new(
        access.owner_uid(),
        access.gid().wrapping_add(1),
        access.directory(),
    )
    .unwrap();
    assert!(wrong_gid.prepare().is_err());
    let link =
        LinkSegment::open_group("example", spec(), access.clone()).unwrap();
    let held = access
        .try_hold_lane(&access.segment_name("example", 240), 0)
        .unwrap()
        .unwrap();
    assert!(
        access
            .try_hold_lane(&access.segment_name("example", 240), 0)
            .unwrap()
            .is_none()
    );
    drop(held);
    let path = access.directory().join(format!(
        "{}.lane0.lock",
        access.segment_name("example", 240).trim_start_matches('/')
    ));
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(scratch.0.join("victim"), &path).unwrap();
    assert!(
        access.try_hold_lane(&access.segment_name("example", 240), 0).is_err()
    );
    assert!(!scratch.0.join("victim").exists());
    let bell = access.bell_path(&access.segment_name("example", 240), "0");
    std::os::unix::fs::symlink(scratch.0.join("victim"), &bell).unwrap();
    assert!(access.bind_bell(&bell).is_err());
    assert!(!scratch.0.join("victim").exists());
    link.unlink().unwrap();
    orbit_stream::Streams::with_group(
        "example",
        2,
        orbit_core::NodeId::ZERO,
        orbit_stream::Incarnation::new(9),
        spec().streams,
        access,
    )
    .unwrap()
    .unlink()
    .unwrap();
}

#[test]
fn group_link_recovers_a_killed_lane_and_its_streams() {
    if let Some(path) = std::env::var_os("ORBIT_GROUP_DEATH_DIR") {
        unsafe {
            libc::umask(0o007);
        }
        let access = access(Path::new(&path));
        let link = LinkSegment::open_group("example", spec(), access).unwrap();
        let joined = link.join("example-doomed", "source", 91).unwrap();
        let bodies = link.bodies(joined.lane, 91).unwrap();
        let (_endpoint, ticket) = bodies.create().unwrap();
        println!("GROUP_TICKET {} {}", joined.lane, ticket);
        std::io::stdout().flush().unwrap();
        let mut input = [0];
        std::io::Read::read_exact(&mut std::io::stdin(), &mut input).unwrap();
        panic!("parent should kill this isolated process");
    }
    if subprocess("group_link_recovers_a_killed_lane_and_its_streams") {
        return;
    }
    unsafe {
        libc::umask(0o007);
    }
    let scratch = Scratch::new();
    let access = access(&scratch.0);
    let link = LinkSegment::open_group("example", spec(), access).unwrap();
    let local = link.join("example-survivor", "target", 90).unwrap();
    let bodies = link.bodies(local.lane, 90).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "group_link_recovers_a_killed_lane_and_its_streams",
            "--nocapture",
        ])
        .env("ORBIT_GROUP_DEATH_DIR", &scratch.0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    let (lane, ticket) = loop {
        line.clear();
        assert_ne!(reader.read_line(&mut line).unwrap(), 0);
        if let Some(data) = line.strip_prefix("GROUP_TICKET ") {
            let mut words = data.split_whitespace();
            break (
                words.next().unwrap().parse::<usize>().unwrap(),
                words.next().unwrap().parse::<Ticket>().unwrap(),
            );
        }
    };
    let endpoint = bodies.accept(ticket).unwrap();
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    let replacement = link.join("example-replacement", "source", 92).unwrap();
    assert_eq!(replacement.lane, lane);
    assert!(
        replacement
            .reclaimed
            .iter()
            .any(|dead| dead.lane == lane && dead.incarnation == 91)
    );
    assert!(matches!(
        endpoint.try_read(&mut [0; 1]),
        Err(orbit_stream::Error::Reset | orbit_stream::Error::Stale(_))
    ));
    drop(endpoint);
    link.inbox().release_lane(local.lane).unwrap();
    link.inbox().release_lane(replacement.lane).unwrap();
    drop(local);
    drop(replacement);
    bodies.unlink().unwrap();
    link.unlink().unwrap();
}

// This fixture never creates OS users/groups. Only its child processes adopt
// numeric credentials; the parent's host credentials remain unchanged.
#[test]
#[ignore = "requires root to run isolated child processes under distinct UIDs"]
fn cross_uid_group_transport() {
    if let Ok(action) = std::env::var("ORBIT_CROSS_UID_ACTION") {
        let directory =
            PathBuf::from(std::env::var_os("ORBIT_CROSS_UID_DIR").unwrap());
        let access =
            GroupAccess::new(61001, 61000, directory.join("ipc")).unwrap();
        if action == "deny" {
            assert!(
                LinkSegment::open_group("example", spec(), access).is_err()
            );
            return;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        runtime.block_on(async {
            let link = Arc::new(
                LinkSegment::open_group("example", spec(), access.clone())
                    .unwrap(),
            );
            if action == "owner" {
                // Other resources remain private despite Link participation.
                let private = ShmRegion::open_or_create(
                    &access.segment_name("private-test", 242),
                    4096,
                )
                .unwrap();
                let joined = link.join("example-target", "target", 1).unwrap();
                let bodies = link.bodies(joined.lane, 1).unwrap();
                let mut rx = link.receiver(joined.lane).unwrap();
                println!("GROUP_READY");
                std::io::stdout().flush().unwrap();
                let mut frame = Vec::new();
                loop {
                    if rx.next(&mut frame).await.unwrap() == Received::Frame {
                        break;
                    }
                }
                let ticket: Ticket =
                    std::str::from_utf8(&frame).unwrap().parse().unwrap();
                let mut endpoint = bodies.accept(ticket).unwrap();
                let mut bytes = [0; 4];
                endpoint.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"ping");
                endpoint.write_all(b"pong").await.unwrap();
                endpoint.finish().unwrap();
                // Wait for peer release before dropping our incarnation.
                let mut eof = [0];
                assert_eq!(endpoint.read(&mut eof).await.unwrap(), 0);
                drop(rx);
                drop(endpoint);
                link.inbox().release_lane(joined.lane).unwrap();
                private.unlink().unwrap();
            } else {
                assert!(
                    ShmRegion::open_existing_read_write(
                        &access.segment_name("private-test", 242),
                        4096,
                        61001,
                        ShmAccessPolicy::OwnerOnly
                    )
                    .is_err()
                );
                let lock = access.directory().join(format!(
                    "{}.lane0.lock",
                    access.segment_name("example", 240).trim_start_matches('/')
                ));
                assert!(std::fs::remove_file(lock).is_err());
                let joined = link.join("example-source", "source", 2).unwrap();
                let bodies = link.bodies(joined.lane, 2).unwrap();
                let (mut endpoint, ticket) = bodies.create().unwrap();
                let target = (0..link.inbox().lanes())
                    .find(|&lane| {
                        link.inbox().service(lane).unwrap().as_deref()
                            == Some("example-target")
                    })
                    .unwrap();
                link.inbox()
                    .write(target, ticket.to_string().as_bytes())
                    .unwrap();
                endpoint.write_all(b"ping").await.unwrap();
                let mut bytes = [0; 4];
                endpoint.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"pong");
                endpoint.finish().unwrap();
                assert!(link.unlink().is_err());
                assert!(bodies.unlink().is_err());
                link.inbox().release_lane(joined.lane).unwrap();
            }
        });
        return;
    }
    assert_eq!(
        unsafe { libc::geteuid() },
        0,
        "run only in an authorized isolated test environment"
    );
    let scratch = Scratch::new();
    std::fs::set_permissions(
        &scratch.0,
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let parent =
        std::ffi::CString::new(scratch.0.as_os_str().as_encoded_bytes())
            .unwrap();
    assert_eq!(unsafe { libc::chown(parent.as_ptr(), 61001, 61000) }, 0);
    let spawn = |action: &str, uid, gid| {
        use std::os::unix::process::CommandExt;
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "cross_uid_group_transport",
                "--nocapture",
            ])
            .env("ORBIT_CROSS_UID_ACTION", action)
            .env("ORBIT_CROSS_UID_DIR", &scratch.0)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        unsafe {
            command.pre_exec(move || {
                if libc::setgroups(1, &gid) != 0
                    || libc::setgid(gid) != 0
                    || libc::setuid(uid) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                libc::umask(0o007);
                Ok(())
            });
        }
        command.spawn().unwrap()
    };
    let mut owner = spawn("owner", 61001, 61000);
    let mut lines = BufReader::new(owner.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        assert_ne!(
            lines.read_line(&mut line).unwrap(),
            0,
            "owner exited before readiness"
        );
        if line.contains("GROUP_READY") {
            break;
        }
    }
    assert!(spawn("deny", 61003, 61003).wait().unwrap().success());
    assert!(spawn("peer", 61002, 61000).wait().unwrap().success());
    assert!(owner.wait().unwrap().success());
    // All test participants are stopped; unlink only these two exact objects.
    let access = GroupAccess::new(61001, 61000, scratch.0.join("ipc")).unwrap();
    for kind in [240, 241] {
        let name = std::ffi::CString::new(access.segment_name("example", kind))
            .unwrap();
        assert_eq!(unsafe { libc::shm_unlink(name.as_ptr()) }, 0);
    }
}

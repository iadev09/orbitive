use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::PathBuf;
use std::process::Command;

use super::*;

const CHILD: &str = "ORBIT_LOCK_IDENTITY_TEST";
const NAME: &str = "ORBIT_LOCK_IDENTITY_NAME";

#[test]
fn locks_ignore_the_runtime_environment() {
    if let Ok(phase) = std::env::var(CHILD) {
        let name = std::env::var(NAME).unwrap();
        let held = phase == "held";
        let fd = open_lock_file(&lock_file_path(&name)).unwrap();
        let acquired = unsafe {
            libc::flock(fd.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB)
        } == 0;
        if !acquired {
            assert_eq!(
                io::Error::last_os_error().kind(),
                io::ErrorKind::WouldBlock
            );
        }
        assert_eq!(acquired, !held, "initialization lock identity diverged");
        let prepared = PreparedLaneLock::open(&name, 0).unwrap();
        assert_eq!(prepared.try_lock().unwrap().is_none(), held);
        assert_eq!(try_hold_lane(&name, 1).unwrap().is_none(), held);
        assert_eq!(try_hold_watcher(&name, 0).unwrap().is_none(), held);
        let uid = unsafe { libc::geteuid() };
        assert_eq!(
            try_lock_fleet_exclusive(&name, uid).unwrap().is_none(),
            held
        );
        assert_eq!(companion_lock_files(&name, uid).unwrap().len(), 4);
        return;
    }

    let name = format!("orbit-li{:x}", std::process::id());
    let directory = std::env::temp_dir()
        .join(format!("orbit-identity-{}", std::process::id()));
    std::fs::DirBuilder::new().mode(0o700).create(&directory).unwrap();
    let (region, initialization) =
        ShmRegion::open_or_create_locked(&format!("/{name}"), 1).unwrap();
    struct Cleanup(ShmRegion, PathBuf, String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.unlink();
            let _ = std::fs::remove_file(fleet_lock_path(&self.2));
            let _ = std::fs::remove_dir(&self.1);
        }
    }
    let cleanup = Cleanup(region, directory, name.clone());
    let prepared = PreparedLaneLock::open(&name, 0).unwrap();
    let lane = prepared.try_lock().unwrap().unwrap();
    let other = try_hold_lane(&name, 1).unwrap().unwrap();
    let watcher = try_hold_watcher(&name, 0).unwrap().unwrap();
    let member = join_fleet_membership(&name).unwrap();
    let locks = (initialization, lane, other, watcher, member);
    let inode = std::fs::metadata(lock_file_path(&name)).unwrap().ino();

    let check = |phase: &str| {
        for configured in [false, true] {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "shm::lock_identity_tests::locks_ignore_the_runtime_environment", "--nocapture"])
                .env(CHILD, phase)
                .env(NAME, &name);
            if configured {
                command
                    .env("XDG_RUNTIME_DIR", &cleanup.1)
                    .env("TMPDIR", &cleanup.1);
            } else {
                command.env_remove("XDG_RUNTIME_DIR").env_remove("TMPDIR");
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                std::fs::metadata(lock_file_path(&name)).unwrap().ino(),
                inode
            );
        }
    };
    check("held");
    drop(locks);
    check("released");
}

#[test]
fn a_region_locks_the_file_its_path_names_and_recreates_it_there() {
    let name = format!("orbit-ld{:x}", std::process::id());
    let (region, initialization) =
        ShmRegion::open_or_create_locked(&format!("/{name}"), 1).unwrap();
    drop(initialization);
    struct Cleanup(ShmRegion);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.unlink();
        }
    }
    let cleanup = Cleanup(region);
    let region = &cleanup.0;
    let refused = |region: &ShmRegion| {
        region.try_lock_exclusive().err().map(|error| error.kind())
    };

    // A section opened through the verified directory holds the same file
    // that a description opened by path contends for.
    let held = region.lock_exclusive().unwrap();
    assert_eq!(refused(region), Some(io::ErrorKind::WouldBlock));
    drop(held);
    assert_eq!(refused(region), None);

    // A removed lock file comes back in that directory, under the same name.
    std::fs::remove_file(lock_file_path(&name)).unwrap();
    let held = region.lock_exclusive().unwrap();
    assert!(lock_file_path(&name).exists());
    assert_eq!(refused(region), Some(io::ErrorKind::WouldBlock));
    drop(held);
}

#[test]
fn a_process_drops_its_shared_lock_with_its_last_holder() {
    let name = format!("orbit-sh{:x}", std::process::id());
    let (region, initialization) =
        ShmRegion::open_or_create_locked(&format!("/{name}"), 1).unwrap();
    drop(initialization);
    struct Cleanup(ShmRegion);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.unlink();
        }
    }
    let cleanup = Cleanup(region);
    let region = &cleanup.0;
    let refused = |region: &ShmRegion| {
        region.try_lock_exclusive().err().map(|error| error.kind())
    };

    // Two holders share one kept description; another description is kept
    // out until the second of them lets go.
    let first = region.lock_shared().unwrap();
    let second = region.lock_shared().unwrap();
    assert_eq!(refused(region), Some(io::ErrorKind::WouldBlock));
    drop(first);
    assert_eq!(refused(region), Some(io::ErrorKind::WouldBlock));
    drop(second);
    assert_eq!(refused(region), None);

    // An exclusive holder of the process waits for its shared holders.
    let shared = region.lock_shared().unwrap();
    std::thread::scope(|scope| {
        let (taken, observed) = std::sync::mpsc::channel();
        scope.spawn(move || {
            let held = region.lock_exclusive().unwrap();
            taken.send(()).unwrap();
            drop(held);
        });
        assert!(observed.try_recv().is_err(), "exclusive waits for shared");
        drop(shared);
        observed
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("exclusive is granted once the last shared holder leaves");
    });
}

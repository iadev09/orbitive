use std::ffi::CString;
use std::fs;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::PathBuf;

use orbit_core::shm::ring_segment_name_for_uid;

#[derive(Debug, Eq, PartialEq)]
pub struct Segment {
    pub kind: u8,
    pub name: String,
    pub size: u64,
}

pub fn effective_uid() -> u32 {
    // SAFETY: `geteuid` has no error path.
    unsafe { libc::geteuid() }
}

pub fn discover(fleet: &str, uid: u32, kind: Option<u8>) -> io::Result<Vec<Segment>> {
    validate_fleet(fleet)?;

    let mut segments = Vec::new();
    if let Some(kind) = kind {
        if let Some(segment) = inspect(fleet, uid, kind)? {
            segments.push(segment);
        }
        return Ok(segments);
    }

    for kind in u8::MIN..=u8::MAX {
        if let Some(segment) = inspect(fleet, uid, kind)? {
            segments.push(segment);
        }
    }
    Ok(segments)
}

pub fn unlink(segment: &Segment) -> io::Result<()> {
    let name = c_name(&segment.name)?;
    // SAFETY: `name` is a valid, NUL-terminated POSIX SHM name.
    let rc = unsafe { libc::shm_unlink(name.as_ptr()) };
    if rc != 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ENOENT) {
            return Err(context(&segment.name, error));
        }
    }

    let lock_path = lock_path(&segment.name);
    match fs::remove_file(&lock_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(context(&lock_path.display().to_string(), error)),
    }
}

fn inspect(fleet: &str, uid: u32, kind: u8) -> io::Result<Option<Segment>> {
    let name = ring_segment_name_for_uid(fleet, kind, uid);
    let c_name = c_name(&name)?;

    let raw_fd = loop {
        // SAFETY: `c_name` is valid and flags are POSIX `shm_open` flags.
        let raw_fd = unsafe { libc::shm_open(c_name.as_ptr(), libc::O_RDONLY, 0) };
        if raw_fd >= 0 {
            break raw_fd;
        }

        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if error.raw_os_error() == Some(libc::ENOENT) {
            return Ok(None);
        }
        return Err(context(&name, error));
    };

    // SAFETY: `raw_fd` was returned by `shm_open` and is uniquely owned here.
    let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `fd` is valid and `stat` points to writable storage.
    let rc = unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(context(&name, io::Error::last_os_error()));
    }

    // SAFETY: `fstat` succeeded and initialized the structure.
    let size = unsafe { stat.assume_init() }.st_size;
    let size = u64::try_from(size).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("SHM object {name} reported a negative size"),
        )
    })?;

    Ok(Some(Segment { kind, name, size }))
}

fn validate_fleet(fleet: &str) -> io::Result<()> {
    if fleet.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "fleet name must not be empty",
        ));
    }
    if fleet.contains('/') || fleet.contains('\0') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "fleet name must not contain '/' or a NUL byte",
        ));
    }
    Ok(())
}

fn c_name(name: &str) -> io::Result<CString> {
    CString::new(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "SHM name has a NUL byte"))
}

fn lock_path(shm_name: &str) -> PathBuf {
    PathBuf::from("/tmp").join(format!("{}.lock", shm_name.trim_start_matches('/')))
}

fn context(target: &str, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{target}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::sync::atomic::{AtomicU8, Ordering};

    use orbit_core::shm::ring_segment_name_for_uid;

    use super::{c_name, discover, effective_uid, unlink};

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

        let found = discover(&fleet, uid, Some(kind)).expect("discovery must succeed");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, kind);
        assert_eq!(found[0].name, name);
        assert!(found[0].size >= 4096);

        unlink(&found[0]).expect("unlink must succeed");
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
        assert!(
            raw_fd >= 0,
            "failed to create {similar_name}: {}",
            io_error()
        );
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
}

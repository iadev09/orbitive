use std::collections::BTreeSet;
use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;
use std::{fs, io};

use orbit_core::shm::{
    SHM_NAMESPACE, companion_lock_files, lock_dir_for_uid,
    ring_segment_name_for_uid,
};

#[derive(Debug, Eq, PartialEq)]
pub struct Segment {
    pub kind: u8,
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub struct FleetSummary {
    pub name: String,
    pub segments: usize,
    pub size: u64,
}

pub fn discover_fleets(uid: u32) -> io::Result<Vec<FleetSummary>> {
    discover_fleets_in(&lock_dir_for_uid(uid), uid)
}

fn discover_fleets_in(
    dir: &Path,
    uid: u32,
) -> io::Result<Vec<FleetSummary>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => return Err(context(&dir.display().to_string(), error)),
    };
    let mut candidates = BTreeSet::new();
    for entry in entries {
        let entry = entry?;
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !file_type.is_file() {
            continue;
        }
        let filename = entry.file_name();
        if let Some(fleet) =
            filename.to_str().and_then(|name| fleet_hint(name, uid))
        {
            candidates.insert(fleet.to_owned());
        }
    }
    let mut fleets = Vec::new();
    for name in candidates {
        let segments = discover(&name, uid, None)?;
        if segments.is_empty() {
            continue;
        }
        let size = segments.iter().try_fold(0_u64, |total, segment| {
            total.checked_add(segment.size).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "fleet size overflow",
                )
            })
        })?;
        fleets.push(FleetSummary { name, segments: segments.len(), size });
    }
    Ok(fleets)
}

fn fleet_hint(
    filename: &str,
    uid: u32,
) -> Option<&str> {
    let stem = filename.strip_prefix(SHM_NAMESPACE)?.strip_prefix('-')?;
    if let Some(fleet) = stem.strip_suffix(".fleet") {
        validate_fleet(fleet).ok()?;
        return Some(fleet);
    }
    let stem = stem.strip_suffix(".lock")?;
    let (rest, owner) = stem.rsplit_once('-')?;
    if owner != uid.to_string() {
        return None;
    }
    let (fleet, kind) = rest.rsplit_once('-')?;
    let kind_number = kind.parse::<u8>().ok()?;
    if kind != kind_number.to_string() {
        return None;
    }
    validate_fleet(fleet).ok()?;
    Some(fleet)
}

pub fn effective_uid() -> u32 {
    // SAFETY: `geteuid` has no error path.
    unsafe { libc::geteuid() }
}

pub fn discover(
    fleet: &str,
    uid: u32,
    kind: Option<u8>,
) -> io::Result<Vec<Segment>> {
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

pub fn unlink(
    segment: &Segment,
    uid: u32,
) -> io::Result<()> {
    let name = c_name(&segment.name)?;
    // SAFETY: `name` is a valid, NUL-terminated POSIX SHM name.
    let rc = unsafe { libc::shm_unlink(name.as_ptr()) };
    if rc != 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ENOENT) {
            return Err(context(&segment.name, error));
        }
    }

    for lock_path in companion_lock_files(&segment.name, uid)? {
        match fs::remove_file(&lock_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(context(&lock_path.display().to_string(), error));
            }
        }
    }
    Ok(())
}

fn inspect(
    fleet: &str,
    uid: u32,
    kind: u8,
) -> io::Result<Option<Segment>> {
    let name = ring_segment_name_for_uid(fleet, kind, uid);
    let c_name = c_name(&name)?;

    let raw_fd = loop {
        // SAFETY: `c_name` is valid and flags are POSIX `shm_open` flags.
        let raw_fd =
            unsafe { libc::shm_open(c_name.as_ptr(), libc::O_RDONLY, 0) };
        if raw_fd >= 0 {
            break raw_fd;
        }

        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        // A stale membership file may belong to a memory-only fleet whose
        // name cannot fit the platform's POSIX SHM namespace.
        if matches!(
            error.raw_os_error(),
            Some(libc::ENOENT | libc::ENAMETOOLONG)
        ) {
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
    CString::new(name).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "SHM name has a NUL byte")
    })
}

fn context(
    target: &str,
    error: io::Error,
) -> io::Error {
    io::Error::new(error.kind(), format!("{target}: {error}"))
}

#[cfg(test)]
#[path = "../tests/shm/mod.rs"]
mod tests;

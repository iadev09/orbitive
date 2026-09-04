//! POSIX shared-memory helpers — V1 substrate for cross-process rings.
//!
//! Wraps `shm_open` / `ftruncate` / `mmap` / `munmap` / `shm_unlink`
//! into a small, RAII-friendly API. Unix-only; Windows support is
//! a separate concern (Win32 named file mapping) that can land later.
//!
//! ## Naming
//!
//! Segments are named `/orbit-{fleet}-{kind}-{uid}` — fleet name from
//! the embedder, KIND from `OrbitTyped::KIND`, UID from `geteuid()`.
//! UID-scoping avoids the `/dev/shm` sticky-bit cross-user collision
//! problem (a stale segment owned by one user blocks another from
//! `shm_unlink`-ing it on next boot).
//!
//! Rings that require a process-recoverable writer lock also open a
//! companion `/tmp/orbit-{fleet}-{kind}-{uid}.lock` file. It carries no
//! ring data or state; it only supplies a regular-file inode for `flock`,
//! because advisory locking on a POSIX SHM descriptor is not uniformly
//! supported across the Unix targets Orbit serves. An unlocked stale
//! companion file is safe to reuse.
//!
//! ## Lifetime
//!
//! [`ShmRegion`] owns the mapped pointer and unmaps on drop. It does
//! NOT `shm_unlink` on drop — the segment lives until an explicit
//! [`ShmRegion::unlink`] call. This matches POSIX convention: a
//! segment with mapped users is not removed; `shm_unlink` only
//! prevents *new* opens, the current mapping stays valid until the
//! last process unmaps.

#![cfg(unix)]

use std::ffi::CString;
use std::fs::OpenOptions;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;

/// Namespace used by Orbit POSIX shared-memory objects.
pub const SHM_NAMESPACE: &str = "orbit";

/// A mapped POSIX SHM region. Drop unmaps; `unlink` removes the
/// underlying name and any companion lock file (only the *creator*
/// should call it on shutdown).
pub struct ShmRegion {
    name: CString,
    lock_path: PathBuf,
    /// Whether this region uses the companion file for ordered writes.
    ///
    /// The descriptor itself is deliberately not retained: every critical
    /// section opens its own file description so a later fork does not inherit
    /// an idle descriptor that can keep a future `flock` alive.
    process_lock: bool,
    ptr: NonNull<u8>,
    len: usize,
    /// True when this handle was the one that *created* the segment
    /// (so it knows to `shm_unlink` if asked). Other attachers see
    /// `false`.
    created: bool,
}

impl ShmRegion {
    /// Open or create a shared-memory segment of `size` bytes,
    /// memory-mapped read/write. Idempotent: if the segment already
    /// exists with the same name and enough mapped bytes, it is reused
    /// (`created = false`). First creation does `ftruncate(size)`;
    /// later opens verify the existing object before mapping it. Some
    /// platforms report a page-rounded SHM size, so a larger `st_size`
    /// is valid; the owning data structure must verify its own header.
    pub fn open_or_create(name: &str, size: usize) -> io::Result<Self> {
        let (region, initialization_lock) = Self::open_or_create_inner(name, size, false)?;
        debug_assert!(initialization_lock.is_none());
        Ok(region)
    }

    /// Open or create a region while holding its process lock through caller
    /// initialization. This prevents a peer from observing the interval
    /// between `shm_open` and the owning data structure's initialized header.
    pub fn open_or_create_locked(name: &str, size: usize) -> io::Result<(Self, ShmRegionLock)> {
        let (region, initialization_lock) = Self::open_or_create_inner(name, size, true)?;
        Ok((
            region,
            initialization_lock.expect("locked SHM open must return its initialization lock"),
        ))
    }

    fn open_or_create_inner(
        name: &str,
        size: usize,
        process_lock: bool,
    ) -> io::Result<(Self, Option<ShmRegionLock>)> {
        let cname = CString::new(name)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "shm name has nul byte"))?;
        let lock_path = lock_file_path(name);
        let initialization_lock = if process_lock {
            Some(lock_path_exclusive(&lock_path)?)
        } else {
            None
        };

        // Try create-exclusive first; if it already exists, open.
        let (raw_fd, created) = unsafe {
            // SAFETY: passing a valid C string and well-known POSIX flags.
            let fd = libc::shm_open(
                cname.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                0o600,
            );
            if fd >= 0 {
                (fd, true)
            } else {
                // Could be EEXIST (already created by a peer) or another error.
                let err = io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EEXIST) {
                    return Err(err);
                }
                let fd = libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600);
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                (fd, false)
            }
        };
        // SAFETY: `raw_fd` was returned by `shm_open` and is now uniquely
        // owned by this scope.
        let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };

        // Size the segment on first creation.
        if created {
            // SAFETY: fd is a valid POSIX fd we just received.
            let rc = unsafe { libc::ftruncate(fd.as_raw_fd(), size as libc::off_t) };
            if rc != 0 {
                let err = io::Error::last_os_error();
                let _ = unsafe { libc::shm_unlink(cname.as_ptr()) };
                return Err(err);
            }
        }

        // Never mmap beyond the real SHM object: access past it can raise
        // SIGBUS. A larger reported size is valid on platforms (notably
        // macOS) that page-round POSIX SHM objects; callers verify their
        // own ABI metadata after mapping.
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let stat_rc = unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) };
        if stat_rc != 0 {
            let err = io::Error::last_os_error();
            if created {
                let _ = unsafe { libc::shm_unlink(cname.as_ptr()) };
            }
            return Err(err);
        }
        let actual_size = unsafe { stat.assume_init() }.st_size;
        if actual_size < 0 || (actual_size as usize) < size {
            if created {
                let _ = unsafe { libc::shm_unlink(cname.as_ptr()) };
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {name} size {actual_size} is smaller than requested mapping {size}"
                ),
            ));
        }

        // Memory-map the segment.
        // SAFETY: fd valid, size positive, flags well-known.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            let err = io::Error::last_os_error();
            if created {
                let _ = unsafe { libc::shm_unlink(cname.as_ptr()) };
            }
            return Err(err);
        }

        // SAFETY: mmap returned a non-null pointer (we just checked).
        let ptr = NonNull::new(ptr.cast::<u8>()).expect("mmap returned non-null on success");

        Ok((
            Self {
                name: cname,
                lock_path,
                process_lock,
                ptr,
                len: size,
                created,
            },
            initialization_lock,
        ))
    }

    /// Raw mapped pointer to the start of the region.
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Length of the mapped region (the `size` passed to `open_or_create`).
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// True when this handle was the one that created the segment.
    /// Useful for picking which process performs first-time
    /// initialization of the header.
    pub fn created(&self) -> bool {
        self.created
    }

    /// Acquire an exclusive cross-process lock tied to this SHM name.
    ///
    /// `flock` ownership is held by the kernel and is released when a process
    /// exits or the descriptor closes, including abnormal termination.
    pub fn lock_exclusive(&self) -> io::Result<ShmRegionLock> {
        if !self.process_lock {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SHM region was opened without a process lock",
            ));
        }
        lock_path_exclusive(&self.lock_path)
    }

    #[cfg(test)]
    pub(crate) fn try_lock_exclusive(&self) -> io::Result<ShmRegionLock> {
        if !self.process_lock {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SHM region was opened without a process lock",
            ));
        }
        let lock_fd = open_lock_file(&self.lock_path)?;
        let rc = unsafe { libc::flock(lock_fd.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            Ok(ShmRegionLock { lock_fd })
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Remove the underlying segment name. Existing mappings stay
    /// valid until each process drops its `ShmRegion`. Use only on
    /// shutdown / fleet teardown by the process that owns lifecycle.
    pub fn unlink(&self) -> io::Result<()> {
        // SAFETY: name is a valid C string.
        let rc = unsafe { libc::shm_unlink(self.name.as_ptr()) };
        let shm_error = if rc != 0 {
            let err = io::Error::last_os_error();
            // ENOENT is fine — segment was already unlinked.
            if err.raw_os_error() == Some(libc::ENOENT) {
                None
            } else {
                Some(err)
            }
        } else {
            None
        };
        let lock_error = match std::fs::remove_file(&self.lock_path) {
            Ok(()) => None,
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => Some(error),
        };
        if let Some(error) = shm_error.or(lock_error) {
            return Err(error);
        }
        Ok(())
    }
}

/// RAII guard for a [`ShmRegion`]'s process-recoverable exclusive lock.
///
/// Semantic crates use this when a current-state transition must be atomic
/// across fleet processes. Dropping the guard releases the kernel lock.
pub struct ShmRegionLock {
    lock_fd: OwnedFd,
}

impl Drop for ShmRegionLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.lock_fd.as_raw_fd(), libc::LOCK_UN) };
    }
}

fn lock_path_exclusive(lock_path: &Path) -> io::Result<ShmRegionLock> {
    lock_fd_exclusive(open_lock_file(lock_path)?)
}

fn open_lock_file(lock_path: &Path) -> io::Result<OwnedFd> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(lock_path)
        .map(Into::into)
}

fn lock_fd_exclusive(lock_fd: OwnedFd) -> io::Result<ShmRegionLock> {
    loop {
        let rc = unsafe { libc::flock(lock_fd.as_raw_fd(), libc::LOCK_EX) };
        if rc == 0 {
            return Ok(ShmRegionLock { lock_fd });
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

impl Drop for ShmRegion {
    fn drop(&mut self) {
        // SAFETY: ptr came from mmap of `self.len` bytes; munmap is the inverse.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

// SAFETY: the underlying region is shared memory and synchronization
// happens at the slot level (atomic seq counters); the handle itself
// is just a pointer + length, safe to send/share.
unsafe impl Send for ShmRegion {}
unsafe impl Sync for ShmRegion {}

/// Build the conventional name for an Orbit ring segment.
pub fn ring_segment_name(fleet_name: &str, kind: u8) -> String {
    // SAFETY: `geteuid` always returns a value; no error path.
    let uid = unsafe { libc::geteuid() };
    ring_segment_name_for_uid(fleet_name, kind, uid)
}

/// Build the conventional name for an Orbit ring segment owned by `uid`.
///
/// This form is intended for inspection and lifecycle tools that need to
/// address a user other than their own effective uid.
pub fn ring_segment_name_for_uid(fleet_name: &str, kind: u8, uid: u32) -> String {
    format!("/{SHM_NAMESPACE}-{fleet_name}-{kind}-{uid}")
}

fn lock_file_path(shm_name: &str) -> PathBuf {
    PathBuf::from("/tmp").join(format!("{}.lock", shm_name.trim_start_matches('/')))
}

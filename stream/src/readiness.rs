//! A descriptor this process's streams become readable on.
//!
//! The doorbell is a word in shared memory, and the driver thread is what
//! turns it into wakes for local tasks. A runtime that cannot park on a
//! word — libuv, asyncio, a foreign event loop, or a reactor that would
//! rather not pay a thread hand-off — parks on this instead: one
//! descriptor per table, signalled by that same driver right after it has
//! drained the node's pending bitmap. No second thread, and still no
//! descriptor per stream.
//!
//! It is edge-triggered and it coalesces. Readable means "something among
//! this process's streams may have changed": drain it, then re-try the
//! non-blocking calls on the streams you hold. A wake nobody needed costs
//! one try; a change never costs a missed wake, because the driver
//! signals after the bits are taken, not before.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::fmt;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

/// The consumer's end: poll it, drain it, re-try your streams.
pub struct Readiness {
    fd: OwnedFd,
}

/// The driver's end. One write per drain; a full descriptor is already
/// readable, so a coalesced signal is not a lost one.
pub(crate) struct Signal {
    fd: OwnedFd,
}

impl Readiness {
    /// Take everything the descriptor has, returning how many signals it
    /// held. Never blocks; a drained descriptor answers `Ok(0)`.
    pub fn drain(&self) -> io::Result<u64> {
        let mut total = 0_u64;
        loop {
            let mut value = 0_u64;
            // SAFETY: a nonblocking descriptor and a u64 of our own.
            let read = unsafe {
                libc::read(
                    self.fd.as_raw_fd(),
                    (&mut value as *mut u64).cast(),
                    size_of::<u64>(),
                )
            };
            if read == size_of::<u64>() as isize {
                total = total.saturating_add(value);
                continue;
            }
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Orbit stream readiness closed while draining",
                ));
            }
            if read < 0 {
                let error = io::Error::last_os_error();
                return match error.raw_os_error() {
                    Some(libc::EINTR) => continue,
                    Some(libc::EAGAIN) => Ok(total),
                    _ => Err(error),
                };
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Orbit stream readiness returned a partial counter",
            ));
        }
    }
}

impl Signal {
    pub(crate) fn signal(&self) {
        let value = 1_u64;
        loop {
            // SAFETY: a nonblocking descriptor and a u64 of our own.
            let written = unsafe {
                libc::write(
                    self.fd.as_raw_fd(),
                    (&value as *const u64).cast(),
                    size_of::<u64>(),
                )
            };
            if written == size_of::<u64>() as isize {
                return;
            }
            if written < 0 {
                match io::Error::last_os_error().raw_os_error() {
                    Some(libc::EINTR) => continue,
                    // Full is already readable: the signal is represented
                    // even though this one could not be added.
                    _ => return,
                }
            }
            return;
        }
    }
}

impl AsRawFd for Readiness {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl AsFd for Readiness {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl fmt::Debug for Readiness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Readiness")
            .field("fd", &self.fd.as_raw_fd())
            .finish_non_exhaustive()
    }
}

/// An `eventfd` where there is one, a pipe where there is not. The two
/// ends are the same descriptor for an eventfd and distinct ones for a
/// pipe, which is why they are named apart.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
pub(crate) fn pair() -> io::Result<(Readiness, Signal)> {
    // SAFETY: a plain syscall with constant flags.
    let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a fresh descriptor this call owns.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let signal = fd.try_clone()?;
    Ok((Readiness { fd }, Signal { fd: signal }))
}

#[cfg(target_os = "macos")]
pub(crate) fn pair() -> io::Result<(Readiness, Signal)> {
    let mut ends = [0 as libc::c_int; 2];
    // SAFETY: a plain syscall writing two descriptors into our array.
    if unsafe { libc::pipe(ends.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both are fresh descriptors this call owns.
    let (read, write) = unsafe {
        (
            OwnedFd::from_raw_fd(ends[0]),
            OwnedFd::from_raw_fd(ends[1]),
        )
    };
    for end in [read.as_raw_fd(), write.as_raw_fd()] {
        // SAFETY: descriptors this call owns; flags are read then set.
        unsafe {
            let flags = libc::fcntl(end, libc::F_GETFL);
            if flags < 0 || libc::fcntl(end, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::fcntl(end, libc::F_SETFD, libc::FD_CLOEXEC) < 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
    Ok((Readiness { fd: read }, Signal { fd: write }))
}

//! A descriptor pair for turning a shared-memory change into fd readiness.
//!
//! The waiting primitive in Orbit is a word ([`crate::sync`]), which a
//! runtime with a reactor of its own cannot park on. The bridge is always
//! the same: something already watching the word — a ring's driver, a
//! stream table's driver — writes a token, and the consumer's poll set
//! wakes. This is that pair, and nothing more: no thread, no policy about
//! who signals or when.
//!
//! An `eventfd` where there is one, a pipe where there is not. Both ends
//! are nonblocking and close-on-exec, so a signal never blocks its writer
//! and neither end survives an `exec`.
//!
//! Readiness is edge-triggered and coalescing by nature: a token says
//! something may have changed, never what or how much. A consumer drains
//! and then re-reads the shared state it cares about.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::fmt;
use std::io;
use std::mem::size_of;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

/// The consumer's end: put it in a poll set, drain it, re-read your state.
pub struct Readiness {
    fd: OwnedFd,
}

/// The signaller's end, held by whoever watches the shared word.
pub struct Signal {
    fd: OwnedFd,
}

impl Readiness {
    /// Take every token the descriptor holds and return how many there
    /// were. Never blocks; a drained descriptor answers `Ok(0)`.
    ///
    /// The count is a local wake count. It says nothing about how many
    /// things changed in shared memory, which is what the consumer must
    /// re-read for itself.
    pub fn drain(&self) -> io::Result<u64> {
        let mut total = 0_u64;
        loop {
            let mut value = 0_u64;
            // SAFETY: a nonblocking descriptor this type owns, and a u64
            // of our own to read into.
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
                    "Orbit readiness closed while draining",
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
                "Orbit readiness returned a partial counter",
            ));
        }
    }
}

impl Signal {
    /// Make the consumer's end readable. A full descriptor is already
    /// readable, so a token that cannot be added is not a lost signal.
    pub fn signal(&self) -> io::Result<()> {
        let value = 1_u64;
        loop {
            // SAFETY: a nonblocking descriptor this type owns, and a u64
            // of our own to write from.
            let written = unsafe {
                libc::write(
                    self.fd.as_raw_fd(),
                    (&value as *const u64).cast(),
                    size_of::<u64>(),
                )
            };
            if written == size_of::<u64>() as isize {
                return Ok(());
            }
            if written < 0 {
                let error = io::Error::last_os_error();
                return match error.raw_os_error() {
                    Some(libc::EINTR) => continue,
                    Some(libc::EAGAIN) => Ok(()),
                    _ => Err(error),
                };
            }
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "Orbit readiness accepted a partial token",
            ));
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

impl AsRawFd for Signal {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl fmt::Debug for Readiness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Readiness")
            .field("fd", &self.fd.as_raw_fd())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signal")
            .field("fd", &self.fd.as_raw_fd())
            .finish_non_exhaustive()
    }
}

/// One `eventfd`, cloned: both ends are the same object, so the order
/// they are dropped in does not matter.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
pub fn pair() -> io::Result<(Readiness, Signal)> {
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

/// A pipe, whose two ends are distinct: closing the read end first makes
/// the write end's `signal` fail, which is how a signaller learns its
/// consumer is gone.
#[cfg(target_os = "macos")]
pub fn pair() -> io::Result<(Readiness, Signal)> {
    let mut raw = [-1; 2];
    // SAFETY: a plain syscall writing two descriptors into our array.
    if unsafe { libc::pipe(raw.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // Own both ends before any fallible setup, so neither leaks.
    // SAFETY: two fresh descriptors this call owns.
    let (read, write) = unsafe {
        (
            OwnedFd::from_raw_fd(raw[0]),
            OwnedFd::from_raw_fd(raw[1]),
        )
    };
    for fd in [&read, &write] {
        // SAFETY: descriptors this call owns.
        let set = unsafe {
            libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) >= 0
                && libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) >= 0
        };
        if !set {
            return Err(io::Error::last_os_error());
        }
    }
    Ok((Readiness { fd: read }, Signal { fd: write }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_survives_the_pair_and_coalesces() {
        let (readiness, signal) = pair().expect("a pair");
        assert_eq!(readiness.drain().expect("empty"), 0);
        signal.signal().expect("signal");
        signal.signal().expect("signal");
        assert!(readiness.drain().expect("drain") >= 1);
        assert_eq!(readiness.drain().expect("drained"), 0);
    }
}

#[cfg(all(test, target_os = "macos"))]
mod pipe_tests {
    use super::*;

    #[test]
    fn a_full_pipe_coalesces_and_rearms() {
        let (read, write) = pair().unwrap();
        for fd in [read.as_raw_fd(), write.as_raw_fd()] {
            assert_ne!(
                unsafe { libc::fcntl(fd, libc::F_GETFL) } & libc::O_NONBLOCK,
                0
            );
            assert_ne!(
                unsafe { libc::fcntl(fd, libc::F_GETFD) } & libc::FD_CLOEXEC,
                0
            );
        }
        // Fill the pipe deliberately, then exercise the bridge's EAGAIN path.
        let token = 1u64;
        loop {
            let n = unsafe { libc::write(write.as_raw_fd(), (&token as *const u64).cast(), 8) };
            if n < 0 {
                assert_eq!(
                    io::Error::last_os_error().raw_os_error(),
                    Some(libc::EAGAIN)
                );
                break;
            }
            assert_eq!(n, 8);
        }
        write.signal().unwrap();
        let mut buffer = [0u64; 128];
        loop {
            let n = unsafe {
                libc::read(
                    read.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    size_of_val(&buffer),
                )
            };
            if n < 0 {
                assert_eq!(
                    io::Error::last_os_error().raw_os_error(),
                    Some(libc::EAGAIN)
                );
                break;
            }
            assert!(n > 0);
        }
        write.signal().unwrap();
        let mut value = 0u64;
        assert_eq!(
            unsafe { libc::read(read.as_raw_fd(), (&mut value as *mut u64).cast(), 8) },
            8
        );
        assert_eq!(value, 1);
    }
}

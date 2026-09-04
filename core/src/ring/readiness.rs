//! Native readiness bridge for an SHM ring.
//!
//! The shared signal is a generation in the ring header, waited through Linux
//! futex or FreeBSD umtx. Each process owns a private `eventfd` and a small
//! blocking driver thread that converts generation changes into fd readiness.
//! Async runtimes can therefore wait on their normal reactor without sharing
//! one drainable eventfd across readers.

#![cfg(any(target_os = "linux", target_os = "freebsd"))]

use std::fmt;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::JoinHandle;

use super::shm::ShmRing;

/// Process-local fd readiness bridge for one shared Orbit ring.
///
/// Every subscribing process creates its own instance. Ring publishers bump a
/// generation stored in SHM and wake all platform waiters; the local driver
/// then marks this fd readable. Multiple publishes may coalesce into one wake,
/// so a consumer must drain the fd and poll the ring through its own cursor.
pub struct RingEventFd {
    fd: OwnedFd,
    ring: Arc<ShmRing>,
    stop: Arc<AtomicBool>,
    driver: Option<JoinHandle<()>>,
}

impl RingEventFd {
    pub(crate) fn new(ring: Arc<ShmRing>) -> io::Result<Self> {
        let raw_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if raw_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        let driver_fd = fd.try_clone()?;
        let stop = Arc::new(AtomicBool::new(false));
        let driver_stop = stop.clone();
        let driver_ring = ring.clone();
        let mut observed = ring.notification_generation().load(Ordering::Acquire);

        let driver = std::thread::Builder::new()
            .name(format!("orbit-ring-{}-eventfd", ring.kind()))
            .spawn(move || {
                while !driver_stop.load(Ordering::Acquire) {
                    let current = driver_ring
                        .notification_generation()
                        .load(Ordering::Acquire);
                    if current != observed {
                        observed = current;
                        if signal_event_fd(driver_fd.as_raw_fd()).is_err() {
                            break;
                        }
                        continue;
                    }

                    if wait_for_generation(driver_ring.notification_generation(), observed).is_err()
                    {
                        break;
                    }
                }
            })?;

        Ok(Self {
            fd,
            ring,
            stop,
            driver: Some(driver),
        })
    }

    pub(crate) fn notify(ring: &ShmRing) -> io::Result<()> {
        ring.notification_generation()
            .fetch_add(1, Ordering::Release);
        wake_all_generation_waiters(ring.notification_generation())
    }

    /// Drain all coalesced wake counts from this non-blocking eventfd.
    ///
    /// Ring events themselves remain in SHM; the returned number is only the
    /// local wake count and must not be interpreted as an event count.
    pub fn drain(&self) -> io::Result<u64> {
        let mut total = 0u64;
        loop {
            let mut value = 0u64;
            let read = unsafe {
                libc::read(
                    self.fd.as_raw_fd(),
                    (&mut value as *mut u64).cast(),
                    std::mem::size_of::<u64>(),
                )
            };
            if read == std::mem::size_of::<u64>() as isize {
                total = total.saturating_add(value);
                continue;
            }
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Orbit eventfd closed while draining",
                ));
            }
            if read < 0 {
                let error = io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(libc::EINTR) => continue,
                    Some(libc::EAGAIN) => return Ok(total),
                    _ => return Err(error),
                }
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Orbit eventfd returned a partial counter",
            ));
        }
    }
}

impl AsRawFd for RingEventFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl AsFd for RingEventFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl fmt::Debug for RingEventFd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RingEventFd")
            .field("fd", &self.fd.as_raw_fd())
            .field("ring_kind", &self.ring.kind())
            .finish_non_exhaustive()
    }
}

impl Drop for RingEventFd {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Change the generation before waking. If the driver passed its stop
        // check but has not entered the platform wait yet, the atomic compare
        // prevents it from parking after our wake and deadlocking join.
        self.ring
            .notification_generation()
            .fetch_add(1, Ordering::Release);
        let _ = wake_all_generation_waiters(self.ring.notification_generation());
        if let Some(driver) = self.driver.take() {
            let _ = driver.join();
        }
    }
}

#[cfg(target_os = "linux")]
fn wait_for_generation(word: &AtomicU32, expected: u32) -> io::Result<()> {
    let result = unsafe {
        libc::syscall(
            libc::SYS_futex,
            word.as_ptr(),
            libc::FUTEX_WAIT,
            expected,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0,
        )
    };
    if result == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        // The generation changed before the kernel parked us, or the driver
        // was interrupted. The outer loop re-checks both generation and stop.
        Some(libc::EAGAIN) | Some(libc::EINTR) => Ok(()),
        _ => Err(error),
    }
}

#[cfg(target_os = "linux")]
fn wake_all_generation_waiters(word: &AtomicU32) -> io::Result<()> {
    let result = unsafe {
        libc::syscall(
            libc::SYS_futex,
            word.as_ptr(),
            libc::FUTEX_WAKE,
            i32::MAX,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0,
        )
    };
    if result >= 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "freebsd")]
fn wait_for_generation(word: &AtomicU32, expected: u32) -> io::Result<()> {
    let result = unsafe {
        libc::_umtx_op(
            word.as_ptr().cast(),
            libc::UMTX_OP_WAIT_UINT,
            expected as libc::c_ulong,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if result == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        // The generation changed before the kernel parked us, or the driver
        // was interrupted. The outer loop re-checks generation and stop.
        Some(libc::EINTR) => Ok(()),
        _ => Err(error),
    }
}

#[cfg(target_os = "freebsd")]
fn wake_all_generation_waiters(word: &AtomicU32) -> io::Result<()> {
    let result = unsafe {
        libc::_umtx_op(
            word.as_ptr().cast(),
            libc::UMTX_OP_WAKE,
            i32::MAX as libc::c_ulong,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn signal_event_fd(fd: RawFd) -> io::Result<()> {
    let value = 1u64;
    loop {
        let written = unsafe {
            libc::write(
                fd,
                (&value as *const u64).cast(),
                std::mem::size_of::<u64>(),
            )
        };
        if written == std::mem::size_of::<u64>() as isize {
            return Ok(());
        }
        if written < 0 {
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::EINTR) => continue,
                // A full eventfd is already readable, so the notification is
                // represented even though this increment could not be added.
                Some(libc::EAGAIN) => return Ok(()),
                _ => return Err(error),
            }
        }
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "Orbit eventfd accepted a partial counter",
        ));
    }
}

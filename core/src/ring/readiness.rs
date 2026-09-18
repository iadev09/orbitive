//! Native readiness bridge for an SHM ring.
//!
//! The shared signal is a generation in the ring header, waited through Linux
//! futex, FreeBSD umtx, or macOS shared address waits. Each process owns a
//! private readiness fd (`eventfd`, or a pipe on macOS) and a small
//! blocking driver thread that converts generation changes into fd readiness.
//! Async runtimes can therefore wait on their normal reactor without sharing
//! one drainable eventfd across readers.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::fmt;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use super::shm::ShmRing;
use crate::readiness::{Readiness, Signal};

/// Process-local fd readiness bridge for one shared Orbit ring.
///
/// The established name is retained on macOS, where a nonblocking pipe backs
/// the fd. Native macOS readiness requires 14.4 or later; older systems return
/// `ErrorKind::Unsupported` from construction so callers can use polling.
///
/// Every subscribing process creates its own instance. Ring publishers bump a
/// generation stored in SHM and wake all platform waiters; the local driver
/// then marks this fd readable. Multiple publishes may coalesce into one wake,
/// so a consumer must drain the fd and poll the ring through its own cursor.
pub struct RingEventFd {
    fd: Readiness,
    ring: Arc<ShmRing>,
    stop: Arc<AtomicBool>,
    driver: Option<JoinHandle<()>>,
}

impl RingEventFd {
    pub(crate) fn new(ring: Arc<ShmRing>) -> io::Result<Self> {
        let (fd, driver_fd) = local_notification_pair()?;
        let stop = Arc::new(AtomicBool::new(false));
        let driver_stop = stop.clone();
        let driver_ring = ring.clone();
        let mut observed = ring.notification_generation().load(Ordering::Acquire);

        // A pipe has two distinct ends; an eventfd is one descriptor cloned.
        #[cfg(target_os = "macos")]
        debug_assert_ne!(fd.as_raw_fd(), driver_fd.as_raw_fd());

        let driver = std::thread::Builder::new()
            .name(format!("orbit-ring-{}-eventfd", ring.kind()))
            .spawn(move || {
                while !driver_stop.load(Ordering::Acquire) {
                    let current = driver_ring
                        .notification_generation()
                        .load(Ordering::Acquire);
                    if current != observed {
                        observed = current;
                        if driver_fd.signal().is_err() {
                            break;
                        }
                        continue;
                    }
                    if crate::sync::wait_word(driver_ring.notification_generation(), observed)
                        .is_err()
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
        crate::sync::wake_word(ring.notification_generation())
    }

    /// Drain coalesced readiness tokens from the nonblocking local fd.
    ///
    /// Ring events themselves remain in SHM; the returned number is only the
    /// local wake count and must not be interpreted as an event count.
    pub fn drain(&self) -> io::Result<u64> {
        self.fd.drain()
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
        let _ = crate::sync::wake_word(self.ring.notification_generation());
        if let Some(driver) = self.driver.take() {
            let _ = driver.join();
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
fn local_notification_pair() -> io::Result<(Readiness, Signal)> {
    crate::readiness::pair()
}

/// macOS needs 14.4 for the shared address wait the driver parks on, so a
/// pair is refused there rather than handed out with nothing to feed it.
#[cfg(target_os = "macos")]
fn local_notification_pair() -> io::Result<(Readiness, Signal)> {
    if crate::sync::macos::api().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Orbit native readiness requires macOS 14.4 or later",
        ));
    }
    crate::readiness::pair()
}

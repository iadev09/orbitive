//! Native readiness bridge for an SHM ring.
//!
//! The shared signal is a generation in the ring header, waited through Linux
//! futex, FreeBSD umtx, or macOS shared address waits. Each process owns a
//! private readiness fd (`eventfd`, or a pipe on macOS) and a small
//! blocking driver thread that converts generation changes into fd readiness.
//! Async runtimes can therefore wait on their normal reactor without sharing
//! one drainable eventfd across readers.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::{fmt, io};

use super::shm::ShmRing;
use crate::readiness::{Readiness, Signal};

/// Process-local fd readiness for one shared Orbit ring.
///
/// Every subscribing process creates its own instance. A small driver thread
/// parks on the ring's generation, counting itself in the ring header while
/// it is parked, and marks this fd readable when the generation moves. It
/// then waits for its consumer to [`drain`](Self::drain) before it parks
/// again. A publisher using [`Fleet::publish_notified`](crate::Fleet) bumps
/// the generation and wakes only when a driver is parked: while every reader
/// is busy draining, a publish is a write and an atomic, with no syscall.
/// Multiple publishes coalesce into one wake.
///
/// The fd is backed by an `eventfd` on Linux and FreeBSD and by a nonblocking
/// pipe on macOS, where native readiness needs 14.4 or later; older systems
/// return `ErrorKind::Unsupported` from construction so callers can poll.
///
/// The contract: drain the fd, then poll the ring through your own cursor.
/// Draining is what lets the driver park.
///
/// **Clear the reactor's readiness before draining, not after.** The driver
/// signals once per drain: a signal that arrives just after [`drain`](Self::drain)
/// is the only one until the next drain, and a reactor that clears readiness
/// after draining (tokio's `clear_ready`) erases it — the consumer then waits
/// for an edge that never comes, and the driver for a drain that never
/// happens.
pub struct RingEventFd {
    fd: Readiness,
    ring: Arc<ShmRing>,
    stop: Arc<AtomicBool>,
    drained: Arc<Drained>,
    driver: Option<JoinHandle<()>>,
}

/// Whether the consumer has taken the last signal. The driver waits here,
/// in this process, until it has — not on the shared word.
#[derive(Default)]
struct Drained {
    pending: Mutex<bool>,
    taken: Condvar,
}

impl RingEventFd {
    pub(crate) fn new(ring: Arc<ShmRing>) -> io::Result<Self> {
        let (fd, driver_fd) = local_notification_pair()?;
        let stop = Arc::new(AtomicBool::new(false));
        let drained = Arc::new(Drained::default());
        let (driver_stop, driver_drained, driver_ring) =
            (stop.clone(), drained.clone(), ring.clone());
        let mut observed =
            ring.notification_generation().load(Ordering::Acquire);

        let driver = std::thread::Builder::new()
            .name(format!("orbit-ring-{}-eventfd", ring.kind()))
            .spawn(move || {
                let generation = driver_ring.notification_generation();
                let waiters = driver_ring.notification_waiters();
                while !driver_stop.load(Ordering::Acquire) {
                    // Count in, then look once more. A publisher bumps the
                    // generation before it reads the count; one of the two
                    // sees the other, so a publish cannot fall between them.
                    waiters.fetch_add(1, Ordering::SeqCst);
                    let current = generation.load(Ordering::SeqCst);
                    if current == observed
                        && !driver_stop.load(Ordering::Acquire)
                    {
                        let parked =
                            crate::sync::wait_word(generation, observed);
                        waiters.fetch_sub(1, Ordering::SeqCst);
                        if parked.is_err() {
                            break;
                        }
                        continue;
                    }
                    waiters.fetch_sub(1, Ordering::SeqCst);
                    if driver_stop.load(Ordering::Acquire) {
                        break;
                    }
                    observed = current;
                    *driver_drained.pending.lock().unwrap() = true;
                    if driver_fd.signal().is_err() {
                        break;
                    }
                    let mut pending = driver_drained.pending.lock().unwrap();
                    while *pending && !driver_stop.load(Ordering::Acquire) {
                        pending = driver_drained.taken.wait(pending).unwrap();
                    }
                }
            })?;

        Ok(Self { fd, ring, stop, drained, driver: Some(driver) })
    }

    /// Wake the ring's parked readers, and only them.
    pub(crate) fn notify(ring: &ShmRing) -> io::Result<()> {
        ring.notification_generation().fetch_add(1, Ordering::SeqCst);
        if ring.notification_waiters().load(Ordering::SeqCst) == 0 {
            return Ok(());
        }
        crate::sync::wake_word(ring.notification_generation())
    }

    /// Drain the fd and let the driver park again. Poll the ring after this.
    pub fn drain(&self) -> io::Result<u64> {
        let tokens = self.fd.drain()?;
        *self.drained.pending.lock().unwrap() = false;
        self.drained.taken.notify_one();
        Ok(tokens)
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
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("RingEventFd")
            .field("fd", &self.fd.as_raw_fd())
            .field("ring_kind", &self.ring.kind())
            .finish_non_exhaustive()
    }
}

impl Drop for RingEventFd {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Release a driver waiting for a drain, then one parked on the word:
        // the generation changes first so a driver between its check and its
        // wait does not park after the wake.
        {
            let _pending = self.drained.pending.lock().unwrap();
            self.drained.taken.notify_one();
        }
        self.ring.notification_generation().fetch_add(1, Ordering::SeqCst);
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

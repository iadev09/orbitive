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
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::JoinHandle;

use super::shm::ShmRing;

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
    fd: OwnedFd,
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

    /// Drain coalesced readiness tokens from the nonblocking local fd.
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
                #[cfg(target_os = "macos")]
                debug_assert_eq!(value, 1, "local pipe carries unit readiness tokens only");
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
                // A full eventfd or pipe is already readable, so the notification is
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

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
fn local_notification_pair() -> io::Result<(OwnedFd, OwnedFd)> {
    let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let driver = fd.try_clone()?;
    Ok((fd, driver))
}

#[cfg(target_os = "macos")]
fn local_notification_pair() -> io::Result<(OwnedFd, OwnedFd)> {
    if macos::api().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Orbit native readiness requires macOS 14.4 or later",
        ));
    }
    let mut raw = [-1; 2];
    if unsafe { libc::pipe(raw.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // Take ownership of both ends before any fallible setup. Drop joins the
    // driver before closing the read end, so writes cannot hit a closed pipe.
    let read = unsafe { OwnedFd::from_raw_fd(raw[0]) };
    let write = unsafe { OwnedFd::from_raw_fd(raw[1]) };
    for fd in [&read, &write] {
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0
            || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } < 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    Ok((read, write))
}

#[cfg(target_os = "macos")]
fn wait_for_generation(word: &AtomicU32, expected: u32) -> io::Result<()> {
    let api = macos::api().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "macOS shared address waits unavailable",
        )
    })?;
    debug_assert_eq!(
        (word.as_ptr() as usize) % size_of::<u32>(),
        0,
        "shared wait word must be naturally aligned"
    );
    // The SHM word is AtomicU32, not u64. Wait and wake must agree on size
    // and shared mode. Apple returns a nonnegative waiter count on success.
    let result = unsafe {
        (api.wait)(
            word.as_ptr().cast(),
            u64::from(expected),
            size_of::<u32>(),
            macos::SHARED,
        )
    };
    if result >= 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EINTR) => Ok(()),
        _ => Err(error),
    }
}

#[cfg(target_os = "macos")]
fn wake_all_generation_waiters(word: &AtomicU32) -> io::Result<()> {
    debug_assert_eq!(
        (word.as_ptr() as usize) % size_of::<u32>(),
        0,
        "shared wake word must be naturally aligned"
    );
    // Older macOS has no native subscribers; publication still succeeds and
    // polling readers see the committed frames.
    let Some(api) = macos::api() else {
        return Ok(());
    };
    loop {
        let result =
            unsafe { (api.wake_all)(word.as_ptr().cast(), size_of::<u32>(), macos::SHARED) };
        if result >= 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            // No waiter is normal: publication can precede subscription or
            // race the driver's compare-and-wait. The generation persists.
            Some(libc::ENOENT) => return Ok(()),
            Some(libc::EINTR) => continue,
            _ => return Err(error),
        }
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

    // OS_SYNC_WAIT_ON_ADDRESS_SHARED and OS_SYNC_WAKE_BY_ADDRESS_SHARED
    // have the same ABI value in <os/os_sync_wait_on_address.h>.
    pub(super) const SHARED: u32 = 1;

    type Wait = unsafe extern "C" fn(*mut libc::c_void, u64, usize, u32) -> libc::c_int;
    type Wake = unsafe extern "C" fn(*mut libc::c_void, usize, u32) -> libc::c_int;

    #[derive(Clone, Copy)]
    pub(super) struct Api {
        pub(super) wait: Wait,
        pub(super) wake_all: Wake,
    }

    const UNRESOLVED: u8 = 0;
    const UNAVAILABLE: u8 = 1;
    const READY: u8 = 2;

    static STATE: AtomicU8 = AtomicU8::new(UNRESOLVED);
    static WAIT: AtomicUsize = AtomicUsize::new(0);
    static WAKE: AtomicUsize = AtomicUsize::new(0);

    /// Resolve the 14.4 entry points, once per process but never by waiting.
    ///
    /// Deliberately not a `OnceLock`. A publisher reaches this from
    /// `wake_all_generation_waiters`, and a publisher may be a process that
    /// was just forked while another thread of its parent was inside the
    /// resolution -- the test harness runs tests on parallel threads, and a
    /// supervisor forks workers with readiness threads alive. The child
    /// inherits the "initializing" state and none of the thread that would
    /// finish it, so a blocking once-cell parks forever. Resolution here is
    /// idempotent: racing callers each `dlsym` the same two symbols and store
    /// the same values, and nobody waits for anybody.
    ///
    /// Resolving lazily rather than linking avoids hard references to
    /// 14.4-only symbols on older deployment targets. libSystem stays loaded
    /// for the life of the process, so the pointers never dangle.
    pub(super) fn api() -> Option<Api> {
        match STATE.load(Ordering::Acquire) {
            READY => Some(load()),
            UNAVAILABLE => None,
            _ => resolve(),
        }
    }

    fn load() -> Api {
        unsafe {
            Api {
                wait: std::mem::transmute::<usize, Wait>(WAIT.load(Ordering::Acquire)),
                wake_all: std::mem::transmute::<usize, Wake>(WAKE.load(Ordering::Acquire)),
            }
        }
    }

    fn resolve() -> Option<Api> {
        let wait = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"os_sync_wait_on_address".as_ptr()) };
        let wake =
            unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"os_sync_wake_by_address_all".as_ptr()) };
        if wait.is_null() || wake.is_null() {
            STATE.store(UNAVAILABLE, Ordering::Release);
            return None;
        }
        // Pointers first, then the state that publishes them.
        WAIT.store(wait as usize, Ordering::Release);
        WAKE.store(wake as usize, Ordering::Release);
        STATE.store(READY, Ordering::Release);
        Some(load())
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn full_local_pipe_coalesces_and_rearms() {
        let (read, write) = local_notification_pair().unwrap();
        for fd in [&read, &write] {
            assert_ne!(
                unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
                0
            );
            assert_ne!(
                unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
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
        signal_event_fd(write.as_raw_fd()).unwrap();
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
        signal_event_fd(write.as_raw_fd()).unwrap();
        let mut value = 0u64;
        assert_eq!(
            unsafe { libc::read(read.as_raw_fd(), (&mut value as *mut u64).cast(), 8) },
            8
        );
        assert_eq!(value, 1);
    }
}

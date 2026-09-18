//! Waiting on a shared 32-bit word, the primitive under every "wake me when
//! this changes" in Orbit: ring readiness, cell changes.
//!
//! [`wait_word`] parks the caller until the word no longer holds `expected`
//! (or spuriously; callers loop). [`wake_word`] wakes every waiter on it. The
//! word may live in shared memory: Linux futex, FreeBSD umtx and macOS
//! `os_sync_wait_on_address` all key waiters by the physical location, so a
//! wake in one process reaches a waiter in another with nothing carried
//! between them. No descriptor, no channel: the memory is the signal.
//!
//! macOS needs 14.4 for the shared form. Below that there is no wait to
//! be had, and [`supported`] says so: a crate that parks on words refuses
//! to open rather than pretending, because a sleep loop wearing the shape
//! of a wait is worse than a clear no.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::io;
#[cfg(target_os = "macos")]
use std::mem::size_of;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

/// Whether this build can park on a shared word at all.
///
/// Linux and FreeBSD always can. macOS can from 14.4; below it, and on
/// any other target, nothing here works and callers should refuse at
/// their own front door rather than degrade quietly.
pub fn supported() -> bool {
    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    {
        true
    }
    #[cfg(target_os = "macos")]
    {
        macos::api().is_some()
    }
}

#[cfg(target_os = "linux")]
pub fn wait_word(word: &AtomicU32, expected: u32) -> io::Result<()> {
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

/// Park until the word no longer holds `expected`, or `timeout` passes.
///
/// `Ok(true)` means something may have changed — a wake, a value that
/// moved before the kernel parked us, or a signal — and the caller
/// re-checks as it does after [`wait_word`]. `Ok(false)` means the
/// timeout passed and nothing else. The timeout is relative and measured
/// on a monotonic clock, so a caller holding a deadline recomputes what
/// is left on each turn of its loop.
#[cfg(target_os = "linux")]
pub fn wait_word_timeout(word: &AtomicU32, expected: u32, timeout: Duration) -> io::Result<bool> {
    // FUTEX_WAIT reads this as relative, on CLOCK_MONOTONIC.
    let left = libc::timespec {
        tv_sec: timeout.as_secs().min(i64::MAX as u64) as libc::time_t,
        tv_nsec: timeout.subsec_nanos() as libc::c_long,
    };
    let result = unsafe {
        libc::syscall(
            libc::SYS_futex,
            word.as_ptr(),
            libc::FUTEX_WAIT,
            expected,
            &left as *const libc::timespec,
            std::ptr::null::<u32>(),
            0,
        )
    };
    if result == 0 {
        return Ok(true);
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EAGAIN) | Some(libc::EINTR) => Ok(true),
        Some(libc::ETIMEDOUT) => Ok(false),
        _ => Err(error),
    }
}

#[cfg(target_os = "linux")]
pub fn wake_word(word: &AtomicU32) -> io::Result<()> {
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
pub fn wait_word(word: &AtomicU32, expected: u32) -> io::Result<()> {
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

/// Park until the word no longer holds `expected`, or `timeout` passes.
///
/// `Ok(true)` means something may have changed — a wake, a value that
/// moved before the kernel parked us, or a signal — and the caller
/// re-checks as it does after [`wait_word`]. `Ok(false)` means the
/// timeout passed and nothing else. The timeout is relative and measured
/// on a monotonic clock, so a caller holding a deadline recomputes what
/// is left on each turn of its loop.
#[cfg(target_os = "freebsd")]
pub fn wait_word_timeout(word: &AtomicU32, expected: u32, timeout: Duration) -> io::Result<bool> {
    // For the UMTX_OP_WAIT family the fourth argument is the size of the
    // timeout structure and the fifth points at it; a bare `timespec` is
    // read as relative.
    let left = libc::timespec {
        tv_sec: timeout.as_secs().min(i64::MAX as u64) as libc::time_t,
        tv_nsec: timeout.subsec_nanos() as libc::c_long,
    };
    let result = unsafe {
        libc::_umtx_op(
            word.as_ptr().cast(),
            libc::UMTX_OP_WAIT_UINT,
            expected as libc::c_ulong,
            size_of::<libc::timespec>() as *mut libc::c_void,
            &left as *const libc::timespec as *mut libc::c_void,
        )
    };
    if result == 0 {
        return Ok(true);
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EINTR) => Ok(true),
        Some(libc::ETIMEDOUT) => Ok(false),
        _ => Err(error),
    }
}

#[cfg(target_os = "freebsd")]
pub fn wake_word(word: &AtomicU32) -> io::Result<()> {
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

#[cfg(target_os = "macos")]
pub fn wait_word(word: &AtomicU32, expected: u32) -> io::Result<()> {
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

/// Park until the word no longer holds `expected`, or `timeout` passes.
///
/// `Ok(true)` means something may have changed — a wake, a value that
/// moved before the kernel parked us, or a signal — and the caller
/// re-checks as it does after [`wait_word`]. `Ok(false)` means the
/// timeout passed and nothing else. The timeout is relative and measured
/// on a monotonic clock, so a caller holding a deadline recomputes what
/// is left on each turn of its loop.
#[cfg(target_os = "macos")]
pub fn wait_word_timeout(word: &AtomicU32, expected: u32, timeout: Duration) -> io::Result<bool> {
    let api = macos::api().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "macOS shared address waits unavailable",
        )
    })?;
    let nanos = timeout.as_nanos().min(u128::from(u64::MAX)) as u64;
    // SAFETY: the same word, size and shared flag as the untimed wait.
    let result = unsafe {
        (api.wait_timeout)(
            word.as_ptr().cast(),
            u64::from(expected),
            size_of::<u32>(),
            macos::SHARED,
            macos::MACH_ABSOLUTE_TIME,
            nanos,
        )
    };
    if result >= 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EINTR) => Ok(true),
        Some(libc::ETIMEDOUT) => Ok(false),
        _ => Err(error),
    }
}

#[cfg(target_os = "macos")]
pub fn wake_word(word: &AtomicU32) -> io::Result<()> {
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
pub(crate) mod macos {
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

    // OS_SYNC_WAIT_ON_ADDRESS_SHARED and OS_SYNC_WAKE_BY_ADDRESS_SHARED
    // have the same ABI value in <os/os_sync_wait_on_address.h>.
    pub(super) const SHARED: u32 = 1;

    /// `os_clockid_t` in <os/clock.h>: the only clock the timed wait
    /// takes, and the one a relative timeout is measured on.
    pub(super) const MACH_ABSOLUTE_TIME: u32 = 32;

    type Wait = unsafe extern "C" fn(*mut libc::c_void, u64, usize, u32) -> libc::c_int;
    type WaitTimeout =
        unsafe extern "C" fn(*mut libc::c_void, u64, usize, u32, u32, u64) -> libc::c_int;
    type Wake = unsafe extern "C" fn(*mut libc::c_void, usize, u32) -> libc::c_int;

    #[derive(Clone, Copy)]
    pub(crate) struct Api {
        pub(super) wait: Wait,
        pub(super) wait_timeout: WaitTimeout,
        pub(super) wake_all: Wake,
    }

    const UNRESOLVED: u8 = 0;
    const UNAVAILABLE: u8 = 1;
    const READY: u8 = 2;

    static STATE: AtomicU8 = AtomicU8::new(UNRESOLVED);
    static WAIT: AtomicUsize = AtomicUsize::new(0);
    static WAIT_TIMEOUT: AtomicUsize = AtomicUsize::new(0);
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
    pub(crate) fn api() -> Option<Api> {
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
                wait_timeout: std::mem::transmute::<usize, WaitTimeout>(
                    WAIT_TIMEOUT.load(Ordering::Acquire),
                ),
                wake_all: std::mem::transmute::<usize, Wake>(WAKE.load(Ordering::Acquire)),
            }
        }
    }

    fn resolve() -> Option<Api> {
        let wait = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"os_sync_wait_on_address".as_ptr()) };
        // Shipped in the same release as the other two; all or nothing.
        let wait_timeout = unsafe {
            libc::dlsym(
                libc::RTLD_DEFAULT,
                c"os_sync_wait_on_address_with_timeout".as_ptr(),
            )
        };
        let wake =
            unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"os_sync_wake_by_address_all".as_ptr()) };
        if wait.is_null() || wait_timeout.is_null() || wake.is_null() {
            STATE.store(UNAVAILABLE, Ordering::Release);
            return None;
        }
        // Pointers first, then the state that publishes them.
        WAIT.store(wait as usize, Ordering::Release);
        WAIT_TIMEOUT.store(wait_timeout as usize, Ordering::Release);
        WAKE.store(wake as usize, Ordering::Release);
        STATE.store(READY, Ordering::Release);
        Some(load())
    }
}

#[cfg(test)]
mod timeout_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    use super::*;

    /// Also pins the unit the platform reads the timeout in: a wrong one
    /// shows up here as a wait that is orders of magnitude off, not as a
    /// wrong answer.
    #[test]
    fn a_timeout_is_a_timeout() {
        if !supported() {
            return;
        }
        let word = AtomicU32::new(7);
        let started = Instant::now();
        assert!(!wait_word_timeout(&word, 7, Duration::from_millis(200)).expect("wait"));
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(150), "returned after {waited:?}");
        assert!(waited < Duration::from_secs(2), "returned after {waited:?}");
    }

    #[test]
    fn a_wake_beats_the_timeout() {
        if !supported() {
            return;
        }
        let word = Arc::new(AtomicU32::new(0));
        let waker = Arc::clone(&word);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            waker.store(1, Ordering::SeqCst);
            let _ = wake_word(&waker);
        });
        let started = Instant::now();
        assert!(wait_word_timeout(&word, 0, Duration::from_secs(10)).expect("wait"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn a_word_that_already_moved_does_not_park_at_all() {
        if !supported() {
            return;
        }
        let word = AtomicU32::new(3);
        let started = Instant::now();
        assert!(wait_word_timeout(&word, 9, Duration::from_secs(30)).expect("wait"));
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}

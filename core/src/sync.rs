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
//! macOS needs 14.4 for the shared form; below that [`wait_word`] answers
//! `ErrorKind::Unsupported` and a caller polls instead.

#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]

use std::io;
#[cfg(target_os = "macos")]
use std::mem::size_of;
use std::sync::atomic::AtomicU32;

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

    type Wait = unsafe extern "C" fn(*mut libc::c_void, u64, usize, u32) -> libc::c_int;
    type Wake = unsafe extern "C" fn(*mut libc::c_void, usize, u32) -> libc::c_int;

    #[derive(Clone, Copy)]
    pub(crate) struct Api {
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

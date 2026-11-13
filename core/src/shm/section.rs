//! One lock-file description per region per process, kept across sections.
//!
//! Opening the lock file for every critical section was most of what a short
//! section cost: a path lookup, a create check, an ownership check and a
//! close, for a lock taken and dropped in microseconds. A region now opens
//! its description once and keeps it.
//!
//! Two hazards came with a kept description, and each has its answer here:
//!
//! - **Threads.** `flock` belongs to the description, not to the thread, so
//!   two threads locking one description do not exclude each other. The
//!   process keeps its own count: the first shared holder takes the shared
//!   `flock` and the last one drops it; an exclusive holder waits for the
//!   process's other holders, then takes the exclusive `flock`.
//! - **`fork`.** A child that inherits the description inherits whatever lock
//!   it carries, so a parent that dies holding it would leave the lock held by
//!   the child. A `pthread_atfork` handler closes every kept description in
//!   the child, and the child never uses its parent's state: it opens a
//!   description per section, as before. `O_CLOEXEC` covers `exec`. A fork
//!   that bypasses libc's `fork` runs no handler and brings the hazard back.
//!
//! A lock file unlinked under a live region (teardown, `orbit clear`) is
//! found by its link count when the process next takes the `flock`, and
//! opened again by path, so the process locks the file its path names.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use crate::process::ProcessOwner;

/// One region's kept description and this process's holders of it.
pub(super) struct Section {
    owner: ProcessOwner,
    fd: Kept,
    state: Mutex<Holders>,
    released: Condvar,
}

#[derive(Default)]
struct Holders {
    shared: usize,
    exclusive: bool,
    /// A first shared holder is taking the shared `flock`.
    locking: bool,
    waiting: usize,
}

impl Section {
    pub(super) fn new(fd: OwnedFd) -> io::Result<Self> {
        Ok(Self {
            owner: ProcessOwner::current()?,
            fd: Kept::new(fd)?,
            state: Mutex::new(Holders::default()),
            released: Condvar::new(),
        })
    }

    /// Whether this process opened it; a child never uses its parent's.
    pub(super) fn is_current(&self) -> bool {
        self.owner.is_current()
    }

    fn holders(&self) -> MutexGuard<'_, Holders> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait<'a>(
        &self,
        mut holders: MutexGuard<'a, Holders>,
    ) -> MutexGuard<'a, Holders> {
        holders.waiting += 1;
        let mut holders =
            self.released.wait(holders).unwrap_or_else(PoisonError::into_inner);
        holders.waiting -= 1;
        holders
    }

    fn wake(
        &self,
        holders: &Holders,
    ) {
        if holders.waiting > 0 {
            self.released.notify_all();
        }
    }

    /// Take the region's lock. `reopen` opens the lock file by path, for a
    /// description whose file was unlinked; it runs only while this process
    /// holds nothing on the description.
    pub(super) fn acquire(
        &self,
        exclusive: bool,
        reopen: impl FnOnce() -> io::Result<OwnedFd>,
    ) -> io::Result<()> {
        let mut holders = self.holders();
        loop {
            if holders.exclusive || holders.locking {
                holders = self.wait(holders);
                continue;
            }
            if !exclusive && holders.shared > 0 {
                holders.shared += 1;
                return Ok(());
            }
            if exclusive && holders.shared > 0 {
                holders = self.wait(holders);
                continue;
            }
            break;
        }
        // Nothing of this process holds the description: take the `flock`
        // outside the state lock, with the others kept out meanwhile.
        if exclusive {
            holders.exclusive = true;
        } else {
            holders.locking = true;
        }
        drop(holders);
        let locked = self.fd.refresh(reopen).and_then(|()| {
            self.fd.flock(if exclusive { libc::LOCK_EX } else { libc::LOCK_SH })
        });
        let mut holders = self.holders();
        if exclusive {
            holders.exclusive = locked.is_ok();
        } else {
            holders.locking = false;
            if locked.is_ok() {
                holders.shared = 1;
            }
        }
        self.wake(&holders);
        locked
    }

    pub(super) fn release(
        &self,
        exclusive: bool,
    ) {
        let mut holders = self.holders();
        if exclusive {
            let _ = self.fd.flock(libc::LOCK_UN);
            holders.exclusive = false;
        } else {
            holders.shared -= 1;
            if holders.shared == 0 {
                // Under the state lock, so no first holder takes it meanwhile.
                let _ = self.fd.flock(libc::LOCK_UN);
            }
        }
        self.wake(&holders);
    }
}

/// A kept descriptor that the fork handler can close in a child.
struct Kept(Arc<AtomicI32>);

impl Kept {
    fn new(fd: OwnedFd) -> io::Result<Self> {
        let raw = fd.as_raw_fd();
        let kept = Arc::new(AtomicI32::new(raw));
        registry::insert(&kept)?;
        std::mem::forget(fd);
        Ok(Self(kept))
    }

    fn raw(&self) -> RawFd {
        self.0.load(Ordering::Acquire)
    }

    fn flock(
        &self,
        operation: libc::c_int,
    ) -> io::Result<()> {
        loop {
            // SAFETY: a plain syscall on a descriptor this value owns.
            if unsafe { libc::flock(self.raw(), operation) } == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    /// Replace the description if its file has been unlinked.
    fn refresh(
        &self,
        reopen: impl FnOnce() -> io::Result<OwnedFd>,
    ) -> io::Result<()> {
        // SAFETY: `stat` is plain data; fstat writes it.
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: a plain syscall on a descriptor this value owns.
        if unsafe { libc::fstat(self.raw(), &mut stat) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if stat.st_nlink > 0 {
            return Ok(());
        }
        let fresh = reopen()?;
        let old = self.0.swap(fresh.as_raw_fd(), Ordering::AcqRel);
        std::mem::forget(fresh);
        // SAFETY: `old` was this value's descriptor and is no longer reachable.
        drop(unsafe { OwnedFd::from_raw_fd(old) });
        Ok(())
    }
}

impl Drop for Kept {
    fn drop(&mut self) {
        registry::remove(&self.0);
        let raw = self.0.swap(-1, Ordering::AcqRel);
        if raw >= 0 {
            // SAFETY: this value owned the descriptor.
            drop(unsafe { OwnedFd::from_raw_fd(raw) });
        }
    }
}

/// Every kept descriptor of this process, for the fork handler.
///
/// The handler runs in a child of a possibly multithreaded parent, where only
/// async-signal-safe work is allowed: it takes no lock and allocates nothing,
/// only swaps each descriptor out and closes it. The list is guarded by a flag
/// that the prepare handler takes before the fork, so the child never sees it
/// half changed.
mod registry {
    use super::*;

    static BUSY: AtomicBool = AtomicBool::new(false);
    static mut KEPT: Vec<*const AtomicI32> = Vec::new();
    static HOOKED: std::sync::Once = std::sync::Once::new();
    /// What `pthread_atfork` returned, for every later caller to see.
    static HOOK_STATUS: AtomicI32 = AtomicI32::new(0);

    fn enter() {
        while BUSY
            .compare_exchange_weak(
                false,
                true,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_err()
        {
            std::hint::spin_loop();
        }
    }

    fn leave() {
        BUSY.store(false, Ordering::Release);
    }

    extern "C" fn prepare() {
        enter();
    }

    extern "C" fn parent() {
        leave();
    }

    extern "C" fn child() {
        // SAFETY: the prepare handler holds BUSY across the fork, so the list
        // is whole; each entry points into an Arc a live `Kept` holds.
        let kept = unsafe { &*std::ptr::addr_of!(KEPT) };
        for &fd in kept {
            // SAFETY: as above.
            let raw = unsafe { &*fd }.swap(-1, Ordering::AcqRel);
            if raw >= 0 {
                // SAFETY: close is async-signal-safe; the child owns its copy.
                unsafe { libc::close(raw) };
            }
        }
        leave();
    }

    pub(super) fn insert(fd: &Arc<AtomicI32>) -> io::Result<()> {
        HOOKED.call_once(|| {
            // SAFETY: the handlers only touch atomics and close descriptors.
            let status = unsafe {
                libc::pthread_atfork(Some(prepare), Some(parent), Some(child))
            };
            HOOK_STATUS.store(status, Ordering::Release);
        });
        let status = HOOK_STATUS.load(Ordering::Acquire);
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status));
        }
        enter();
        // SAFETY: BUSY is held.
        unsafe { (*std::ptr::addr_of_mut!(KEPT)).push(Arc::as_ptr(fd)) };
        leave();
        Ok(())
    }

    pub(super) fn remove(fd: &Arc<AtomicI32>) {
        let target = Arc::as_ptr(fd);
        enter();
        // SAFETY: BUSY is held.
        unsafe {
            (*std::ptr::addr_of_mut!(KEPT)).retain(|&entry| entry != target)
        };
        leave();
    }
}

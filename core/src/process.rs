//! Process ownership for handles that must survive being copied by fork.
//!
//! Acquire ownership during boot/open. A child opens fresh handles. An
//! inherited owner never becomes current again, even after the child opens
//! its own handles. On Unix systems without a kernel-cleared mapping this
//! relies on the libc fork path invoking pthread_atfork handlers.

use std::io;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

static CURRENT: AtomicPtr<AtomicU32> = AtomicPtr::new(ptr::null_mut());

/// A process identity captured when a resource is opened.
///
/// The marker is process-private and lives until exit. Checking ownership
/// performs one atomic load and does not call into the kernel.
#[derive(Clone, Copy)]
pub struct ProcessOwner {
    marker: &'static AtomicU32,
    pid: u32,
}

#[cfg(all(test, target_os = "linux", target_arch = "x86_64"))]
#[test]
fn kernel_marker_invalidates_on_raw_fork_and_stays_invalid_after_rejoin() {
    let parent = ProcessOwner::current().unwrap();
    assert_eq!(parent.pid(), std::process::id());
    // SAFETY: the child only accesses private atomics and makes raw OS calls.
    let child = unsafe { libc::syscall(libc::SYS_fork) } as libc::pid_t;
    assert!(child >= 0, "fork failed");
    if child == 0 {
        let invalid = !parent.is_current();
        let joined = ProcessOwner::current().is_ok_and(|owner| {
            owner.is_current() && owner.pid() == std::process::id()
        });
        // SAFETY: exit without using inherited runtime state.
        unsafe {
            libc::_exit(i32::from(!(invalid && joined && !parent.is_current())))
        };
    }
    let mut status = 0;
    // SAFETY: child is the PID returned by fork and status is writable.
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
    assert!(libc::WIFEXITED(status));
    assert_eq!(libc::WEXITSTATUS(status), 0);
    assert!(parent.is_current());
}

impl ProcessOwner {
    pub fn current() -> io::Result<Self> {
        loop {
            let previous = CURRENT.load(Ordering::Acquire);
            // SAFETY: published mappings stay alive, including in a child.
            if let Some(marker) = unsafe { previous.as_ref() } {
                let pid = marker.load(Ordering::Relaxed);
                if pid != 0 {
                    return Ok(Self { marker, pid });
                }
            }

            let candidate = platform::Marker::new()?;
            let pid = std::process::id();
            candidate.initialize(pid);
            if CURRENT
                .compare_exchange(
                    previous,
                    candidate.as_ptr(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(Self { marker: candidate.publish(), pid });
            }
        }
    }

    /// The PID captured at boot/open, including on an inherited handle.
    pub fn pid(self) -> u32 {
        self.pid
    }

    /// Whether this resource belongs to the process using it.
    #[inline]
    pub fn is_current(self) -> bool {
        self.marker.load(Ordering::Relaxed) != 0
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
mod platform {
    use std::io;
    use std::mem::size_of;
    use std::ptr::{self, NonNull};
    use std::sync::atomic::AtomicU32;

    pub(super) struct Marker {
        ptr: NonNull<AtomicU32>,
        len: usize,
    }

    impl Marker {
        pub(super) fn new() -> io::Result<Self> {
            // SAFETY: sysconf takes only the constant selector.
            let len = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            if len < size_of::<AtomicU32>() as libc::c_long {
                return Err(io::Error::other(
                    "invalid process marker page size",
                ));
            }
            let len = len as usize;
            // SAFETY: this requests a new private anonymous page.
            let page = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANON,
                    -1,
                    0,
                )
            };
            if page == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            let Some(ptr) = NonNull::new(page.cast()) else {
                // SAFETY: mmap returned an otherwise valid mapping at zero.
                unsafe { libc::munmap(page, len) };
                return Err(io::Error::other("null process marker mapping"));
            };
            let marker = Self { ptr, len };
            // SAFETY: the range is the private mapping allocated above.
            #[cfg(target_os = "linux")]
            let status =
                unsafe { libc::madvise(page, len, libc::MADV_WIPEONFORK) };
            #[cfg(target_os = "freebsd")]
            let status =
                unsafe { libc::minherit(page, len, libc::INHERIT_ZERO) };
            if status != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(marker)
        }

        pub(super) fn initialize(
            &self,
            pid: u32,
        ) {
            // SAFETY: this mapping is aligned and has not been published yet.
            unsafe { self.ptr.as_ptr().write(AtomicU32::new(pid)) };
        }

        pub(super) fn as_ptr(&self) -> *mut AtomicU32 {
            self.ptr.as_ptr()
        }

        pub(super) fn publish(self) -> &'static AtomicU32 {
            let ptr = self.ptr;
            // Keep old markers mapped in a child so inherited owners remain
            // invalid even after CURRENT points to the child's new marker.
            std::mem::forget(self);
            // SAFETY: the initialized mapping now lives until process exit.
            unsafe { ptr.as_ref() }
        }
    }

    impl Drop for Marker {
        fn drop(&mut self) {
            // SAFETY: only unpublished candidates are unmapped here.
            unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
mod platform {
    use std::io;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[cfg(unix)]
    use super::CURRENT;
    #[cfg(unix)]
    use std::sync::atomic::AtomicBool;

    #[cfg(unix)]
    static HOOK_READY: AtomicBool = AtomicBool::new(false);

    #[cfg(unix)]
    extern "C" fn after_fork() {
        // SAFETY: a published marker always lives until process exit.
        if let Some(marker) =
            unsafe { CURRENT.load(Ordering::Acquire).as_ref() }
        {
            marker.store(0, Ordering::Relaxed);
        }
    }

    pub(super) struct Marker(Box<AtomicU32>);

    impl Marker {
        pub(super) fn new() -> io::Result<Self> {
            #[cfg(unix)]
            if !HOOK_READY.load(Ordering::Acquire) {
                // SAFETY: the child handler accesses only lock-free atomics.
                // Concurrent startup can register the same idempotent handler.
                let status = unsafe {
                    libc::pthread_atfork(None, None, Some(after_fork))
                };
                if status != 0 {
                    return Err(io::Error::from_raw_os_error(status));
                }
                HOOK_READY.store(true, Ordering::Release);
            }
            Ok(Self(Box::new(AtomicU32::new(0))))
        }

        pub(super) fn initialize(
            &self,
            pid: u32,
        ) {
            self.0.store(pid, Ordering::Relaxed);
        }

        pub(super) fn as_ptr(&self) -> *mut AtomicU32 {
            ptr::from_ref(&*self.0).cast_mut()
        }

        pub(super) fn publish(self) -> &'static AtomicU32 {
            Box::leak(self.0)
        }
    }

    use std::ptr;
}

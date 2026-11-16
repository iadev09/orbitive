//! The parking sequence of one slot, separate from its value.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::Result;

#[repr(C)]
pub(crate) struct Wake {
    sequence: AtomicU32,
    waiters: AtomicU32,
}

impl Wake {
    /// Only at a quiescent reset, when nothing is parked.
    pub(crate) fn clear(&self) {
        self.sequence.store(0, Ordering::Relaxed);
        self.waiters.store(0, Ordering::Relaxed);
    }

    /// After a value write. `SeqCst`, as the write before it: the load
    /// half of the writer's store-then-load pair.
    #[inline]
    pub(crate) fn changed(&self) {
        if self.waiters.load(Ordering::SeqCst) != 0 {
            self.notify();
        }
    }

    /// Retain the wake before issuing it, including when nobody is parked.
    pub(crate) fn notify(&self) {
        // Wraps. A waiter parks on a stale sequence only if exactly 2^32
        // notifies land between its recheck and its park; the next write
        // wakes it, because it is still registered.
        self.sequence.fetch_add(1, Ordering::SeqCst);
        crate::wake_on(&self.sequence);
    }

    /// Register, snapshot the sequence, then recheck the value and the
    /// interrupt. A later notify changes the word the kernel compares; an
    /// earlier one orders the caller's write before the recheck.
    pub(crate) fn wait_if(
        &self,
        should_wait: impl FnOnce() -> bool,
    ) -> Result<()> {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        struct Registration<'a>(&'a AtomicU32);
        impl Drop for Registration<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _registration = Registration(&self.waiters);
        let observed = self.sequence.load(Ordering::SeqCst);
        if should_wait() {
            #[cfg(test)]
            before_park();
            crate::wait_on(&self.sequence, observed)?;
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    pub(crate) static BEFORE_PARK: std::cell::RefCell<Option<Box<dyn FnMut()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn before_park() {
    BEFORE_PARK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook();
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};

    use orbit_core::Fleet;

    use super::BEFORE_PARK;
    use crate::Orbitals;

    fn orbitals() -> Orbitals {
        Orbitals::new(Arc::new(Fleet::join("example", 1).unwrap())).unwrap()
    }

    /// The waiter says when it has registered and is about to park. The
    /// write then lands on a parked thread or in the gap before the park;
    /// neither may lose it.
    #[test]
    fn a_write_wakes_a_waiter() {
        let value = orbitals().allocate(0_i64).unwrap();
        let waiter = value.clone();
        let (ready_tx, ready_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            BEFORE_PARK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    let _ = ready_tx.send(());
                }))
            });
            waiter.wait(0)
        });
        ready_rx.recv().unwrap();
        assert_eq!(value.fetch_add(5), 0);
        assert_eq!(thread.join().unwrap().unwrap(), 5);
    }

    #[test]
    fn an_interrupt_between_check_and_park_is_retained() {
        let value = orbitals().allocate(7_u64).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let interrupt = stop.clone();
        let waiter = value.clone();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            BEFORE_PARK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }))
            });
            waiter.wait_until(7, &interrupt)
        });
        // The waiter has passed its last check and has not parked yet.
        ready_rx.recv().unwrap();
        stop.store(true, Ordering::SeqCst);
        value.notify();
        resume_tx.send(()).unwrap();
        assert_eq!(
            thread.join().unwrap().unwrap(),
            None,
            "one interrupt wake must suffice"
        );
        assert_eq!(value.load(), 7, "cancellation is not a value write");
    }
}
